use ironbeam_core::*;

fn small() -> (Graph, Registry) {
    let p = Pipeline::default();
    let values = p.from_vec("input", vec![1_i32, 2, 3, 4]).unwrap();
    let doubled = values.map_named("double/v1", |x| x * 2).unwrap();
    let even = doubled.filter_named("even/v1", |x| x % 2 == 0).unwrap();
    even.output("result").unwrap();
    p.snapshot().unwrap()
}

#[test]
fn canonical_identity_ignores_local_order_and_rejects_mismatch() {
    let (a, _) = small();
    let mut b = a.clone();
    b.nodes.reverse();
    for (index, node) in b.nodes.iter_mut().enumerate() {
        node.local_id = index + 100;
    }
    assert_eq!(a.canonical_json().unwrap(), b.canonical_json().unwrap());
    assert_eq!(a.fingerprint().unwrap(), b.fingerprint().unwrap());
    b.node_mut_for_test("double/v1").time.initial_timestamp_ns = 1;
    assert_ne!(a.fingerprint().unwrap(), b.fingerprint().unwrap());
    let identity = ExecutionIdentity {
        graph_fingerprint: a.fingerprint().unwrap(),
        binary_build_id: "build-1".into(),
        registry_manifest: "functions-1".into(),
        coder_manifest: "coders-1".into(),
        source_config_digest: "source-1".into(),
    };
    assert_eq!(identity.verify(&identity), Ok(()));
    let mut worker = identity.clone();
    worker.binary_build_id = "build-2".into();
    assert_eq!(
        identity.verify(&worker).unwrap_err().kind,
        ErrorKind::Incompatible
    );
}

trait NodeMut {
    fn node_mut_for_test(&mut self, key: &str) -> &mut Node;
}
impl NodeMut for Graph {
    fn node_mut_for_test(&mut self, key: &str) -> &mut Node {
        self.nodes.iter_mut().find(|n| n.key == key).unwrap()
    }
}

#[test]
fn validates_versions_edges_cycles_and_coders() {
    let (a, _) = small();
    let mut b = a.clone();
    b.version.major += 1;
    assert_eq!(b.validate().unwrap_err().kind, ErrorKind::Incompatible);
    let mut b = a.clone();
    b.node_mut_for_test("result").input = Some("missing".into());
    assert_eq!(b.validate().unwrap_err().kind, ErrorKind::Graph);
    let mut b = a.clone();
    b.node_mut_for_test("input").input = Some("result".into());
    assert_eq!(b.validate().unwrap_err().kind, ErrorKind::Graph);
    let mut b = a.clone();
    b.node_mut_for_test("even/v1").input = Some("result".into());
    assert!(b.validate().unwrap_err().message.contains("cycle"));
    let mut b = a.clone();
    b.node_mut_for_test("even/v1").coder_id = "wrong".into();
    assert!(b.validate().unwrap_err().message.contains("coder"));
    let mut b = a.clone();
    b.node_mut_for_test("result").coder_version.major += 1;
    assert_eq!(b.validate().unwrap_err().kind, ErrorKind::Incompatible);
    let mut b = a;
    b.nodes.push(b.nodes[0].clone());
    assert_eq!(b.validate().unwrap_err().kind, ErrorKind::Graph);
}

#[test]
fn unbounded_metadata_and_capability_are_explicit() {
    let p = Pipeline::default();
    let events = p
        .unbounded_source::<i32>(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            -1,
        )
        .unwrap();
    assert_eq!(
        events.global_combine_marker("illegal").err().unwrap().kind,
        ErrorKind::Graph
    );
    let mapped = events.map_named("normalize/v1", |x| x + 1).unwrap();
    let windowed = mapped
        .fixed_windows("five-minutes", 300_000_000_000)
        .unwrap();
    windowed.append_sink("out", Replay::Idempotent).unwrap();
    let (graph, _) = p.snapshot().unwrap();
    let source = graph.node("events").unwrap();
    assert_eq!(source.time.initial_timestamp_ns, -1);
    assert_eq!(source.time.window, Window::Global);
    assert_eq!(graph.node("normalize/v1").unwrap().time, source.time);
    assert_eq!(
        graph.node("five-minutes").unwrap().time.window,
        Window::Fixed {
            size_ns: 300_000_000_000
        }
    );
    assert_eq!(
        graph.node("out").unwrap().boundedness,
        Boundedness::Unbounded
    );
    match &source.operation {
        Operation::Source { progress } => {
            assert!(progress.stable_split_id && progress.reports_event_time);
            assert_eq!(progress.replay, Replay::AtLeastOnce);
        }
        _ => panic!("expected source"),
    }
    let error = graph
        .validate_for("batch", &Capabilities::default())
        .unwrap_err();
    assert_eq!(error.node.as_deref(), Some("events"));
    assert_eq!(
        error.kind,
        ErrorKind::UnsupportedCapability {
            capability: "unbounded-source".into()
        }
    );
    assert_eq!(error.retry, RetryClass::Permanent);
}

#[test]
fn custom_capability_and_builder_rejections() {
    let p = Pipeline::default();
    let input = p.from_vec("input", vec![1_i32]).unwrap();
    assert!(p.from_vec("input", vec![2_i32]).is_err());
    assert!(input.fixed_windows("bad-window", 0).is_err());
    assert!(
        input
            .custom_named("bad", "unnamespaced", GRAPH_VERSION)
            .is_err()
    );
    input
        .custom_named("custom", "vendor/op", GRAPH_VERSION)
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = p.snapshot().unwrap();
    let error = graph
        .validate_for("batch", &Capabilities::default())
        .unwrap_err();
    assert_eq!(
        error.kind,
        ErrorKind::UnsupportedCapability {
            capability: "vendor/op".into()
        }
    );
}

#[test]
fn event_time_is_preserved_and_pre_epoch_windows_are_checked() {
    assert_eq!(
        Window::Fixed { size_ns: 100 }.assign(-1).unwrap(),
        Some(WindowInterval {
            start_ns: -100,
            end_ns: 0
        })
    );
    assert!(Window::Fixed { size_ns: 100 }.assign(i64::MAX).is_err());
    let p = Pipeline::default();
    let input = p.from_vec("input", vec![1_i32]).unwrap();
    input
        .map_named("map", |x| x + 1)
        .unwrap()
        .fixed_windows("window", 100)
        .unwrap()
        .output("out")
        .unwrap();
    let (_, registry) = p.snapshot().unwrap();
    let source = registry.open_source("input").unwrap().next().unwrap();
    let mapped = registry.apply("map", source).unwrap().unwrap();
    assert_eq!(mapped.timestamp_ns, 0);
    assert_eq!(mapped.window, Window::Global);
    let assigned = registry.apply("window", mapped).unwrap().unwrap();
    assert_eq!(
        assigned.assigned_window,
        Some(WindowInterval {
            start_ns: 0,
            end_ns: 100
        })
    );
    assert_eq!(*assigned.value.downcast::<i32>().unwrap(), 2);
}

#[test]
fn malformed_descriptors_fail_before_lowering() {
    let (baseline, _) = small();
    let mut graph = baseline.clone();
    graph.checkpoint_version.major += 1;
    assert_eq!(graph.validate().unwrap_err().kind, ErrorKind::Incompatible);
    let mut graph = baseline.clone();
    graph.node_mut_for_test("input").key.clear();
    assert_eq!(graph.validate().unwrap_err().kind, ErrorKind::Graph);
    let mut graph = baseline.clone();
    graph.node_mut_for_test("input").type_id.clear();
    assert_eq!(graph.validate().unwrap_err().kind, ErrorKind::Graph);
    let mut graph = baseline.clone();
    graph.node_mut_for_test("input").semantic_version.major = 0;
    assert_eq!(graph.validate().unwrap_err().kind, ErrorKind::Graph);
    let mut graph = baseline.clone();
    graph.node_mut_for_test("input").input = Some("result".into());
    assert_eq!(graph.validate().unwrap_err().kind, ErrorKind::Graph);
    let mut graph = baseline.clone();
    graph.node_mut_for_test("result").boundedness = Boundedness::Unbounded;
    assert!(
        graph
            .validate()
            .unwrap_err()
            .message
            .contains("boundedness")
    );
    let mut graph = baseline.clone();
    graph.node_mut_for_test("result").time.initial_timestamp_ns = 42;
    assert!(graph.validate().unwrap_err().message.contains("event time"));
    let mut graph = baseline.clone();
    graph.node_mut_for_test("even/v1").operation = Operation::Custom {
        semantic_id: "bad".into(),
        version: Version { major: 0, minor: 0 },
    };
    assert!(graph.validate().unwrap_err().message.contains("namespaced"));
    let mut graph = baseline;
    graph.node_mut_for_test("even/v1").time.window = Window::Fixed { size_ns: -1 };
    assert!(graph.validate().unwrap_err().message.contains("positive"));

    let p = Pipeline::default();
    p.from_vec("input", vec![1_i32])
        .unwrap()
        .fixed_windows("window", 100)
        .unwrap()
        .output("result")
        .unwrap();
    let (mut graph, _) = p.snapshot().unwrap();
    graph.node_mut_for_test("window").time.window = Window::Global;
    assert!(
        graph
            .validate()
            .unwrap_err()
            .message
            .contains("fixed-window")
    );

    let p = Pipeline::default();
    p.from_vec("input", vec![1_i32])
        .unwrap()
        .custom_named("custom", "vendor/op", GRAPH_VERSION)
        .unwrap()
        .output("result")
        .unwrap();
    let (mut graph, _) = p.snapshot().unwrap();
    graph.node_mut_for_test("custom").semantic_id = "vendor/other".into();
    assert!(
        graph
            .validate()
            .unwrap_err()
            .message
            .contains("identity differs")
    );
}

#[test]
fn capabilities_validate_each_requested_operation() {
    let p = Pipeline::default();
    p.from_vec("input", vec![1_i32])
        .unwrap()
        .fixed_windows("window", 100)
        .unwrap()
        .append_sink("sink", Replay::Idempotent)
        .unwrap();
    let (graph, _) = p.snapshot().unwrap();
    let mut caps = Capabilities::default();
    assert_eq!(
        graph
            .validate_for("test", &caps)
            .unwrap_err()
            .node
            .as_deref(),
        Some("window")
    );
    caps.fixed_window = true;
    assert_eq!(
        graph
            .validate_for("test", &caps)
            .unwrap_err()
            .node
            .as_deref(),
        Some("sink")
    );
    caps.append_sink = true;
    graph.validate_for("test", &caps).unwrap();

    let p = Pipeline::default();
    p.from_vec("input", vec![1_i32])
        .unwrap()
        .custom_named("custom", "vendor/known", GRAPH_VERSION)
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = p.snapshot().unwrap();
    caps.custom_operations.insert("vendor/known".into());
    graph.validate_for("test", &caps).unwrap();
}
