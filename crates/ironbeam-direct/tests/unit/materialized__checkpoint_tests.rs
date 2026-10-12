// Unit tests for src/materialized.rs.
use super::*;
use crate::{CheckpointConfig, DirectRunner};
use ironbeam_core::{ErrorKind, InputEdge, Output, Pipeline};
use std::sync::Arc;

fn manager<T: Element>(output: &Output<T>, config: &CheckpointConfig) -> CheckpointManager {
    let (graph, registry) = output.snapshot().unwrap();
    let plan = crate::planner::lower(&graph, &registry, output.key(), 1).unwrap();
    let identity = Identity::new::<T>(
        plan.summary.graph_fingerprint,
        registry
            .source_config_digest(&plan.summary.source_key)
            .unwrap()
            .to_owned(),
        plan.summary.source_key,
        output.key().to_owned(),
        config.binary_build_id.clone(),
    );
    CheckpointManager::new(config, identity, Arc::new(MetricsState::default())).unwrap()
}

#[test]
fn grouped_fast_routes_decline_incomplete_or_non_linear_graphs() {
    fn declines(graph: &Graph, registry: &Registry, config: &JobConfig) -> bool {
        let metrics = MetricsState::default();
        let mut emit = |_: Envelope, _: &[String]| Ok(());
        !try_drain_fold_sink(graph, registry, "out", config, &metrics, &mut emit).unwrap()
    }

    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![(1_i32, 2_i32)]).unwrap();
    let grouped = source.group_by_key_named("group").unwrap();
    grouped
        .map_named("map", |row| row)
        .unwrap()
        .output("out")
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let config = JobConfig::default();
    assert_eq!(
        checkpointed_group_route(&graph, "out"),
        Some((
            "group".into(),
            "source".into(),
            Vec::new(),
            vec!["map".into()]
        ))
    );
    assert!(declines(
        &graph,
        &registry,
        &JobConfig {
            parallelism: 2,
            ..JobConfig::default()
        }
    ));
    assert!(declines(&graph, &Registry::default(), &config));

    let mut no_parent = graph.clone();
    no_parent
        .nodes
        .iter_mut()
        .find(|node| node.key == "out")
        .unwrap()
        .input = None;
    assert!(checkpointed_group_route(&no_parent, "out").is_none());
    assert!(declines(&no_parent, &registry, &config));

    let mut absent_parent = graph.clone();
    absent_parent
        .nodes
        .iter_mut()
        .find(|node| node.key == "out")
        .unwrap()
        .input = Some("missing".into());
    assert!(checkpointed_group_route(&absent_parent, "out").is_none());
    assert!(declines(&absent_parent, &registry, &config));

    let mut no_fold = graph.clone();
    no_fold
        .nodes
        .iter_mut()
        .find(|node| node.key == "out")
        .unwrap()
        .input = Some("source".into());
    assert!(checkpointed_group_route(&no_fold, "out").is_none());
    assert!(declines(&no_fold, &registry, &config));

    let mut detached_map = graph.clone();
    detached_map
        .nodes
        .iter_mut()
        .find(|node| node.key == "map")
        .unwrap()
        .input = None;
    assert!(checkpointed_group_route(&detached_map, "out").is_none());
    assert!(declines(&detached_map, &registry, &config));

    let mut multi_input_map = graph.clone();
    let map = multi_input_map
        .nodes
        .iter_mut()
        .find(|node| node.key == "map")
        .unwrap();
    map.additional_inputs.push(InputEdge {
        key: "source".into(),
        element: map.input_element.clone().unwrap(),
    });
    assert!(checkpointed_group_route(&multi_input_map, "out").is_none());
    assert!(declines(&multi_input_map, &registry, &config));

    let mut multi_input_group = graph;
    let group = multi_input_group
        .nodes
        .iter_mut()
        .find(|node| node.key == "group")
        .unwrap();
    group.additional_inputs.push(InputEdge {
        key: "source".into(),
        element: group.input_element.clone().unwrap(),
    });
    assert!(checkpointed_group_route(&multi_input_group, "out").is_none());
    assert!(declines(&multi_input_group, &registry, &config));
}

#[test]
fn wait_fast_route_declines_incomplete_or_non_linear_graphs() {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .unwrap();
    let pipeline = Pipeline::default();
    let main = pipeline.from_vec("main", vec![1_i32]).unwrap();
    let signal = pipeline.from_vec("signal", vec![2_i32]).unwrap();
    main.wait_on_named("wait", &signal)
        .unwrap()
        .map_named("map", |value| value)
        .unwrap()
        .output("out")
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let config = JobConfig::default();
    let metrics = MetricsState::default();
    let declines = |graph: &Graph| {
        try_wait_sink_route(&pool, graph, &registry, "out", &config, &metrics)
            .unwrap()
            .is_none()
    };

    let mut no_parent = graph.clone();
    no_parent
        .nodes
        .iter_mut()
        .find(|node| node.key == "out")
        .unwrap()
        .input = None;
    assert!(declines(&no_parent));

    let mut absent_parent = graph.clone();
    absent_parent
        .nodes
        .iter_mut()
        .find(|node| node.key == "out")
        .unwrap()
        .input = Some("missing".into());
    assert!(declines(&absent_parent));

    let mut multi_input_map = graph.clone();
    let map = multi_input_map
        .nodes
        .iter_mut()
        .find(|node| node.key == "map")
        .unwrap();
    map.additional_inputs.push(InputEdge {
        key: "signal".into(),
        element: map.input_element.clone().unwrap(),
    });
    assert!(declines(&multi_input_map));

    let mut detached_map = graph.clone();
    detached_map
        .nodes
        .iter_mut()
        .find(|node| node.key == "map")
        .unwrap()
        .input = None;
    assert!(declines(&detached_map));

    let mut wrong_operation = graph.clone();
    wrong_operation
        .nodes
        .iter_mut()
        .find(|node| node.key == "wait")
        .unwrap()
        .operation = Operation::Reshuffle;
    assert!(declines(&wrong_operation));

    let mut unbounded_wait = graph.clone();
    unbounded_wait
        .nodes
        .iter_mut()
        .find(|node| node.key == "wait")
        .unwrap()
        .boundedness = Boundedness::Unbounded;
    assert!(declines(&unbounded_wait));

    let mut detached_wait = graph;
    detached_wait
        .nodes
        .iter_mut()
        .find(|node| node.key == "wait")
        .unwrap()
        .input = None;
    assert!(declines(&detached_wait));
}

#[test]
fn fold_state_requires_the_same_direct_group_route_and_valid_source_offset() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![(1_i32, 2_i32)]).unwrap();
    let direct = source
        .group_by_key_named("group")
        .unwrap()
        .output("out")
        .unwrap();
    let plain = source.output("plain").unwrap();
    let (graph, _) = direct.snapshot().unwrap();
    assert_eq!(
        checkpointed_group_route(&graph, "out"),
        Some(("group".into(), "source".into(), Vec::new(), Vec::new()))
    );
    assert!(checkpointed_group_route(&graph, "source").is_none());
    assert!(checkpointed_group_route(&graph, plain.key()).is_none());
    let dir = tempfile::tempdir().unwrap();
    let checkpoint = CheckpointConfig {
        directory: dir.path().to_path_buf(),
        binary_build_id: "route-test".into(),
        ..CheckpointConfig::default()
    };
    manager(&direct, &checkpoint)
        .save_fold_state(2, b"state", 1024)
        .unwrap();
    let config = JobConfig {
        checkpoint: Some(checkpoint),
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner
            .run_collect(&direct, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );

    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![(1_i32, 2_i32)]).unwrap();
    let mapped = source.map_named("map", |pair| pair).unwrap();
    let output = mapped
        .group_by_key_named("group")
        .unwrap()
        .output("out")
        .unwrap();
    let (graph, _) = output.snapshot().unwrap();
    assert_eq!(
        checkpointed_group_route(&graph, "out"),
        Some((
            "group".into(),
            "source".into(),
            vec!["map".into()],
            Vec::new()
        ))
    );
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![(1_i32, 2_i32)]).unwrap();
    let shuffled = source.reshuffle_named("shuffle").unwrap();
    let output = shuffled
        .group_by_key_named("group")
        .unwrap()
        .output("out")
        .unwrap();
    let (graph, _) = output.snapshot().unwrap();
    assert!(checkpointed_group_route(&graph, "out").is_none());
    let dir = tempfile::tempdir().unwrap();
    let checkpoint = CheckpointConfig {
        directory: dir.path().to_path_buf(),
        binary_build_id: "route-test".into(),
        ..CheckpointConfig::default()
    };
    manager(&output, &checkpoint)
        .save_fold_state(0, b"state", 1024)
        .unwrap();
    let config = JobConfig {
        checkpoint: Some(checkpoint),
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
}

#[test]
fn checkpointed_group_preserves_the_materialized_source_budget() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", vec![(1_i32, 2_i32)])
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .output("out")
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let config = JobConfig {
        memory_budget_bytes: 1,
        checkpoint: Some(CheckpointConfig {
            directory: dir.path().to_path_buf(),
            binary_build_id: "budget-test".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Resource
    );
}

#[test]
fn checkpointed_group_rejects_state_larger_than_the_job_budget() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", vec![(1_i32, "x".repeat(5000))])
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .output("out")
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let config = JobConfig {
        memory_budget_bytes: 1024,
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().to_path_buf(),
            binary_build_id: "state-budget-test".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Resource
    );
    assert!(!directory.path().join("latest.json").exists());
}

#[test]
fn checkpointed_group_rejects_invalid_fold_payload() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", vec![(1_i32, 2_i32)])
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .output("out")
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let checkpoint = CheckpointConfig {
        directory: dir.path().to_path_buf(),
        binary_build_id: "corrupt-state-test".into(),
        ..CheckpointConfig::default()
    };
    manager(&output, &checkpoint)
        .save_fold_state(0, b"invalid fold payload", 1024)
        .unwrap();
    let config = JobConfig {
        checkpoint: Some(checkpoint),
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
}

#[test]
fn staged_group_attributes_source_clone_panics_to_the_source() {
    struct PanicClone(i32);
    impl Clone for PanicClone {
        fn clone(&self) -> Self {
            panic!("source clone failed");
        }
    }
    impl Element for PanicClone {
        const TYPE_ID: &'static str = "test/staged-group-panic-clone";
        const CODER_ID: &'static str = "test/staged-group-panic-clone-i32";
        fn supports_coder() -> bool {
            true
        }
        fn encode(&self) -> Result<Vec<u8>, Error> {
            Ok(self.0.to_le_bytes().to_vec())
        }
        fn decode(bytes: &[u8]) -> Result<Self, Error> {
            Ok(Self(i32::decode(bytes)?))
        }
    }
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", vec![(1_i32, PanicClone(2))])
        .unwrap()
        .map_named("map", |pair| pair)
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .output("out")
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let error = DirectRunner
        .run_collect(
            &output,
            &JobConfig {
                checkpoint: Some(CheckpointConfig {
                    directory: directory.path().to_path_buf(),
                    binary_build_id: "clone-panic-test".into(),
                    ..CheckpointConfig::default()
                }),
                ..JobConfig::default()
            },
        )
        .err()
        .unwrap();
    assert!(matches!(error.kind, ErrorKind::Execution));
    assert_eq!(error.node.as_deref(), Some("source"));
}

#[test]
fn staged_group_attributes_transform_panics_to_the_transform() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", vec![(1_i32, 2_i32)])
        .unwrap()
        .map_named("panic-map", |_: (i32, i32)| -> (i32, i32) {
            panic!("map failed");
        })
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .output("out")
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let error = DirectRunner
        .run_collect(
            &output,
            &JobConfig {
                checkpoint: Some(CheckpointConfig {
                    directory: directory.path().to_path_buf(),
                    binary_build_id: "map-panic-test".into(),
                    ..CheckpointConfig::default()
                }),
                ..JobConfig::default()
            },
        )
        .err()
        .unwrap();
    assert!(matches!(error.kind, ErrorKind::Execution));
    assert_eq!(error.node.as_deref(), Some("panic-map"));
}
