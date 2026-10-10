//! Test-only Dataflow lowering prototype; no public Runner trait or cloud execution.
use ironbeam_core::{Capabilities, Graph, Operation, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};

fn lower_dataflow_preview(graph: &Graph) -> Result<Vec<String>, ironbeam_core::Error> {
    graph.validate_for("dataflow-preview", &Capabilities::default())?;
    let mut labels = Vec::new();
    for node in &graph.nodes {
        let transform = match node.operation {
            Operation::Source { .. } => "Impulse/materialized-source",
            Operation::Map | Operation::Filter | Operation::FlatMap => "ParDo",
            Operation::Flatten => "Flatten",
            Operation::GroupByKey => "GroupByKey",
            Operation::JoinInner => "CoGroupByKey/ParDo",
            Operation::SideInputMap => "ParDo/SideInput",
            Operation::Output => "ResultExport",
            _ => {
                return Err(ironbeam_core::Error::unsupported(
                    &node.key,
                    "dataflow-preview",
                    "lowering",
                ));
            }
        };
        labels.push(format!("{}:{transform}", node.key));
    }
    Ok(labels)
}

#[test]
fn same_descriptor_lowers_to_direct_and_dataflow_preview() {
    let p = Pipeline::default();
    let source = p.from_vec("input", vec![1_i32, 2, 3, 4]).unwrap();
    let output = source
        .map_named("double/v1", |x| x * 2)
        .unwrap()
        .filter_named("even/v1", |x| x % 2 == 0)
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = output.snapshot().unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        [2, 4, 6, 8]
    );
    assert_eq!(
        lower_dataflow_preview(&graph).unwrap(),
        [
            "input:Impulse/materialized-source",
            "double/v1:ParDo",
            "even/v1:ParDo",
            "result:ResultExport",
        ]
    );
    assert!(graph.fingerprint().unwrap().starts_with("sha256-v1:"));
}

#[test]
fn dataflow_preview_rejects_phase4_operations_before_lowering() {
    use ironbeam_core::{Error, ErrorKind, Replay, SinkMode, SinkSpec, TypedSink};
    struct Noop;
    impl TypedSink<i32> for Noop {
        fn write(&mut self, _: i32) -> Result<(), Error> {
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }
    let pipeline = Pipeline::default();
    let left = pipeline.from_vec("left", vec![(1_i32, 2_i32)]).unwrap();
    let right = pipeline.from_vec("right", vec![(1_i32, 3_i32)]).unwrap();
    left.group_by_key_named("group").unwrap();
    left.join_inner_named("join", &right).unwrap();
    let values = left.map_named("values", |(_, value)| value).unwrap();
    values
        .combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
        .unwrap();
    values
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::FinalResult,
                replay: Replay::Idempotent,
                commit: true,
            },
            || Ok(Box::new(Noop)),
        )
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let error = lower_dataflow_preview(&graph).unwrap_err();
    assert_eq!(
        error.kind,
        ErrorKind::UnsupportedCapability {
            capability: "group-by-key".into()
        }
    );
    assert_eq!(error.node.as_deref(), Some("group"));
    let caps = Capabilities {
        group_by_key: true,
        join_inner: true,
        global_combine: true,
        ..Capabilities::default()
    };
    assert_eq!(
        graph.validate_for("fake", &caps).unwrap_err().kind,
        ErrorKind::UnsupportedCapability {
            capability: "final-result-sink".into()
        }
    );
}

#[test]
fn dataflow_preview_rejects_file_source_before_opening_it() {
    use ironbeam_core::{ErrorKind, Replay, SourceProgress};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    let opened = Arc::new(AtomicBool::new(false));
    let flag = opened.clone();
    let pipeline = Pipeline::default();
    pipeline
        .bounded_source_with_semantic::<i32>(
            "file",
            "ironbeam/io/jsonl-source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "file-digest".into(),
            None,
            move || {
                flag.store(true, Ordering::SeqCst);
                Ok(Box::new(std::iter::once(Ok(1_i32))))
            },
        )
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let error = lower_dataflow_preview(&graph).unwrap_err();
    assert_eq!(
        error.kind,
        ErrorKind::UnsupportedCapability {
            capability: "ironbeam/io/jsonl-source".into()
        }
    );
    assert!(!opened.load(Ordering::SeqCst));
}
