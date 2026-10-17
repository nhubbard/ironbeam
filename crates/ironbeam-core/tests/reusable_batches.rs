use ironbeam_core::{DynOp, Envelope, ErrorKind, PaneInfo, Partition, Pipeline, Version, Window};
use std::sync::Arc;

struct DoubleBatch;

impl DynOp for DoubleBatch {
    fn apply(&self, input: Partition) -> Partition {
        let values = *input.downcast::<Vec<i32>>().unwrap();
        Box::new(
            values
                .into_iter()
                .map(|value| value * 2)
                .collect::<Vec<_>>(),
        )
    }
}

struct WrongOutput;

impl DynOp for WrongOutput {
    fn apply(&self, _: Partition) -> Partition {
        Box::new(vec!["wrong".to_owned()])
    }
}

fn envelope(value: impl Send + 'static) -> Envelope {
    Envelope {
        pane: PaneInfo::FIRST_LAST_ON_TIME,
        value: Box::new(value),
        timestamp_ns: 42,
        window: Window::Global,
        assigned_window: None,
    }
}

#[test]
fn reusable_operator_processes_explicit_batches_and_keeps_metadata() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32, 2]).unwrap();
    source
        .apply_dyn_transform_with_semantic_named::<i32>(
            "double",
            2,
            "tests/double-batch",
            Version { major: 1, minor: 0 },
            Arc::new(DoubleBatch),
        )
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    graph.validate().unwrap();
    assert!(graph.node("double/batches").is_some());
    let output = registry
        .apply_many("double", envelope(vec![1_i32, 2]))
        .unwrap();
    assert_eq!(output.len(), 2);
    assert_eq!(output[0].value.downcast_ref::<i32>(), Some(&2));
    assert_eq!(output[1].value.downcast_ref::<i32>(), Some(&4));
    assert!(
        output
            .iter()
            .all(|row| row.timestamp_ns == 42 && row.pane == PaneInfo::FIRST_LAST_ON_TIME)
    );
    assert_eq!(
        registry
            .apply_many("double", envelope(1_i32))
            .err()
            .unwrap()
            .kind,
        ErrorKind::Execution
    );
}

#[test]
fn reusable_operator_rejects_wrong_output_type() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    source
        .apply_dyn_transform_with_semantic_named::<i32>(
            "wrong",
            2,
            "tests/wrong-output",
            Version { major: 1, minor: 0 },
            Arc::new(WrongOutput),
        )
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert_eq!(
        registry
            .apply_many("wrong", envelope(vec![1_i32]))
            .err()
            .unwrap()
            .kind,
        ErrorKind::Execution
    );
}
