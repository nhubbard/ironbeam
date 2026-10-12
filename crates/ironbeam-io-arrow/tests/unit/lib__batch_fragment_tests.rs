// Unit tests for src/lib.rs.
use super::*;
use arrow::{
    array::Int32Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

fn batch(name: &str, values: &[i32]) -> ArrowBatch {
    let schema = std::sync::Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]));
    ArrowBatch(
        RecordBatch::try_new(
            schema,
            vec![std::sync::Arc::new(Int32Array::from(values.to_vec()))],
        )
        .unwrap(),
    )
}

#[test]
fn publishes_multiple_bundle_batches_as_one_native_file() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        arrow_ipc_batch_fragment_store(dir.path().join("stage"), "job", "graph", "sink").unwrap();
    let target = dir.path().join("batches.arrow");
    for (bundle, values) in [("a", vec![1, 2]), ("b", vec![3])] {
        let mut frame = Vec::new();
        encode_fragment_row(&batch("value", &values), &mut frame).unwrap();
        store.local.stage(bundle, 1, &frame).unwrap();
        store.local.accept(bundle).unwrap();
    }
    assert_eq!(store.accepted_fragments().unwrap().len(), 2);
    let store = store.require_bundle_attempts(["a", "b"]).unwrap();
    let published = publish_arrow_ipc_batch_fragments(&store, 2, &target).unwrap();
    assert_eq!(published.rows, 2);
    assert_eq!(published.fragments, 2);
    assert_eq!(
        publish_arrow_ipc_batch_fragments(&store, 2, &target).unwrap(),
        published
    );
    let decoded = read_arrow_ipc_batches(&fs::read(&target).unwrap()).unwrap();
    assert_eq!(decoded.len(), 2);
    let mut lengths = decoded
        .iter()
        .map(|batch| batch.0.num_rows())
        .collect::<Vec<_>>();
    lengths.sort_unstable();
    assert_eq!(lengths, [1, 2]);
    assert_eq!(decoded[0].0.schema(), decoded[1].0.schema());
    let mut values = decoded
        .iter()
        .flat_map(|batch| {
            batch
                .0
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    values.sort_unstable();
    assert_eq!(values, [1, 2, 3]);
    assert!(publish_arrow_ipc_batch_fragments(&store, 3, &target).is_err());
}

#[test]
fn rejects_bad_frames_counts_and_schemas_before_publication() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("bad.arrow");
    let corrupt =
        arrow_ipc_batch_fragment_store(dir.path().join("corrupt"), "job", "graph", "sink").unwrap();
    corrupt
        .local
        .stage("bad", 1, &100_u64.to_le_bytes())
        .unwrap();
    corrupt.local.accept("bad").unwrap();
    assert!(publish_arrow_ipc_batch_fragments(&corrupt, 1, &target).is_err());
    assert!(!target.exists());
    let wrong =
        arrow_ipc_batch_fragment_store(dir.path().join("wrong"), "job", "graph", "sink").unwrap();
    let mut frame = Vec::new();
    encode_fragment_row(&batch("value", &[1]), &mut frame).unwrap();
    wrong.local.stage("wrong", 2, &frame).unwrap();
    wrong.local.accept("wrong").unwrap();
    assert!(publish_arrow_ipc_batch_fragments(&wrong, 2, &target).is_err());
    assert!(!target.exists());
    let mismatch =
        arrow_ipc_batch_fragment_store(dir.path().join("mismatch"), "job", "graph", "sink")
            .unwrap();
    mismatch.local.stage("a", 1, &frame).unwrap();
    mismatch.local.accept("a").unwrap();
    let mut second = Vec::new();
    encode_fragment_row(&batch("other", &[2]), &mut second).unwrap();
    mismatch.local.stage("b", 1, &second).unwrap();
    mismatch.local.accept("b").unwrap();
    assert!(publish_arrow_ipc_batch_fragments(&mismatch, 2, &target).is_err());
    assert!(!target.exists());
}

#[test]
fn empty_file_and_sink_registration() {
    let dir = tempfile::tempdir().unwrap();
    let store = arrow_ipc_batch_fragment_store(dir.path().join("stage"), "job", "graph", "sink")
        .unwrap()
        .require_bundle_attempts([] as [&str; 0])
        .unwrap();
    let target = dir.path().join("empty.arrow");
    assert_eq!(
        publish_arrow_ipc_batch_fragments(&store, 0, &target)
            .unwrap()
            .rows,
        0
    );
    assert!(
        read_arrow_ipc_batches(&fs::read(target).unwrap())
            .unwrap()
            .is_empty()
    );
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec("batches", vec![batch("value", &[1])])
        .unwrap();
    arrow_ipc_batch_fragment_sink(&input, "sink", dir.path().join("other"), "job", "graph")
        .unwrap();
    assert!(arrow_ipc_batch_fragment_store("relative", "job", "graph", "sink").is_err());
}
