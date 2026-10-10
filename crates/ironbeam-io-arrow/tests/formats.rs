use ironbeam_core::{Boundedness, Capabilities, Element, ErrorKind, Operation, Pipeline, SinkMode};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_io_arrow::*;
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Row {
    id: i32,
    name: String,
}
impl Element for Row {
    const TYPE_ID: &'static str = "tests/row";
    const CODER_ID: &'static str = "tests/row-json";
}
fn rows() -> Vec<Row> {
    vec![
        Row {
            id: 1,
            name: "Ada".into(),
        },
        Row {
            id: 2,
            name: "Lin".into(),
        },
    ]
}

type MakeSink = fn(
    &ironbeam_core::PCollection<Row>,
    &str,
    &std::path::Path,
) -> Result<ironbeam_core::Sink<Row>, ironbeam_core::Error>;
type MakeSource = fn(
    &Pipeline,
    &str,
    &std::path::Path,
) -> Result<ironbeam_core::PCollection<Row>, ironbeam_core::Error>;
fn round_trip(extension: &str, make_sink: MakeSink, make_source: MakeSource) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(format!("rows.{extension}"));
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("values", rows()).unwrap();
    let sink = make_sink(&input, "file", &path).unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let node = graph.node("file").unwrap();
    assert!(
        matches!(&node.operation, Operation::Sink { spec } if spec.mode == SinkMode::FinalResult && spec.commit)
    );
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    assert!(path.exists());
    let next = Pipeline::default();
    let output = make_source(&next, "read", &path)
        .unwrap()
        .output("output")
        .unwrap();
    let (graph, _) = next.snapshot().unwrap();
    let source = graph.node("read").unwrap();
    assert!(source.semantic_id.starts_with("ironbeam/io/"));
    assert!(matches!(
        graph
            .validate_for("dataflow-preview", &Capabilities::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    assert_eq!(source.boundedness, Boundedness::Bounded);
    assert!(
        matches!(&source.operation, Operation::Source { progress } if progress.stable_split_id && !progress.reports_event_time)
    );
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        rows()
    );
    fs::write(&path, b"changed").unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
    let empty_path = dir.path().join(format!("empty.{extension}"));
    let empty = Pipeline::default();
    let empty_input = empty.from_vec("empty-values", Vec::<Row>::new()).unwrap();
    let empty_sink = make_sink(&empty_input, "empty-file", &empty_path).unwrap();
    DirectRunner
        .run_sink(&empty_sink, &JobConfig::default())
        .unwrap();
    let empty_reader = Pipeline::default();
    let empty_output = make_source(&empty_reader, "empty-read", &empty_path)
        .unwrap()
        .output("empty-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&empty_output, &JobConfig::default())
            .unwrap()
            .0,
        Vec::<Row>::new()
    );
}
#[test]
fn arrow_ipc_indexed_shards_keep_batch_boundaries_and_rows() {
    use arrow::datatypes::FieldRef;
    use arrow::ipc::writer::FileWriter;
    use serde_arrow::schema::{SchemaLike, TracingOptions};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("batches.arrow");
    let fields = Vec::<FieldRef>::from_type::<Row>(TracingOptions::default()).unwrap();
    let batches = (0..3)
        .map(|id| {
            serde_arrow::to_record_batch(
                &fields,
                &vec![Row {
                    id,
                    name: format!("row-{id}"),
                }],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let file = fs::File::create(&path).unwrap();
    let mut writer = FileWriter::try_new(file, &batches[0].schema()).unwrap();
    for batch in &batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();

    let pipeline = Pipeline::default();
    assert!(arrow_ipc_batch_sharded_source::<Row>(&pipeline, "bad", &path, 0).is_err());
    let rows = arrow_ipc_batch_sharded_source::<Row>(&pipeline, "rows", &path, 1)
        .unwrap()
        .output("rows-out")
        .unwrap();
    let batch_output = arrow_ipc_batches_indexed_source(&pipeline, "batches", &path, 2)
        .unwrap()
        .output("batches-out")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert!(graph.node("rows/batch-2").is_some());
    assert!(graph.node("batches/batch-1").is_some());
    for parallelism in [1, 3] {
        let config = JobConfig {
            parallelism,
            ..JobConfig::default()
        };
        let actual_rows = DirectRunner.run_collect(&rows, &config).unwrap().0;
        assert_eq!(
            actual_rows,
            (0..3)
                .map(|id| Row {
                    id,
                    name: format!("row-{id}"),
                })
                .collect::<Vec<_>>()
        );
        let actual_batches = DirectRunner.run_collect(&batch_output, &config).unwrap().0;
        assert_eq!(actual_batches.len(), 3);
        assert!(actual_batches.iter().all(|batch| batch.0.num_rows() == 1));
    }
    fs::write(&path, b"changed").unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&rows, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
}

#[test]
fn arrow_ipc_row_contract() {
    round_trip(
        "arrow",
        |i, k, p| arrow_ipc_sink(i, k, p),
        |p, k, f| arrow_ipc_source(p, k, f),
    );
}
#[test]
fn arrow_ipc_lazy_row_contract() {
    round_trip(
        "arrow",
        |i, k, p| arrow_ipc_sink(i, k, p),
        |p, k, f| arrow_ipc_streaming_source(p, k, f),
    );
}

#[cfg(feature = "parallel-io")]
#[test]
fn arrow_ipc_record_shards_and_parallel_rows_preserve_order() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("parallel.arrow");
    let expected = (0..7)
        .map(|id| Row {
            id,
            name: format!("row-{id}"),
        })
        .collect::<Vec<_>>();
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", expected.clone()).unwrap();
    assert!(arrow_ipc_parallel_sink(&input, "bad", &path, 0).is_err());
    DirectRunner
        .run_sink(
            &arrow_ipc_parallel_sink(&input, "write", &path, 5).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let reader = Pipeline::default();
    let output = arrow_ipc_sharded_source::<Row>(&reader, "read", &path, 2)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        expected
    );
}

#[test]
fn arrow_ipc_batch_contract_and_coder() {
    let directory = tempfile::tempdir().unwrap();
    let input_path = directory.path().join("input.arrow");
    let output_path = directory.path().join("output.arrow");
    let write_pipeline = Pipeline::default();
    let row_collection = write_pipeline.from_vec("rows", rows()).unwrap();
    let row_sink = arrow_ipc_sink(&row_collection, "row-sink", &input_path).unwrap();
    DirectRunner
        .run_sink(&row_sink, &JobConfig::default())
        .unwrap();

    let batch_pipeline = Pipeline::default();
    let batches = arrow_ipc_batches_source(&batch_pipeline, "batches", &input_path).unwrap();
    let lazy_batches =
        arrow_ipc_batches_streaming_source(&batch_pipeline, "lazy-batches", &input_path)
            .unwrap()
            .output("lazy-batches-out")
            .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&lazy_batches, &JobConfig::default())
            .unwrap()
            .0[0]
            .0
            .num_rows(),
        2
    );
    let output = batches.output("batches-out").unwrap();
    let collected = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap()
        .0;
    assert_eq!(collected.len(), 1);
    assert_eq!(collected[0].0.num_rows(), 2);
    let shard_pipeline = Pipeline::default();
    let batch_shards =
        arrow_ipc_batches_sharded_source(&shard_pipeline, "batch-shards", &input_path, 1)
            .unwrap()
            .output("batch-shards-out")
            .unwrap();
    let sharded = DirectRunner
        .run_collect(&batch_shards, &JobConfig::default())
        .unwrap()
        .0;
    assert_eq!(sharded.len(), 1);
    assert_eq!(sharded[0].0.num_rows(), 2);
    #[cfg(feature = "parallel-io")]
    {
        let parallel_path = directory.path().join("parallel.arrow");
        let parallel_pipeline = Pipeline::default();
        let input = parallel_pipeline
            .from_vec(
                "input-batches",
                vec![collected[0].clone(), collected[0].clone()],
            )
            .unwrap();
        DirectRunner
            .run_sink(
                &arrow_ipc_batches_parallel_sink(&input, "parallel", &parallel_path, 2).unwrap(),
                &JobConfig::default(),
            )
            .unwrap();
        let reader = Pipeline::default();
        let output = arrow_ipc_batches_source(&reader, "parallel-read", &parallel_path)
            .unwrap()
            .output("parallel-out")
            .unwrap();
        let batches = DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0;
        assert_eq!(batches.len(), 2);
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.0.num_rows())
                .collect::<Vec<_>>(),
            [2, 2]
        );
    }
    let encoded = collected[0].encode().unwrap();
    assert_eq!(ArrowBatch::decode(&encoded).unwrap().0.num_rows(), 2);
    assert!(ArrowBatch::decode(&encoded[..encoded.len() - 1]).is_err());
    let serialized = serde_json::to_vec(&collected[0]).unwrap();
    let restored_batch: ArrowBatch = serde_json::from_slice(&serialized).unwrap();
    assert_eq!(restored_batch.0.num_rows(), 2);
    let batch_sink = arrow_ipc_batches_sink(&batches, "batch-sink", &output_path).unwrap();
    DirectRunner
        .run_sink(&batch_sink, &JobConfig::default())
        .unwrap();

    let read_pipeline = Pipeline::default();
    let restored = arrow_ipc_source::<Row>(&read_pipeline, "restored", &output_path)
        .unwrap()
        .output("restored-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&restored, &JobConfig::default())
            .unwrap()
            .0,
        rows()
    );
}
