use ironbeam_core::{Element, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
#[cfg(feature = "parallel-io")]
use ironbeam_io_csv::csv_parallel_sink;
use ironbeam_io_csv::{
    csv_glob_source, csv_sharded_source, csv_sink, csv_source, csv_streaming_source,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct Row {
    id: i32,
    name: String,
}
impl Element for Row {
    const TYPE_ID: &'static str = "tests/csv-row";
    const CODER_ID: &'static str = "tests/csv-row-json";
}

fn collect(source: impl FnOnce(&Pipeline) -> ironbeam_core::PCollection<Row>) -> Vec<Row> {
    let pipeline = Pipeline::default();
    let output = source(&pipeline).output("out").unwrap();
    DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap()
        .0
}

#[test]
fn csv_read_modes_agree_on_multiline_records() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.csv");
    std::fs::write(&path, b"id,name\n1,\"Ada\nLovelace\"\n2,Lin\n").unwrap();
    let expected = vec![
        Row {
            id: 1,
            name: "Ada\nLovelace".into(),
        },
        Row {
            id: 2,
            name: "Lin".into(),
        },
    ];
    assert_eq!(
        collect(|p| csv_source(p, "source", &path).unwrap()),
        expected
    );
    assert_eq!(
        collect(|p| csv_streaming_source(p, "source", &path).unwrap()),
        expected
    );
    assert_eq!(
        collect(|p| csv_sharded_source(p, "source", &path, 1).unwrap()),
        expected
    );
    assert!(csv_sharded_source::<Row>(&Pipeline::default(), "bad", &path, 0).is_err());
}

#[test]
fn csv_sink_commits_and_glob_reads_sorted_files() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.csv");
    let b = dir.path().join("b.csv");
    let values = vec![
        Row {
            id: 1,
            name: "Ada".into(),
        },
        Row {
            id: 2,
            name: "Lin".into(),
        },
    ];
    for (path, value) in [(&a, &values[0]), (&b, &values[1])] {
        let pipeline = Pipeline::default();
        let input = pipeline.from_vec("input", vec![value.clone()]).unwrap();
        let sink = csv_sink(&input, "sink", path).unwrap();
        assert!(!path.exists());
        DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    }
    let pattern = dir.path().join("*.csv").to_string_lossy().into_owned();
    assert_eq!(
        collect(|p| csv_glob_source(p, "source", &pattern).unwrap()),
        values
    );
}

#[cfg(feature = "parallel-io")]
#[test]
fn csv_parallel_sink_writes_one_header_and_ordered_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.csv");
    let values = (0..7)
        .map(|id| Row {
            id,
            name: format!("row-{id}"),
        })
        .collect::<Vec<_>>();
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", values.clone()).unwrap();
    assert!(csv_parallel_sink(&input, "bad", &path, 0).is_err());
    let sink = csv_parallel_sink(&input, "sink", &path, 3).unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    assert_eq!(
        std::fs::read_to_string(&path)
            .unwrap()
            .matches("id,name")
            .count(),
        1
    );
    assert_eq!(collect(|p| csv_source(p, "source", &path).unwrap()), values);
}

#[test]
fn csv_source_rejects_changes_after_graph_construction() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.csv");
    std::fs::write(&path, b"id,name\n1,Ada\n").unwrap();
    let pipeline = Pipeline::default();
    let output = csv_source::<Row>(&pipeline, "source", &path)
        .unwrap()
        .output("out")
        .unwrap();
    std::fs::write(&path, b"id,name\n2,Lin\n").unwrap();
    assert!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .is_err()
    );
}

#[test]
fn csv_sink_rejects_unnamed_values_without_replacing_output() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.csv");
    std::fs::write(&path, b"existing output\n").unwrap();
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", vec![1_i32, 2]).unwrap();
    let sink = csv_sink(&input, "sink", &path).unwrap();
    assert!(DirectRunner.run_sink(&sink, &JobConfig::default()).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"existing output\n");
}

#[test]
fn csv_sink_writes_an_empty_collection() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.csv");
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", Vec::<Row>::new()).unwrap();
    let sink = csv_sink(&input, "sink", &path).unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"");
    assert_eq!(
        collect(|p| csv_source(p, "source", &path).unwrap()),
        Vec::<Row>::new()
    );
}

#[cfg(feature = "parallel-io")]
#[test]
fn csv_parallel_sink_rejects_unnamed_values_without_replacing_output() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.csv");
    std::fs::write(&path, b"existing output\n").unwrap();
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", vec![1_i32, 2]).unwrap();
    let sink = csv_parallel_sink(&input, "sink", &path, 2).unwrap();
    assert!(DirectRunner.run_sink(&sink, &JobConfig::default()).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"existing output\n");
}

#[cfg(feature = "compression-gzip")]
#[test]
fn csv_gzip_round_trips_through_eager_lazy_and_sharded_sources() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.csv.gz");
    let values = vec![
        Row {
            id: 1,
            name: "Ada".into(),
        },
        Row {
            id: 2,
            name: "Lin".into(),
        },
    ];
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", values.clone()).unwrap();
    let sink = csv_sink(&input, "sink", &path).unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    assert_eq!(&std::fs::read(&path).unwrap()[..2], &[0x1f, 0x8b]);
    assert_eq!(collect(|p| csv_source(p, "eager", &path).unwrap()), values);
    assert_eq!(
        collect(|p| csv_streaming_source(p, "lazy", &path).unwrap()),
        values
    );
    assert_eq!(
        collect(|p| csv_sharded_source(p, "shards", &path, 1).unwrap()),
        values
    );
}
