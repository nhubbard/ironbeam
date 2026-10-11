use ironbeam_core::{Element, ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
#[cfg(feature = "parallel-io")]
use ironbeam_io_msgpack::msgpack_parallel_sink;
use ironbeam_io_msgpack::{
    msgpack_glob_source, msgpack_sharded_source, msgpack_sink, msgpack_source,
    msgpack_streaming_source,
};

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Row {
    id: i32,
    label: String,
}
impl Element for Row {
    const TYPE_ID: &'static str = "msgpack-test/row";
    const CODER_ID: &'static str = "msgpack-test/row-v1";
}
fn rows() -> Vec<Row> {
    vec![
        Row {
            id: 1,
            label: "Ada".into(),
        },
        Row {
            id: 2,
            label: "Lin".into(),
        },
    ]
}
fn collect(collection: &ironbeam_core::PCollection<Row>) -> Vec<Row> {
    let output = collection.output("out").unwrap();
    DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap()
        .0
}

#[test]
fn msgpack_bounded_lazy_sharded_and_glob_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.msgpack");
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", rows()).unwrap();
    DirectRunner
        .run_sink(
            &msgpack_sink(&input, "write", &path).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    assert_eq!(
        collect(&msgpack_source::<Row>(&Pipeline::default(), "read", &path).unwrap()),
        rows()
    );
    assert_eq!(
        collect(&msgpack_streaming_source::<Row>(&Pipeline::default(), "lazy", &path).unwrap()),
        rows()
    );
    assert_eq!(
        collect(&msgpack_sharded_source::<Row>(&Pipeline::default(), "shards", &path, 1).unwrap()),
        rows()
    );
    let pattern = format!("{}/*.msgpack", dir.path().display());
    assert_eq!(
        collect(&msgpack_glob_source::<Row>(&Pipeline::default(), "glob", &pattern).unwrap()),
        rows()
    );
}

#[test]
fn malformed_msgpack_is_an_io_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.msgpack");
    std::fs::write(&path, [0xc1]).unwrap();
    for source in [
        msgpack_source::<Row>(&Pipeline::default(), "read", &path).unwrap(),
        msgpack_streaming_source::<Row>(&Pipeline::default(), "lazy", &path).unwrap(),
    ] {
        let output = source.output("out").unwrap();
        assert_eq!(
            DirectRunner
                .run_collect(&output, &JobConfig::default())
                .err()
                .unwrap()
                .kind,
            ErrorKind::Io
        );
    }
}

#[test]
fn removed_lazy_input_fails_at_execution() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("removed.msgpack");
    std::fs::write(&path, [0x90]).unwrap();
    let output = msgpack_streaming_source::<Row>(&Pipeline::default(), "lazy", &path)
        .unwrap()
        .output("out")
        .unwrap();
    std::fs::remove_file(&path).unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
}

#[test]
fn failed_msgpack_serialization_preserves_previous_result() {
    #[derive(Clone)]
    struct Fails;
    impl Element for Fails {
        const TYPE_ID: &'static str = "msgpack-test/fails";
        const CODER_ID: &'static str = "msgpack-test/fails";
    }
    impl serde::Serialize for Fails {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("cannot encode row"))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("existing.msgpack");
    std::fs::write(&path, b"previous result").unwrap();
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", vec![Fails]).unwrap();
    let sink = msgpack_sink(&input, "write", &path).unwrap();
    assert_eq!(
        DirectRunner
            .run_sink(&sink, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
    assert_eq!(std::fs::read(&path).unwrap(), b"previous result");
}

#[cfg(feature = "parallel-io")]
#[test]
fn parallel_msgpack_preserves_record_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("parallel.msgpack");
    let expected = (0..7)
        .map(|id| Row {
            id,
            label: format!("row-{id}"),
        })
        .collect::<Vec<_>>();
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", expected.clone()).unwrap();
    assert!(msgpack_parallel_sink(&input, "bad", &path, 0).is_err());
    DirectRunner
        .run_sink(
            &msgpack_parallel_sink(&input, "write", &path, 5).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    assert_eq!(
        collect(&msgpack_source::<Row>(&Pipeline::default(), "read", &path).unwrap()),
        expected
    );
}

#[cfg(feature = "compression-gzip")]
#[test]
fn gzip_msgpack_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.msgpack.gz");
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", rows()).unwrap();
    DirectRunner
        .run_sink(
            &msgpack_sink(&input, "write", &path).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    assert_eq!(
        collect(&msgpack_streaming_source::<Row>(&Pipeline::default(), "read", &path).unwrap()),
        rows()
    );
}
