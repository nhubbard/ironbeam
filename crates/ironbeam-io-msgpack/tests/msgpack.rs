use ironbeam_core::{Element, Envelope, ErrorKind, Pipeline, Window};
use ironbeam_direct::{DirectRunner, JobConfig};
#[cfg(feature = "parallel-io")]
use ironbeam_io_msgpack::msgpack_parallel_sink;
use ironbeam_io_msgpack::{
    msgpack_fragment_sink, msgpack_fragment_store, msgpack_glob_source, msgpack_sharded_source,
    msgpack_sink, msgpack_source, msgpack_streaming_source, publish_msgpack_fragments,
};

#[test]
fn finalized_msgpack_bundles_publish_one_valid_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let staging = dir.path().join("staging");
    let target = dir.path().join("final.msgpack");
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", rows()).unwrap();
    let sink = msgpack_fragment_sink(&input, "final", &staging, "job", "graph").unwrap();
    let (_, registry) = sink.snapshot().unwrap();
    assert!(registry.sink_requires_bundle_finalization("final"));
    let store = msgpack_fragment_store(&staging, "job", "graph", "final").unwrap();
    for (bundle, row) in [
        ("bundle-1", rows()[0].clone()),
        ("bundle-2", rows()[1].clone()),
    ] {
        let mut session = registry.open_sink("final").unwrap();
        session.begin_bundle(bundle).unwrap();
        session
            .write(Envelope {
                pane: ironbeam_core::PaneInfo::NO_FIRING,
                value: Box::new(row),
                timestamp_ns: 0,
                window: Window::Global,
                assigned_window: None,
            })
            .unwrap();
        session.commit().unwrap();
        assert!(publish_msgpack_fragments::<Row>(&store, 2, &target).is_err());
        session.finalize_bundle().unwrap();
    }
    assert_eq!(store.accepted_fragments().unwrap().len(), 2);
    assert!(!target.exists());
    let store = store
        .require_bundle_attempts(["bundle-1", "bundle-2"])
        .unwrap();
    let result = publish_msgpack_fragments::<Row>(&store, 2, &target).unwrap();
    assert_eq!(result.rows, 2);
    assert_eq!(
        publish_msgpack_fragments::<Row>(&store, 2, &target).unwrap(),
        result
    );
    let mut actual =
        collect(&msgpack_source::<Row>(&Pipeline::default(), "verify", &target).unwrap());
    actual.sort_by_key(|row| row.id);
    assert_eq!(actual, rows());
    assert!(publish_msgpack_fragments::<Row>(&store, 3, &target).is_err());
    assert!(
        publish_msgpack_fragments::<Row>(&store, 2, dir.path().join("bad.msgpack.gz")).is_err()
    );
}

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
    let staging = dir.path().join("fragment-stage");
    let fragment = msgpack_fragment_sink(&input, "fragment", &staging, "job", "graph").unwrap();
    let (_, registry) = fragment.snapshot().unwrap();
    let mut session = registry.open_sink("fragment").unwrap();
    session.begin_bundle("bad-bundle").unwrap();
    assert_eq!(
        session
            .write(Envelope {
                pane: ironbeam_core::PaneInfo::NO_FIRING,
                value: Box::new(Fails),
                timestamp_ns: 0,
                window: Window::Global,
                assigned_window: None,
            })
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
    session.abort().unwrap();
    assert!(
        msgpack_fragment_store(&staging, "job", "graph", "fragment")
            .unwrap()
            .accepted_fragments()
            .unwrap()
            .is_empty()
    );
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
