use ironbeam_core::{Element, ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
#[cfg(feature = "parallel-io")]
use ironbeam_io_xml::xml_parallel_sink;
use ironbeam_io_xml::{xml_glob_source, xml_sink, xml_source, xml_streaming_source};

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Row {
    id: i32,
    name: String,
}
impl Element for Row {
    const TYPE_ID: &'static str = "xml-test/row";
    const CODER_ID: &'static str = "xml-test/row-v1";
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

#[test]
fn xml_source_sink_streaming_and_glob_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.xml");
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", rows()).unwrap();
    DirectRunner
        .run_sink(
            &xml_sink(&input, "write", &path).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    for read in [
        xml_source::<Row>(&Pipeline::default(), "read", &path).unwrap(),
        xml_streaming_source::<Row>(&Pipeline::default(), "stream", &path, 1).unwrap(),
        xml_glob_source::<Row>(
            &Pipeline::default(),
            "glob",
            &format!("{}/*.xml", dir.path().display()),
        )
        .unwrap(),
    ] {
        let output = read.output("out").unwrap();
        assert_eq!(
            DirectRunner
                .run_collect(&output, &JobConfig::default())
                .unwrap()
                .0,
            rows()
        );
    }
}

#[test]
fn malformed_xml_is_an_io_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.xml");
    std::fs::write(&path, b"<records><item><id>bad</id></item>").unwrap();
    let output = xml_source::<Row>(&Pipeline::default(), "source", &path)
        .unwrap()
        .output("out")
        .unwrap();
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
fn empty_xml_and_non_utf8_input_are_explicit() {
    let dir = tempfile::tempdir().unwrap();
    let empty = dir.path().join("empty.xml");
    std::fs::write(&empty, b"<records/>").unwrap();
    let output = xml_source::<Row>(&Pipeline::default(), "empty", &empty)
        .unwrap().output("empty-out").unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        Vec::<Row>::new()
    );

    let bad = dir.path().join("non-utf8.xml");
    std::fs::write(&bad, [0xff]).unwrap();
    let output = xml_source::<Row>(&Pipeline::default(), "bad", &bad)
        .unwrap().output("bad-out").unwrap();
    assert_eq!(DirectRunner.run_collect(&output, &JobConfig::default()).err().unwrap().kind, ErrorKind::Io);
}

#[test]
fn failed_xml_serialization_preserves_previous_result() {
    #[derive(Clone)]
    struct Fails;
    impl Element for Fails {
        const TYPE_ID: &'static str = "xml-test/fails";
        const CODER_ID: &'static str = "xml-test/fails";
    }
    impl serde::Serialize for Fails {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("cannot encode row"))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("existing.xml");
    std::fs::write(&path, b"previous result").unwrap();
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", vec![Fails]).unwrap();
    let sink = xml_sink(&input, "write", &path).unwrap();
    assert_eq!(DirectRunner.run_sink(&sink, &JobConfig::default()).err().unwrap().kind, ErrorKind::Io);
    assert_eq!(std::fs::read(&path).unwrap(), b"previous result");
}

#[cfg(feature = "parallel-io")]
#[test]
fn parallel_xml_merges_ordered_shards() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("parallel.xml");
    let expected = (0..7)
        .map(|id| Row {
            id,
            name: format!("row-{id}"),
        })
        .collect::<Vec<_>>();
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", expected.clone()).unwrap();
    assert!(xml_parallel_sink(&input, "bad", &path, 0).is_err());
    DirectRunner
        .run_sink(
            &xml_parallel_sink(&input, "write", &path, 5).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let output = xml_source::<Row>(&Pipeline::default(), "read", &path)
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

#[cfg(feature = "compression-gzip")]
#[test]
fn gzip_xml_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.xml.gz");
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", rows()).unwrap();
    DirectRunner
        .run_sink(
            &xml_sink(&input, "write", &path).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let output = xml_streaming_source::<Row>(&Pipeline::default(), "read", &path, 1)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        rows()
    );
}
