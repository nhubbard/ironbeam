// Unit tests for src/lib.rs.
use super::*;
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct Row {
    id: u32,
    name: String,
}

impl Element for Row {
    const TYPE_ID: &'static str = "csv-fragment-test-row";
    const CODER_ID: &'static str = "csv-fragment-test-json";
}

fn session(store: &CsvFragmentStore) -> CsvFragmentSink<Row> {
    CsvFragmentSink {
        store: store.clone(),
        bundle_id: None,
        writer: None,
        rows: 0,
        staged: false,
        marker: std::marker::PhantomData,
    }
}

#[test]
fn merges_bundles_with_one_header_and_multiline_values() {
    let dir = tempfile::tempdir().unwrap();
    let stage = dir.path().join("stage");
    let store = csv_fragment_store(&stage, "job", "graph", "sink").unwrap();
    let output = dir.path().join("result.csv");
    let mut first = session(&store);
    first.begin_bundle("first").unwrap();
    first
        .write(Row {
            id: 1,
            name: "Ada,\nLovelace".into(),
        })
        .unwrap();
    first.commit().unwrap();
    assert!(publish_csv_fragments::<Row>(&store, 1, &output).is_err());
    first.finalize_bundle().unwrap();
    let mut second = session(&store);
    second.begin_bundle("second").unwrap();
    second
        .write(Row {
            id: 2,
            name: "Lin".into(),
        })
        .unwrap();
    second.commit().unwrap();
    second.finalize_bundle().unwrap();
    let store = store.require_bundle_attempts(["first", "second"]).unwrap();
    let result = publish_csv_fragments::<Row>(&store, 2, &output).unwrap();
    assert_eq!(result.rows, 2);
    assert_eq!(result.fragments, 2);
    assert_eq!(
        publish_csv_fragments::<Row>(&store, 2, &output).unwrap(),
        result
    );
    assert_eq!(
        std::fs::read_to_string(&output)
            .unwrap()
            .matches("id,name")
            .count(),
        1
    );
    let mut rows = read_csv::<Row>(&std::fs::read(&output).unwrap()).unwrap();
    rows.sort_by_key(|row| row.id);
    assert_eq!(
        rows,
        vec![
            Row {
                id: 1,
                name: "Ada,\nLovelace".into()
            },
            Row {
                id: 2,
                name: "Lin".into()
            },
        ]
    );
    assert!(publish_csv_fragments::<Row>(&store, 3, &output).is_err());
}

#[test]
fn rejects_header_mismatch_and_bad_rows_without_publishing() {
    let dir = tempfile::tempdir().unwrap();
    let stage = dir.path().join("stage");
    let store = csv_fragment_store(&stage, "job", "graph", "sink").unwrap();
    let output = dir.path().join("result.csv");
    store.local.stage("a", 1, b"id,name\n1,Ada\n").unwrap();
    store.local.accept("a").unwrap();
    store.local.stage("b", 1, b"name,id\nLin,2\n").unwrap();
    store.local.accept("b").unwrap();
    let store = store.require_bundle_attempts(["a", "b"]).unwrap();
    assert!(publish_csv_fragments::<Row>(&store, 2, &output)
        .err().unwrap().to_string().contains("headers disagree"));
    assert!(!output.exists());
    let valid = csv_fragment_store(dir.path().join("other"), "job", "graph", "sink").unwrap();
    valid
        .local
        .stage("a", 1, b"id,name\nnot-a-number,Ada\n")
        .unwrap();
    valid.local.accept("a").unwrap();
    let valid = valid.require_bundle_attempts(["a"]).unwrap();
    assert!(publish_csv_fragments::<Row>(&valid, 1, &output)
        .err().unwrap().to_string().contains("csv-fragment"));
    assert!(!output.exists());
    assert!(publish_csv_fragments::<Row>(&valid, 1, dir.path().join("result.csv.gz")).is_err());
}

#[test]
fn empty_bundle_and_recovery_are_durable() {
    let dir = tempfile::tempdir().unwrap();
    let store = csv_fragment_store(dir.path().join("stage"), "job", "graph", "sink").unwrap();
    let output = dir.path().join("empty.csv");
    let mut bundle = session(&store);
    assert!(
        bundle
            .write(Row {
                id: 0,
                name: String::new()
            })
            .is_err()
    );
    bundle.begin_bundle("empty").unwrap();
    bundle.commit().unwrap();
    let mut restarted = session(&store);
    restarted.recover_bundle_finalization("empty").unwrap();
    bundle.finalize_bundle().unwrap();
    let store = store.require_bundle_attempts(["empty"]).unwrap();
    assert_eq!(
        publish_csv_fragments::<Row>(&store, 0, &output)
            .unwrap()
            .rows,
        0
    );
    assert_eq!(std::fs::read(&output).unwrap(), b"");
    assert!(csv_fragment_store("relative", "job", "graph", "sink").is_err());
}

#[test]
fn sink_registration_and_bundle_failures_preserve_staging() {
    let dir = tempfile::tempdir().unwrap();
    let stage = dir.path().join("stage");
    let store = csv_fragment_store(&stage, "job", "graph", "sink").unwrap();
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec(
            "input",
            vec![Row {
                id: 1,
                name: "Ada".into(),
            }],
        )
        .unwrap();
    csv_fragment_sink(&input, "sink", &stage, "job", "graph").unwrap();
    assert!(csv_fragment_sink(&input, "bad", "relative", "job", "graph").is_err());
    assert!(csv_fragment_store(&stage, "job", "", "sink").is_err());
    let mut bundle = session(&store);
    assert!(bundle.begin_bundle("").is_err());
    assert!(bundle.commit().is_err());
    assert!(bundle.finalize_bundle().is_err());
    bundle.begin_bundle("attempt").unwrap();
    assert!(bundle.begin_bundle("again").is_err());
    assert!(bundle.finalize_bundle().is_err());
    bundle
        .write(Row {
            id: 1,
            name: "Ada".into(),
        })
        .unwrap();
    bundle.commit().unwrap();
    assert!(store.accepted_fragments().unwrap().is_empty());
    bundle.abort().unwrap();
    assert!(store.accepted_fragments().unwrap().is_empty());
    assert!(bundle.finalize_bundle().is_err());
    let mut unnamed = CsvFragmentSink::<i32> {
        store,
        bundle_id: None,
        writer: None,
        rows: 0,
        staged: false,
        marker: std::marker::PhantomData,
    };
    unnamed.begin_bundle("unnamed").unwrap();
    assert!(unnamed.write(1).is_err());
}

#[test]
fn rejects_empty_payload_with_rows_and_wrong_fragment_count() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("result.csv");
    let store =
        csv_fragment_store(dir.path().join("empty-with-data"), "job", "graph", "sink").unwrap();
    store.local.stage("a", 0, b"id,name\n").unwrap();
    store.local.accept("a").unwrap();
    let store = store.require_bundle_attempts(["a"]).unwrap();
    assert!(publish_csv_fragments::<Row>(&store, 0, &output)
        .err().unwrap().to_string().contains("empty CSV fragment has a payload"));
    assert!(!output.exists());
    let store = csv_fragment_store(dir.path().join("wrong-count"), "job", "graph", "sink").unwrap();
    store.local.stage("a", 2, b"id,name\n1,Ada\n").unwrap();
    store.local.accept("a").unwrap();
    let store = store.require_bundle_attempts(["a"]).unwrap();
    assert!(publish_csv_fragments::<Row>(&store, 2, &output)
        .err().unwrap().to_string().contains("decoded row count disagrees"));
    assert!(!output.exists());
}

#[test]
fn registered_fragment_sink_publishes_an_accepted_bundle() {
    let dir = tempfile::tempdir().unwrap();
    let stage = dir.path().join("stage");
    let output = dir.path().join("result.csv");
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", vec![Row { id: 1, name: "Ada".into() }]).unwrap();
    csv_fragment_sink(&input, "sink", &stage, "job", "graph").unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let mut sink = registry.open_sink("sink").unwrap();
    sink.begin_bundle("bundle").unwrap();
    sink.write(ironbeam_core::Envelope {
        pane: ironbeam_core::PaneInfo::NO_FIRING,
        value: Box::new(Row { id: 1, name: "Ada".into() }),
        timestamp_ns: 0,
        window: ironbeam_core::Window::Global,
        assigned_window: None,
    }).unwrap();
    sink.commit().unwrap();
    sink.finalize_bundle().unwrap();
    let store = csv_fragment_store(stage, "job", "graph", "sink")
        .unwrap()
        .require_bundle_attempts(["bundle"])
        .unwrap();
    assert_eq!(publish_csv_fragments::<Row>(&store, 1, &output).unwrap().rows, 1);
    assert_eq!(read_csv::<Row>(&std::fs::read(output).unwrap()).unwrap(), [Row { id: 1, name: "Ada".into() }]);
}

#[test]
fn malformed_fragment_headers_fail_before_publication() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("result.csv");
    for (name, payload, expected) in [
        ("empty", b"".as_slice(), "missing a header"),
        ("invalid-utf8", b"\xff".as_slice(), "csv-fragment"),
    ] {
        let store = csv_fragment_store(dir.path().join(name), "job", "graph", "sink").unwrap();
        store.local.stage("attempt", 1, payload).unwrap();
        store.local.accept("attempt").unwrap();
        let store = store.require_bundle_attempts(["attempt"]).unwrap();
        assert!(publish_csv_fragments::<Row>(&store, 1, &output)
            .err().unwrap().to_string().contains(expected));
        assert!(!output.exists());
    }
}
