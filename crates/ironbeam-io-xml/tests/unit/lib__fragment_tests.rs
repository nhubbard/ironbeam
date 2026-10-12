// Unit tests for src/lib.rs.
use super::*;

#[derive(Clone, Debug, PartialEq, serde::Deserialize, Serialize)]
struct Row {
    id: i32,
    name: String,
}

impl Element for Row {
    const TYPE_ID: &'static str = "xml-fragment-test-row";
    const CODER_ID: &'static str = "xml-fragment-test-json";
}

#[test]
fn merges_multiple_item_fragments_into_one_document() {
    let dir = tempfile::tempdir().unwrap();
    let stage = dir.path().join("stage");
    let store = xml_fragment_store(&stage, "job", "graph", "sink").unwrap();
    let output = dir.path().join("result.xml");
    for (bundle, row) in [
        (
            "a",
            Row {
                id: 1,
                name: "Ada & Lin".into(),
            },
        ),
        (
            "b",
            Row {
                id: 2,
                name: "<B>".into(),
            },
        ),
    ] {
        let mut bytes = Vec::new();
        encode_xml_item(&row, &mut bytes).unwrap();
        store.local.stage(bundle, 1, &bytes).unwrap();
        store.local.accept(bundle).unwrap();
    }
    let store = store.require_bundle_attempts(["a", "b"]).unwrap();
    let result = publish_xml_fragments::<Row>(&store, 2, &output).unwrap();
    assert_eq!(result.fragments, 2);
    assert_eq!(
        publish_xml_fragments::<Row>(&store, 2, &output).unwrap(),
        result
    );
    let mut decoded = read_xml::<Row>(&std::fs::read(&output).unwrap()).unwrap();
    decoded.sort_by_key(|row| row.id);
    assert_eq!(
        decoded,
        [
            Row {
                id: 1,
                name: "Ada & Lin".into()
            },
            Row {
                id: 2,
                name: "<B>".into()
            },
        ]
    );
    assert!(publish_xml_fragments::<Row>(&store, 3, &output).is_err());
}

#[test]
fn malformed_fragment_and_empty_result_are_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("result.xml");
    let invalid = xml_fragment_store(dir.path().join("invalid"), "job", "graph", "sink").unwrap();
    invalid
        .local
        .stage("a", 1, b"<item><id>bad</id></item>")
        .unwrap();
    invalid.local.accept("a").unwrap();
    assert!(publish_xml_fragments::<Row>(&invalid, 1, &output).is_err());
    assert!(!output.exists());
    let empty = xml_fragment_store(dir.path().join("empty"), "job", "graph", "sink")
        .unwrap()
        .require_bundle_attempts([] as [&str; 0])
        .unwrap();
    assert!(xml_fragment_store("relative", "job", "graph", "sink").is_err());
    assert!(publish_xml_fragments::<Row>(&empty, 0, dir.path().join("result.xml.gz")).is_err());
    assert_eq!(
        publish_xml_fragments::<Row>(&empty, 0, &output)
            .unwrap()
            .rows,
        0
    );
    assert_eq!(
        read_xml::<Row>(&std::fs::read(&output).unwrap()).unwrap(),
        []
    );
}
#[test]
fn fragment_sink_registration_and_row_count_validation() {
    let dir = tempfile::tempdir().unwrap();
    let stage = dir.path().join("stage");
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
    xml_fragment_sink(&input, "sink", &stage, "job", "graph").unwrap();
    assert!(xml_fragment_sink(&input, "bad", "relative", "job", "graph").is_err());
    assert!(xml_fragment_store(&stage, "job", "", "sink").is_err());
    let store = xml_fragment_store(&stage, "job", "graph", "sink").unwrap();
    assert!(store.accepted_fragments().unwrap().is_empty());
    store
        .local
        .stage("bundle", 2, b"<item><id>1</id><name>Ada</name></item>")
        .unwrap();
    store.local.accept("bundle").unwrap();
    assert_eq!(store.accepted_fragments().unwrap().len(), 1);
    let output = dir.path().join("wrong-count.xml");
    assert!(publish_xml_fragments::<Row>(&store, 2, &output).is_err());
    assert!(!output.exists());
}
