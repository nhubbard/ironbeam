// Unit tests for src/lib.rs.
use super::*;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Row {
    id: i32,
    label: String,
}
impl Element for Row {
    const TYPE_ID: &'static str = "tests/arrow-fragment-row";
    const CODER_ID: &'static str = "tests/arrow-fragment-json";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        serde_json::to_vec(self).map_err(|error| Error::incompatible(error.to_string()))
    }
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        serde_json::from_slice(bytes).map_err(|error| Error::incompatible(error.to_string()))
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Uncoded(i32);
impl Element for Uncoded {
    const TYPE_ID: &'static str = "tests/arrow-fragment-uncoded";
    const CODER_ID: &'static str = "tests/arrow-fragment-uncoded";
}

#[test]
fn merges_bundles_into_native_arrow_file_and_reopens_rows() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        arrow_ipc_row_fragment_store::<Row>(dir.path().join("stage"), "job", "graph", "sink")
            .unwrap();
    let target = dir.path().join("rows.arrow");
    for (bundle, row) in [
        (
            "a",
            Row {
                id: 1,
                label: "Ada".into(),
            },
        ),
        (
            "b",
            Row {
                id: 2,
                label: "Lin".into(),
            },
        ),
    ] {
        let mut frame = Vec::new();
        encode_fragment_row(&row, &mut frame).unwrap();
        store.local.stage(bundle, 1, &frame).unwrap();
        store.local.accept(bundle).unwrap();
    }
    assert_eq!(store.accepted_fragments().unwrap().len(), 2);
    let store = store.require_bundle_attempts(["a", "b"]).unwrap();
    let result = publish_arrow_ipc_row_fragments(&store, 2, &target).unwrap();
    assert_eq!(result.rows, 2);
    assert_eq!(result.fragments, 2);
    assert_eq!(
        publish_arrow_ipc_row_fragments(&store, 2, &target).unwrap(),
        result
    );
    let mut actual = read_arrow_ipc::<Row>(&fs::read(&target).unwrap()).unwrap();
    actual.sort_by_key(|row| row.id);
    assert_eq!(
        actual,
        [
            Row {
                id: 1,
                label: "Ada".into()
            },
            Row {
                id: 2,
                label: "Lin".into()
            },
        ]
    );
    assert!(publish_arrow_ipc_row_fragments(&store, 3, &target).is_err());
}

#[test]
fn rejects_malformed_rows_and_wrong_counts_without_publication() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("bad.arrow");
    let store = arrow_ipc_row_fragment_store::<Row>(dir.path().join("bad"), "job", "graph", "sink")
        .unwrap();
    store.local.stage("short", 1, b"abc").unwrap();
    store.local.accept("short").unwrap();
    let store = store.require_bundle_attempts(["short"]).unwrap();
    assert!(publish_arrow_ipc_row_fragments(&store, 1, &target).is_err());
    assert!(!target.exists());
    let store =
        arrow_ipc_row_fragment_store::<Row>(dir.path().join("wrong"), "job", "graph", "sink")
            .unwrap();
    let mut frame = Vec::new();
    encode_fragment_row(
        &Row {
            id: 1,
            label: "Ada".into(),
        },
        &mut frame,
    )
    .unwrap();
    store.local.stage("wrong", 2, &frame).unwrap();
    store.local.accept("wrong").unwrap();
    let store = store.require_bundle_attempts(["wrong"]).unwrap();
    assert!(publish_arrow_ipc_row_fragments(&store, 2, &target).is_err());
    assert!(!target.exists());
    let store =
        arrow_ipc_row_fragment_store::<Row>(dir.path().join("length"), "job", "graph", "sink")
            .unwrap();
    store
        .local
        .stage("length", 1, &100_u64.to_le_bytes())
        .unwrap();
    store.local.accept("length").unwrap();
    let store = store.require_bundle_attempts(["length"]).unwrap();
    assert!(publish_arrow_ipc_row_fragments(&store, 1, &target).is_err());
    assert!(!target.exists());
}

#[test]
fn writes_multiple_batches_and_empty_file_and_rejects_uncoded_rows() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        arrow_ipc_row_fragment_store::<Row>(dir.path().join("many"), "job", "graph", "sink")
            .unwrap();
    let mut frames = Vec::new();
    for id in 0..1_030 {
        encode_fragment_row(
            &Row {
                id,
                label: format!("row-{id}"),
            },
            &mut frames,
        )
        .unwrap();
    }
    store.local.stage("many", 1_030, &frames).unwrap();
    store.local.accept("many").unwrap();
    let store = store.require_bundle_attempts(["many"]).unwrap();
    let target = dir.path().join("many.arrow");
    assert_eq!(
        publish_arrow_ipc_row_fragments(&store, 1_030, &target)
            .unwrap()
            .rows,
        1_030
    );
    assert_eq!(
        read_arrow_ipc::<Row>(&fs::read(&target).unwrap())
            .unwrap()
            .len(),
        1_030
    );
    let empty =
        arrow_ipc_row_fragment_store::<Row>(dir.path().join("empty"), "job", "graph", "sink")
            .unwrap()
            .require_bundle_attempts([] as [&str; 0])
            .unwrap();
    let empty_target = dir.path().join("empty.arrow");
    assert_eq!(
        publish_arrow_ipc_row_fragments(&empty, 0, &empty_target)
            .unwrap()
            .rows,
        0
    );
    assert_eq!(
        read_arrow_ipc::<Row>(&fs::read(empty_target).unwrap()).unwrap(),
        Vec::<Row>::new()
    );
    let pipeline = Pipeline::default();
    let coded = pipeline
        .from_vec(
            "coded",
            vec![Row {
                id: 1,
                label: "ok".into(),
            }],
        )
        .unwrap();
    arrow_ipc_row_fragment_sink(
        &coded,
        "coded-sink",
        dir.path().join("stage"),
        "job",
        "graph",
    )
    .unwrap();
    assert!(arrow_ipc_row_fragment_store::<Row>("relative", "job", "graph", "sink").is_err());
    let input = pipeline.from_vec("uncoded", vec![Uncoded(1)]).unwrap();
    assert!(
        arrow_ipc_row_fragment_sink(&input, "sink", dir.path().join("stage"), "job", "graph",)
            .is_err()
    );
    assert!(
        arrow_ipc_row_fragment_store::<Uncoded>(dir.path().join("stage"), "job", "graph", "sink",)
            .is_err()
    );
}
