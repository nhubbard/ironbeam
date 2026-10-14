// Unit tests for src/lib.rs.
use super::*;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Row {
    id: i32,
    label: String,
}
impl Element for Row {
    const TYPE_ID: &'static str = "tests/parquet-fragment-row";
    const CODER_ID: &'static str = "tests/parquet-fragment-json";
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
struct Uncoded {
    value: i32,
}
impl Element for Uncoded {
    const TYPE_ID: &'static str = "tests/parquet-uncoded";
    const CODER_ID: &'static str = "tests/parquet-uncoded";
}

#[test]
fn publishes_multiple_coded_bundles_as_one_native_parquet_file() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        parquet_fragment_store::<Row>(dir.path().join("stage"), "job", "graph", "sink").unwrap();
    let output = dir.path().join("result.parquet");
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
    let store = store.require_bundle_attempts(["a", "b"]).unwrap();
    let published = publish_parquet_fragments::<Row>(&store, 2, &output).unwrap();
    assert_eq!(published.rows, 2);
    assert_eq!(published.fragments, 2);
    assert_eq!(
        publish_parquet_fragments::<Row>(&store, 2, &output).unwrap(),
        published
    );
    let mut rows = read_parquet::<Row>(&fs::read(&output).unwrap()).unwrap();
    rows.sort_by_key(|row| row.id);
    assert_eq!(
        rows,
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
    assert!(publish_parquet_fragments::<Row>(&store, 3, &output).is_err());
}

#[test]
fn rejects_corrupt_frames_and_wrong_counts_without_publication() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("result.parquet");
    let broken =
        parquet_fragment_store::<Row>(dir.path().join("broken"), "job", "graph", "sink").unwrap();
    broken.local.stage("a", 1, &100_u64.to_le_bytes()).unwrap();
    broken.local.accept("a").unwrap();
    let broken = broken.require_bundle_attempts(["a"]).unwrap();
    assert!(publish_parquet_fragments::<Row>(&broken, 1, &output).is_err());
    assert!(!output.exists());
    let wrong =
        parquet_fragment_store::<Row>(dir.path().join("wrong"), "job", "graph", "sink").unwrap();
    let mut frame = Vec::new();
    encode_fragment_row(
        &Row {
            id: 1,
            label: "Ada".into(),
        },
        &mut frame,
    )
    .unwrap();
    wrong.local.stage("a", 2, &frame).unwrap();
    wrong.local.accept("a").unwrap();
    let wrong = wrong.require_bundle_attempts(["a"]).unwrap();
    assert!(publish_parquet_fragments::<Row>(&wrong, 2, &output).is_err());
    assert!(!output.exists());
    assert!(parquet_fragment_store::<Row>("relative", "job", "graph", "sink").is_err());
}

#[test]
fn writes_multiple_record_batches_and_empty_result() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        parquet_fragment_store::<Row>(dir.path().join("stage"), "job", "graph", "sink").unwrap();
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
    let output = dir.path().join("many.parquet");
    assert_eq!(
        publish_parquet_fragments::<Row>(&store, 1_030, &output)
            .unwrap()
            .rows,
        1_030
    );
    let decoded = read_parquet::<Row>(&fs::read(&output).unwrap()).unwrap();
    assert_eq!(decoded.len(), 1_030);
    assert_eq!(
        decoded.iter().map(|row| row.id).sum::<i32>(),
        (0..1_030).sum::<i32>()
    );
    let empty = parquet_fragment_store::<Row>(dir.path().join("empty"), "job", "graph", "sink")
        .unwrap()
        .require_bundle_attempts([] as [&str; 0])
        .unwrap();
    let empty_output = dir.path().join("empty.parquet");
    assert_eq!(
        publish_parquet_fragments::<Row>(&empty, 0, &empty_output)
            .unwrap()
            .rows,
        0
    );
    assert_eq!(
        read_parquet::<Row>(&fs::read(empty_output).unwrap()).unwrap(),
        Vec::<Row>::new()
    );
}

#[test]
fn rejects_uncoded_types_and_incomplete_frames_before_publishing() {
    let dir = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec(
            "input",
            vec![Row {
                id: 1,
                label: "x".into(),
            }],
        )
        .unwrap();
    parquet_fragment_sink(&input, "sink", dir.path().join("stage"), "job", "graph").unwrap();
    let uncoded = pipeline
        .from_vec("uncoded", vec![Uncoded { value: 1 }])
        .unwrap();
    assert!(
        parquet_fragment_sink(&uncoded, "bad", dir.path().join("stage"), "job", "graph").is_err()
    );
    assert!(
        parquet_fragment_store::<Uncoded>(dir.path().join("stage"), "job", "graph", "bad").is_err()
    );
    let store =
        parquet_fragment_store::<Row>(dir.path().join("broken"), "job", "graph", "sink").unwrap();
    assert!(store.accepted_fragments().unwrap().is_empty());
    store.local.stage("short", 1, b"abc").unwrap();
    store.local.accept("short").unwrap();
    let store = store.require_bundle_attempts(["short"]).unwrap();
    assert_eq!(store.accepted_fragments().unwrap().len(), 1);
    let output = dir.path().join("out.parquet");
    assert!(publish_parquet_fragments::<Row>(&store, 1, &output).is_err());
    assert!(!output.exists());
}
