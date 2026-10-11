#![cfg(feature = "cloud")]
use ironbeam_cloud_objects::{LocalObjectIO, ObjectIO, ObjectStores};
use ironbeam_core::{Element, ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_io_jsonl::{cloud_jsonl_glob_source, cloud_jsonl_sink, cloud_jsonl_source};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
struct Row {
    id: u64,
    name: String,
}
impl Element for Row {
    const TYPE_ID: &'static str = "tests/cloud-row";
    const CODER_ID: &'static str = "tests/cloud-row-local";
}

#[test]
fn registered_local_provider_runs_jsonl_graph() {
    let dir = tempfile::tempdir().unwrap();
    let mut objects = ObjectStores::new();
    objects
        .register("file", LocalObjectIO::new(dir.path()).unwrap())
        .unwrap();
    let storage = objects.provider("file").unwrap();
    let rows = vec![Row {
        id: 3,
        name: "Local".into(),
    }];
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", rows.clone()).unwrap();
    let sink = cloud_jsonl_sink(
        &input,
        "write",
        Arc::clone(&storage),
        "bucket",
        "rows.jsonl",
    )
    .unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    assert_eq!(
        objects.read("file://bucket/rows.jsonl").unwrap(),
        b"{\"id\":3,\"name\":\"Local\"}\n"
    );
    let reader = Pipeline::default();
    let output = cloud_jsonl_source::<Row, _>(&reader, "read", storage, "bucket", "rows.jsonl")
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        rows
    );
}

#[test]
fn jsonl_object_sink_writes_expected_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(LocalObjectIO::new(dir.path()).unwrap());
    let rows = vec![
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
    let input = pipeline.from_vec("input", rows).unwrap();
    let sink = cloud_jsonl_sink(
        &input,
        "write",
        Arc::clone(&storage),
        "bucket",
        "rows.jsonl",
    )
    .unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    assert_eq!(
        storage.get_object("bucket", "rows.jsonl").unwrap(),
        b"{\"id\":1,\"name\":\"Ada\"}\n{\"id\":2,\"name\":\"Lin\"}\n"
    );
}

#[test]
fn cloud_jsonl_graph_source_and_sink_bind_object_content() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(LocalObjectIO::new(dir.path()).unwrap());
    let rows = vec![
        Row {
            id: 5,
            name: "Ada".into(),
        },
        Row {
            id: 6,
            name: "Lin".into(),
        },
    ];
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", rows.clone()).unwrap();
    let sink = cloud_jsonl_sink(
        &input,
        "write",
        Arc::clone(&storage),
        "bucket",
        "rows.jsonl",
    )
    .unwrap();
    assert!(!storage.object_exists("bucket", "rows.jsonl").unwrap());
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    let reader = Pipeline::default();
    let output = cloud_jsonl_source::<Row, _>(
        &reader,
        "read",
        Arc::clone(&storage),
        "bucket",
        "rows.jsonl",
    )
    .unwrap()
    .output("out")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        rows
    );
    storage
        .put_object("bucket", "rows.jsonl", b"{\"id\":7,\"name\":\"Changed\"}\n")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
}

#[test]
fn cloud_jsonl_globs_preserve_sorted_objects_and_stable_keys() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(LocalObjectIO::new(dir.path()).unwrap());
    storage
        .put_object("bucket", "data/b.jsonl", b"{\"id\":2,\"name\":\"Lin\"}\n")
        .unwrap();
    storage
        .put_object("bucket", "data/a.jsonl", b"{\"id\":1,\"name\":\"Ada\"}\n")
        .unwrap();
    let pipeline = Pipeline::default();
    let output = cloud_jsonl_glob_source::<Row, _>(
        &pipeline,
        "glob",
        Arc::clone(&storage),
        "bucket",
        "data/*.jsonl",
    )
    .unwrap()
    .output("out")
    .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let original_keys = graph
        .nodes
        .iter()
        .filter(|node| node.key.starts_with("glob/object-"))
        .map(|node| node.key.clone())
        .collect::<Vec<_>>();
    assert_eq!(original_keys.len(), 2);
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        [
            Row {
                id: 1,
                name: "Ada".into()
            },
            Row {
                id: 2,
                name: "Lin".into()
            }
        ]
    );
    storage
        .put_object("bucket", "data/0.jsonl", b"{\"id\":0,\"name\":\"Zero\"}\n")
        .unwrap();
    let revised = Pipeline::default();
    cloud_jsonl_glob_source::<Row, _>(
        &revised,
        "glob",
        Arc::clone(&storage),
        "bucket",
        "data/*.jsonl",
    )
    .unwrap();
    let (graph, _) = revised.snapshot().unwrap();
    assert!(original_keys.iter().all(|key| graph.node(key).is_some()));
    let one = Pipeline::default();
    let output = cloud_jsonl_glob_source::<Row, _>(
        &one,
        "one",
        Arc::clone(&storage),
        "bucket",
        "data/a.jsonl",
    )
    .unwrap()
    .output("one-out")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        [Row {
            id: 1,
            name: "Ada".into()
        }]
    );
    storage
        .put_object(
            "bucket",
            "data/nested/c.jsonl",
            b"{\"id\":3,\"name\":\"Nested\"}\n",
        )
        .unwrap();
    let deep = Pipeline::default();
    let nested = cloud_jsonl_glob_source::<Row, _>(
        &deep,
        "deep",
        Arc::clone(&storage),
        "bucket",
        "data/**/*.jsonl",
    )
    .unwrap()
    .output("deep-out")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&nested, &JobConfig::default())
            .unwrap()
            .0,
        [Row {
            id: 3,
            name: "Nested".into()
        }]
    );
    let single = Pipeline::default();
    let one_character = cloud_jsonl_glob_source::<Row, _>(
        &single,
        "single",
        Arc::clone(&storage),
        "bucket",
        "data/?.jsonl",
    )
    .unwrap()
    .output("single-out")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&one_character, &JobConfig::default())
            .unwrap()
            .0
            .into_iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        [0, 1, 2]
    );
    storage
        .put_object(
            "bucket",
            "data/a[1].jsonl",
            b"{\"id\":4,\"name\":\"Bracket\"}\n",
        )
        .unwrap();
    let literal = Pipeline::default();
    let bracket = cloud_jsonl_glob_source::<Row, _>(
        &literal,
        "bracket",
        Arc::clone(&storage),
        "bucket",
        "data/a[1].jsonl",
    )
    .unwrap()
    .output("bracket-out")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&bracket, &JobConfig::default())
            .unwrap()
            .0,
        [Row {
            id: 4,
            name: "Bracket".into()
        }]
    );
    assert!(
        cloud_jsonl_glob_source::<Row, _>(
            &one,
            "missing",
            Arc::clone(&storage),
            "bucket",
            "missing/*.jsonl"
        )
        .is_err()
    );
}

#[cfg(any(
    feature = "compression-gzip",
    feature = "compression-zstd",
    feature = "compression-bzip2",
    feature = "compression-xz"
))]
fn compressed_object_round_trip(extension: &str, magic: &[u8]) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(LocalObjectIO::new(dir.path()).unwrap());
    let object = format!("rows.jsonl.{extension}");
    let rows = vec![Row {
        id: 3,
        name: "Max".into(),
    }];
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", rows.clone()).unwrap();
    let sink = cloud_jsonl_sink(&input, "write", Arc::clone(&storage), "bucket", &object).unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    assert!(
        storage
            .get_object("bucket", &object)
            .unwrap()
            .starts_with(magic)
    );
    let reader = Pipeline::default();
    let output = cloud_jsonl_source::<Row, _>(&reader, "read", storage, "bucket", &object)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        rows
    );
}

#[cfg(feature = "compression-gzip")]
#[test]
fn cloud_jsonl_gzip_object_round_trip() {
    compressed_object_round_trip("gz", &[0x1f, 0x8b]);
}
#[cfg(not(feature = "compression-gzip"))]
#[test]
fn cloud_jsonl_gzip_sink_fails_without_codec_before_writing() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(LocalObjectIO::new(dir.path()).unwrap());
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec(
            "input",
            vec![Row {
                id: 1,
                name: "A".into(),
            }],
        )
        .unwrap();
    let sink = cloud_jsonl_sink(
        &input,
        "write",
        Arc::clone(&storage),
        "bucket",
        "rows.jsonl.gz",
    )
    .unwrap();
    let error = DirectRunner
        .run_sink(&sink, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Io);
    assert!(!storage.object_exists("bucket", "rows.jsonl.gz").unwrap());
}
#[cfg(feature = "compression-zstd")]
#[test]
fn cloud_jsonl_zstd_object_round_trip() {
    compressed_object_round_trip("zst", &[0x28, 0xb5, 0x2f, 0xfd]);
}
#[cfg(feature = "compression-bzip2")]
#[test]
fn cloud_jsonl_bzip2_object_round_trip() {
    compressed_object_round_trip("bz2", b"BZ");
}
#[cfg(feature = "compression-xz")]
#[test]
fn cloud_jsonl_xz_object_round_trip() {
    compressed_object_round_trip("xz", &[0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00]);
}
