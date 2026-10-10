#![cfg(feature = "cloud")]
use ironbeam_cloud::{
    FakeObjectIO, ObjectIO,
    readers::{read_cloud_jsonl_vec, write_cloud_jsonl_vec},
};
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
fn v5_cloud_reexport_round_trips_jsonl() {
    let storage = FakeObjectIO::new();
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
    assert_eq!(
        write_cloud_jsonl_vec(&storage, "bucket", "rows.jsonl", &rows).unwrap(),
        2
    );
    assert_eq!(
        read_cloud_jsonl_vec::<Row, _>(&storage, "bucket", "rows.jsonl").unwrap(),
        rows
    );
}

#[test]
fn cloud_jsonl_graph_source_and_sink_bind_object_content() {
    let storage = Arc::new(FakeObjectIO::new());
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
    let storage = Arc::new(FakeObjectIO::new());
    write_cloud_jsonl_vec(
        &*storage,
        "bucket",
        "data/b.jsonl",
        &[Row {
            id: 2,
            name: "Lin".into(),
        }],
    )
    .unwrap();
    write_cloud_jsonl_vec(
        &*storage,
        "bucket",
        "data/a.jsonl",
        &[Row {
            id: 1,
            name: "Ada".into(),
        }],
    )
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
    write_cloud_jsonl_vec(
        &*storage,
        "bucket",
        "data/0.jsonl",
        &[Row {
            id: 0,
            name: "Zero".into(),
        }],
    )
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

#[cfg(feature = "compression-gzip")]
#[test]
fn v5_cloud_reexport_round_trips_compressed_jsonl() {
    let storage = FakeObjectIO::new();
    let rows = vec![Row {
        id: 3,
        name: "Max".into(),
    }];
    assert_eq!(
        write_cloud_jsonl_vec(&storage, "bucket", "rows.jsonl.gz", &rows).unwrap(),
        1
    );
    assert_eq!(
        read_cloud_jsonl_vec::<Row, _>(&storage, "bucket", "rows.jsonl.gz").unwrap(),
        rows
    );
}
