#![cfg(feature = "cloud")]

use ironbeam_cloud_objects::{LocalObjectIO, ObjectStores};
use ironbeam_core::{Element, ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_io_arrow::{
    arrow_ipc_sink, cloud_arrow_ipc_batches_streaming_uri_prefix_source,
    cloud_arrow_ipc_batches_streaming_uri_source, cloud_arrow_ipc_streaming_uri_prefix_source,
    cloud_arrow_ipc_streaming_uri_source,
};

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Row {
    id: i32,
    name: String,
}

impl Element for Row {
    const TYPE_ID: &'static str = "arrow-cloud-test/row";
    const CODER_ID: &'static str = "arrow-cloud-test/row-v1";
}

fn stores(root: &std::path::Path) -> ObjectStores {
    let mut stores = ObjectStores::new();
    stores
        .register("file", LocalObjectIO::new(root).unwrap())
        .unwrap();
    stores
}

fn write_rows(path: &std::path::Path, rows: Vec<Row>) {
    let pipeline = Pipeline::default();
    let values = pipeline.from_vec("seed", rows).unwrap();
    let sink = arrow_ipc_sink(&values, "write", path).unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
}

#[test]
fn object_rows_and_batches_read_sorted_prefixes() {
    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("bucket/input");
    std::fs::create_dir_all(&input).unwrap();
    let first = Row {
        id: 1,
        name: "Ada".into(),
    };
    let second = Row {
        id: 2,
        name: "Lin".into(),
    };
    let third = Row {
        id: 3,
        name: "Kay".into(),
    };
    write_rows(&input.join("a.arrow"), vec![first.clone(), second.clone()]);
    write_rows(&input.join("b.arrow"), vec![third.clone()]);
    let stores = stores(directory.path());
    let source = cloud_arrow_ipc_streaming_uri_source::<Row>(
        &Pipeline::default(),
        "single",
        &stores,
        "file://bucket/input/a.arrow",
    )
    .unwrap()
    .output("out")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&source, &JobConfig::default())
            .unwrap()
            .0,
        [first.clone(), second.clone()]
    );
    let source = cloud_arrow_ipc_streaming_uri_prefix_source::<Row>(
        &Pipeline::default(),
        "prefix",
        &stores,
        "file://bucket/input/",
    )
    .unwrap()
    .output("out")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&source, &JobConfig::default())
            .unwrap()
            .0,
        [first, second, third]
    );

    let source = cloud_arrow_ipc_batches_streaming_uri_source(
        &Pipeline::default(),
        "batch",
        &stores,
        "file://bucket/input/a.arrow",
    )
    .unwrap()
    .output("out")
    .unwrap();
    let batches = DirectRunner
        .run_collect(&source, &JobConfig::default())
        .unwrap()
        .0;
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.0.num_rows())
            .collect::<Vec<_>>(),
        [2]
    );
    let source = cloud_arrow_ipc_batches_streaming_uri_prefix_source(
        &Pipeline::default(),
        "batches",
        &stores,
        "file://bucket/input/",
    )
    .unwrap()
    .output("out")
    .unwrap();
    let batches = DirectRunner
        .run_collect(&source, &JobConfig::default())
        .unwrap()
        .0;
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.0.num_rows())
            .collect::<Vec<_>>(),
        [2, 1]
    );
}

#[test]
fn object_change_is_rejected_before_ipc_reader_opens() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("bucket/a.arrow");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    write_rows(
        &path,
        vec![Row {
            id: 1,
            name: "Ada".into(),
        }],
    );
    let output = cloud_arrow_ipc_streaming_uri_source::<Row>(
        &Pipeline::default(),
        "object",
        &stores(directory.path()),
        "file://bucket/a.arrow",
    )
    .unwrap()
    .output("out")
    .unwrap();
    write_rows(
        &path,
        vec![Row {
            id: 2,
            name: "Lin".into(),
        }],
    );
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .expect("changed object must fail")
            .kind,
        ErrorKind::Recovery
    );
}

#[test]
fn invalid_uri_unbound_provider_and_empty_prefix_fail() {
    let directory = tempfile::tempdir().unwrap();
    let stores = stores(directory.path());
    assert!(
        cloud_arrow_ipc_streaming_uri_source::<Row>(
            &Pipeline::default(),
            "invalid",
            &stores,
            "not-a-uri"
        )
        .is_err()
    );
    assert!(
        cloud_arrow_ipc_streaming_uri_source::<Row>(
            &Pipeline::default(),
            "unbound",
            &stores,
            "gs://bucket/a.arrow"
        )
        .is_err()
    );
    assert!(
        cloud_arrow_ipc_streaming_uri_prefix_source::<Row>(
            &Pipeline::default(),
            "empty",
            &stores,
            "file://bucket/empty/"
        )
        .is_err()
    );
}
