#![cfg(feature = "cloud")]

use ironbeam_cloud_objects::{LocalObjectIO, ObjectStores};
use ironbeam_core::{Element, ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_io_parquet::{
    cloud_parquet_uri_prefix_source, cloud_parquet_uri_source, parquet_sink,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct Row {
    id: i32,
    label: String,
}

impl Element for Row {
    const TYPE_ID: &'static str = "tests/cloud-parquet-row";
    const CODER_ID: &'static str = "tests/cloud-parquet-row-json";
}

#[test]
fn object_uri_source_binds_content_and_rejects_changed_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("seed.parquet");
    let rows = vec![
        Row {
            id: 1,
            label: "Ada".into(),
        },
        Row {
            id: 2,
            label: "Lin".into(),
        },
    ];
    let seed = Pipeline::default();
    let source = seed.from_vec("seed", rows.clone()).unwrap();
    let sink = parquet_sink(&source, "write", &file).unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();

    let mut stores = ObjectStores::new();
    stores
        .register(
            "file",
            LocalObjectIO::new(directory.path().join("objects")).unwrap(),
        )
        .unwrap();
    let uri = "file://bucket/input/rows.parquet";
    stores.write(uri, &std::fs::read(file).unwrap()).unwrap();
    let pipeline = Pipeline::default();
    let output = cloud_parquet_uri_source::<Row>(&pipeline, "read", &stores, uri)
        .unwrap()
        .output("output")
        .unwrap();
    let original_fingerprint = pipeline.snapshot().unwrap().0.fingerprint().unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        rows
    );
    stores.write(uri, b"changed").unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
    let changed = Pipeline::default();
    cloud_parquet_uri_source::<Row>(&changed, "read", &stores, uri).unwrap();
    assert_ne!(
        original_fingerprint,
        changed.snapshot().unwrap().0.fingerprint().unwrap()
    );
    std::fs::remove_file(directory.path().join("objects/bucket/input/rows.parquet")).unwrap();
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
fn object_uri_source_rejects_invalid_or_unbound_locations() {
    let pipeline = Pipeline::default();
    let stores = ObjectStores::new();
    assert!(cloud_parquet_uri_source::<Row>(&pipeline, "invalid", &stores, "not-a-uri").is_err());
    assert!(
        cloud_parquet_uri_source::<Row>(&pipeline, "missing", &stores, "gs://bucket/x.parquet")
            .is_err()
    );
    assert_eq!(pipeline.snapshot().unwrap().0.nodes.len(), 0);
    let directory = tempfile::tempdir().unwrap();
    let mut stores = ObjectStores::new();
    stores
        .register("file", LocalObjectIO::new(directory.path()).unwrap())
        .unwrap();
    assert!(
        cloud_parquet_uri_source::<Row>(
            &pipeline,
            "missing-object",
            &stores,
            "file://bucket/missing.parquet"
        )
        .is_err()
    );
}

#[test]
fn prefix_source_binds_sorted_object_set_and_each_objects_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let mut stores = ObjectStores::new();
    stores
        .register(
            "file",
            LocalObjectIO::new(directory.path().join("objects")).unwrap(),
        )
        .unwrap();
    let prefix = "file://bucket/input/";
    let empty = Pipeline::default();
    assert!(cloud_parquet_uri_prefix_source::<Row>(&empty, "read", &stores, prefix).is_err());
    assert_eq!(empty.snapshot().unwrap().0.nodes.len(), 0);

    let write = |id, name: &str| {
        let path = directory.path().join(format!("{name}.parquet"));
        let pipeline = Pipeline::default();
        let row = Row {
            id,
            label: name.into(),
        };
        let source = pipeline.from_vec("seed", vec![row]).unwrap();
        let sink = parquet_sink(&source, "write", &path).unwrap();
        DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
        std::fs::read(path).unwrap()
    };
    let second = write(2, "b");
    let first = write(1, "a");
    stores
        .write("file://bucket/input/a.parquet", &first)
        .unwrap();
    let singleton = Pipeline::default();
    let one = cloud_parquet_uri_prefix_source::<Row>(&singleton, "read", &stores, prefix)
        .unwrap()
        .output("output")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&one, &JobConfig::default())
            .unwrap()
            .0
            .len(),
        1
    );
    stores
        .write("file://bucket/input/b.parquet", &second)
        .unwrap();
    let pipeline = Pipeline::default();
    let collection =
        cloud_parquet_uri_prefix_source::<Row>(&pipeline, "read", &stores, prefix).unwrap();
    let output = collection.output("output").unwrap();
    let fingerprint = pipeline.snapshot().unwrap().0.fingerprint().unwrap();
    let mut actual = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap()
        .0;
    actual.sort_by_key(|row| row.id);
    assert_eq!(
        actual,
        vec![
            Row {
                id: 1,
                label: "a".into()
            },
            Row {
                id: 2,
                label: "b".into()
            }
        ]
    );
    stores
        .write("file://bucket/input/c.parquet", &write(3, "c"))
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0
            .len(),
        2
    );
    let expanded = Pipeline::default();
    cloud_parquet_uri_prefix_source::<Row>(&expanded, "read", &stores, prefix).unwrap();
    assert_ne!(
        fingerprint,
        expanded.snapshot().unwrap().0.fingerprint().unwrap()
    );
    stores
        .write("file://bucket/input/b.parquet", b"changed")
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
