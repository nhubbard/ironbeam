#![cfg(feature = "cloud")]

use ironbeam_cloud_objects::{LocalObjectIO, ObjectStores};
use ironbeam_core::{Element, ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_io_parquet::{cloud_parquet_uri_source, parquet_sink};
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
    assert!(pipeline.snapshot().unwrap().0.nodes.is_empty());
}
