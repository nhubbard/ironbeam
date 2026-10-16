#![cfg(feature = "cloud")]

use ironbeam_cloud_objects::{LocalObjectIO, ObjectStores};
use ironbeam_core::{Element, ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_io_msgpack::{
    cloud_msgpack_streaming_uri_prefix_source, cloud_msgpack_streaming_uri_source,
};

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
struct Row {
    id: i32,
    name: String,
}

impl Element for Row {
    const TYPE_ID: &'static str = "msgpack-cloud-test/row";
    const CODER_ID: &'static str = "msgpack-cloud-test/row-v1";
}

fn stores(root: &std::path::Path) -> ObjectStores {
    let mut stores = ObjectStores::new();
    stores
        .register("file", LocalObjectIO::new(root).unwrap())
        .unwrap();
    stores
}

fn write_rows(path: &std::path::Path, rows: &[Row]) {
    let mut bytes = Vec::new();
    for row in rows {
        rmp_serde::encode::write(&mut bytes, row).unwrap();
    }
    std::fs::write(path, bytes).unwrap();
}

#[test]
fn object_uri_and_prefix_decode_sorted_sequences() {
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
    write_rows(&input.join("a.msgpack"), &[first.clone(), second.clone()]);
    write_rows(&input.join("b.msgpack"), std::slice::from_ref(&third));
    let stores = stores(directory.path());
    let single = cloud_msgpack_streaming_uri_source::<Row>(
        &Pipeline::default(),
        "object",
        &stores,
        "file://bucket/input/a.msgpack",
    )
    .unwrap()
    .output("out")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&single, &JobConfig::default())
            .unwrap()
            .0,
        vec![first.clone(), second.clone()]
    );
    let prefix = cloud_msgpack_streaming_uri_prefix_source::<Row>(
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
            .run_collect(&prefix, &JobConfig::default())
            .unwrap()
            .0,
        vec![first, second, third]
    );
}

#[test]
fn object_change_is_rejected_before_decoding() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("bucket/a.msgpack");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    write_rows(
        &path,
        &[Row {
            id: 1,
            name: "Ada".into(),
        }],
    );
    let output = cloud_msgpack_streaming_uri_source::<Row>(
        &Pipeline::default(),
        "object",
        &stores(directory.path()),
        "file://bucket/a.msgpack",
    )
    .unwrap()
    .output("out")
    .unwrap();
    write_rows(
        &path,
        &[Row {
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
        cloud_msgpack_streaming_uri_source::<Row>(
            &Pipeline::default(),
            "invalid",
            &stores,
            "not-a-uri"
        )
        .is_err()
    );
    assert!(
        cloud_msgpack_streaming_uri_source::<Row>(
            &Pipeline::default(),
            "unbound",
            &stores,
            "gs://bucket/a.msgpack"
        )
        .is_err()
    );
    assert!(
        cloud_msgpack_streaming_uri_prefix_source::<Row>(
            &Pipeline::default(),
            "empty",
            &stores,
            "file://bucket/empty/"
        )
        .is_err()
    );
}
