#![cfg(feature = "cloud")]

use ironbeam_cloud_objects::{LocalObjectIO, ObjectStores};
use ironbeam_core::{Element, ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_io_xml::{cloud_xml_uri_prefix_source, cloud_xml_uri_source};

#[derive(Clone, Debug, PartialEq, serde::Deserialize)]
struct Row {
    id: i32,
    name: String,
}

impl Element for Row {
    const TYPE_ID: &'static str = "xml-cloud-test/row";
    const CODER_ID: &'static str = "xml-cloud-test/row-v1";
}

fn stores(root: &std::path::Path) -> ObjectStores {
    let mut stores = ObjectStores::new();
    stores
        .register("file", LocalObjectIO::new(root).unwrap())
        .unwrap();
    stores
}

#[test]
fn xml_object_uri_and_prefix_read_sorted_documents() {
    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("bucket/input");
    std::fs::create_dir_all(&input).unwrap();
    std::fs::write(
        input.join("a.xml"),
        "<records><item><id>1</id><name>Ada</name></item></records>",
    )
    .unwrap();
    std::fs::write(
        input.join("b.xml"),
        "<records><item><id>2</id><name>Lin</name></item></records>",
    )
    .unwrap();
    let stores = stores(directory.path());
    let single = cloud_xml_uri_source::<Row>(
        &Pipeline::default(),
        "xml-object",
        &stores,
        "file://bucket/input/a.xml",
    )
    .unwrap()
    .output("single-out")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&single, &JobConfig::default())
            .unwrap()
            .0,
        vec![Row {
            id: 1,
            name: "Ada".into()
        }]
    );

    let prefix = cloud_xml_uri_prefix_source::<Row>(
        &Pipeline::default(),
        "xml-prefix",
        &stores,
        "file://bucket/input/",
    )
    .unwrap()
    .output("prefix-out")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&prefix, &JobConfig::default())
            .unwrap()
            .0,
        vec![
            Row {
                id: 1,
                name: "Ada".into()
            },
            Row {
                id: 2,
                name: "Lin".into()
            },
        ]
    );
}

#[test]
fn xml_object_rejects_changed_content_on_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("bucket/input.xml");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        &path,
        "<records><item><id>1</id><name>Ada</name></item></records>",
    )
    .unwrap();
    let stores = stores(directory.path());
    let output = cloud_xml_uri_source::<Row>(
        &Pipeline::default(),
        "xml-object",
        &stores,
        "file://bucket/input.xml",
    )
    .unwrap()
    .output("out")
    .unwrap();
    std::fs::write(
        &path,
        "<records><item><id>2</id><name>Lin</name></item></records>",
    )
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .expect("changed XML object must fail")
            .kind,
        ErrorKind::Recovery
    );
}

#[test]
fn xml_object_rejects_invalid_uri_and_empty_prefix() {
    let directory = tempfile::tempdir().unwrap();
    let stores = stores(directory.path());
    assert!(
        cloud_xml_uri_source::<Row>(&Pipeline::default(), "invalid", &stores, "not-a-uri",)
            .is_err()
    );
    assert!(
        cloud_xml_uri_source::<Row>(
            &Pipeline::default(),
            "unbound",
            &stores,
            "gs://bucket/input.xml",
        )
        .is_err()
    );
    assert!(
        cloud_xml_uri_prefix_source::<Row>(
            &Pipeline::default(),
            "empty",
            &stores,
            "file://bucket/empty/",
        )
        .is_err()
    );
}
