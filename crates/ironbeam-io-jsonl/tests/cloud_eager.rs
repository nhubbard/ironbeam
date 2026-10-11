#![cfg(feature = "cloud")]

use ironbeam_cloud_fakes::FakeObjectIO;
use ironbeam_cloud_objects::{ErrorKind, ObjectIO};
use ironbeam_io_jsonl::{
    expand_cloud_glob, expand_cloud_glob_required, read_cloud_jsonl_glob, read_cloud_jsonl_vec,
    write_cloud_jsonl_vec,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct Row {
    id: u32,
}

#[test]
fn bounded_object_helpers_roundtrip_and_expand_sorted_globs() {
    let storage = FakeObjectIO::new();
    assert_eq!(
        write_cloud_jsonl_vec(&storage, "bucket", "data/b.jsonl", &[Row { id: 2 }]).unwrap(),
        1
    );
    assert_eq!(
        write_cloud_jsonl_vec(&storage, "bucket", "data/a.jsonl", &[Row { id: 1 }]).unwrap(),
        1
    );
    assert_eq!(
        read_cloud_jsonl_vec::<Row, _>(&storage, "bucket", "data/a.jsonl").unwrap(),
        [Row { id: 1 }]
    );
    assert_eq!(
        expand_cloud_glob(&storage, "bucket", "data/*.jsonl").unwrap(),
        ["data/a.jsonl", "data/b.jsonl"]
    );
    assert_eq!(
        read_cloud_jsonl_glob::<Row, _>(&storage, "bucket", "data/*.jsonl").unwrap(),
        [Row { id: 1 }, Row { id: 2 }]
    );
    assert_eq!(
        expand_cloud_glob(&storage, "bucket", "none/*.jsonl").unwrap(),
        Vec::<String>::new()
    );
    assert_eq!(
        expand_cloud_glob_required(&storage, "bucket", "none/*.jsonl")
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
}

#[test]
fn object_decode_errors_include_the_line_and_key() {
    let storage = FakeObjectIO::new();
    storage
        .put_object("bucket", "data/bad.jsonl", b"\n{\"id\":1}\nnot-json\n")
        .unwrap();
    let error = read_cloud_jsonl_vec::<Row, _>(&storage, "bucket", "data/bad.jsonl").unwrap_err();
    assert_eq!(error.kind, ErrorKind::InvalidInput);
    assert!(error.message.contains("data/bad.jsonl line 3"));
}

#[cfg(feature = "compression-gzip")]
#[test]
fn gzip_object_roundtrip_uses_the_format_codec() {
    let storage = FakeObjectIO::new();
    write_cloud_jsonl_vec(&storage, "bucket", "data/rows.jsonl.gz", &[Row { id: 7 }]).unwrap();
    let encoded = storage.get_object("bucket", "data/rows.jsonl.gz").unwrap();
    assert!(encoded.starts_with(&[0x1f, 0x8b]));
    assert_eq!(
        read_cloud_jsonl_vec::<Row, _>(&storage, "bucket", "data/rows.jsonl.gz").unwrap(),
        [Row { id: 7 }]
    );
}

#[cfg(not(feature = "compression-gzip"))]
#[test]
fn disabled_gzip_codec_fails_before_writing_an_object() {
    let storage = FakeObjectIO::new();
    let error = write_cloud_jsonl_vec(&storage, "bucket", "data/rows.jsonl.gz", &[Row { id: 7 }])
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::InvalidInput);
    assert_eq!(
        storage
            .get_object("bucket", "data/rows.jsonl.gz")
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
}
