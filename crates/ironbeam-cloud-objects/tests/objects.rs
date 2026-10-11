use ironbeam_cloud_objects::{
    CloudIOError, ErrorKind, LocalObjectIO, ObjectIO, ObjectLocation, ObjectStores,
};
use std::sync::Arc;

#[test]
fn arc_provider_delegates_every_object_operation() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(LocalObjectIO::new(dir.path()).unwrap());
    provider.put_object("source", "nested/a", b"a").unwrap();
    assert_eq!(provider.get_object("source", "nested/a").unwrap(), b"a");
    assert!(provider.object_exists("source", "nested/a").unwrap());
    assert_eq!(provider.get_metadata("source", "nested/a").unwrap().size, 1);
    assert_eq!(
        provider
            .list_objects("source", Some("nested/"))
            .unwrap()
            .len(),
        1
    );
    provider
        .copy_object("source", "nested/a", "target", "copied")
        .unwrap();
    assert_eq!(provider.get_object("target", "copied").unwrap(), b"a");
    provider.delete_object("target", "copied").unwrap();
    assert!(!provider.object_exists("target", "copied").unwrap());
}

#[test]
fn invalid_locations_and_error_context_remain_explicit() {
    for uri in [
        "missing-scheme",
        "://bucket/key",
        "S3://bucket/key",
        "s3:///key",
        "s3://../key",
        "s3://bucket?query/key",
    ] {
        assert_eq!(
            ObjectLocation::parse(uri).unwrap_err().kind,
            ErrorKind::InvalidInput
        );
    }
    let error = CloudIOError::new(ErrorKind::NotFound, "missing").with_source("provider");
    assert_eq!(error.to_string(), "NotFound: missing");
    assert_eq!(error.source.as_deref(), Some("provider"));
    let stores = ObjectStores::new();
    assert_eq!(
        stores.provider("s3").err().unwrap().kind,
        ErrorKind::InvalidInput
    );
}

#[test]
fn local_object_errors_distinguish_absence_from_invalid_paths() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalObjectIO::new(dir.path()).unwrap();
    assert_eq!(
        store.list_objects("missing", None).unwrap_err().kind,
        ErrorKind::NotFound
    );
    assert!(!store.object_exists("missing", "key").unwrap());
    assert_eq!(
        store.get_object("missing", "key").unwrap_err().kind,
        ErrorKind::NotFound
    );
    assert_eq!(
        store.put_object("bad/name", "key", b"x").unwrap_err().kind,
        ErrorKind::InvalidInput
    );
    assert_eq!(
        store.object_exists("bucket", "../escape").unwrap_err().kind,
        ErrorKind::InvalidInput
    );
    store.put_object("bucket", "regular", b"x").unwrap();
    std::fs::create_dir_all(dir.path().join("bucket/directory")).unwrap();
    assert_eq!(
        store.get_metadata("bucket", "directory").unwrap_err().kind,
        ErrorKind::InvalidInput
    );
}

#[test]
fn routes_objects_across_schemes() {
    let dir = tempfile::tempdir().unwrap();
    let source_dir = tempfile::tempdir().unwrap();
    let mut stores = ObjectStores::new();
    stores
        .register("mem", LocalObjectIO::new(source_dir.path()).unwrap())
        .unwrap();
    stores
        .register("file", LocalObjectIO::new(dir.path()).unwrap())
        .unwrap();

    stores
        .write("mem://source/a/b.jsonl", b"{\"id\":1}\n")
        .unwrap();
    assert_eq!(
        stores.read("mem://source/a/b.jsonl").unwrap(),
        b"{\"id\":1}\n"
    );
    stores
        .copy("mem://source/a/b.jsonl", "file://output/copy.jsonl")
        .unwrap();
    assert!(stores.exists("file://output/copy.jsonl").unwrap());
    assert_eq!(stores.metadata("file://output/copy.jsonl").unwrap().size, 9);
    assert_eq!(
        stores.list("file://output/copy").unwrap()[0].0.uri(),
        "file://output/copy.jsonl"
    );
    stores.delete("file://output/copy.jsonl").unwrap();
    assert!(!stores.exists("file://output/copy.jsonl").unwrap());
}

#[test]
fn copies_prefixes_across_providers_and_preserves_relative_keys() {
    let dir = tempfile::tempdir().unwrap();
    let source_dir = tempfile::tempdir().unwrap();
    let mut stores = ObjectStores::new();
    stores
        .register("mem", LocalObjectIO::new(source_dir.path()).unwrap())
        .unwrap();
    stores
        .register("file", LocalObjectIO::new(dir.path()).unwrap())
        .unwrap();
    stores.write("mem://source/jobs/a", b"a").unwrap();
    stores.write("mem://source/jobs/nested/b", b"b").unwrap();
    stores.write("mem://source/jobs-other/c", b"c").unwrap();
    assert_eq!(
        stores
            .copy_prefix("mem://source/jobs/", "file://archive/run/")
            .unwrap(),
        2
    );
    assert_eq!(stores.read("file://archive/run/a").unwrap(), b"a");
    assert_eq!(stores.read("file://archive/run/nested/b").unwrap(), b"b");
    assert!(!stores.exists("file://archive/run/-other/c").unwrap());
    assert_eq!(
        stores
            .copy_prefix("mem://source/none/", "file://archive/empty/")
            .unwrap(),
        0
    );
    assert_eq!(
        stores
            .copy_prefix("mem://source/jobs/", "mem://source/jobs/backup/")
            .unwrap(),
        2
    );
    assert_eq!(stores.read("mem://source/jobs/backup/a").unwrap(), b"a");
}

#[test]
fn rejects_unknown_provider_bad_uri_and_path_escape() {
    let dir = tempfile::tempdir().unwrap();
    let mut stores = ObjectStores::new();
    stores
        .register("file", LocalObjectIO::new(dir.path()).unwrap())
        .unwrap();
    assert_eq!(
        stores
            .register("file", LocalObjectIO::new(dir.path()).unwrap())
            .unwrap_err()
            .kind,
        ErrorKind::AlreadyExists
    );
    assert_eq!(
        stores.read("gs://bucket/key").unwrap_err().kind,
        ErrorKind::InvalidInput
    );
    assert_eq!(
        ObjectLocation::parse("bad uri").unwrap_err().kind,
        ErrorKind::InvalidInput
    );
    assert_eq!(
        stores
            .write("file://bucket/../escape", b"x")
            .unwrap_err()
            .kind,
        ErrorKind::InvalidInput
    );
    assert_eq!(
        stores
            .write("file://bucket/absolute\\path", b"x")
            .unwrap_err()
            .kind,
        ErrorKind::InvalidInput
    );
}

#[cfg(unix)]
#[test]
fn local_store_rejects_symlink_escape() {
    use std::os::unix::fs::symlink;
    let dir = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let store = LocalObjectIO::new(dir.path()).unwrap();
    std::fs::create_dir(dir.path().join("bucket")).unwrap();
    symlink(outside.path(), dir.path().join("bucket/escape")).unwrap();
    let mut stores = ObjectStores::new();
    stores.register("file", store).unwrap();
    assert_eq!(
        stores
            .write("file://bucket/escape/object", b"x")
            .unwrap_err()
            .kind,
        ErrorKind::InvalidInput
    );
    assert!(!outside.path().join("object").exists());
}
