use ironbeam_cloud::{ErrorKind, FakeObjectIO, LocalObjectIO, ObjectLocation, ObjectStores};

#[test]
fn routes_objects_across_schemes() {
    let dir = tempfile::tempdir().unwrap();
    let mut stores = ObjectStores::new();
    stores.register("mem", FakeObjectIO::new()).unwrap();
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
fn rejects_unknown_provider_bad_uri_and_path_escape() {
    let dir = tempfile::tempdir().unwrap();
    let mut stores = ObjectStores::new();
    stores
        .register("file", LocalObjectIO::new(dir.path()).unwrap())
        .unwrap();
    assert_eq!(
        stores
            .register("file", FakeObjectIO::new())
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
