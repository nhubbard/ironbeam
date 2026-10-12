// Unit tests for src/lib.rs.
use super::*;
use object_store::memory::InMemory;

#[test]
fn adapter_preserves_conditional_creation_and_prefix_listing() {
    let adapter = RemoteObjectIO::new(|_| Ok(Arc::new(InMemory::new()))).unwrap();
    adapter
        .put_object_if_absent("bucket", "job/result/part-a.accepted", b"first")
        .unwrap();
    let duplicate = adapter
        .put_object_if_absent("bucket", "job/result/part-a.accepted", b"changed")
        .unwrap_err();
    assert_eq!(duplicate.kind, ErrorKind::AlreadyExists);
    assert_eq!(
        adapter
            .get_object("bucket", "job/result/part-a.accepted")
            .unwrap(),
        b"first"
    );
    adapter
        .put_object_if_absent("bucket", "job/result-extra/part-b", b"other")
        .unwrap();
    adapter
        .put_object_if_absent("bucket", "job/result/part-c.accepted", b"third")
        .unwrap();
    let listed = adapter
        .list_objects("bucket", Some("job/result/"))
        .unwrap()
        .into_iter()
        .map(|object| object.key)
        .collect::<Vec<_>>();
    assert_eq!(
        listed,
        ["job/result/part-a.accepted", "job/result/part-c.accepted"]
    );
    assert!(
        !adapter
            .object_exists("another-bucket", "job/result/part-a.accepted")
            .unwrap()
    );
}
