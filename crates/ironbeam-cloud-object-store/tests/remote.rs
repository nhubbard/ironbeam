use ironbeam_cloud::{ErrorKind, ObjectIO, ObjectStores};
use ironbeam_cloud_object_store::RemoteObjectIO;
use object_store::{ObjectStore, memory::InMemory};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[test]
fn routes_and_caches_bucket_clients_with_real_object_store_operations() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let adapter = RemoteObjectIO::new(move |_| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(InMemory::new()) as Arc<dyn ObjectStore>)
    })
    .unwrap();
    let mut objects = ObjectStores::new();
    objects.register("remote", adapter).unwrap();

    objects.write("remote://one/data/a.jsonl", b"a").unwrap();
    objects.write("remote://one/data/ab.jsonl", b"ab").unwrap();
    assert_eq!(objects.read("remote://one/data/a.jsonl").unwrap(), b"a");
    assert_eq!(objects.list("remote://one/data/a").unwrap().len(), 2);
    objects
        .copy("remote://one/data/a.jsonl", "remote://two/copied.jsonl")
        .unwrap();
    assert_eq!(objects.read("remote://two/copied.jsonl").unwrap(), b"a");
    assert_eq!(
        objects.metadata("remote://two/copied.jsonl").unwrap().size,
        1
    );
    objects.delete("remote://one/data/a.jsonl").unwrap();
    assert!(!objects.exists("remote://one/data/a.jsonl").unwrap());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn missing_object_and_invalid_path_keep_meaningful_errors() {
    let adapter =
        RemoteObjectIO::new(|_| Ok(Arc::new(InMemory::new()) as Arc<dyn ObjectStore>)).unwrap();
    assert_eq!(
        adapter.get_object("bucket", "missing").unwrap_err().kind,
        ErrorKind::NotFound
    );
    assert_eq!(
        adapter
            .put_object("bucket", "../escape", b"x")
            .unwrap_err()
            .kind,
        ErrorKind::InvalidInput
    );
}
