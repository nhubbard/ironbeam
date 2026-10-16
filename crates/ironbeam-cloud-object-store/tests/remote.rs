use ironbeam_cloud_object_store::{RemoteObjectIO, object_stores_from_env};
use ironbeam_cloud_objects::{ErrorKind, ObjectIO, ObjectStores};
use object_store::{ObjectStore, chunked::ChunkedStore, memory::InMemory};
use std::io::Read;
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
    objects
        .create("remote://one/data/immutable", b"first")
        .unwrap();
    assert_eq!(
        objects
            .create("remote://one/data/immutable", b"second")
            .unwrap_err()
            .kind,
        ErrorKind::AlreadyExists,
    );
    assert_eq!(
        objects.read("remote://one/data/immutable").unwrap(),
        b"first"
    );
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

#[test]
fn sequential_remote_read_survives_provider_drop_and_short_reads() {
    let adapter = RemoteObjectIO::new(|_| {
        let memory = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        Ok(Arc::new(ChunkedStore::new(memory, 1024)) as Arc<dyn ObjectStore>)
    })
    .unwrap();
    let data = (0..200_000)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect::<Vec<_>>();
    adapter.put_object("bucket", "records.bin", &data).unwrap();
    assert_eq!(
        adapter.open_object("bucket", "missing").err().unwrap().kind,
        ErrorKind::NotFound
    );
    let mut reader = adapter.open_object("bucket", "records.bin").unwrap();
    drop(adapter);
    let mut received = Vec::new();
    let mut buffer = [0_u8; 37];
    loop {
        let count = reader.read(&mut buffer).unwrap();
        if count == 0 {
            break;
        }
        received.extend_from_slice(&buffer[..count]);
    }
    assert_eq!(received, data);
    assert_eq!(reader.read(&mut buffer).unwrap(), 0);
}

#[test]
fn streaming_conditional_create_bounds_upload_and_preserves_existing_object() {
    let memory = Arc::new(InMemory::new());
    let store = Arc::clone(&memory);
    let adapter =
        RemoteObjectIO::new(move |_| Ok(Arc::clone(&store) as Arc<dyn ObjectStore>)).unwrap();
    let bytes = vec![0x5a; 6 * 1024 * 1024 + 17];
    adapter
        .put_object_if_absent_from("bucket", "results/final.bin", &mut bytes.as_slice())
        .unwrap();
    assert_eq!(
        adapter.get_object("bucket", "results/final.bin").unwrap(),
        bytes
    );
    adapter
        .put_object_if_absent_from("bucket", "results/empty.bin", &mut b"".as_slice())
        .unwrap();
    assert_eq!(
        adapter.get_object("bucket", "results/empty.bin").unwrap(),
        Vec::<u8>::new()
    );
    assert_eq!(
        adapter
            .put_object_if_absent_from("bucket", "results/final.bin", &mut b"changed".as_slice())
            .unwrap_err()
            .kind,
        ErrorKind::AlreadyExists,
    );
    assert_eq!(
        adapter.get_object("bucket", "results/final.bin").unwrap(),
        bytes
    );
    assert!(
        adapter
            .list_objects("bucket", Some("__ironbeam_uploads/"))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn streaming_conditional_create_aborts_after_reader_failure() {
    struct FailingReader(usize);
    impl Read for FailingReader {
        fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
            if self.0 == 0 {
                return Err(std::io::Error::other("injected reader failure"));
            }
            let count = output.len().min(self.0);
            output[..count].fill(7);
            self.0 -= count;
            Ok(count)
        }
    }
    let memory = Arc::new(InMemory::new());
    let store = Arc::clone(&memory);
    let adapter =
        RemoteObjectIO::new(move |_| Ok(Arc::clone(&store) as Arc<dyn ObjectStore>)).unwrap();
    let error = adapter
        .put_object_if_absent_from(
            "bucket",
            "results/incomplete.bin",
            &mut FailingReader(6 * 1024 * 1024),
        )
        .unwrap_err();
    assert!(error.message.contains("injected reader failure"));
    assert!(
        !adapter
            .object_exists("bucket", "results/incomplete.bin")
            .unwrap()
    );
    assert!(
        adapter
            .list_objects("bucket", Some("__ironbeam_uploads/"))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn concurrent_streaming_creates_publish_one_complete_object() {
    let memory = Arc::new(InMemory::new());
    let store = Arc::clone(&memory);
    let adapter = Arc::new(
        RemoteObjectIO::new(move |_| Ok(Arc::clone(&store) as Arc<dyn ObjectStore>)).unwrap(),
    );
    let gate = Arc::new(std::sync::Barrier::new(2));
    let workers = [0x11_u8, 0x22_u8].map(|byte| {
        let adapter = Arc::clone(&adapter);
        let gate = Arc::clone(&gate);
        std::thread::spawn(move || {
            let bytes = vec![byte; 6 * 1024 * 1024];
            gate.wait();
            adapter.put_object_if_absent_from("bucket", "results/raced.bin", &mut bytes.as_slice())
        })
    });
    let outcomes = workers.map(|worker| worker.join().unwrap());
    assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| outcome
                .as_ref()
                .is_err_and(|error| error.kind == ErrorKind::AlreadyExists))
            .count(),
        1
    );
    let published = adapter.get_object("bucket", "results/raced.bin").unwrap();
    assert_eq!(published.len(), 6 * 1024 * 1024);
    assert!(published.iter().all(|byte| *byte == published[0]));
}

#[test]
fn environment_router_registers_each_enabled_provider_lazily() {
    let stores = object_stores_from_env().unwrap();
    assert_eq!(stores.provider("s3").is_ok(), cfg!(feature = "aws"));
    assert_eq!(stores.provider("gs").is_ok(), cfg!(feature = "gcp"));
    assert_eq!(stores.provider("az").is_ok(), cfg!(feature = "azure"));
}
