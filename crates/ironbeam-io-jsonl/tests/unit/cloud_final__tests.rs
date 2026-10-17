// Unit tests for src/cloud_final.rs.
use super::*;
use ironbeam_cloud_objects::LocalObjectIO;

#[test]
fn bundle_writer_and_record_reader_enforce_resource_bounds() {
    let mut bytes = Vec::new();
    let mut writer = BoundedBytes {
        bytes: &mut bytes,
        limit: 4,
    };
    writer.write_all(b"1234").unwrap();
    assert!(writer.write_all(b"5").is_err());
    assert_eq!(bytes, b"1234");
    let mut reader = BufReader::new(b"one\ntwo\n".as_slice());
    let mut line = Vec::new();
    assert!(read_record(&mut reader, &mut line).unwrap());
    assert_eq!(line, b"one");
    assert!(read_record(&mut reader, &mut line).unwrap());
    assert_eq!(line, b"two");
    assert!(!read_record(&mut reader, &mut line).unwrap());
    let mut incomplete = BufReader::new(b"missing newline".as_slice());
    assert!(read_record(&mut incomplete, &mut line).is_err());
}

#[test]
fn bundle_finalization_and_publication_round_trip() {
    let directory = tempfile::tempdir().unwrap();
    let storage: Arc<dyn ObjectIO> = Arc::new(LocalObjectIO::new(directory.path()).unwrap());
    let store = cloud_jsonl_fragment_store(
        Arc::clone(&storage),
        ("stage", "job/result"),
        ("output", "result.jsonl"),
        "job",
        "graph",
        "sink",
    )
    .unwrap();
    let mut sink = CloudJsonlFragmentSink::<u64> {
        store: store.clone(),
        bundle_id: None,
        bytes: Vec::new(),
        rows: 0,
        staged: false,
        failed: false,
        marker: std::marker::PhantomData,
    };
    sink.begin_bundle("attempt").unwrap();
    sink.write(3).unwrap();
    sink.write(4).unwrap();
    sink.commit().unwrap();
    assert!(
        publish_cloud_jsonl_fragments::<u64>(
            &store.require_bundle_attempts(["attempt"]).unwrap(),
            2,
        )
        .is_err()
    );
    sink.finalize_bundle().unwrap();
    let completed = store.require_bundle_attempts(["attempt"]).unwrap();
    assert_eq!(
        publish_cloud_jsonl_fragments::<u64>(&completed, 2)
            .unwrap()
            .rows,
        2
    );
    assert_eq!(
        storage.get_object("output", "result.jsonl").unwrap(),
        b"3\n4\n"
    );
    assert_eq!(
        publish_cloud_jsonl_fragments::<u64>(&completed, 2)
            .unwrap()
            .rows,
        2
    );
}

#[test]
fn partial_record_cannot_cross_attempt_boundary() {
    let directory = tempfile::tempdir().unwrap();
    let storage: Arc<dyn ObjectIO> = Arc::new(LocalObjectIO::new(directory.path()).unwrap());
    let store = cloud_jsonl_fragment_store(
        Arc::clone(&storage),
        ("stage", "job/result"),
        ("output", "result.jsonl"),
        "job",
        "graph",
        "sink",
    )
    .unwrap();
    store.stage("a", 0, b"{").unwrap();
    store.accept("a").unwrap();
    store.stage("b", 1, b"}\n").unwrap();
    store.accept("b").unwrap();
    let completed = store.require_bundle_attempts(["a", "b"]).unwrap();
    assert!(publish_cloud_jsonl_fragments::<serde_json::Value>(&completed, 1).is_err());
    assert!(!storage.object_exists("output", "result.jsonl").unwrap());
}
