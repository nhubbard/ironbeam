// Unit tests for src/object_final.rs.
use super::*;
use ironbeam_cloud_objects::LocalObjectIO;

fn store() -> (tempfile::TempDir, Arc<dyn ObjectIO>, ObjectFinalResult) {
    let directory = tempfile::tempdir().unwrap();
    let storage: Arc<dyn ObjectIO> = Arc::new(LocalObjectIO::new(directory.path()).unwrap());
    let result = ObjectFinalResult::new(
        Arc::clone(&storage),
        ("stage", "job/result"),
        ("output", "result.jsonl"),
        "job",
        "graph",
        "sink",
    )
    .unwrap();
    (directory, storage, result)
}

#[allow(clippy::unnecessary_wraps)] // Matches the validator callback signature.
fn count_lines(bytes: &[u8]) -> Result<u64, Error> {
    Ok(bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .count() as u64)
}

#[allow(clippy::naive_bytecount)] // Avoid another dependency for this test helper.
fn count_streamed_lines(reader: &mut dyn Read) -> Result<u64, Error> {
    let mut count = 0_u64;
    let mut buffer = [0_u8; 4096];
    loop {
        let read = reader.read(&mut buffer).map_err(stream_error)?;
        if read == 0 {
            break;
        }
        count += buffer[..read].iter().filter(|byte| **byte == b'\n').count() as u64;
    }
    Ok(count)
}

#[test]
fn streaming_publication_crosses_buffers_and_retries_without_replacing() {
    let (_directory, storage, result) = store();
    let large = b"a\n".repeat(200_000);
    result.stage("large", 200_000, &large).unwrap();
    result.accept("large").unwrap();
    let bound = result.require_bundle_attempts(["large"]).unwrap();
    let published = bound
        .publish_validated_streaming(200_000, count_streamed_lines)
        .unwrap();
    assert_eq!(published.bytes, large.len() as u64);
    assert_eq!(
        bound
            .publish_validated_streaming(200_000, count_streamed_lines)
            .unwrap(),
        published
    );
    assert_eq!(storage.get_object("output", "result.jsonl").unwrap(), large);
    storage
        .put_object("output", "result.jsonl", b"different")
        .unwrap();
    assert!(
        bound
            .publish_validated_streaming(200_000, count_streamed_lines)
            .is_err()
    );
}

#[test]
fn accepted_attempts_publish_only_with_exact_manifest_and_are_idempotent() {
    let (_directory, storage, result) = store();
    result.stage("attempt-b", 1, b"2\n").unwrap();
    result.stage("attempt-a", 1, b"1\n").unwrap();
    result.stage("attempt-a", 1, b"1\n").unwrap();
    assert!(result.stage("attempt-a", 1, b"changed\n").is_err());
    result.accept("attempt-b").unwrap();
    result.accept("attempt-a").unwrap();
    result.accept("attempt-a").unwrap();
    assert!(
        result
            .publish_validated_concatenated(2, count_lines)
            .is_err()
    );
    assert!(
        result
            .require_bundle_attempts(["attempt-a"])
            .unwrap()
            .publish_validated_concatenated(2, count_lines)
            .is_err()
    );
    let bound = result
        .require_bundle_attempts(["attempt-a", "attempt-b"])
        .unwrap();
    let published = bound
        .publish_validated_concatenated(2, count_lines)
        .unwrap();
    assert_eq!(published.rows, 2);
    assert_eq!(published.fragments, 2);
    assert_eq!(
        bound
            .publish_validated_concatenated(2, count_lines)
            .unwrap(),
        published
    );
    let output = storage.get_object("output", "result.jsonl").unwrap();
    assert_eq!(count_lines(&output).unwrap(), 2);
}

#[test]
fn pending_and_unknown_attempts_block_publication() {
    let (_directory, _storage, result) = store();
    result.stage("expected", 1, b"1\n").unwrap();
    let bound = result.require_bundle_attempts(["expected"]).unwrap();
    assert!(
        bound
            .publish_validated_concatenated(1, count_lines)
            .is_err()
    );
    assert!(
        bound
            .publish_validated_streaming(1, count_streamed_lines)
            .is_err()
    );
    result.abandon("expected").unwrap();
    result.stage("extra", 1, b"2\n").unwrap();
    result.accept("extra").unwrap();
    assert!(
        bound
            .publish_validated_concatenated(1, count_lines)
            .is_err()
    );
    assert!(
        bound
            .publish_validated_streaming(1, count_streamed_lines)
            .is_err()
    );
}

#[test]
fn corrupt_fragment_and_conflicting_output_fail_closed() {
    let (_directory, storage, result) = store();
    result.stage("a", 1, b"1\n").unwrap();
    result.accept("a").unwrap();
    let accepted = result.key("a", "accepted").unwrap();
    storage.put_object("stage", &accepted, b"bad").unwrap();
    let bound = result.require_bundle_attempts(["a"]).unwrap();
    assert!(
        bound
            .publish_validated_concatenated(1, count_lines)
            .is_err()
    );
    assert!(
        bound
            .publish_validated_streaming(1, count_streamed_lines)
            .is_err()
    );
    storage.delete_object("stage", &accepted).unwrap();
    result.stage("a", 1, b"1\n").unwrap();
    result.accept("a").unwrap();
    storage
        .put_object("output", "result.jsonl", b"other\n")
        .unwrap();
    assert!(
        bound
            .publish_validated_concatenated(1, count_lines)
            .is_err()
    );
    assert!(
        bound
            .publish_validated_streaming(1, count_streamed_lines)
            .is_err()
    );
    assert_eq!(
        storage.get_object("output", "result.jsonl").unwrap(),
        b"other\n"
    );
}

#[test]
fn empty_manifest_creates_empty_output_without_staging_bucket() {
    let (_directory, storage, result) = store();
    let bound = result.require_bundle_attempts([] as [&str; 0]).unwrap();
    assert_eq!(
        bound
            .publish_validated_concatenated(0, count_lines)
            .unwrap()
            .bytes,
        0
    );
    assert_eq!(
        bound
            .publish_validated_streaming(0, count_streamed_lines)
            .unwrap()
            .bytes,
        0
    );
    assert_eq!(
        storage.get_object("output", "result.jsonl").unwrap().len(),
        0
    );
}
