// Unit tests for src/sequence_final.rs.
use super::*;

#[allow(clippy::trivially_copy_pass_by_ref)] // Matches the fragment encoder callback type.
fn encode_u32(value: &u32, output: &mut dyn Write) -> Result<(), Error> {
    output.write_all(&value.to_le_bytes()).map_err(io)
}

fn session(store: &LocalFinalResult) -> SequenceFragmentSink<u32> {
    SequenceFragmentSink {
        store: store.clone(),
        encode: encode_u32,
        bundle_id: None,
        spool: None,
        rows: 0,
        staged: false,
    }
}

fn count_u32(path: &Path) -> Result<u64, Error> {
    let bytes = std::fs::read(path).map_err(io)?;
    if bytes.len() % 4 != 0 {
        return Err(Error::incompatible("incomplete u32 record"));
    }
    Ok((bytes.len() / 4) as u64)
}

#[test]
fn retry_abort_scope_and_decoded_count_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let stage = dir.path().join("stage");
    let store = sequence_fragment_store(&stage, "job", "graph", "sink", "test-u32").unwrap();
    assert!(sequence_fragment_store("relative", "job", "graph", "sink", "test-u32").is_err());
    assert!(sequence_fragment_store(&stage, "job", "", "sink", "test-u32").is_err());
    assert!(sequence_fragment_store(&stage, "job", "graph", "sink", "").is_err());
    let output = dir.path().join("result.bin");
    let bound = store.require_bundle_attempts(["accepted"]).unwrap();
    let mut aborted = session(&store);
    assert!(aborted.write(1).is_err());
    aborted.begin_bundle("aborted").unwrap();
    aborted.write(1).unwrap();
    aborted.commit().unwrap();
    assert!(publish_sequence_fragments(&bound, 0, &output, count_u32).is_err());
    aborted.abort().unwrap();
    assert!(store.accepted_fragments().unwrap().is_empty());
    let mut accepted = session(&store);
    accepted.begin_bundle("accepted").unwrap();
    accepted.write(2).unwrap();
    accepted.commit().unwrap();
    let mut restarted = session(&store);
    assert!(restarted.recover_bundle_finalization("missing").is_err());
    restarted.recover_bundle_finalization("accepted").unwrap();
    accepted.finalize_bundle().unwrap();
    assert!(publish_sequence_fragments(&bound, 2, &output, count_u32).is_err());
    assert!(!output.exists());
    let other = sequence_fragment_store(&stage, "job", "graph", "sink", "other").unwrap();
    assert!(other.accepted_fragments().is_err());
    let ambiguous_stage = dir.path().join("ambiguous-stage");
    let first =
        sequence_fragment_store(&ambiguous_stage, "job", "graph", "sink", "test\0u32").unwrap();
    first.stage("bundle", 1, &1_u32.to_le_bytes()).unwrap();
    first.accept("bundle").unwrap();
    let split_scope =
        sequence_fragment_store(&ambiguous_stage, "job", "graph\0test", "sink", "u32").unwrap();
    assert!(split_scope.accepted_fragments().is_err());
    assert_eq!(
        publish_sequence_fragments(&bound, 1, &output, count_u32)
            .unwrap()
            .rows,
        1
    );
    assert_eq!(std::fs::read(&output).unwrap(), 2_u32.to_le_bytes());
}

#[test]
fn empty_bundle_requires_commit_and_acceptance() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        sequence_fragment_store(dir.path().join("stage"), "job", "graph", "sink", "test-u32")
            .unwrap();
    let output = dir.path().join("empty.bin");
    let bound = store.require_bundle_attempts(["empty"]).unwrap();
    let mut sink = session(&store);
    assert!(sink.commit().is_err());
    assert!(sink.finalize_bundle().is_err());
    assert!(sink.begin_bundle("").is_err());
    sink.begin_bundle("empty").unwrap();
    assert!(sink.begin_bundle("again").is_err());
    assert!(sink.finalize_bundle().is_err());
    sink.commit().unwrap();
    assert!(publish_sequence_fragments(&bound, 0, &output, count_u32).is_err());
    sink.finalize_bundle().unwrap();
    sink.finalize_bundle().unwrap();
    assert_eq!(
        publish_sequence_fragments(&bound, 0, &output, count_u32)
            .unwrap()
            .rows,
        0
    );
    assert_eq!(std::fs::read(&output).unwrap(), Vec::<u8>::new());
    sink.abort().unwrap();
    assert_eq!(store.accepted_fragments().unwrap().len(), 1);
}

#[test]
fn malformed_accepted_payload_cannot_publish() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        sequence_fragment_store(dir.path().join("stage"), "job", "graph", "sink", "test-u32")
            .unwrap();
    store.stage("bundle", 1, &[1, 2, 3]).unwrap();
    store.accept("bundle").unwrap();
    let target = dir.path().join("result.bin");
    let bound = store.require_bundle_attempts(["bundle"]).unwrap();
    assert!(publish_sequence_fragments(&bound, 1, &target, count_u32).is_err());
    assert!(!target.exists());
}
