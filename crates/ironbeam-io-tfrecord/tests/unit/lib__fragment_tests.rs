// Unit tests for src/lib.rs.
use super::*;

fn session(store: &LocalFinalResult) -> TfrecordFragmentSink {
    TfrecordFragmentSink {
        store: store.clone(),
        bundle_id: None,
        spool: None,
        rows: 0,
        staged: false,
    }
}

#[test]
fn finalized_fragments_publish_valid_tfrecord_only_after_acceptance() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalFinalResult::new(temp.path().join("stage"), "job", "graph", "sink").unwrap();
    let output = temp.path().join("rows.tfrecord");
    let mut first = session(&store);
    assert!(first.write(b"early".to_vec()).is_err());
    assert!(first.begin_bundle("").is_err());
    assert!(first.finalize_bundle().is_err());
    first.begin_bundle("bundle-1").unwrap();
    assert!(first.begin_bundle("another").is_err());
    assert!(first.finalize_bundle().is_err());
    first.write(b"one".to_vec()).unwrap();
    first.write(b"two".to_vec()).unwrap();
    first.commit().unwrap();
    assert!(publish_tfrecord_fragments(&store, 2, &output).is_err());
    assert!(!output.exists());
    let mut conflicting = session(&store);
    conflicting.begin_bundle("bundle-1").unwrap();
    conflicting.write(b"other".to_vec()).unwrap();
    assert!(conflicting.commit().is_err());
    conflicting.abort().unwrap();
    let mut restarted = session(&store);
    assert!(restarted.recover_bundle_finalization("missing").is_err());
    restarted.recover_bundle_finalization("bundle-1").unwrap();
    first.finalize_bundle().unwrap();
    first.finalize_bundle().unwrap();
    let mut empty = session(&store);
    empty.begin_bundle("bundle-empty").unwrap();
    empty.commit().unwrap();
    empty.finalize_bundle().unwrap();
    let store = store
        .require_bundle_attempts(["bundle-1", "bundle-empty"])
        .unwrap();
    let published = publish_tfrecord_fragments(&store, 2, &output).unwrap();
    assert_eq!(published.rows, 2);
    assert_eq!(published.fragments, 2);
    assert_eq!(
        publish_tfrecord_fragments(&store, 2, &output).unwrap(),
        published
    );
    assert_eq!(
        read_tfrecord(&std::fs::read(&output).unwrap()).unwrap(),
        [b"one".to_vec(), b"two".to_vec()]
    );
    let path = std::fs::read_dir(temp.path().join("stage"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.metadata().unwrap().len() > 120)
        .unwrap();
    let mut bytes = std::fs::read(&path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    std::fs::write(path, bytes).unwrap();
    assert!(publish_tfrecord_fragments(&store, 2, temp.path().join("corrupt.tfrecord")).is_err());
}

#[test]
fn builder_binds_identity_and_aborted_attempt_never_publishes() {
    let temp = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", vec![b"row".to_vec()]).unwrap();
    assert!(tfrecord_fragment_sink(&input, "relative", "relative-stage", "job", "graph").is_err());
    let staging = temp.path().join("stage");
    let sink = tfrecord_fragment_sink(&input, "fragment", &staging, "job", "graph").unwrap();
    let (_, registry) = sink.snapshot().unwrap();
    assert!(
        registry
            .sink_config_digest("fragment")
            .unwrap()
            .contains("tfrecord-fragments-v1")
    );
    assert!(registry.sink_requires_bundle_finalization("fragment"));
    let mut session = registry.open_sink("fragment").unwrap();
    assert!(session.requires_bundle_finalization());
    session.begin_bundle("aborted").unwrap();
    session.commit().unwrap();
    let store = LocalFinalResult::new(&staging, "job", "graph", "fragment").unwrap();
    assert!(store.accepted_fragments().unwrap().is_empty());
    session.abort().unwrap();
    assert!(std::fs::read_dir(&staging).unwrap().next().is_none());
    assert!(publish_tfrecord_fragments(&store, 1, temp.path().join("absent.tfrecord")).is_err());
}

#[test]
fn publication_validates_decoded_count_and_crc() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalFinalResult::new(temp.path().join("stage"), "job", "graph", "sink").unwrap();
    let mut frame = Vec::new();
    write_frames(&mut frame, &[b"one".to_vec()]).unwrap();
    store.stage("wrong-count", 2, &frame).unwrap();
    store.accept("wrong-count").unwrap();
    let store = store.require_bundle_attempts(["wrong-count"]).unwrap();
    let output = temp.path().join("result.tfrecord");
    assert!(publish_tfrecord_fragments(&store, 2, temp.path().join("result.tfrecord.gz")).is_err());
    assert!(publish_tfrecord_fragments(&store, 2, &output).is_err());
    assert!(!output.exists());
    let other = LocalFinalResult::new(temp.path().join("other"), "job", "graph", "sink").unwrap();
    let last = frame.len() - 1;
    frame[last] ^= 1;
    other.stage("bad-crc", 1, &frame).unwrap();
    other.accept("bad-crc").unwrap();
    let other = other.require_bundle_attempts(["bad-crc"]).unwrap();
    assert!(publish_tfrecord_fragments(&other, 1, &output).is_err());
    assert!(!output.exists());
}
