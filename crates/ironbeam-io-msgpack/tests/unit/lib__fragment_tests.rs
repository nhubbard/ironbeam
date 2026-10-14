// Unit tests for src/lib.rs.
use super::*;

#[test]
fn lazy_reader_stops_after_invalid_record() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("invalid.msgpack");
    std::fs::write(&path, [0xc1]).unwrap();
    let mut rows = read_msgpack_lazy::<u32>(path).unwrap();
    assert!(rows.next().unwrap().is_err());
    assert!(rows.next().is_none());
}

#[test]
fn invalid_record_cannot_replace_existing_result() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("result.msgpack");
    std::fs::write(&target, b"previous result").unwrap();
    let store = msgpack_fragment_store(dir.path().join("staging"), "job", "graph", "sink").unwrap();
    store.local.stage("bundle", 1, &[0xc1]).unwrap();
    store.local.accept("bundle").unwrap();
    let store = store.require_bundle_attempts(["bundle"]).unwrap();
    assert!(publish_msgpack_fragments::<u32>(&store, 1, &target).is_err());
    assert_eq!(std::fs::read(&target).unwrap(), b"previous result");
}
