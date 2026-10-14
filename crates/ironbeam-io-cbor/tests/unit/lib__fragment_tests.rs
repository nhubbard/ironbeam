// Unit tests for src/lib.rs.
use super::*;

#[test]
fn invalid_record_cannot_replace_existing_result() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("result.cbor");
    std::fs::write(&target, b"previous result").unwrap();
    let store = cbor_fragment_store(dir.path().join("staging"), "job", "graph", "sink").unwrap();
    store.local.stage("bundle", 1, &[0xff]).unwrap();
    store.local.accept("bundle").unwrap();
    let store = store.require_bundle_attempts(["bundle"]).unwrap();
    assert!(publish_cbor_fragments::<u32>(&store, 1, &target).is_err());
    assert_eq!(std::fs::read(&target).unwrap(), b"previous result");
}
