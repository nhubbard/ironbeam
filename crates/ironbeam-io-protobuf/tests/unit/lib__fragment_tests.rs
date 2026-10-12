// Unit tests for src/lib.rs.
use super::*;
use prost::Message as _;

#[test]
fn malformed_delimited_record_cannot_replace_existing_result() {
    #[derive(Clone, PartialEq, prost::Message)]
    struct Row {
        #[prost(int32, tag = "1")]
        value: i32,
    }
    impl Element for Row {
        const TYPE_ID: &'static str = "protobuf-test/row";
        const CODER_ID: &'static str = "protobuf-test/row-v1";
    }
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("result.pb");
    std::fs::write(&target, b"previous result").unwrap();
    let store = protobuf_fragment_store(dir.path().join("stage"), "job", "graph", "sink").unwrap();
    store.local.stage("bundle", 1, &[0xff]).unwrap();
    store.local.accept("bundle").unwrap();
    assert!(publish_protobuf_fragments::<Row>(&store, 1, &target).is_err());
    assert_eq!(std::fs::read(&target).unwrap(), b"previous result");
}

#[test]
fn protobuf_publisher_requires_the_completed_attempt_manifest() {
    #[derive(Clone, PartialEq, prost::Message)]
    struct Row {
        #[prost(int32, tag = "1")]
        value: i32,
    }
    impl Element for Row {
        const TYPE_ID: &'static str = "protobuf-test/manifest-row";
        const CODER_ID: &'static str = "protobuf-test/manifest-row-v1";
    }
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("result.pb");
    let store = protobuf_fragment_store(dir.path().join("stage"), "job", "graph", "sink").unwrap();
    store
        .local
        .stage(
            "accepted-bundle",
            1,
            &Row { value: 7 }.encode_length_delimited_to_vec(),
        )
        .unwrap();
    store.local.accept("accepted-bundle").unwrap();
    let wrong = store
        .clone()
        .require_bundle_attempts(["other-bundle"])
        .unwrap();
    assert!(publish_protobuf_fragments::<Row>(&wrong, 1, &target).is_err());
    assert!(!target.exists());
    let correct = store.require_bundle_attempts(["accepted-bundle"]).unwrap();
    assert_eq!(
        publish_protobuf_fragments::<Row>(&correct, 1, &target)
            .unwrap()
            .rows,
        1
    );
}
