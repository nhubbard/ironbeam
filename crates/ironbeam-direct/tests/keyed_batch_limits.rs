use ironbeam_core::{Element, ErrorKind, Pipeline, TestStreamEvent};
use ironbeam_direct::DirectCollectExt;

#[test]
fn bounded_keyed_batches_obey_count_and_encoded_byte_limits() {
    let pipeline = Pipeline::default();
    let short_bytes = "a".to_owned().encoded_len().unwrap();
    let batches = pipeline
        .from_vec(
            "values",
            vec![
                (1_i32, "a".to_owned()),
                (1, "bbb".to_owned()),
                (1, "c".to_owned()),
                (1, "d".to_owned()),
                (1, "oversized".to_owned()),
                (2, "e".to_owned()),
            ],
        )
        .unwrap()
        .group_into_batches_with_limits_named("batches", 2, short_bytes * 2)
        .unwrap();
    assert_eq!(
        batches.collect_seq().unwrap(),
        [
            (1, vec!["a".to_owned()]),
            (1, vec!["bbb".to_owned()]),
            (1, vec!["c".to_owned(), "d".to_owned()]),
            (1, vec!["oversized".to_owned()]),
            (2, vec!["e".to_owned()]),
        ]
    );
}

#[test]
fn byte_limited_batches_reject_invalid_or_unbounded_graphs() {
    let pipeline = Pipeline::default();
    let bounded = pipeline.from_vec("bounded", vec![(1_i32, 2_i32)]).unwrap();
    for (key, count, bytes) in [("count", 0, 1), ("bytes", 1, 0)] {
        let error = bounded
            .group_into_batches_with_limits_named(key, count, bytes)
            .err()
            .unwrap();
        assert_eq!(error.kind, ErrorKind::Graph);
    }
    let unbounded = pipeline
        .test_stream_source(
            "unbounded",
            vec![TestStreamEvent::Elements(vec![((1_i32, 2_i32), 0)])],
        )
        .unwrap();
    let error = unbounded
        .group_into_batches_with_limits_named("unbounded-batches", 2, 8)
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Graph);
    assert!(error.message.contains("bounded"));
    let (graph, _) = pipeline.snapshot().unwrap();
    assert!(graph.node("count").is_none());
    assert!(graph.node("bytes").is_none());
    assert!(graph.node("unbounded-batches").is_none());
}
