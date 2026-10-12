// Unit tests for src/lib.rs.
use super::*;
use std::panic::{AssertUnwindSafe, catch_unwind};

#[test]
fn semantic_map_records_handler_identity_and_rejects_invalid_versions() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![2_i32]).unwrap();
    let version = Version { major: 2, minor: 1 };
    source
        .map_with_semantic_named("twice", "example/twice", version, |value| value * 2)
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let node = graph.node("twice").unwrap();
    assert_eq!(node.operation, Operation::Map);
    assert_eq!(node.semantic_id, "example/twice");
    assert_eq!(node.semantic_version, version);
    assert!(registry.has_single_transform("twice"));
    assert!(
        source
            .map_with_semantic_named("bad-id", "twice", version, |value| value)
            .is_err()
    );
    assert!(
        source
            .map_with_semantic_named("builtin-id", "ironbeam/map", version, |value| value)
            .is_err()
    );
    assert!(
        source
            .map_with_semantic_named(
                "bad-version",
                "example/twice",
                Version { major: 0, minor: 1 },
                |value| value,
            )
            .is_err()
    );
    assert_eq!(pipeline.snapshot().unwrap().0.nodes.len(), 2);
    let changed = Pipeline::default();
    changed
        .from_vec("source", vec![2_i32])
        .unwrap()
        .map_with_semantic_named(
            "twice",
            "example/twice",
            Version { major: 2, minor: 2 },
            |value| value * 2,
        )
        .unwrap();
    assert_ne!(
        graph.fingerprint().unwrap(),
        changed.snapshot().unwrap().0.fingerprint().unwrap()
    );
}

#[test]
fn semantic_filter_and_flat_map_record_versions_in_graph_identity() {
    let build = |minor| {
        let pipeline = Pipeline::default();
        let source = pipeline.from_vec("source", vec![1_i32, 2, 3]).unwrap();
        let version = Version { major: 1, minor };
        let filtered = source
            .filter_with_semantic_named("filter", "example/odd", version, |value| value % 2 != 0)
            .unwrap();
        filtered
            .flat_map_with_semantic_named("expand", "example/duplicate", version, |value| {
                vec![value, value]
            })
            .unwrap();
        pipeline.snapshot().unwrap().0
    };
    let graph = build(0);
    assert_eq!(graph.node("filter").unwrap().operation, Operation::Filter);
    assert_eq!(graph.node("expand").unwrap().operation, Operation::FlatMap);
    assert_eq!(graph.node("filter").unwrap().semantic_id, "example/odd");
    assert_eq!(
        graph.node("expand").unwrap().semantic_id,
        "example/duplicate"
    );
    assert_ne!(
        graph.fingerprint().unwrap(),
        build(1).fingerprint().unwrap()
    );
    let source = Pipeline::default().from_vec("source", vec![1_i32]).unwrap();
    assert!(
        source
            .filter_with_semantic_named(
                "bad-filter",
                "ironbeam/odd",
                Version { major: 1, minor: 0 },
                |_| true
            )
            .is_err()
    );
    assert!(
        source
            .flat_map_with_semantic_named(
                "bad-expand",
                "example/duplicate",
                Version { major: 0, minor: 1 },
                |value| vec![value]
            )
            .is_err()
    );
}

#[test]
fn semantic_batch_handler_records_application_version() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32, 2]).unwrap();
    let version = Version { major: 2, minor: 3 };
    source
        .map_batches_with_semantic_named("batch", 2, "example/partition", version, |values| {
            values.iter().rev().copied().collect::<Vec<_>>()
        })
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let node = graph.node("batch").unwrap();
    assert_eq!(node.operation, Operation::FlatMap);
    assert_eq!(node.semantic_id, "example/partition");
    assert_eq!(node.semantic_version, version);
    let result = registry
        .apply_many(
            "batch",
            Envelope {
                value: Box::new(vec![1_i32, 2]),
                timestamp_ns: 0,
                window: Window::Global,
                assigned_window: None,
            },
        )
        .unwrap();
    assert_eq!(
        result
            .into_iter()
            .map(|event| *event.value.downcast::<i32>().unwrap())
            .collect::<Vec<_>>(),
        [2, 1]
    );
    assert!(
        source
            .map_batches_with_semantic_named(
                "invalid-batch",
                2,
                "example/partition",
                Version { major: 0, minor: 1 },
                <[i32]>::to_vec,
            )
            .is_err()
    );
    assert!(
        source
            .apply_transform_with_semantic_named(
                "zero-batch",
                0,
                "example/partition",
                version,
                |values| values,
            )
            .is_err()
    );
    assert!(
        pipeline
            .snapshot()
            .unwrap()
            .0
            .node("zero-batch/batches")
            .is_none()
    );
}

#[test]
fn batch_size_is_part_of_graph_identity() {
    let build = |size| {
        let pipeline = Pipeline::default();
        pipeline
            .from_vec("source", vec![1_i32, 2, 3])
            .unwrap()
            .map_batches_with_semantic_named(
                "batch",
                size,
                "example/partition",
                Version { major: 1, minor: 0 },
                <[i32]>::to_vec,
            )
            .unwrap();
        pipeline.snapshot().unwrap().0
    };
    let two = build(2);
    let three = build(3);
    assert_eq!(
        two.node("batch/batches").unwrap().semantic_id,
        "ironbeam/batch-elements/2"
    );
    assert_eq!(
        three.node("batch/batches").unwrap().semantic_id,
        "ironbeam/batch-elements/3"
    );
    assert_ne!(two.fingerprint().unwrap(), three.fingerprint().unwrap());
}

#[test]
fn byte_and_keyed_batch_limits_are_part_of_graph_identity() {
    let bytes = |limit| {
        let pipeline = Pipeline::default();
        pipeline
            .from_vec("source", vec!["one".to_owned(), "two".to_owned()])
            .unwrap()
            .batch_by_size_named("batch", limit, String::len)
            .unwrap();
        pipeline.snapshot().unwrap().0
    };
    let four_bytes = bytes(4);
    let five_bytes = bytes(5);
    assert_eq!(
        four_bytes.node("batch").unwrap().semantic_id,
        "ironbeam/batch-by-size/4"
    );
    assert_ne!(
        four_bytes.fingerprint().unwrap(),
        five_bytes.fingerprint().unwrap()
    );

    let keyed = |limit| {
        let pipeline = Pipeline::default();
        pipeline
            .from_vec("source", vec![(1_i32, 2_i32), (1, 3)])
            .unwrap()
            .group_into_batches_named("batch", limit)
            .unwrap();
        pipeline.snapshot().unwrap().0
    };
    let two_values = keyed(2);
    let three_values = keyed(3);
    assert_eq!(
        two_values.node("batch").unwrap().semantic_id,
        "ironbeam/group-into-batches/2"
    );
    assert_ne!(
        two_values.fingerprint().unwrap(),
        three_values.fingerprint().unwrap()
    );
}

#[test]
fn ordered_keyed_batch_size_is_part_of_graph_identity() {
    let build = |size| {
        let pipeline = Pipeline::default();
        pipeline
            .from_vec("source", vec![(1_i32, (1_i32, 2_i32))])
            .unwrap()
            .map_values_batches_ordered_by_named(
                "batch",
                size,
                |value| value.0,
                <[(i32, i32)]>::to_vec,
            )
            .unwrap();
        pipeline.snapshot().unwrap().0
    };
    let two = build(2);
    let three = build(3);
    assert_eq!(
        two.node("batch").unwrap().semantic_id,
        "ironbeam/map-values-ordered-batches/2"
    );
    assert_ne!(two.fingerprint().unwrap(), three.fingerprint().unwrap());

    let unbounded = Pipeline::default()
        .test_stream_source(
            "events",
            vec![TestStreamEvent::Elements(vec![(
                (1_i32, (1_i32, 2_i32)),
                1,
            )])],
        )
        .unwrap();
    assert!(
        unbounded
            .map_values_batches_ordered_by_named(
                "unbounded-global-batch",
                2,
                |value| value.0,
                <[(i32, i32)]>::to_vec,
            )
            .is_err()
    );
}

#[test]
fn bundle_recovery_is_explicit_and_typed_adapter_forwards_the_exact_id() {
    struct RecoveringSink;
    impl TypedSink<i32> for RecoveringSink {
        fn write(&mut self, _: i32) -> Result<(), Error> {
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn recover_bundle_finalization(&mut self, instruction_id: &str) -> Result<(), Error> {
            if instruction_id == "bundle-7" {
                Ok(())
            } else {
                Err(Error::incompatible("wrong bundle ID"))
            }
        }
    }
    struct OrdinarySession;
    impl SinkSession for OrdinarySession {
        fn write(&mut self, _: Envelope) -> Result<(), Error> {
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }

    let mut direct = OrdinarySession;
    assert!(direct.recover_bundle_finalization("bundle-7").is_err());
    direct
        .write(Envelope {
            value: Box::new(1_i32),
            timestamp_ns: 0,
            window: Window::Global,
            assigned_window: None,
        })
        .unwrap();
    direct.commit().unwrap();
    direct.abort().unwrap();
    let mut typed = SinkAdapter {
        key: "sink".into(),
        inner: Box::new(RecoveringSink),
    };
    assert!(typed.recover_bundle_finalization("bundle-7").is_ok());
    assert!(typed.recover_bundle_finalization("other").is_err());
    typed
        .write(Envelope {
            value: Box::new(1_i32),
            timestamp_ns: 0,
            window: Window::Global,
            assigned_window: None,
        })
        .unwrap();
    typed.commit().unwrap();
    typed.abort().unwrap();
}

#[test]
fn reusable_operator_default_hints_are_explicit() {
    struct Identity;
    impl DynOp for Identity {
        fn apply(&self, input: Partition) -> Partition {
            input
        }
    }
    let op: &dyn DynOp = &Identity;
    assert!(!op.key_preserving());
    assert!(!op.value_only());
    assert!(!op.reorder_safe_with_value_only());
    assert_eq!(op.cost_hint(), 10);
    assert!(!op.cardinality_reducing());
    assert_eq!(op.limit_n(), None);
    assert_eq!(op.cardinality_multiplier_hint(), 1.0);
    assert_eq!(*op.apply(Box::new(7_i32)).downcast::<i32>().unwrap(), 7);
}

#[test]
fn poisoned_builder_reports_typed_errors() {
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let inner = pipeline.inner.clone();
    assert!(
        catch_unwind(AssertUnwindSafe(move || {
            let _guard = inner.lock().unwrap();
            panic!("poison builder");
        }))
        .is_err()
    );
    assert_eq!(pipeline.snapshot().err().unwrap().kind, ErrorKind::Graph);
    assert_eq!(
        pipeline
            .from_vec("another", vec![2_i32])
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    assert_eq!(
        input.map_named("map", |x| x + 1).err().unwrap().kind,
        ErrorKind::Graph
    );
}

#[test]
fn direct_test_stream_splits_and_resumes_with_watermarks() {
    let pipeline = Pipeline::default();
    pipeline
        .test_stream_source(
            "events",
            vec![
                TestStreamEvent::Elements(vec![(1_i32, 1), (2, 2), (3, 3)]),
                TestStreamEvent::AdvanceWatermark(10),
                TestStreamEvent::AdvanceProcessingTime(5),
                TestStreamEvent::Elements(vec![(4, 11)]),
                TestStreamEvent::AdvanceWatermark(20),
            ],
        )
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let mut reader = registry.open_unbounded_source("events", None).unwrap();
    let UnboundedPoll::Ready(first) = reader.poll(2).unwrap() else {
        panic!("first test stream poll must be ready");
    };
    assert_eq!(first.elements.len(), 2);
    assert_eq!(first.progress.event_time_ns, None);
    let mut resumed = registry
        .open_unbounded_source("events", Some(first.progress))
        .unwrap();
    let UnboundedPoll::Ready(remaining_first_event) = resumed.poll(2).unwrap() else {
        panic!("remaining test stream element must be ready");
    };
    assert_eq!(
        *remaining_first_event.elements[0]
            .value
            .downcast_ref::<i32>()
            .unwrap(),
        3
    );
    let UnboundedPoll::Ready(first_watermark) = resumed.poll(2).unwrap() else {
        panic!("first test stream watermark must be ready");
    };
    assert_eq!(first_watermark.progress.event_time_ns, Some(10));
    let mut resumed = registry
        .open_unbounded_source("events", Some(first_watermark.progress))
        .unwrap();
    let mut seen = Vec::new();
    loop {
        let UnboundedPoll::Ready(batch) = resumed.poll(2).unwrap() else {
            panic!("finite test stream must make progress");
        };
        for event in batch.elements {
            seen.push(*event.value.downcast::<i32>().unwrap());
        }
        if batch.progress.completed {
            assert_eq!(batch.progress.event_time_ns, Some(20));
            break;
        }
    }
    assert_eq!(seen, [4]);
    let invalid = ProgressSignal {
        source_key: "events".into(),
        split_id: "test-stream".into(),
        checkpoint_position: Some(vec![255; 16]),
        event_time_ns: None,
        completed: false,
    };
    assert_eq!(
        registry
            .open_unbounded_source("events", Some(invalid))
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
    for cursor in [
        vec![1],
        [u64::MAX.to_le_bytes(), 0_u64.to_le_bytes()].concat(),
        [1_u64.to_le_bytes(), 1_u64.to_le_bytes()].concat(),
        [5_u64.to_le_bytes(), 1_u64.to_le_bytes()].concat(),
    ] {
        let invalid = ProgressSignal {
            source_key: "events".into(),
            split_id: "test-stream".into(),
            checkpoint_position: Some(cursor),
            event_time_ns: None,
            completed: false,
        };
        assert_eq!(
            registry
                .open_unbounded_source("events", Some(invalid))
                .err()
                .unwrap()
                .kind,
            ErrorKind::Recovery
        );
    }
}
