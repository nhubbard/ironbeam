// Unit tests for src/stream.rs.
use super::*;
use ironbeam_core::Pipeline;

#[test]
fn window_state_roundtrip_preserves_metadata_and_rejects_tampering() {
    let pipeline = Pipeline::default();
    pipeline.from_vec("main", vec![7_i32]).unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let interval = WindowInterval {
        start_ns: 0,
        end_ns: 10,
    };
    let event = Envelope {
        pane: ironbeam_core::PaneInfo {
            is_first: false,
            is_last: true,
            timing: ironbeam_core::PaneTiming::Late,
            index: 1,
            nonspeculative_index: 1,
            contains_element_metadata: false,
        },
        value: Box::new(7_i32),
        timestamp_ns: 3,
        window: Window::Fixed { size_ns: 10 },
        assigned_window: Some(interval),
    };
    let rows = BTreeMap::from([((0, 10), vec![event])]);
    let encoded = encode_window_rows(&registry, "main", &rows, 1024).unwrap();
    assert_eq!(
        encode_window_rows(&registry, "main", &rows, encoded.len() - 1)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Resource
    );
    let (restored, buffered) = restore_window_rows(&registry, "main", &encoded, 1).unwrap();
    assert_eq!(buffered, 1);
    let restored = &restored[&(0, 10)][0];
    assert_eq!(*restored.value.downcast_ref::<i32>().unwrap(), 7);
    assert_eq!(restored.timestamp_ns, 3);
    assert_eq!(restored.assigned_window, Some(interval));
    assert_eq!(restored.pane.timing, ironbeam_core::PaneTiming::Late);
    assert_eq!(restored.pane.index, 1);

    let mut legacy: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    legacy["windows"][0]["events"][0]
        .as_object_mut()
        .unwrap()
        .remove("pane");
    let legacy_bytes = serde_json::to_vec(&legacy).unwrap();
    let (legacy_rows, _) = restore_window_rows(&registry, "main", &legacy_bytes, 1).unwrap();
    assert_eq!(
        legacy_rows[&(0, 10)][0].pane,
        ironbeam_core::PaneInfo::NO_FIRING
    );

    let original: CodedWindowRows = serde_json::from_slice(&encoded).unwrap();
    let mut duplicated = original;
    duplicated.windows.push(CodedWindow {
        start_ns: 0,
        end_ns: 10,
        events: vec![CodedEvent {
            pane: ironbeam_core::PaneInfo::NO_FIRING,
            value: 7_i32.encode().unwrap(),
            timestamp_ns: 3,
            window: Window::Fixed { size_ns: 10 },
            assigned_window: Some(interval),
        }],
    });
    let bytes = serde_json::to_vec(&duplicated).unwrap();
    assert_eq!(
        restore_window_rows(&registry, "main", &bytes, 2)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );

    duplicated.windows.pop();
    duplicated.windows[0].events[0].assigned_window = None;
    let bytes = serde_json::to_vec(&duplicated).unwrap();
    assert_eq!(
        restore_window_rows(&registry, "main", &bytes, 1)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
    duplicated.windows[0].events[0].assigned_window = Some(interval);
    duplicated.windows[0].events[0].timestamp_ns = 13;
    let bytes = serde_json::to_vec(&duplicated).unwrap();
    assert_eq!(
        restore_window_rows(&registry, "main", &bytes, 1)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
}

#[test]
fn sliding_window_checkpoint_restores_each_overlapping_assignment() {
    let pipeline = Pipeline::default();
    pipeline.from_vec("main", vec![7_i32]).unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let window = Window::Sliding {
        size_ns: 4,
        period_ns: 2,
        offset_ns: 0,
    };
    let intervals = window
        .assign_all(1)
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let rows = intervals
        .iter()
        .map(|interval| {
            (
                (interval.start_ns, interval.end_ns),
                vec![Envelope {
                    pane: ironbeam_core::PaneInfo::NO_FIRING,
                    value: Box::new(7_i32),
                    timestamp_ns: 1,
                    window: window.clone(),
                    assigned_window: Some(*interval),
                }],
            )
        })
        .collect::<BTreeMap<_, _>>();
    let encoded = encode_window_rows(&registry, "main", &rows, 1024).unwrap();
    let (restored, buffered) = restore_window_rows(&registry, "main", &encoded, 2).unwrap();
    assert_eq!(buffered, 2);
    assert_eq!(restored.len(), 2);
    for interval in intervals {
        assert_eq!(
            restored[&(interval.start_ns, interval.end_ns)][0].assigned_window,
            Some(interval)
        );
    }
}

#[test]
fn window_state_rejects_invalid_version_windows_and_element_budget() {
    let registry = Registry::default();
    let encoded =
        |version, windows| serde_json::to_vec(&CodedWindowRows { version, windows }).unwrap();
    assert_eq!(
        restore_window_rows(&registry, "main", &encoded(2, vec![]), 1)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Incompatible
    );
    for window in [
        CodedWindow {
            start_ns: 1,
            end_ns: 1,
            events: vec![CodedEvent {
                pane: ironbeam_core::PaneInfo::NO_FIRING,
                value: vec![],
                timestamp_ns: 1,
                window: Window::Global,
                assigned_window: None,
            }],
        },
        CodedWindow {
            start_ns: 0,
            end_ns: 2,
            events: vec![],
        },
    ] {
        assert_eq!(
            restore_window_rows(&registry, "main", &encoded(1, vec![window]), 1)
                .err()
                .unwrap()
                .kind,
            ErrorKind::Recovery
        );
    }
    let window = CodedWindow {
        start_ns: 0,
        end_ns: 2,
        events: vec![CodedEvent {
            pane: ironbeam_core::PaneInfo::NO_FIRING,
            value: vec![],
            timestamp_ns: 1,
            window: Window::Global,
            assigned_window: None,
        }],
    };
    assert_eq!(
        restore_window_rows(&registry, "main", &encoded(1, vec![window]), 0)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
}

#[test]
fn grouped_state_rejects_corrupt_header_window_and_trailing_bytes() {
    let registry = Registry::default();
    for bytes in [
        b"IBGS".as_slice(),
        b"wrong\0\0\0\0".as_slice(),
        b"IBGS1\0\0\0".as_slice(),
        b"IBGS1\0\0\0\0trailing".as_slice(),
    ] {
        assert!(restore_grouped_folds(&registry, "group", bytes, 1).is_err());
    }
    let mut invalid_window = b"IBGS1".to_vec();
    invalid_window.extend_from_slice(&1_u32.to_le_bytes());
    invalid_window.extend_from_slice(&1_i64.to_le_bytes());
    invalid_window.extend_from_slice(&1_i64.to_le_bytes());
    invalid_window.extend_from_slice(&1_u64.to_le_bytes());
    invalid_window.extend_from_slice(&0_u64.to_le_bytes());
    assert_eq!(
        restore_grouped_folds(&registry, "group", &invalid_window, 1)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
}

#[test]
fn grouped_state_roundtrip_rejects_budget_and_duplicate_open_window() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![(1_i32, 7_i32)])
        .unwrap()
        .group_by_key_named("groups")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let interval = WindowInterval {
        start_ns: 0,
        end_ns: 10,
    };
    let mut fold = registry.open_fold("groups").unwrap();
    fold.add(Envelope {
        pane: ironbeam_core::PaneInfo::NO_FIRING,
        value: Box::new((1_i32, 7_i32)),
        timestamp_ns: 3,
        window: Window::Fixed { size_ns: 10 },
        assigned_window: Some(interval),
    })
    .unwrap();
    let folds = BTreeMap::from([((0, 10), (fold, 1_usize))]);
    let encoded = encode_grouped_folds(&folds, 1024, "groups").unwrap();
    assert_eq!(
        encode_grouped_folds(&folds, encoded.len() - 1, "groups")
            .err()
            .unwrap()
            .kind,
        ErrorKind::Resource
    );
    let (restored, buffered) = restore_grouped_folds(&registry, "groups", &encoded, 1).unwrap();
    assert_eq!(buffered, 1);
    assert_eq!(restored.len(), 1);

    let mut over_budget = encoded.clone();
    over_budget[25..33].copy_from_slice(&2_u64.to_le_bytes());
    assert_eq!(
        restore_grouped_folds(&registry, "groups", &over_budget, 1)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
    let mut empty_window = encoded.clone();
    empty_window[25..33].copy_from_slice(&0_u64.to_le_bytes());
    assert_eq!(
        restore_grouped_folds(&registry, "groups", &empty_window, 1)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
    let mut duplicate = encoded.clone();
    duplicate[5..9].copy_from_slice(&2_u32.to_le_bytes());
    duplicate.extend_from_slice(&encoded[9..]);
    assert_eq!(
        restore_grouped_folds(&registry, "groups", &duplicate, 2)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
}
