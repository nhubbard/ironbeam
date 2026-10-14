// Unit tests for src/v5_executor.rs.
use super::{
    append_state_backed_values, assemble_fold_boundary_row, assemble_group_boundary_rows,
    decode_boundary_record, decode_group_boundary_parts, decode_group_boundary_record,
    decode_group_boundary_rows, decode_keyed_boundary_record, decode_tagged_boundary_record,
    decode_timestamp_ns, encode_timestamp_millis, matching_reshuffle_ancestor, GroupBoundary,
    V5BundleExecutor, WireValue, BEAM_GLOBAL_WINDOW_END_MS,
};
use crate::coders::{
    decode_windowed_value, encode_interval_windowed_value, encode_length_prefix, encode_varint, encode_windowed_value,
    encode_windowed_value_with_metadata, PaneInfo, PaneTiming,
};
use ironbeam_core::{CombineFn, Element, Pipeline, Window};
use ironbeam_graphx::v5::collection_id;

struct Sum;

#[test]
fn fragmented_fn_data_metadata_stays_out_of_user_element() {
    let pane = PaneInfo {
        contains_element_metadata: true,
        ..PaneInfo::NO_FIRING
    };
    let mut body = Vec::new();
    encode_length_prefix(b"row", &mut body).unwrap();
    let mut encoded = Vec::new();
    encode_windowed_value_with_metadata(12, pane, Some(b"\x08\x01"), &body, &mut encoded)
        .unwrap();
    for prefix in 0..encoded.len() {
        assert!(decode_boundary_record(&encoded[..prefix], false)
            .unwrap()
            .is_none());
    }
    let (decoded, used) = decode_boundary_record(&encoded, false).unwrap().unwrap();
    assert_eq!(used, encoded.len());
    assert_eq!(decoded.bytes, b"row");
    assert_eq!(decoded.element_metadata.as_deref(), Some(b"\x08\x01".as_slice()));
    assert_eq!(decoded.pane, pane);
}

#[test]
fn elementwise_portable_step_preserves_runner_pane() {
    let pipeline = Pipeline::default();
    pipeline.from_vec("source", vec![1_i32]).unwrap()
        .map_named("mapped", |value| value + 1).unwrap()
        .output("result").unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let executor = V5BundleExecutor::new(&pipeline, "pane-test").unwrap();
    let pane = PaneInfo {
        is_first: false,
        is_last: true,
        timing: PaneTiming::Late,
        index: 2,
        nonspeculative_index: 1,
        contains_element_metadata: true,
    };
    let produced = executor.apply("mapped", WireValue {
        bytes: 1_i32.encode().unwrap(),
        timestamp_ns: 0,
        window: None,
        pane,
        element_metadata: Some(vec![0x08, 0x01]),
    }).unwrap();
    assert_eq!(produced.len(), 1);
    assert_eq!(produced[0].pane, pane);
    assert_eq!(produced[0].element_metadata.as_deref(), Some([0x08, 0x01].as_slice()));
    assert_eq!(*registry.decode_element("mapped", &produced[0].bytes)
        .unwrap().downcast::<i32>().unwrap(), 2);
    let mut encoded = Vec::new();
    encode_windowed_value_with_metadata(
        0, produced[0].pane, produced[0].element_metadata.as_deref(),
        &produced[0].bytes, &mut encoded,
    ).unwrap();
    let (decoded, _) = decode_windowed_value(&encoded).unwrap();
    assert_eq!(decoded.pane, pane);
    assert_eq!(decoded.element_metadata, Some([0x08, 0x01].as_slice()));
}

impl CombineFn<i32, i32, i32> for Sum {
    fn create(&self) -> i32 {
        0
    }
    fn add_input(&self, total: &mut i32, value: i32) {
        *total += value;
    }
    fn merge(&self, total: &mut i32, other: i32) {
        *total += other;
    }
    fn finish(&self, total: i32) -> i32 {
        total
    }
}

#[test]
fn keyed_fold_boundary_keeps_input_time_and_rejects_short_value() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![(1_i32, 2_i32)])
        .unwrap()
        .combine_values_lifted_named("sum", Sum)
        .unwrap()
        .output("result")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let (key, first) = registry
        .split_group_input("sum", &(1_i32, 2_i32).encode().unwrap())
        .unwrap();
    let (_, second) = registry
        .split_group_input("sum", &(1_i32, 3_i32).encode().unwrap())
        .unwrap();
    let tagged = |timestamp_ns: i64, value: Vec<u8>| {
        let mut bytes = timestamp_ns.to_be_bytes().to_vec();
        bytes.extend(value);
        bytes
    };
    let group = GroupBoundary {
        key: key.clone(),
        values: vec![tagged(2_000_000, first), tagged(3_000_000, second)],
        state_token: None,
        timestamp_ns: i64::MAX,
        assigned_window: None,
        pane: PaneInfo::NO_FIRING,
        element_metadata: None,
    };
    let executor = V5BundleExecutor::new(&pipeline, "keyed-fold-test").unwrap();
    let wire = assemble_fold_boundary_row(group).unwrap();
    let result = executor.apply_fold_group("sum", &wire).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].timestamp_ns, 2_000_000);
    let output = registry
        .decode_element("sum", &result[0].bytes)
        .unwrap()
        .downcast::<(i32, i32)>()
        .unwrap();
    assert_eq!(*output, (1, 5));
    let malformed = assemble_fold_boundary_row(GroupBoundary {
        key,
        values: vec![vec![1, 2, 3]],
        state_token: None,
        timestamp_ns: i64::MAX,
        assigned_window: None,
        pane: PaneInfo::NO_FIRING,
        element_metadata: None,
    })
    .unwrap();
    assert!(executor.apply_fold_group("sum", &malformed).is_err());
}

#[test]
fn global_fold_boundary_emits_identity_for_seed_only() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("numbers", Vec::<i32>::new())
        .unwrap()
        .combine_globally_lifted_named("sum", Sum)
        .unwrap()
        .output("result")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let executor = V5BundleExecutor::new(&pipeline, "global-fold-test").unwrap();
    let seed = assemble_fold_boundary_row(GroupBoundary {
        key: Vec::new(),
        values: vec![Vec::new()],
        state_token: None,
        timestamp_ns: 0,
        assigned_window: None,
        pane: PaneInfo::NO_FIRING,
        element_metadata: None,
    })
    .unwrap();
    let result = executor.apply_fold_group("sum", &seed).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].timestamp_ns, 0);
    let value = registry
        .decode_element("sum", &result[0].bytes)
        .unwrap()
        .downcast::<i32>()
        .unwrap();
    assert_eq!(*value, 0);
    let bad_key = assemble_fold_boundary_row(GroupBoundary {
        key: vec![1],
        values: vec![Vec::new()],
        state_token: None,
        timestamp_ns: 0,
        assigned_window: None,
        pane: PaneInfo::NO_FIRING,
        element_metadata: None,
    })
    .unwrap();
    assert!(executor.apply_fold_group("sum", &bad_key).is_err());
}

#[test]
fn vector_global_combine_groups_values_and_emits_empty_identity() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("numbers", vec![2_i32, 3])
        .unwrap()
        .combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
        .unwrap()
        .output("result")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let executor = V5BundleExecutor::new(&pipeline, "vector-global-test").unwrap();
    let values = [2_i32, 3]
        .into_iter()
        .map(|value| {
            let (_, encoded) = registry
                .split_group_input("sum", &value.encode().unwrap())
                .unwrap();
            let mut timed = 2_000_000_i64.to_be_bytes().to_vec();
            timed.extend(encoded);
            timed
        })
        .collect();
    let grouped = assemble_fold_boundary_row(GroupBoundary {
        key: Vec::new(),
        values,
        state_token: None,
        timestamp_ns: i64::MAX,
        assigned_window: None,
        pane: PaneInfo::NO_FIRING,
        element_metadata: None,
    })
    .unwrap();
    let result = executor.apply_fold_group("sum", &grouped).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(
        *registry
            .decode_element("sum", &result[0].bytes)
            .unwrap()
            .downcast::<i32>()
            .unwrap(),
        5
    );
    let seed = assemble_fold_boundary_row(GroupBoundary {
        key: Vec::new(),
        values: vec![Vec::new()],
        state_token: None,
        timestamp_ns: 0,
        assigned_window: None,
        pane: PaneInfo::NO_FIRING,
        element_metadata: None,
    })
    .unwrap();
    let empty = executor.apply_fold_group("sum", &seed).unwrap();
    assert_eq!(
        *registry
            .decode_element("sum", &empty[0].bytes)
            .unwrap()
            .downcast::<i32>()
            .unwrap(),
        0
    );
}

#[test]
fn reshuffle_alias_walks_multiple_boundaries_but_not_user_transforms() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let first = source.reshuffle_named("first").unwrap();
    let second = first.reshuffle_named("second").unwrap();
    second
        .reshuffle_named("third")
        .unwrap()
        .output("result")
        .unwrap();
    let mapped = first.map_named("mapped", |value| value).unwrap();
    mapped
        .reshuffle_named("after-map")
        .unwrap()
        .output("other-result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert_eq!(
        matching_reshuffle_ancestor(&graph, "third", &collection_id("third")),
        Some(("third", false))
    );
    assert_eq!(
        matching_reshuffle_ancestor(&graph, "third", &collection_id("first")),
        Some(("first", true))
    );
    assert_eq!(
        matching_reshuffle_ancestor(&graph, "third", &collection_id("source")),
        Some(("source", true))
    );
    assert_eq!(
        matching_reshuffle_ancestor(&graph, "after-map", &collection_id("source")),
        None
    );
    assert_eq!(
        matching_reshuffle_ancestor(&graph, "third", &collection_id("mapped")),
        None
    );
}

#[test]
fn decodes_fragmented_join_group_into_multiple_timed_rows() {
    let pipeline = Pipeline::default();
    let left = pipeline
        .from_vec("left", vec![("a".to_owned(), 1_i32)])
        .unwrap();
    let right = pipeline
        .from_vec("right", vec![("a".to_owned(), true)])
        .unwrap();
    left.join_full_named("join", &right).unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let (key, first) = registry
        .split_join_input(
            "join",
            0,
            &("a".to_owned(), 1_i32).encode().unwrap(),
            1_000_000,
        )
        .unwrap();
    let (_, second) = registry
        .split_join_input(
            "join",
            0,
            &("a".to_owned(), 2_i32).encode().unwrap(),
            2_000_000,
        )
        .unwrap();
    let (_, right) = registry
        .split_join_input(
            "join",
            1,
            &("a".to_owned(), true).encode().unwrap(),
            3_000_000,
        )
        .unwrap();
    let mut body = Vec::new();
    encode_length_prefix(&key, &mut body).unwrap();
    body.extend_from_slice(&3_i32.to_be_bytes());
    for value in [&first, &second, &right] {
        encode_length_prefix(value, &mut body).unwrap();
    }
    let mut record = Vec::new();
    encode_interval_windowed_value(9, 0, 10, &body, &mut record).unwrap();
    let window = Window::Fixed {
        size_ns: 10_000_000,
    };
    for prefix in 0..record.len() {
        assert!(decode_group_boundary_rows(
            &record[..prefix],
            &registry,
            "join",
            window.clone(),
            true
        )
        .unwrap()
        .is_none());
    }
    let (rows, consumed) = decode_group_boundary_rows(&record, &registry, "join", window, true)
        .unwrap()
        .unwrap();
    assert_eq!(consumed, record.len());
    assert_eq!(rows.len(), 2);
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row.timestamp_ns, (index as i64 + 1) * 1_000_000);
        assert_eq!(
            <(String, (Option<i32>, Option<bool>))>::decode(&row.bytes).unwrap(),
            ("a".to_owned(), (Some(index as i32 + 1), Some(true)))
        );
        assert_eq!(row.window.unwrap().end_ns, 10_000_000);
    }
}

#[test]
fn decodes_fragmented_and_concatenated_runner_records() {
    let mut first_body = Vec::new();
    encode_length_prefix(b"first", &mut first_body).unwrap();
    let mut first = Vec::new();
    encode_windowed_value(2, &first_body, &mut first).unwrap();
    let mut second_body = Vec::new();
    encode_length_prefix(b"second", &mut second_body).unwrap();
    let mut second = Vec::new();
    encode_windowed_value(3, &second_body, &mut second).unwrap();

    for prefix in 0..first.len() {
        assert!(decode_boundary_record(&first[..prefix], false)
            .unwrap()
            .is_none());
    }
    let mut joined = first.clone();
    joined.extend_from_slice(&second);
    let (one, consumed) = decode_boundary_record(&joined, false).unwrap().unwrap();
    assert_eq!(one.bytes, b"first");
    assert_eq!(one.timestamp_ns, 2_000_000);
    assert_eq!(consumed, first.len());
    let (two, consumed) = decode_boundary_record(&joined[consumed..], false)
        .unwrap()
        .unwrap();
    assert_eq!(two.bytes, b"second");
    assert_eq!(two.timestamp_ns, 3_000_000);
    assert_eq!(consumed, second.len());
}

#[test]
fn decodes_interval_window_at_runner_boundary() {
    let mut body = Vec::new();
    encode_length_prefix(b"row", &mut body).unwrap();
    let mut record = Vec::new();
    encode_interval_windowed_value(12, 10, 20, &body, &mut record).unwrap();
    for prefix in 0..record.len() {
        assert!(decode_boundary_record(&record[..prefix], true)
            .unwrap()
            .is_none());
    }
    let (value, consumed) = decode_boundary_record(&record, true).unwrap().unwrap();
    assert_eq!(consumed, record.len());
    assert_eq!(value.bytes, b"row");
    assert_eq!(value.timestamp_ns, 12_000_000);
    assert_eq!(
        value.window.unwrap(),
        ironbeam_core::WindowInterval {
            start_ns: 10_000_000,
            end_ns: 20_000_000,
        }
    );
}

#[test]
fn decodes_keyed_fanout_runner_records() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![("a".to_owned(), 7_i32)])
        .unwrap()
        .group_by_key_named("group")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let typed = ("a".to_owned(), 7_i32).encode().unwrap();
    let (key, value) = registry.split_group_input("group", &typed).unwrap();
    let mut body = Vec::new();
    encode_length_prefix(&key, &mut body).unwrap();
    encode_length_prefix(&value, &mut body).unwrap();
    let mut record = Vec::new();
    encode_windowed_value(3, &body, &mut record).unwrap();
    for prefix in 0..record.len() {
        assert!(
            decode_keyed_boundary_record(&record[..prefix], &registry, "group", false)
                .unwrap()
                .is_none()
        );
    }
    let (decoded, consumed) = decode_keyed_boundary_record(&record, &registry, "group", false)
        .unwrap()
        .unwrap();
    assert_eq!(consumed, record.len());
    assert_eq!(decoded.bytes, typed);
    assert_eq!(decoded.timestamp_ns, 3_000_000);
}

#[test]
fn decodes_fragmented_tagged_cogroup_and_interval_group_output() {
    let pipeline = Pipeline::default();
    let left = pipeline
        .from_vec("left", vec![("a".to_owned(), 4_i32)])
        .unwrap();
    let right = pipeline
        .from_vec("right", vec![("a".to_owned(), true)])
        .unwrap();
    ironbeam_core::cogroup_by_key_2_named("cogroup", &left, &right).unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let (key, left_tagged) = registry
        .split_cogroup_input("cogroup", 0, &("a".to_owned(), 4_i32).encode().unwrap())
        .unwrap();
    let (_, right_tagged) = registry
        .split_cogroup_input("cogroup", 1, &("a".to_owned(), true).encode().unwrap())
        .unwrap();
    let mut tagged_body = Vec::new();
    encode_length_prefix(&key, &mut tagged_body).unwrap();
    encode_length_prefix(&left_tagged, &mut tagged_body).unwrap();
    let mut tagged_record = Vec::new();
    encode_interval_windowed_value(4, 0, 10, &tagged_body, &mut tagged_record).unwrap();
    for prefix in 0..tagged_record.len() {
        assert!(
            decode_tagged_boundary_record(&tagged_record[..prefix], true)
                .unwrap()
                .is_none()
        );
    }
    let (decoded_tagged, consumed) = decode_tagged_boundary_record(&tagged_record, true)
        .unwrap()
        .unwrap();
    assert_eq!(consumed, tagged_record.len());
    assert_eq!(decoded_tagged.bytes, tagged_body);

    let mut grouped_body = Vec::new();
    encode_length_prefix(&key, &mut grouped_body).unwrap();
    grouped_body.extend_from_slice(&2_i32.to_be_bytes());
    encode_length_prefix(&left_tagged, &mut grouped_body).unwrap();
    encode_length_prefix(&right_tagged, &mut grouped_body).unwrap();
    let mut grouped_record = Vec::new();
    encode_interval_windowed_value(9, 0, 10, &grouped_body, &mut grouped_record).unwrap();
    for prefix in 0..grouped_record.len() {
        assert!(decode_group_boundary_record(
            &grouped_record[..prefix],
            &registry,
            "cogroup",
            true,
            true
        )
        .unwrap()
        .is_none());
    }
    let (decoded, consumed) =
        decode_group_boundary_record(&grouped_record, &registry, "cogroup", true, true)
            .unwrap()
            .unwrap();
    assert_eq!(consumed, grouped_record.len());
    assert_eq!(decoded.timestamp_ns, 9_000_000);
    assert_eq!(
        decoded.window.unwrap(),
        ironbeam_core::WindowInterval {
            start_ns: 0,
            end_ns: 10_000_000,
        }
    );
    assert_eq!(
        <(String, (Vec<i32>, Vec<bool>))>::decode(&decoded.bytes).unwrap(),
        ("a".to_owned(), (vec![4], vec![true]))
    );
}

#[test]
fn decodes_fixed_and_chunked_grouped_runner_records() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![("a".to_owned(), 1_i32)])
        .unwrap()
        .group_by_key_named("group")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    for chunked in [false, true] {
        let mut body = Vec::new();
        encode_length_prefix(b"a", &mut body).unwrap();
        body.extend_from_slice(&(if chunked { -1_i32 } else { 2_i32 }).to_be_bytes());
        if chunked {
            body.push(2);
        }
        encode_length_prefix(&1_i32.encode().unwrap(), &mut body).unwrap();
        encode_length_prefix(&2_i32.encode().unwrap(), &mut body).unwrap();
        if chunked {
            body.push(0);
        }
        let mut record = Vec::new();
        encode_windowed_value(BEAM_GLOBAL_WINDOW_END_MS, &body, &mut record).unwrap();
        record[12] = crate::coders::PANE_INFO_FIRST_LAST_ON_TIME;
        for prefix in 0..record.len() {
            assert!(decode_group_boundary_record(
                &record[..prefix],
                &registry,
                "group",
                false,
                false
            )
            .unwrap()
            .is_none());
        }
        let (decoded, consumed) =
            decode_group_boundary_record(&record, &registry, "group", false, false)
                .unwrap()
                .unwrap();
        assert_eq!(consumed, record.len());
        assert_eq!(decoded.timestamp_ns, i64::MAX);
        assert_eq!(
            <(String, Vec<i32>)>::decode(&decoded.bytes).unwrap(),
            ("a".to_owned(), vec![1, 2])
        );
    }
    assert_eq!(
        encode_timestamp_millis(i64::MAX).unwrap(),
        BEAM_GLOBAL_WINDOW_END_MS
    );
    assert_eq!(
        decode_timestamp_ns(BEAM_GLOBAL_WINDOW_END_MS).unwrap(),
        i64::MAX
    );
    assert!(decode_timestamp_ns(BEAM_GLOBAL_WINDOW_END_MS - 1).is_err());
    assert!(encode_timestamp_millis(1).is_err());
    let mut empty_chunk = Vec::new();
    encode_length_prefix(b"a", &mut empty_chunk).unwrap();
    empty_chunk.extend_from_slice(&(-1_i32).to_be_bytes());
    empty_chunk.push(0);
    let mut empty_record = Vec::new();
    encode_windowed_value(0, &empty_chunk, &mut empty_record).unwrap();
    let (decoded, _) =
        decode_group_boundary_record(&empty_record, &registry, "group", false, false)
            .unwrap()
            .unwrap();
    assert_eq!(
        <(String, Vec<i32>)>::decode(&decoded.bytes).unwrap(),
        ("a".to_owned(), Vec::new())
    );
    let mut state_backed = Vec::new();
    encode_length_prefix(b"a", &mut state_backed).unwrap();
    state_backed.extend_from_slice(&(-1_i32).to_be_bytes());
    state_backed.push(1);
    encode_length_prefix(&1_i32.encode().unwrap(), &mut state_backed).unwrap();
    encode_varint(u64::MAX, &mut state_backed).unwrap();
    encode_length_prefix(b"runner-token", &mut state_backed).unwrap();
    let mut record = Vec::new();
    encode_windowed_value(0, &state_backed, &mut record).unwrap();
    for prefix in 0..record.len() {
        assert!(
            decode_group_boundary_parts(&record[..prefix], Window::Global)
                .unwrap()
                .is_none()
        );
    }
    let (mut group, consumed) = decode_group_boundary_parts(&record, Window::Global)
        .unwrap()
        .unwrap();
    assert_eq!(consumed, record.len());
    assert_eq!(group.values, vec![1_i32.encode().unwrap()]);
    assert_eq!(
        group.state_token.as_deref(),
        Some(b"runner-token".as_slice())
    );
    let mut suffix = Vec::new();
    encode_length_prefix(&2_i32.encode().unwrap(), &mut suffix).unwrap();
    append_state_backed_values(&mut group, &suffix).unwrap();
    let rows =
        assemble_group_boundary_rows(group, &registry, "group", Window::Global, false).unwrap();
    assert_eq!(
        <(String, Vec<i32>)>::decode(&rows[0].bytes).unwrap(),
        ("a".to_owned(), vec![1, 2])
    );
    assert!(
        decode_group_boundary_record(&record, &registry, "group", false, false)
            .err()
            .unwrap()
            .to_string()
            .contains("Fn State API")
    );
}
