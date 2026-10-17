// Unit tests for src/windowing.rs.
use super::*;

#[test]
fn fixed_strategy_encodes_millisecond_size_and_normalized_offset() {
    let strategy = fixed_windowing_strategy(2_500_000_000, -500_000_000).unwrap();
    assert_eq!(strategy.window_coder_id, ids::CODER_INTERVAL_WINDOW);
    assert_eq!(
        strategy.output_time,
        output_time::Enum::EarliestInPane as i32
    );
    let spec = strategy.window_fn.unwrap();
    assert_eq!(spec.urn, urns::WINDOW_FN_FIXED);
    let payload = FixedWindowsPayload::decode(spec.payload.as_slice()).unwrap();
    assert_eq!(
        payload.size.unwrap(),
        (prost_types::Duration {
            seconds: 2,
            nanos: 500_000_000
        })
    );
    assert_eq!(
        payload.offset.unwrap(),
        (prost_types::Timestamp {
            seconds: 2,
            nanos: 0
        })
    );
}

#[test]
fn global_groups_also_use_earliest_input_time() {
    assert_eq!(
        global_windowing_strategy().output_time,
        output_time::Enum::EarliestInPane as i32
    );
}

#[test]
fn fixed_strategy_rejects_submillisecond_and_nonpositive_windows() {
    assert!(fixed_windowing_strategy(0, 0).is_err());
    assert!(fixed_windowing_strategy(-1_000_000, 0).is_err());
    assert!(fixed_windowing_strategy(1_000_001, 0).is_err());
    assert!(fixed_windowing_strategy(1_000_000, 1).is_err());
}

#[test]
fn sliding_function_encodes_overlap_and_normalized_negative_offset() {
    let spec = sliding_window_fn(5_000_000_000, 2_000_000_000, -500_000_000).unwrap();
    assert_eq!(spec.urn, urns::WINDOW_FN_SLIDING);
    let payload = SlidingWindowsPayload::decode(spec.payload.as_slice()).unwrap();
    assert_eq!(payload.size.unwrap().seconds, 5);
    assert_eq!(payload.period.unwrap().seconds, 2);
    assert_eq!(payload.offset.unwrap().seconds, 1);
    assert_eq!(
        sliding_window_fn(1_000_000, 0, 0).unwrap_err(),
        "sliding window period must be a positive whole millisecond"
    );
    assert!(sliding_window_fn(1_000_001, 1_000_000, 0).is_err());
    assert!(sliding_window_fn(1_000_000, 1_000_000, 1).is_err());
}

#[test]
fn session_function_encodes_gap_and_rejects_submillisecond_gap() {
    let spec = session_window_fn(2_500_000_000).unwrap();
    assert_eq!(spec.urn, urns::WINDOW_FN_SESSIONS);
    let payload = SessionWindowsPayload::decode(spec.payload.as_slice()).unwrap();
    assert_eq!(
        payload.gap_size.unwrap(),
        (prost_types::Duration {
            seconds: 2,
            nanos: 500_000_000,
        })
    );
    assert!(session_window_fn(0).is_err());
    assert!(session_window_fn(1_000_001).is_err());
    let strategy = session_windowing_strategy(2_500_000_000).unwrap();
    assert_eq!(strategy.merge_status, merge_status::Enum::NeedsMerge as i32);
    assert_eq!(strategy.window_coder_id, ids::CODER_INTERVAL_WINDOW);
    assert!(strategy.assigns_to_one_window);
    assert_eq!(strategy.allowed_lateness, 0);
}
