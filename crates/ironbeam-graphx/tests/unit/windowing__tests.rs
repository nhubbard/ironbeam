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
