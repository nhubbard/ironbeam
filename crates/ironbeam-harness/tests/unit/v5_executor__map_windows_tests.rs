use super::*;

#[test]
fn map_windows_body_waits_for_complete_key_and_interval() {
    let mut body = Vec::new();
    encode_length_prefix(b"window-key", &mut body).unwrap();
    let key_len = body.len();
    crate::coders::encode_interval_window(100, 200, &mut body).unwrap();
    for len in 0..key_len {
        assert!(try_map_window_body_len(&body[..len], WindowCoder::Global)
            .unwrap()
            .is_none());
    }
    assert_eq!(
        try_map_window_body_len(&body, WindowCoder::Global).unwrap(),
        Some((key_len, key_len))
    );
    for len in key_len..body.len() {
        assert!(try_map_window_body_len(&body[..len], WindowCoder::Interval)
            .unwrap()
            .is_none());
    }
    assert_eq!(
        try_map_window_body_len(&body, WindowCoder::Interval).unwrap(),
        Some((key_len, body.len()))
    );
    assert!(try_map_window_body_len(&[0xff; 10], WindowCoder::Global).is_err());
}

#[test]
fn fixed_and_sliding_mapping_use_main_window_max_timestamp() {
    let fixed = WindowMapping::Fixed { size_millis: 100, offset_millis: 20 };
    assert_eq!(mapped_target_window(121, fixed).unwrap(), (120, 220));
    assert_eq!(mapped_target_window(120, fixed).unwrap(), (20, 120));
    assert_eq!(mapped_target_window(-1, fixed).unwrap(), (-80, 20));

    let sliding = WindowMapping::Sliding { size_millis: 150, period_millis: 100, offset_millis: 20 };
    assert_eq!(mapped_target_window(221, sliding).unwrap(), (120, 270));
    assert_eq!(mapped_target_window(220, sliding).unwrap(), (120, 270));
    assert_eq!(mapped_target_window(-1, sliding).unwrap(), (-80, 70));
    assert_eq!(mapped_target_window(
        75,
        WindowMapping::Sliding { size_millis: 50, period_millis: 100, offset_millis: 0 }
    ).unwrap(), (100, 150));
    assert_eq!(mapped_target_window(
        -25,
        WindowMapping::Sliding { size_millis: 50, period_millis: 100, offset_millis: 0 }
    ).unwrap(), (0, 50));
    assert!(mapped_target_window(i64::MAX, WindowMapping::Fixed { size_millis: 100, offset_millis: 0 }).is_err());
}
