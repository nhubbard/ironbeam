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
