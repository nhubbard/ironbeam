use super::*;
use crate::coders::{decode_interval_window, decode_length_prefix, encode_interval_window};

fn nested_string(value: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    encode_length_prefix(value.as_bytes(), &mut bytes).unwrap();
    bytes
}

#[test]
fn timer_clear_and_set_match_beam_nested_field_order() {
    // The pinned Beam TimerCoderImpl writes nested key, nested UTF-8 tag,
    // TupleSequence windows, clear bit, then two InstantCoders and pane.
    let clear = TimerRecord {
        encoded_key: nested_string("key"),
        dynamic_tag: "tag".into(),
        encoded_windows: vec![Vec::new()], // GlobalWindowCoder
        clear: true,
        fire_timestamp_millis: None,
        hold_timestamp_millis: None,
        pane: None,
    };
    let mut bytes = Vec::new();
    clear.encode(&mut bytes).unwrap();
    assert_eq!(bytes, b"\x03key\x03tag\x00\x00\x00\x01\x01");
    let (decoded, consumed) = decode_timer_record(
        &bytes,
        |input| Ok(decode_length_prefix(input)?.1),
        |_| Ok(0),
    )
    .unwrap();
    assert_eq!(consumed, bytes.len());
    assert_eq!(decoded, clear);

    let set = TimerRecord {
        clear: false,
        fire_timestamp_millis: Some(123_000),
        hold_timestamp_millis: Some(456_000),
        pane: Some(PaneInfo::NO_FIRING),
        ..clear
    };
    bytes.clear();
    set.encode(&mut bytes).unwrap();
    assert_eq!(
        bytes,
        [
            b"\x03key\x03tag\x00\x00\x00\x01\x00".as_slice(),
            b"\x80\x00\x00\x00\x00\x01\xe0\x78",
            b"\x80\x00\x00\x00\x00\x06\xf5\x40",
            b"\x0f",
        ]
        .concat()
    );
    let (decoded, consumed) = decode_timer_record(
        &bytes,
        |input| Ok(decode_length_prefix(input)?.1),
        |_| Ok(0),
    )
    .unwrap();
    assert_eq!(consumed, bytes.len());
    assert_eq!(decoded, set);
}

#[test]
fn timer_interval_windows_and_concatenated_records_round_trip() {
    let mut interval = Vec::new();
    encode_interval_window(0, 10, &mut interval).unwrap();
    let timer = TimerRecord {
        encoded_key: nested_string("k"),
        dynamic_tag: String::new(),
        encoded_windows: vec![interval.clone()],
        clear: false,
        fire_timestamp_millis: Some(-5),
        hold_timestamp_millis: Some(9),
        pane: Some(PaneInfo::FIRST_LAST_ON_TIME),
    };
    let mut bytes = Vec::new();
    timer.encode(&mut bytes).unwrap();
    let first_len = bytes.len();
    timer.encode(&mut bytes).unwrap();
    let decode = |input: &[u8]| {
        decode_timer_record(
            input,
            |input| Ok(decode_length_prefix(input)?.1),
            |input| Ok(decode_interval_window(input)?.1),
        )
    };
    let (first, used) = decode(&bytes).unwrap();
    assert_eq!(used, first_len);
    assert_eq!(first, timer);
    let (second, used) = decode(&bytes[first_len..]).unwrap();
    assert_eq!(used, first_len);
    assert_eq!(second, timer);
}

#[test]
fn timer_rejects_malformed_fields_without_partial_write() {
    let invalid = TimerRecord {
        encoded_key: Vec::new(), // Zero-byte key coders are valid.
        dynamic_tag: String::new(),
        encoded_windows: vec![Vec::new()],
        clear: true,
        fire_timestamp_millis: Some(0),
        hold_timestamp_millis: None,
        pane: None,
    };
    let mut output = vec![9];
    assert!(invalid.encode(&mut output).is_err());
    assert_eq!(output, [9]);
    let valid = TimerRecord {
        fire_timestamp_millis: None,
        ..invalid
    };
    let mut bytes = Vec::new();
    valid.encode(&mut bytes).unwrap();
    let decode = |input: &[u8]| decode_timer_record(input, |_| Ok(0), |_| Ok(0));
    assert_eq!(decode(&bytes).unwrap().0, valid);
    assert!(decode(&bytes[..bytes.len() - 1]).is_err());
    let mut bad_bit = bytes.clone();
    *bad_bit.last_mut().unwrap() = 2;
    assert!(decode(&bad_bit).is_err());
    let mut bad_count = bytes;
    bad_count[1..5].copy_from_slice(&(-1_i32).to_be_bytes());
    assert!(decode(&bad_count).is_err());
}

#[test]
fn partial_timer_decoder_waits_at_each_field_boundary() {
    let record = TimerRecord {
        encoded_key: nested_string("key"),
        dynamic_tag: "tag".into(),
        encoded_windows: vec![Vec::new()],
        clear: false,
        fire_timestamp_millis: Some(123),
        hold_timestamp_millis: Some(456),
        pane: Some(PaneInfo {
            is_first: false,
            is_last: false,
            index: 300,
            nonspeculative_index: 299,
            ..PaneInfo::FIRST_LAST_ON_TIME
        }),
    };
    let mut bytes = Vec::new();
    record.encode(&mut bytes).unwrap();
    for end in 0..bytes.len() {
        assert!(
            try_decode_timer_record(&bytes[..end], try_nested_bytes_len, |_| Ok(Some(0)),)
                .unwrap()
                .is_none(),
            "prefix ending at byte {end} appeared complete"
        );
    }
    let (decoded, used) = try_decode_timer_record(&bytes, try_nested_bytes_len, |_| Ok(Some(0)))
        .unwrap()
        .unwrap();
    assert_eq!(used, bytes.len());
    assert_eq!(decoded, record);
}
