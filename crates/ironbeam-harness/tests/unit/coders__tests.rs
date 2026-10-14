// Unit tests for src/coders.rs.
use super::*;

fn enc_vi(n: u64) -> Vec<u8> {
    let mut v = Vec::new();
    encode_varint(n, &mut v).unwrap();
    v
}

#[test]
fn varint_vectors() {
    // Known values from protobuf spec / Beam tests.
    assert_eq!(enc_vi(0), vec![0x00]);
    assert_eq!(enc_vi(1), vec![0x01]);
    assert_eq!(enc_vi(127), vec![0x7F]);
    assert_eq!(enc_vi(128), vec![0x80, 0x01]);
    assert_eq!(enc_vi(300), vec![0xAC, 0x02]);
    assert_eq!(
        enc_vi(u64::MAX),
        vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]
    );
}

#[test]
fn varint_roundtrip() {
    for n in [0u64, 1, 42, 127, 128, 16384, u32::MAX as u64, u64::MAX] {
        let bytes = enc_vi(n);
        let mut r = std::io::Cursor::new(&bytes);
        assert_eq!(decode_varint(&mut r).unwrap(), n, "failed for {n}");
    }
}

#[test]
fn varint_truncated_errors() {
    // 0x80 alone says "more bytes coming" but there are none.
    let bytes = [0x80u8];
    let mut r = std::io::Cursor::new(&bytes[..]);
    assert!(decode_varint(&mut r).is_err());
}

#[test]
fn varint_rejects_out_of_range_tenth_byte() {
    let mut bytes = [0xffu8; 10];
    bytes[9] = 0x02;
    assert!(decode_varint(&mut bytes.as_slice()).is_err());
    bytes[9] = 0x81;
    assert!(decode_varint(&mut bytes.as_slice()).is_err());
}

#[test]
fn length_prefix_roundtrip() {
    for body in [
        &b""[..],
        &b"hello"[..],
        &[0u8; 300][..], // crosses 1-byte varint boundary
    ] {
        let mut encoded = Vec::new();
        encode_length_prefix(body, &mut encoded).unwrap();
        let (decoded, consumed) = decode_length_prefix(&encoded).unwrap();
        assert_eq!(decoded, body);
        assert_eq!(consumed, encoded.len());
    }
}

#[test]
fn length_prefix_refuses_truncated() {
    // declared length 10, but only 3 bytes present
    let encoded = [10u8, b'a', b'b', b'c'];
    assert!(decode_length_prefix(&encoded).is_err());
}

#[test]
fn length_prefix_rejects_oversized_header() {
    let bytes = enc_vi(u64::MAX);
    assert!(decode_length_prefix(&bytes).is_err());
}

#[test]
fn interval_window_matches_beam_end_and_duration_wire_shape() {
    let mut encoded = Vec::new();
    encode_interval_window(-10, 0, &mut encoded).unwrap();
    assert_eq!(encoded, [0x80, 0, 0, 0, 0, 0, 0, 0, 0x0a]);
    assert_eq!(decode_interval_window(&encoded).unwrap(), ((-10, 0), 9));

    encoded.clear();
    encode_interval_window(1_000, 2_000, &mut encoded).unwrap();
    assert_eq!(
        decode_interval_window(&encoded).unwrap(),
        ((1_000, 2_000), encoded.len())
    );
    assert_eq!(&encoded[8..], &[0xe8, 0x07]);
}

#[test]
fn interval_window_rejects_invalid_ranges_and_wire_values() {
    assert!(encode_interval_window(5, 5, &mut Vec::new()).is_err());
    assert!(encode_interval_window(5, 4, &mut Vec::new()).is_err());
    assert!(encode_interval_window(i64::MIN, i64::MAX, &mut Vec::new()).is_err());
    assert!(decode_interval_window(&[0; 7]).is_err());
    assert!(decode_interval_window(&[0x80, 0, 0, 0, 0, 0, 0, 0]).is_err());
    assert!(decode_interval_window(&[0x80, 0, 0, 0, 0, 0, 0, 0, 0]).is_err());
    let mut overflow = i64::MIN.to_be_bytes().to_vec();
    overflow[0] ^= 0x80;
    overflow.push(1);
    assert!(decode_interval_window(&overflow).is_err());
}

#[test]
fn windowed_value_roundtrip() {
    for ts in [0i64, -1, 1, i64::MIN, i64::MAX, 1_700_000_000_000] {
        let element = b"the-element-bytes";
        let mut out = Vec::new();
        encode_windowed_value(ts, element, &mut out).unwrap();
        assert_eq!(out.len(), 13 + element.len());
        let (wv, hdr) = decode_windowed_value(&out).unwrap();
        assert_eq!(wv.timestamp_millis, ts);
        assert_eq!(wv.element, element);
        assert_eq!(hdr, 13);
    }
}

#[test]
fn interval_windowed_value_roundtrips_across_fragmented_headers() {
    let mut encoded = Vec::new();
    encode_interval_windowed_value(12, 10, 20, b"row", &mut encoded).unwrap();
    for prefix in 0..22 {
        assert!(try_decode_windowed_value(&encoded[..prefix], true)
            .unwrap()
            .is_none());
    }
    let (value, header) = try_decode_windowed_value(&encoded, true).unwrap().unwrap();
    assert_eq!(header, 22);
    assert_eq!(value.timestamp_millis, 12);
    assert_eq!(value.interval_millis, Some((10, 20)));
    assert_eq!(value.element, b"row");
    assert!(decode_windowed_value(&encoded).is_err());
}

#[test]
fn windowed_value_matches_beam_fixture_shape() {
    // Beam standard_coders.yaml windowed_value + global_window + varint(1)
    // fixture: 7F FF FF FF FF FF FF FF  00 00 00 01  0F  01
    // The wrapped element here is varint(1) = [0x01], and the timestamp is
    // -1 (Long.MAX_VALUE after XOR).
    let mut out = Vec::new();
    encode_windowed_value(-1, &[0x01], &mut out).unwrap();
    assert_eq!(
        out,
        vec![
            0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // timestamp = -1
            0x00, 0x00, 0x00, 0x01, // one window
            0x0F, // NO_FIRING pane
            0x01, // element body (varint(1))
        ]
    );
}

#[test]
fn windowed_value_rejects_non_global_window_count() {
    let mut out = Vec::new();
    encode_windowed_value(0, &[], &mut out).unwrap();
    out[8..12].copy_from_slice(&2i32.to_be_bytes()); // claim 2 windows
    assert!(decode_windowed_value(&out).is_err());
}

#[test]
fn windowed_value_accepts_runner_pane_timings_and_indices() {
    let mut out = Vec::new();
    encode_windowed_value(0, b"value", &mut out).unwrap();
    out[12] = 0x00; // A valid single-byte early pane.
    assert_eq!(decode_windowed_value(&out).unwrap().0.element, b"value");

    out[12] = 0x10; // one pane index follows.
    out.insert(13, 7);
    assert_eq!(decode_windowed_value(&out).unwrap().0.element, b"value");

    out[12] = 0x20; // two pane indices follow.
    out.insert(14, 8);
    assert_eq!(decode_windowed_value(&out).unwrap().0.element, b"value");
    out[12] = 0x30;
    assert!(decode_windowed_value(&out).is_err());

    out.truncate(13);
    out[12] = 0x10;
    assert!(try_decode_windowed_value(&out, false).unwrap().is_none());
    out.extend([0x80; 10]);
    assert!(decode_windowed_value(&out).is_err());
}

#[test]
fn windowed_value_accepts_single_on_time_grouped_pane() {
    let mut out = Vec::new();
    encode_windowed_value(0, b"grouped", &mut out).unwrap();
    out[12] = PANE_INFO_FIRST_LAST_ON_TIME;
    let (decoded, consumed) = decode_windowed_value(&out).unwrap();
    assert_eq!(decoded.element, b"grouped");
    assert_eq!(consumed, 13);
    assert_eq!(decoded.pane, PaneInfo::FIRST_LAST_ON_TIME);
}

#[test]
fn beam_pane_headers_round_trip_all_timings_and_index_forms() {
    let panes = [
        PaneInfo::NO_FIRING,
        PaneInfo::FIRST_LAST_ON_TIME,
        PaneInfo {
            is_first: true,
            is_last: false,
            timing: PaneTiming::Early,
            index: 0,
            nonspeculative_index: -1,
        },
        PaneInfo {
            is_first: false,
            is_last: false,
            timing: PaneTiming::OnTime,
            index: 7,
            nonspeculative_index: 7,
        },
        PaneInfo {
            is_first: false,
            is_last: true,
            timing: PaneTiming::Late,
            index: 128,
            nonspeculative_index: 3,
        },
    ];
    for pane in panes {
        let mut encoded = Vec::new();
        encode_interval_windowed_value_with_pane(2, 0, 10, pane, b"row", &mut encoded).unwrap();
        for prefix in 0..encoded.len() - 3 {
            assert!(try_decode_windowed_value(&encoded[..prefix], true)
                .unwrap()
                .is_none_or(|(_, header)| prefix >= header));
        }
        let (decoded, _) = try_decode_windowed_value(&encoded, true).unwrap().unwrap();
        assert_eq!(decoded.pane, pane);
        assert_eq!(decoded.element, b"row");
    }
    assert!(encode_pane_info(
        PaneInfo {
            index: 3,
            ..PaneInfo::NO_FIRING
        },
        &mut Vec::new()
    )
    .is_err());
    assert!(encode_pane_info(
        PaneInfo {
            nonspeculative_index: 0,
            timing: PaneTiming::Early,
            ..PaneInfo::NO_FIRING
        },
        &mut Vec::new()
    )
    .is_err());
}

#[test]
fn kv_is_concatenation() {
    let k = {
        let mut v = Vec::new();
        encode_length_prefix(b"foo", &mut v).unwrap();
        v
    };
    let val = {
        let mut v = Vec::new();
        encode_length_prefix(b"bar", &mut v).unwrap();
        v
    };
    let mut kv = Vec::new();
    encode_kv(&k, &val, &mut kv).unwrap();
    assert_eq!(&kv[..k.len()], k.as_slice());
    assert_eq!(&kv[k.len()..], val.as_slice());
}
