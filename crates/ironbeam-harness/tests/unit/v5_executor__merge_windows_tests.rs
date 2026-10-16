use super::*;

fn encoded_request(windows: &[(i64, i64)], chunked: bool) -> Vec<u8> {
    let mut bytes = Vec::new();
    encode_length_prefix(b"nonce", &mut bytes).unwrap();
    if chunked {
        bytes.extend_from_slice(&(-1_i32).to_be_bytes());
        encode_varint(windows.len() as u64, &mut bytes).unwrap();
    } else {
        bytes.extend_from_slice(&(windows.len() as i32).to_be_bytes());
    }
    for &(start, end) in windows {
        encode_interval_window(start, end, &mut bytes).unwrap();
    }
    if chunked {
        encode_varint(0, &mut bytes).unwrap();
    }
    bytes
}

#[test]
fn merge_request_waits_for_complete_known_and_chunked_iterables() {
    let windows = [(0, 5), (4, 9), (9, 14)];
    for chunked in [false, true] {
        let request = encoded_request(&windows, chunked);
        for size in 0..request.len() {
            assert!(try_merge_windows_body(&request[..size]).unwrap().is_none());
        }
        let (used, nonce, decoded) = try_merge_windows_body(&request).unwrap().unwrap();
        assert_eq!(used, request.len());
        assert_eq!(nonce, &request[..6]);
        assert_eq!(decoded, windows);
    }
    let mut malformed = encoded_request(&[], false);
    malformed[6..10].copy_from_slice(&(-2_i32).to_be_bytes());
    assert!(try_merge_windows_body(&malformed).is_err());
}

#[test]
fn session_merge_keeps_touching_windows_separate_and_merges_bridges() {
    let request = encoded_request(&[(9, 14), (4, 9), (0, 5), (20, 25), (0, 5)], false);
    let (_, nonce, windows) = try_merge_windows_body(&request).unwrap().unwrap();
    let result = merge_windows_body(nonce, windows).unwrap();
    let mut expected = Vec::new();
    encode_length_prefix(b"nonce", &mut expected).unwrap();
    expected.extend_from_slice(&2_i32.to_be_bytes());
    encode_interval_window(9, 14, &mut expected).unwrap();
    encode_interval_window(20, 25, &mut expected).unwrap();
    expected.extend_from_slice(&1_i32.to_be_bytes());
    encode_interval_window(0, 9, &mut expected).unwrap();
    expected.extend_from_slice(&2_i32.to_be_bytes());
    encode_interval_window(0, 5, &mut expected).unwrap();
    encode_interval_window(4, 9, &mut expected).unwrap();
    assert_eq!(result, expected);

    let bridge_request = encoded_request(&[(0, 5), (8, 13), (4, 9)], false);
    let (_, nonce, windows) = try_merge_windows_body(&bridge_request)
        .unwrap().unwrap();
    let result = merge_windows_body(nonce, windows).unwrap();
    let mut expected = Vec::new();
    encode_length_prefix(b"nonce", &mut expected).unwrap();
    expected.extend_from_slice(&0_i32.to_be_bytes());
    expected.extend_from_slice(&1_i32.to_be_bytes());
    encode_interval_window(0, 13, &mut expected).unwrap();
    expected.extend_from_slice(&3_i32.to_be_bytes());
    for (start, end) in [(0, 5), (4, 9), (8, 13)] {
        encode_interval_window(start, end, &mut expected).unwrap();
    }
    assert_eq!(result, expected);
}

#[test]
fn merge_request_rejects_invalid_windows_and_counts() {
    let mut zero = encoded_request(&[(0, 0)], false);
    assert!(try_merge_windows_body(&zero).is_err());
    zero = encoded_request(&[], false);
    zero[6..10].copy_from_slice(&(100_001_i32).to_be_bytes());
    assert!(try_merge_windows_body(&zero).is_err());
    let mut chunked = encoded_request(&[], true);
    chunked.truncate(10);
    chunked.extend_from_slice(&[0xff; 10]);
    assert!(try_merge_windows_body(&chunked).is_err());
}
