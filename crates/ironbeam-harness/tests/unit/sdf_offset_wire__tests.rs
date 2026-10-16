use super::*;

#[test]
fn round_trip_matches_beam_kv_coder_shape() {
    let range = OffsetRange::new(2, 5).unwrap();
    let bytes = pair_with_restriction(&[0x2a], range).unwrap();
    let restriction = range.encode_beam().unwrap();
    assert_eq!(&bytes[..2], &[1, 0x2a]);
    assert_eq!(bytes[2] as usize, restriction.len());
    assert_eq!(&bytes[3..], restriction);
    assert_eq!(decode_pair(&bytes).unwrap().restriction, range);
}

#[test]
fn splitting_and_drain_preserve_every_bounded_position() {
    let range = OffsetRange::new(-2, 5).unwrap();
    let pair = pair_with_restriction(b"item", range).unwrap();
    let chunks = split_and_size(&pair, 3).unwrap();
    let restrictions: Vec<_> = chunks
        .iter()
        .map(|chunk| decode_sized(chunk).unwrap().pair.restriction)
        .collect();
    assert_eq!(
        restrictions,
        [
            OffsetRange::new(-2, 1).unwrap(),
            OffsetRange::new(1, 4).unwrap(),
            OffsetRange::new(4, 5).unwrap(),
        ]
    );
    for chunk in chunks {
        let sized = decode_sized(&chunk).unwrap();
        assert_eq!(sized.pair.element, b"item");
        assert_eq!(
            sized.size,
            (sized.pair.restriction.stop - sized.pair.restriction.start) as f64
        );
        assert_eq!(truncate_sized(&chunk).unwrap(), chunk);
    }
    assert!(split_and_size(&pair, 0).is_err());
    let empty = pair_with_restriction(b"item", OffsetRange::new(2, 2).unwrap()).unwrap();
    assert!(split_and_size(&empty, 3).unwrap().is_empty());
}

#[test]
fn full_signed_span_splits_without_overflow() {
    let pair =
        pair_with_restriction(b"item", OffsetRange::new(i64::MIN, i64::MAX).unwrap()).unwrap();
    let chunks = split_and_size(&pair, u64::MAX - 1).unwrap();
    assert_eq!(chunks.len(), 2);
    assert_eq!(
        decode_sized(&chunks[0]).unwrap().pair.restriction.start,
        i64::MIN
    );
    assert_eq!(
        decode_sized(&chunks[0]).unwrap().pair.restriction.stop,
        i64::MAX - 1
    );
    assert_eq!(
        decode_sized(&chunks[1]).unwrap().pair.restriction.start,
        i64::MAX - 1
    );
    assert_eq!(
        decode_sized(&chunks[1]).unwrap().pair.restriction.stop,
        i64::MAX
    );
}

#[test]
fn malformed_restrictions_and_size_are_rejected() {
    let pair = pair_with_restriction(b"item", OffsetRange::new(0, 2).unwrap()).unwrap();
    let mut trailing = pair.clone();
    trailing.push(0);
    assert!(decode_pair(&trailing).is_err());
    assert!(decode_pair(&pair[..pair.len() - 1]).is_err());
    let mut invalid_range = vec![0, 2, 5, 2];
    assert!(decode_pair(&invalid_range).is_err());
    invalid_range[2] = 3;
    assert!(decode_pair(&invalid_range).is_err());
    let mut sized = encode_sized(&SizedOffsetPair {
        pair: decode_pair(&pair).unwrap(),
        size: 2.0,
    })
    .unwrap();
    let size_start = sized.len() - 8;
    sized[size_start..].copy_from_slice(&f64::NAN.to_be_bytes());
    assert!(decode_sized(&sized).is_err());
}
