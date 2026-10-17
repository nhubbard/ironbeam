// Unit tests for src/sdf_wire.rs.
use super::*;

#[test]
fn pair_and_sized_wire_match_beam_coder_shape() {
    let pair = SourcePair {
        element: vec![0x2a],
        restriction: SourceRestriction::initial("events"),
    };
    let body = encode_pair(&pair).unwrap();
    assert_eq!(&body[..2], &[1, 0x2a]);
    assert_eq!(decode_pair("events", &body).unwrap(), pair);

    let sized = SizedSourcePair { pair, size: 1.5 };
    let body = encode_sized(&sized).unwrap();
    assert_eq!(&body[body.len() - 8..], &1.5_f64.to_be_bytes());
    assert_eq!(decode_sized("events", &body).unwrap(), sized);
}

#[test]
fn one_split_source_stages_preserve_the_restriction() {
    let pair = pair_with_restriction("events", &[]).unwrap();
    let sized = split_and_size("events", &pair).unwrap();
    let decoded = decode_sized("events", &sized).unwrap();
    assert_eq!(decoded.pair.element, Vec::<u8>::new());
    assert_eq!(
        decoded.pair.restriction,
        SourceRestriction::initial("events")
    );
    assert_eq!(decoded.size, 1.0);
    assert!(pair_with_restriction("events", b"not an Impulse").is_err());
}

#[test]
fn drain_drops_unbounded_restriction_after_validating_wire_identity() {
    let pair = pair_with_restriction("events", &[]).unwrap();
    let sized = split_and_size("events", &pair).unwrap();
    assert_eq!(truncate_sized("events", &sized).unwrap(), None);
    assert!(truncate_sized("different-source", &sized).is_err());
    assert!(truncate_sized("events", &sized[..sized.len() - 1]).is_err());
}

#[test]
fn fragmented_pair_and_sized_bodies_report_complete_lengths() {
    let pair = pair_with_restriction("events", &[]).unwrap();
    for len in 0..pair.len() {
        assert_eq!(try_pair_body_len(&pair[..len]).unwrap(), None);
    }
    assert_eq!(try_pair_body_len(&pair).unwrap(), Some(pair.len()));
    let sized = split_and_size("events", &pair).unwrap();
    for len in 0..sized.len() {
        assert_eq!(try_sized_body_len(&sized[..len]).unwrap(), None);
    }
    assert_eq!(try_sized_body_len(&sized).unwrap(), Some(sized.len()));
    assert!(try_pair_body_len(&[0xff; 10]).is_err());
}

#[test]
fn rejects_wrong_identity_trailing_bytes_and_invalid_size() {
    let pair = SourcePair {
        element: Vec::new(),
        restriction: SourceRestriction::initial("events"),
    };
    let mut body = encode_pair(&pair).unwrap();
    assert!(decode_pair("other", &body).is_err());
    body.push(0);
    assert!(decode_pair("events", &body).is_err());
    for size in [f64::NAN, f64::INFINITY, -1.0] {
        assert!(encode_sized(&SizedSourcePair {
            pair: pair.clone(),
            size
        })
        .is_err());
    }
    let mut sized = encode_sized(&SizedSourcePair { pair, size: 1.0 }).unwrap();
    sized.pop();
    assert!(decode_sized("events", &sized).is_err());
}
