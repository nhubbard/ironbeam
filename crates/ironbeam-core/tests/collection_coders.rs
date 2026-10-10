use ironbeam_core::Element;
use std::collections::{HashMap, HashSet};

#[test]
fn set_and_map_coders_ignore_hash_iteration_order() {
    let a: HashSet<i32> = [3, 1, 2].into_iter().collect();
    let b: HashSet<i32> = [2, 3, 1].into_iter().collect();
    let bytes = a.encode().unwrap();
    assert_eq!(bytes, b.encode().unwrap());
    assert_eq!(HashSet::<i32>::decode(&bytes).unwrap(), a);

    let a: HashMap<String, i32> = [("b".into(), 2), ("a".into(), 1)].into_iter().collect();
    let b: HashMap<String, i32> = [("a".into(), 1), ("b".into(), 2)].into_iter().collect();
    let bytes = a.encode().unwrap();
    assert_eq!(bytes, b.encode().unwrap());
    assert_eq!(HashMap::<String, i32>::decode(&bytes).unwrap(), a);
}

#[test]
fn collection_coders_reject_truncated_and_trailing_bytes() {
    let set: HashSet<i32> = [1, 2].into_iter().collect();
    let mut bytes = set.encode().unwrap();
    assert!(HashSet::<i32>::decode(&bytes[..bytes.len() - 1]).is_err());
    bytes.push(0);
    assert!(HashSet::<i32>::decode(&bytes).is_err());

    let map: HashMap<String, i32> = [("a".into(), 1)].into_iter().collect();
    let mut bytes = map.encode().unwrap();
    assert!(HashMap::<String, i32>::decode(&bytes[..bytes.len() - 1]).is_err());
    bytes.push(0);
    assert!(HashMap::<String, i32>::decode(&bytes).is_err());
}

#[test]
fn float_coders_preserve_special_bit_patterns() {
    for value in [
        0.0_f64,
        -0.0,
        f64::INFINITY,
        f64::from_bits(0x7ff8_0000_0000_0001),
    ] {
        assert_eq!(
            f64::decode(&value.encode().unwrap()).unwrap().to_bits(),
            value.to_bits()
        );
    }
    for value in [0.0_f32, -0.0, f32::INFINITY, f32::from_bits(0x7fc0_0001)] {
        assert_eq!(
            f32::decode(&value.encode().unwrap()).unwrap().to_bits(),
            value.to_bits()
        );
    }
}

#[test]
fn result_coder_preserves_success_and_failure() {
    let ok: Result<i32, String> = Ok(7);
    let failed: Result<i32, String> = Err("bad".into());
    assert_eq!(
        Result::<i32, String>::decode(&ok.encode().unwrap()).unwrap(),
        ok
    );
    assert_eq!(
        Result::<i32, String>::decode(&failed.encode().unwrap()).unwrap(),
        failed
    );
    assert!(Result::<i32, String>::decode(&[2, 0]).is_err());
}
