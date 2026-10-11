use ironbeam_core::{Element, Error, ErrorKind};
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

    let map: HashMap<String, i32> = std::iter::once(("a".into(), 1)).collect();
    let mut bytes = map.encode().unwrap();
    assert!(HashMap::<String, i32>::decode(&bytes[..bytes.len() - 1]).is_err());
    bytes.push(0);
    assert!(HashMap::<String, i32>::decode(&bytes).is_err());
}

#[test]
fn collection_coders_reject_missing_headers_and_incomplete_tuple_parts() {
    assert_eq!(
        HashSet::<i32>::decode(&[0; 7]).unwrap_err().kind,
        ErrorKind::Incompatible
    );
    assert_eq!(
        HashMap::<i32, i32>::decode(&[0; 7]).unwrap_err().kind,
        ErrorKind::Incompatible
    );

    let mut tuple = (1_i32, 2_i32, 3_i32).encode().unwrap();
    tuple.push(0);
    assert_eq!(
        <(i32, i32, i32)>::decode(&tuple).unwrap_err().kind,
        ErrorKind::Incompatible
    );
    assert_eq!(
        <(i32, i32, i32)>::decode(&tuple[..tuple.len() - 5])
            .unwrap_err()
            .kind,
        ErrorKind::Incompatible
    );
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
    assert!(Result::<i32, String>::decode(&[]).is_err());
    let mut trailing = ok.encode().unwrap();
    trailing.push(0);
    assert_eq!(
        Result::<i32, String>::decode(&trailing).unwrap_err().kind,
        ErrorKind::Incompatible
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
struct Colliding(i32);
impl Element for Colliding {
    const TYPE_ID: &'static str = "test/colliding";
    const CODER_ID: &'static str = "test/colliding-v1";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        Ok(vec![0])
    }
    fn decode(_: &[u8]) -> Result<Self, Error> {
        Ok(Self(0))
    }
}

#[test]
fn collection_coders_reject_ambiguous_encoded_keys() {
    let set: HashSet<Colliding> = [Colliding(1), Colliding(2)].into();
    assert_eq!(set.encode().unwrap_err().kind, ErrorKind::Incompatible);
    let map: HashMap<Colliding, i32> = [(Colliding(1), 1), (Colliding(2), 2)].into();
    assert_eq!(map.encode().unwrap_err().kind, ErrorKind::Incompatible);
}

#[test]
fn collection_coders_reject_duplicate_wire_members() {
    let one_set: HashSet<i32> = [7].into();
    let mut set_bytes = one_set.encode().unwrap();
    set_bytes[..8].copy_from_slice(&2_u64.to_le_bytes());
    let member = set_bytes[8..].to_vec();
    set_bytes.extend_from_slice(&member);
    assert_eq!(
        HashSet::<i32>::decode(&set_bytes).unwrap_err().kind,
        ErrorKind::Incompatible
    );

    let one_map: HashMap<i32, i32> = [(7, 9)].into();
    let mut map_bytes = one_map.encode().unwrap();
    map_bytes[..8].copy_from_slice(&2_u64.to_le_bytes());
    let entry = map_bytes[8..].to_vec();
    map_bytes.extend_from_slice(&entry);
    assert_eq!(
        HashMap::<i32, i32>::decode(&map_bytes).unwrap_err().kind,
        ErrorKind::Incompatible
    );
}

#[test]
fn nested_container_coders_reject_unsupported_element_types() {
    #[derive(Eq, PartialEq, Hash)]
    struct Opaque;
    impl Element for Opaque {
        const TYPE_ID: &'static str = "test/opaque-collection";
        const CODER_ID: &'static str = "test/opaque-collection";
    }
    assert!(matches!(
        Vec::<Opaque>::decode(&[]).err().unwrap().kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    assert!(Vec::<Opaque>::new().encode().is_err());
    assert!(Option::<Opaque>::decode(&[]).is_err());
    assert!(Some(Opaque).encode().is_err());
    assert!(HashSet::<Opaque>::decode(&[]).is_err());
}
