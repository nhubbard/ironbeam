use ironbeam_core::{Element, OrdF64};
use std::collections::HashSet;

#[test]
fn ordered_f64_has_consistent_total_order_equality_hash_and_coder() {
    let negative_zero = OrdF64(-0.0);
    let positive_zero = OrdF64(0.0);
    assert!(negative_zero < positive_zero);
    assert_ne!(negative_zero, positive_zero);
    let first_nan = OrdF64(f64::from_bits(0x7ff8_0000_0000_0001));
    let same_nan = OrdF64(f64::from_bits(0x7ff8_0000_0000_0001));
    let other_nan = OrdF64(f64::from_bits(0x7ff8_0000_0000_0002));
    assert_eq!(first_nan, same_nan);
    assert_ne!(first_nan, other_nan);
    let unique = HashSet::from([negative_zero, positive_zero, first_nan, same_nan, other_nan]);
    assert_eq!(unique.len(), 4);
    for value in unique {
        let encoded = value.encode().unwrap();
        assert_eq!(encoded.len(), value.encoded_len().unwrap());
        assert_eq!(OrdF64::decode(&encoded).unwrap(), value);
    }
    assert!(OrdF64::decode(&[0; 7]).is_err());
}
