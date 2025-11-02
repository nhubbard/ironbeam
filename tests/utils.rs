use rustflow::utils::OrdF64;

#[test]
fn test_ordf64_from_f64() {
    let val: f64 = 3.14;
    let ord: OrdF64 = val.into();
    assert_eq!(ord.0, 3.14);

    // Test with special values
    let nan: OrdF64 = f64::NAN.into();
    assert!(nan.0.is_nan());

    let inf: OrdF64 = f64::INFINITY.into();
    assert!(inf.0.is_infinite());

    let neg_inf: OrdF64 = f64::NEG_INFINITY.into();
    assert!(neg_inf.0.is_infinite() && neg_inf.0.is_sign_negative());
}

#[test]
fn test_f64_from_ordf64() {
    let ord = OrdF64(2.71);
    let val: f64 = ord.into();
    assert_eq!(val, 2.71);

    // Test with special values
    let nan_ord = OrdF64(f64::NAN);
    let nan_val: f64 = nan_ord.into();
    assert!(nan_val.is_nan());

    let inf_ord = OrdF64(f64::INFINITY);
    let inf_val: f64 = inf_ord.into();
    assert!(inf_val.is_infinite());
}

#[test]
fn test_ordf64_ordering() {
    let a = OrdF64(1.0);
    let b = OrdF64(2.0);
    let c = OrdF64(1.0);

    assert!(a < b);
    assert!(b > a);
    assert_eq!(a, c);

    // Test with negative values
    let neg = OrdF64(-1.0);
    assert!(neg < a);

    // Test that NaN has consistent ordering
    let nan1 = OrdF64(f64::NAN);
    let nan2 = OrdF64(f64::NAN);
    // NaN should compare equal to itself with total_cmp
    assert_eq!(nan1.cmp(&nan2), std::cmp::Ordering::Equal);

    // NaN should be ordered consistently with other values
    let normal = OrdF64(1.0);
    let _ = nan1.cmp(&normal); // Should not panic
}

#[test]
fn test_ordf64_in_collections() {
    use std::collections::BinaryHeap;

    let mut heap = BinaryHeap::new();
    heap.push(OrdF64(3.14));
    heap.push(OrdF64(2.71));
    heap.push(OrdF64(1.41));
    heap.push(OrdF64(0.57));

    // Should pop in descending order
    assert_eq!(heap.pop().unwrap().0, 3.14);
    assert_eq!(heap.pop().unwrap().0, 2.71);
    assert_eq!(heap.pop().unwrap().0, 1.41);
    assert_eq!(heap.pop().unwrap().0, 0.57);
}

#[test]
fn test_ordf64_sorting() {
    let mut values = vec![
        OrdF64(5.0),
        OrdF64(2.0),
        OrdF64(8.0),
        OrdF64(1.0),
        OrdF64(9.0),
    ];

    values.sort();

    assert_eq!(values[0].0, 1.0);
    assert_eq!(values[1].0, 2.0);
    assert_eq!(values[2].0, 5.0);
    assert_eq!(values[3].0, 8.0);
    assert_eq!(values[4].0, 9.0);
}

#[test]
fn test_ordf64_special_values_ordering() {
    let neg_inf = OrdF64(f64::NEG_INFINITY);
    let neg_one = OrdF64(-1.0);
    let zero = OrdF64(0.0);
    let one = OrdF64(1.0);
    let inf = OrdF64(f64::INFINITY);
    let nan = OrdF64(f64::NAN);

    // Test ordering relationships
    assert!(neg_inf < neg_one);
    assert!(neg_one < zero);
    assert!(zero < one);
    assert!(one < inf);

    // NaN should have a consistent position (typically at the end with total_cmp)
    // The exact position depends on f64::total_cmp implementation
    let _ = nan.cmp(&zero); // Should not panic
    let _ = nan.cmp(&inf); // Should not panic
}
