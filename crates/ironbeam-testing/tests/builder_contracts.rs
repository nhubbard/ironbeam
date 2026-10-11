use ironbeam_testing::{
    KVTestDataBuilder, TestDataBuilder, pseudo_random_data, pseudo_random_kvs, sequential_data,
    sequential_kvs, skewed_kvs,
};

#[test]
fn numeric_and_keyed_builders_keep_order_and_repetition() {
    let builder = TestDataBuilder::<i32>::new();
    assert!(builder.is_empty());
    let builder = builder
        .add_range(1..=3)
        .add_value(4)
        .add_values(vec![5])
        .add_repeated(6, 2);
    assert_eq!(builder.len(), 7);
    assert_eq!(builder.build(), vec![1, 2, 3, 4, 5, 6, 6]);

    let builder = KVTestDataBuilder::new();
    assert!(builder.is_empty());
    let builder = builder
        .add_kv("a", 1)
        .add_kvs(vec![("b", 2)])
        .add_key_with_values("a", vec![3, 4]);
    assert_eq!(builder.len(), 4);
    assert_eq!(
        builder.build(),
        vec![("a", 1), ("b", 2), ("a", 3), ("a", 4)]
    );
}

#[test]
fn deterministic_generators_handle_empty_and_wide_ranges() {
    assert_eq!(sequential_data(2, 1), Vec::<i32>::new());
    assert_eq!(
        sequential_kvs("k", 1, 2),
        vec![("k_1".into(), 1), ("k_2".into(), 2)]
    );
    assert_eq!(skewed_kvs(0), Vec::<(String, i32)>::new());
    assert_eq!(
        skewed_kvs(6)
            .iter()
            .filter(|(key, _)| key == "hot_key")
            .count(),
        3
    );

    let small = pseudo_random_data(20, -5, 5);
    assert_eq!(small, pseudo_random_data(20, -5, 5));
    assert!(small.iter().all(|value| (-5..5).contains(value)));
    let wide = pseudo_random_data(20, i32::MIN, i32::MAX);
    assert!(wide.iter().all(|value| *value < i32::MAX));
    let keyed = pseudo_random_kvs(3, 4, i32::MIN, i32::MAX);
    assert_eq!(keyed.len(), 12);
    assert_eq!(keyed.iter().filter(|(key, _)| key == "key_2").count(), 4);
    assert!(std::panic::catch_unwind(|| pseudo_random_data(1, 5, 5)).is_err());
    assert!(std::panic::catch_unwind(|| pseudo_random_kvs(1, 1, 5, 4)).is_err());
}
