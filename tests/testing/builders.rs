use ironbeam::testing::*;

#[test]
fn test_data_builder_basic() {
    let data = TestDataBuilder::new()
        .add_value(1)
        .add_value(2)
        .add_value(3)
        .build();

    assert_eq!(data, vec![1, 2, 3]);
}

#[test]
fn test_data_builder_range() {
    let data = TestDataBuilder::<i32>::new().add_range(1..=5).build();

    assert_eq!(data, vec![1, 2, 3, 4, 5]);
}

#[test]
fn test_data_builder_repeated() {
    let data = TestDataBuilder::new().add_repeated(42, 3).build();

    assert_eq!(data, vec![42, 42, 42]);
}

#[test]
fn test_kv_builder() {
    let data = KVTestDataBuilder::new()
        .add_kv("a", 1)
        .add_kv("b", 2)
        .build();

    assert_eq!(data, vec![("a", 1), ("b", 2)]);
}

#[test]
fn test_kv_builder_key_with_values() {
    let data = KVTestDataBuilder::new()
        .add_key_with_values("a", vec![1, 2, 3])
        .build();

    assert_eq!(data, vec![("a", 1), ("a", 2), ("a", 3)]);
}

#[test]
fn test_sequential_data() {
    let data = sequential_data(1, 5);
    assert_eq!(data, vec![1, 2, 3, 4, 5]);
}

#[test]
fn test_pseudo_random_data_deterministic() {
    let data1 = pseudo_random_data(10, 0, 100);
    let data2 = pseudo_random_data(10, 0, 100);
    assert_eq!(data1, data2); // Should be identical (deterministic)
}

#[test]
fn test_pseudo_random_data_range() {
    let data = pseudo_random_data(100, 10, 20);
    for val in data {
        assert!((10..20).contains(&val));
    }
}

#[test]
fn test_skewed_kvs() {
    let data = skewed_kvs(10);
    assert_eq!(data.len(), 10);

    // Count occurrences of hot_key
    let hot_count = data.iter().filter(|(k, _)| k == "hot_key").count();
    assert!(hot_count >= 4); // Should appear roughly 50% of the time
}
