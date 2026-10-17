use ironbeam_testing::{
    PAssert, assert_all, assert_any, assert_collection_size, assert_collections_equal,
    assert_collections_unordered_equal, assert_contains, assert_grouped_kv_equal,
    assert_kv_collections_equal, assert_maps_equal, assert_none,
};
use std::collections::HashMap;

#[test]
fn unordered_assertions_preserve_duplicate_counts() {
    assert_collections_unordered_equal(&[1, 2, 1], &[2, 1, 1]);
    assert!(
        std::panic::catch_unwind(|| {
            assert_collections_unordered_equal(&[1, 2, 1], &[2, 1, 2]);
        })
        .is_err()
    );

    assert_grouped_kv_equal(
        vec![("b", vec![3]), ("a", vec![1, 2, 1])],
        vec![("a", vec![2, 1, 1]), ("b", vec![3])],
    );
    assert!(
        std::panic::catch_unwind(|| {
            assert_grouped_kv_equal(vec![("a", vec![1, 2, 1])], vec![("a", vec![1, 2, 2])]);
        })
        .is_err()
    );
}

#[test]
fn fluent_assertions_report_failures_and_support_chaining() {
    let data = [2, 1, 2];
    PAssert::that(&data)
        .contains_in_any_order(&[1, 2, 2])
        .unwrap()
        .has_count(3)
        .unwrap()
        .all_match(|value| *value > 0)
        .unwrap();
    assert!(PAssert::that(&Vec::<i32>::new()).is_empty().is_ok());
    assert!(
        PAssert::that(&data)
            .is_empty()
            .unwrap_err()
            .to_string()
            .contains("empty")
    );
    assert!(
        PAssert::that(&data)
            .has_count(2)
            .unwrap_err()
            .to_string()
            .contains('2')
    );
    assert!(
        PAssert::that(&data)
            .all_match(|value| *value == 2)
            .unwrap_err()
            .to_string()
            .contains("index 1")
    );
    let message = PAssert::that(&data)
        .contains_in_any_order(&[1, 2, 3])
        .unwrap_err()
        .to_string();
    assert!(message.contains("Missing"));
    assert!(message.contains("Extra"));
}

#[test]
fn assertion_helpers_check_keys_values_and_predicates() {
    assert_collections_equal(&[1, 2], &[1, 2]);
    assert_kv_collections_equal(vec![("b", 2), ("a", 1)], vec![("a", 1), ("b", 2)]);
    assert_all(&[2, 4], |value| value % 2 == 0);
    assert_any(&[1, 2], |value| value % 2 == 0);
    assert_none(&[1, 3], |value| value % 2 == 0);
    assert_collection_size(&[1, 2], 2);
    assert_contains(&[1, 2], &2);
    let actual = HashMap::from([("a", 1), ("b", 2)]);
    let expected = HashMap::from([("b", 2), ("a", 1)]);
    assert_maps_equal(&actual, &expected);

    assert!(std::panic::catch_unwind(|| assert_collections_equal(&[1, 2], &[1, 3])).is_err());
    assert!(
        std::panic::catch_unwind(|| assert_kv_collections_equal(vec![("a", 1)], vec![("a", 2)]))
            .is_err()
    );
    assert!(
        std::panic::catch_unwind(|| assert_kv_collections_equal(vec![("a", 1)], vec![("b", 1)]))
            .is_err()
    );
    assert!(std::panic::catch_unwind(|| assert_all(&[1, 2], |value| *value > 1)).is_err());
    assert!(std::panic::catch_unwind(|| assert_any(&[1, 3], |value| value % 2 == 0)).is_err());
    assert!(std::panic::catch_unwind(|| assert_none(&[1, 2], |value| value % 2 == 0)).is_err());
    assert!(std::panic::catch_unwind(|| assert_collection_size(&[1, 2], 1)).is_err());
    assert!(std::panic::catch_unwind(|| assert_contains(&[1, 2], &3)).is_err());
    assert!(
        std::panic::catch_unwind(|| assert_maps_equal(
            &actual,
            &HashMap::from([("a", 2), ("b", 2)])
        ))
        .is_err()
    );
    assert!(
        std::panic::catch_unwind(|| assert_maps_equal(
            &actual,
            &HashMap::from([("a", 1), ("c", 3)])
        ))
        .is_err()
    );
}
