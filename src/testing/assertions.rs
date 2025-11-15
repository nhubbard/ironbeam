//! Assertion functions for testing pipeline outputs.
//!
//! This module provides specialized assertion functions for comparing
//! collections produced by pipelines with expected results.

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::{BuildHasher, Hash};

/// Assert that two collections are equal in order and content.
///
/// This function compares two vectors element-by-element and panics with
/// a detailed message if they differ.
///
/// # Panics
///
/// Panics if the collections differ in length or content.
///
/// # Example
///
/// ```
/// use ironbeam::testing::assert_collections_equal;
///
/// let actual = vec![1, 2, 3];
/// let expected = vec![1, 2, 3];
/// assert_collections_equal(&actual, &expected);
/// ```
pub fn assert_collections_equal<T: Debug + PartialEq>(actual: &[T], expected: &[T]) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "Collection length mismatch:\n  Expected length: {}\n  Actual length: {}\n  Expected: {expected:?}\n  Actual: {actual:?}",
        expected.len(),
        actual.len()
    );

    for (i, (a, e)) in actual.iter().zip(expected.iter()).enumerate() {
        assert_eq!(
            a, e,
            "Collection mismatch at index {i}:\n  Expected: {e:?}\n  Actual: {a:?}\n  Full expected: {expected:?}\n  Full actual: {actual:?}"
        );
    }
}

/// Assert that two collections contain the same elements, ignoring order.
///
/// This function converts both collections to sets and compares them.
/// Useful for testing transformations that may not preserve element order.
///
/// # Panics
///
/// Panics if the collections differ in content (ignoring order).
///
/// # Example
///
/// ```
/// use ironbeam::testing::assert_collections_unordered_equal;
///
/// let actual = vec![3, 1, 2];
/// let expected = vec![1, 2, 3];
/// assert_collections_unordered_equal(&actual, &expected);
/// ```
pub fn assert_collections_unordered_equal<T: Debug + Eq + Hash>(actual: &[T], expected: &[T]) {
    let actual_set: HashSet<_> = actual.iter().collect();
    let expected_set: HashSet<_> = expected.iter().collect();

    assert_eq!(
        actual.len(),
        expected.len(),
        "Collection length mismatch:\n  Expected length: {}\n  Actual length: {}\n  Expected: {expected:?}\n  Actual: {actual:?}",
        expected.len(),
        actual.len()
    );

    if actual_set != expected_set {
        let missing: Vec<_> = expected_set.difference(&actual_set).collect();
        let extra: Vec<_> = actual_set.difference(&expected_set).collect();

        panic!(
            "Collection content mismatch:\n  Missing elements: {missing:?}\n  Extra elements: {extra:?}\n  Expected: {expected:?}\n  Actual: {actual:?}"
        );
    }
}

/// Assert that two collections of key-value pairs are equal after sorting by key.
///
/// This function is specifically designed for testing grouped or aggregated data
/// where the order of keys may vary between runs but should be consistent when sorted.
///
/// # Panics
///
/// Panics if the collections differ after sorting by key.
///
/// # Example
///
/// ```
/// use ironbeam::testing::assert_kv_collections_equal;
///
/// let actual = vec![("b", 2), ("a", 1)];
/// let expected = vec![("a", 1), ("b", 2)];
/// assert_kv_collections_equal(actual, expected);
/// ```
pub fn assert_kv_collections_equal<K, V>(mut actual: Vec<(K, V)>, mut expected: Vec<(K, V)>)
where
    K: Debug + Ord,
    V: Debug + PartialEq,
{
    actual.sort_by(|a, b| a.0.cmp(&b.0));
    expected.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(
        actual.len(),
        expected.len(),
        "Collection length mismatch:\n  Expected length: {}\n  Actual length: {}\n  Expected: {expected:?}\n  Actual: {actual:?}",
        expected.len(),
        actual.len()
    );

    for (i, ((ak, av), (ek, ev))) in actual.iter().zip(expected.iter()).enumerate() {
        assert!(
            !(ak != ek || av != ev),
            "Collection mismatch at index {i} after sorting:\n  Expected: ({ek:?}, {ev:?})\n  Actual: ({ak:?}, {av:?})\n  Full expected: {expected:?}\n  Full actual: {actual:?}"
        );
    }
}

/// Assert that two collections of key-value pairs with grouped values are equal.
///
/// This function compares grouped data (e.g., output from `group_by_key`) where
/// each key maps to a vector of values. The keys are compared in sorted order,
/// and the value vectors are compared as unordered sets.
///
/// # Panics
///
/// Panics if the collections differ in keys or values.
///
/// # Example
///
/// ```
/// use ironbeam::testing::assert_grouped_kv_equal;
///
/// let actual = vec![("a", vec![1, 2]), ("b", vec![3])];
/// let expected = vec![("a", vec![2, 1]), ("b", vec![3])];
/// assert_grouped_kv_equal(actual, expected);
/// ```
pub fn assert_grouped_kv_equal<K, V>(mut actual: Vec<(K, Vec<V>)>, mut expected: Vec<(K, Vec<V>)>)
where
    K: Debug + Ord,
    V: Debug + Eq + Hash,
{
    actual.sort_by(|a, b| a.0.cmp(&b.0));
    expected.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(
        actual.len(),
        expected.len(),
        "Collection length mismatch:\n  Expected length: {}\n  Actual length: {}",
        expected.len(),
        actual.len()
    );

    for (i, ((ak, av), (ek, ev))) in actual.iter().zip(expected.iter()).enumerate() {
        assert_eq!(
            ak, ek,
            "Key mismatch at index {i}:\n  Expected: {ek:?}\n  Actual: {ak:?}"
        );

        let av_set: HashSet<_> = av.iter().collect();
        let ev_set: HashSet<_> = ev.iter().collect();

        assert_eq!(
            av_set, ev_set,
            "Value mismatch for key {ak:?} at index {i}:\n  Expected values: {ev:?}\n  Actual values: {av:?}"
        );
    }
}

/// Assert that all elements in a collection satisfy a predicate.
///
/// # Panics
///
/// Panics if any element does not satisfy the predicate.
///
/// # Example
///
/// ```
/// use ironbeam::testing::assert_all;
///
/// let data = vec![2, 4, 6, 8];
/// assert_all(&data, |x| x % 2 == 0);
/// ```
pub fn assert_all<T: Debug>(collection: &[T], predicate: impl Fn(&T) -> bool) {
    for (i, item) in collection.iter().enumerate() {
        assert!(
            predicate(item),
            "Predicate failed for element at index {i}:\n  Element: {item:?}\n  Collection: {collection:?}"
        );
    }
}

/// Assert that at least one element in a collection satisfies a predicate.
///
/// # Panics
///
/// Panics if no elements satisfy the predicate.
///
/// # Example
///
/// ```
/// use ironbeam::testing::assert_any;
///
/// let data = vec![1, 2, 3, 4];
/// assert_any(&data, |x| x % 2 == 0);
/// ```
pub fn assert_any<T: Debug>(collection: &[T], predicate: impl Fn(&T) -> bool) {
    assert!(
        collection.iter().any(&predicate),
        "No elements satisfied the predicate:\n  Collection: {collection:?}"
    );
}

/// Assert that no elements in a collection satisfy a predicate.
///
/// # Panics
///
/// Panics if any element satisfies the predicate.
///
/// # Example
///
/// ```
/// use ironbeam::testing::assert_none;
///
/// let data = vec![1, 3, 5, 7];
/// assert_none(&data, |x| x % 2 == 0);
/// ```
pub fn assert_none<T: Debug>(collection: &[T], predicate: impl Fn(&T) -> bool) {
    for (i, item) in collection.iter().enumerate() {
        assert!(
            !predicate(item),
            "Predicate unexpectedly succeeded for element at index {i}:\n  Element: {item:?}\n  Collection: {collection:?}"
        );
    }
}

/// Assert that a collection has the expected size.
///
/// # Panics
///
/// Panics if the collection size doesn't match the expected size.
///
/// # Example
///
/// ```
/// use ironbeam::testing::assert_collection_size;
///
/// let data = vec![1, 2, 3];
/// assert_collection_size(&data, 3);
/// ```
pub fn assert_collection_size<T>(collection: &[T], expected_size: usize) {
    assert_eq!(
        collection.len(),
        expected_size,
        "Collection size mismatch:\n  Expected: {expected_size}\n  Actual: {}",
        collection.len()
    );
}

/// Assert that a collection contains a specific element.
///
/// # Panics
///
/// Panics if the element is not found in the collection.
///
/// # Example
///
/// ```
/// use ironbeam::testing::assert_contains;
///
/// let data = vec![1, 2, 3, 4];
/// assert_contains(&data, &3);
/// ```
pub fn assert_contains<T: Debug + PartialEq>(collection: &[T], element: &T) {
    assert!(
        collection.contains(element),
        "Element not found in collection:\n  Looking for: {element:?}\n  Collection: {collection:?}"
    );
}

/// Assert that two hashmaps are equal.
///
/// # Panics
///
/// Panics if the hashmaps differ in keys or values.
///
/// # Example
///
/// ```
/// use ironbeam::testing::assert_maps_equal;
/// use std::collections::HashMap;
///
/// let mut actual = HashMap::new();
/// actual.insert("a", 1);
/// actual.insert("b", 2);
///
/// let mut expected = HashMap::new();
/// expected.insert("a", 1);
/// expected.insert("b", 2);
///
/// assert_maps_equal(&actual, &expected);
/// ```
pub fn assert_maps_equal<K, V, S: BuildHasher>(
    actual: &HashMap<K, V, S>,
    expected: &HashMap<K, V, S>,
) where
    K: Debug + Eq + Hash,
    V: Debug + PartialEq,
{
    assert_eq!(
        actual.len(),
        expected.len(),
        "HashMap size mismatch:\n  Expected size: {}\n  Actual size: {}\n  Expected: {expected:?}\n  Actual: {actual:?}",
        expected.len(),
        actual.len()
    );

    for (key, expected_value) in expected {
        match actual.get(key) {
            Some(actual_value) if actual_value == expected_value => {}
            Some(actual_value) => {
                panic!(
                    "HashMap value mismatch for key {key:?}:\n  Expected: {expected_value:?}\n  Actual: {actual_value:?}"
                );
            }
            None => {
                panic!("HashMap missing key: {key:?}");
            }
        }
    }
}
