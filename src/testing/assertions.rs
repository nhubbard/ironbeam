//! Assertion functions and fluent assertion builder for testing pipeline outputs.
//!
//! This module provides specialized assertion functions and the [`PAssert`] builder
//! for comparing collections produced by pipelines with expected results.

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::{BuildHasher, Hash};

use anyhow::{Result, bail};

/// A fluent assertion builder for testing pipeline collection outputs.
///
/// `PAssert` wraps a slice reference and exposes method-chaining assertion
/// helpers. Each method returns `Result<&Self>` so checks can be composed
/// with the `?` operator and the entire chain short-circuits on the first
/// failure.
///
/// # Example
///
/// ```
/// use ironbeam::testing::PAssert;
/// # use anyhow::Result;
///
/// # fn main() -> Result<()> {
/// let result = vec![3, 1, 2];
///
/// PAssert::that(&result)
///     .contains_in_any_order(&[1, 2, 3])?
///     .has_count(3)?
///     .all_match(|x| *x > 0)?;
///
/// let empty: Vec<i32> = vec![];
/// PAssert::that(&empty).is_empty()?;
/// # Ok(())
/// # }
/// ```
#[must_use = "call assertion methods to verify the collection"]
#[derive(Debug)]
pub struct PAssert<'a, T> {
    data: &'a [T],
}

impl<'a, T: Debug> PAssert<'a, T> {
    /// Create a new `PAssert` wrapping `data`.
    ///
    /// `data` is borrowed for the lifetime `'a` of the returned builder, so
    /// assertions borrow-check against the same slice without copying.
    pub const fn that(data: &'a [T]) -> Self {
        Self { data }
    }

    /// Assert that the collection is empty.
    ///
    /// # Errors
    ///
    /// Returns an error if the collection contains one or more elements.
    ///
    /// # Example
    ///
    /// ```
    /// use ironbeam::testing::PAssert;
    /// # use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let empty: Vec<i32> = vec![];
    /// PAssert::that(&empty).is_empty()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn is_empty(&self) -> Result<&Self> {
        if !self.data.is_empty() {
            bail!(
                "Expected an empty collection, but it contained {} element(s): {:?}",
                self.data.len(),
                self.data,
            );
        }
        Ok(self)
    }

    /// Assert that the collection has exactly `expected` elements.
    ///
    /// # Errors
    ///
    /// Returns an error if the element count does not match `expected`.
    ///
    /// # Example
    ///
    /// ```
    /// use ironbeam::testing::PAssert;
    /// # use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let data = vec![1, 2, 3];
    /// PAssert::that(&data).has_count(3)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn has_count(&self, expected: usize) -> Result<&Self> {
        if self.data.len() != expected {
            bail!(
                "Expected {} element(s), but got {}: {:?}",
                expected,
                self.data.len(),
                self.data,
            );
        }
        Ok(self)
    }

    /// Assert that every element satisfies `predicate`.
    ///
    /// Evaluates elements in order and reports the index and value of the
    /// first element that fails.
    ///
    /// # Errors
    ///
    /// Returns an error identifying the first element that does not satisfy
    /// `predicate`.
    ///
    /// # Example
    ///
    /// ```
    /// use ironbeam::testing::PAssert;
    /// # use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let data = vec![2, 4, 6];
    /// PAssert::that(&data).all_match(|x| x % 2 == 0)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn all_match(&self, predicate: impl Fn(&T) -> bool) -> Result<&Self> {
        for (i, item) in self.data.iter().enumerate() {
            if !predicate(item) {
                bail!(
                    "Predicate failed at index {i}: {:?}\n  Collection: {:?}",
                    item,
                    self.data,
                );
            }
        }
        Ok(self)
    }
}

impl<T: Debug + Eq + Hash> PAssert<'_, T> {
    /// Assert that the collection contains exactly the same elements as
    /// `expected`, ignoring order.
    ///
    /// Uses multiset (bag) semantics: duplicate elements must appear the same
    /// number of times in both collections.
    ///
    /// # Errors
    ///
    /// Returns an error listing missing and extra elements when the multisets
    /// differ.
    ///
    /// # Example
    ///
    /// ```
    /// use ironbeam::testing::PAssert;
    /// # use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let data = vec![3, 1, 2];
    /// PAssert::that(&data).contains_in_any_order(&[1, 2, 3])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn contains_in_any_order(&self, expected: &[T]) -> Result<&Self> {
        let mut actual_counts: HashMap<&T, usize> = HashMap::new();
        for item in self.data {
            *actual_counts.entry(item).or_insert(0) += 1;
        }

        let mut expected_counts: HashMap<&T, usize> = HashMap::new();
        for item in expected {
            *expected_counts.entry(item).or_insert(0) += 1;
        }

        if actual_counts == expected_counts {
            return Ok(self);
        }

        let mut missing: Vec<(&T, usize)> = Vec::new();
        for (item, &exp_count) in &expected_counts {
            let act_count = actual_counts.get(item).copied().unwrap_or(0);
            if act_count < exp_count {
                missing.push((item, exp_count - act_count));
            }
        }

        let mut extra: Vec<(&T, usize)> = Vec::new();
        for (item, &act_count) in &actual_counts {
            let exp_count = expected_counts.get(item).copied().unwrap_or(0);
            if act_count > exp_count {
                extra.push((item, act_count - exp_count));
            }
        }

        bail!(
            "Collection content mismatch:\n  Missing (item, count): {:?}\n  Extra (item, count): {:?}\n  Expected: {:?}\n  Actual: {:?}",
            missing,
            extra,
            expected,
            self.data,
        );
    }
}

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
