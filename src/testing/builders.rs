//! Test data builders for creating test datasets fluently.

use std::ops::{Add, RangeInclusive};

/// A fluent builder for creating test data.
///
/// `TestDataBuilder` provides a convenient API for constructing test datasets
/// with various patterns like ranges, repeated values, and custom generators.
///
/// # Example
///
/// ```
/// use ironbeam::testing::TestDataBuilder;
///
/// let data = TestDataBuilder::new()
///     .add_range(1..=10)
///     .add_value(100)
///     .add_repeated(42, 5)
///     .build();
///
/// assert_eq!(data.len(), 16); // 10 + 1 + 5
/// ```
#[derive(Default)]
pub struct TestDataBuilder<T> {
    data: Vec<T>,
}

impl<T> TestDataBuilder<T> {
    /// Create a new empty test data builder.
    #[must_use]
    pub const fn new() -> Self {
        Self { data: Vec::new() }
    }

    /// Add a single value to the dataset.
    #[must_use]
    pub fn add_value(mut self, value: T) -> Self {
        self.data.push(value);
        self
    }

    /// Add multiple values to the dataset.
    #[must_use]
    pub fn add_values(mut self, values: Vec<T>) -> Self {
        self.data.extend(values);
        self
    }

    /// Add a repeated value to the dataset.
    ///
    /// # Example
    ///
    /// ```
    /// use ironbeam::testing::TestDataBuilder;
    ///
    /// let data = TestDataBuilder::new()
    ///     .add_repeated(42, 3)
    ///     .build();
    ///
    /// assert_eq!(data, vec![42, 42, 42]);
    /// ```
    #[must_use]
    pub fn add_repeated(mut self, value: T, count: usize) -> Self
    where
        T: Clone,
    {
        for _ in 0..count {
            self.data.push(value.clone());
        }
        self
    }

    /// Build and return the test dataset.
    #[must_use]
    pub fn build(self) -> Vec<T> {
        self.data
    }

    /// Get the current size of the dataset being built.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the dataset is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

// Specialized methods for numeric types
impl<T> TestDataBuilder<T>
where
    T: Copy + From<i32> + Add<Output = T> + PartialOrd,
{
    /// Add a range of values to the dataset.
    ///
    /// # Example
    ///
    /// ```
    /// use ironbeam::testing::TestDataBuilder;
    ///
    /// let data = TestDataBuilder::<i32>::new()
    ///     .add_range(1..=5)
    ///     .build();
    ///
    /// assert_eq!(data, vec![1, 2, 3, 4, 5]);
    /// ```
    #[must_use]
    pub fn add_range(mut self, range: RangeInclusive<i32>) -> Self {
        for i in range {
            self.data.push(T::from(i));
        }
        self
    }
}

/// Builder for creating key-value test data.
///
/// This builder is specialized for creating `Vec<(K, V)>` datasets
/// commonly used in keyed operations testing.
///
/// # Example
///
/// ```
/// use ironbeam::testing::KVTestDataBuilder;
///
/// let data = KVTestDataBuilder::new()
///     .add_kv("a", 1)
///     .add_kv("b", 2)
///     .add_kv("a", 3)
///     .build();
///
/// assert_eq!(data.len(), 3);
/// ```
#[derive(Default)]
pub struct KVTestDataBuilder<K, V> {
    data: Vec<(K, V)>,
}

impl<K, V> KVTestDataBuilder<K, V> {
    /// Create a new empty key-value test data builder.
    #[must_use]
    pub const fn new() -> Self {
        Self { data: Vec::new() }
    }

    /// Add a single key-value pair.
    #[must_use]
    pub fn add_kv(mut self, key: K, value: V) -> Self {
        self.data.push((key, value));
        self
    }

    /// Add multiple key-value pairs.
    #[must_use]
    pub fn add_kvs(mut self, kvs: Vec<(K, V)>) -> Self {
        self.data.extend(kvs);
        self
    }

    /// Add multiple values for the same key.
    ///
    /// # Example
    ///
    /// ```
    /// use ironbeam::testing::KVTestDataBuilder;
    ///
    /// let data = KVTestDataBuilder::new()
    ///     .add_key_with_values("a", vec![1, 2, 3])
    ///     .build();
    ///
    /// assert_eq!(data, vec![("a", 1), ("a", 2), ("a", 3)]);
    /// ```
    #[must_use]
    pub fn add_key_with_values(mut self, key: K, values: Vec<V>) -> Self
    where
        K: Clone,
    {
        for value in values {
            self.data.push((key.clone(), value));
        }
        self
    }

    /// Build and return the key-value dataset.
    #[must_use]
    pub fn build(self) -> Vec<(K, V)> {
        self.data
    }

    /// Get the current size of the dataset being built.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the dataset is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

/// Generate sequential numeric test data.
///
/// # Example
///
/// ```
/// use ironbeam::testing::sequential_data;
///
/// let data = sequential_data(1, 5);
/// assert_eq!(data, vec![1, 2, 3, 4, 5]);
/// ```
#[must_use]
pub fn sequential_data(start: i32, end: i32) -> Vec<i32> {
    (start..=end).collect()
}

/// Generate sequential key-value pairs where keys are formatted strings.
///
/// # Example
///
/// ```
/// use ironbeam::testing::sequential_kvs;
///
/// let data = sequential_kvs("key", 1, 3);
/// assert_eq!(data, vec![
///     ("key_1".to_string(), 1),
///     ("key_2".to_string(), 2),
///     ("key_3".to_string(), 3),
/// ]);
/// ```
#[must_use]
pub fn sequential_kvs(key_prefix: &str, start: i32, end: i32) -> Vec<(String, i32)> {
    (start..=end)
        .map(|i| (format!("{key_prefix}_{i}"), i))
        .collect()
}

/// Generate test data with skewed key distribution (simulates real-world data).
///
/// Creates a dataset where some keys appear much more frequently than others,
/// useful for testing aggregation performance with realistic data distributions.
///
/// # Example
///
/// ```
/// use ironbeam::testing::skewed_kvs;
///
/// let data = skewed_kvs(100);
/// // "hot_key" will appear ~50% of the time
/// // "warm_key_X" will appear ~30% of the time
/// // "cold_key_X" will appear ~20% of the time
/// assert_eq!(data.len(), 100);
/// ```
#[must_use]
pub fn skewed_kvs(count: usize) -> Vec<(String, i32)> {
    let mut data = Vec::with_capacity(count);
    for i in 0..count {
        let key = if i % 2 == 0 {
            "hot_key".to_string() // 50% hot key
        } else if i % 5 == 0 {
            format!("warm_key_{}", i % 3) // ~10% warm keys
        } else {
            format!("cold_key_{i}") // the rest are unique cold keys
        };
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_possible_wrap,
            clippy::cast_sign_loss
        )]
        data.push((key, i as i32));
    }
    data
}

/// Generate random-like deterministic test data using a simple LCG.
///
/// This generates deterministic "random" data that's reproducible across test runs.
/// Uses a linear congruential generator with a fixed seed.
///
/// # Example
///
/// ```
/// use ironbeam::testing::pseudo_random_data;
///
/// let data = pseudo_random_data(10, 0, 100);
/// assert_eq!(data.len(), 10);
/// // All values will be between 0 and 100
/// for &val in &data {
///     assert!(val >= 0 && val < 100);
/// }
/// ```
#[must_use]
pub fn pseudo_random_data(count: usize, min: i32, max: i32) -> Vec<i32> {
    let mut data = Vec::with_capacity(count);
    let mut seed: u32 = 12345; // Fixed seed for reproducibility

    let range = (max - min).cast_unsigned();

    for _ in 0..count {
        // Simple LCG parameters
        seed = seed.wrapping_mul(1_103_515_245).wrapping_add(12_345);
        let val = ((seed / 65536) % range).cast_signed() + min;
        data.push(val);
    }

    data
}

/// Generate key-value pairs with pseudo-random values.
///
/// # Example
///
/// ```
/// use ironbeam::testing::pseudo_random_kvs;
///
/// let data = pseudo_random_kvs(5, 3, 0, 100);
/// // Creates ~5 keys with ~3 values each, values in range [0, 100)
/// ```
#[must_use]
pub fn pseudo_random_kvs(
    num_keys: usize,
    values_per_key: usize,
    value_min: i32,
    value_max: i32,
) -> Vec<(String, i32)> {
    let mut data = Vec::new();
    let mut seed: u32 = 54321; // Different seed than pseudo_random_data

    let range = (value_max - value_min).cast_unsigned();

    for key_idx in 0..num_keys {
        let key = format!("key_{key_idx}");
        for _ in 0..values_per_key {
            seed = seed.wrapping_mul(1_103_515_245).wrapping_add(12_345);
            let val = value_min + ((seed / 65536) % range).cast_signed();
            data.push((key.clone(), val));
        }
    }

    data
}
