//! Enhanced filter transforms with convenience methods.
//!
//! This module provides convenience methods for common filtering patterns, complementing
//! the basic [`PCollection::filter`] method. These helpers reduce boilerplate for
//! simple comparisons and enable filtering on computed values without separate map steps.
//!
//! ## Overview
//!
//! - **Comparison methods** for primitive types:
//!   - [`filter_eq`](PCollection::filter_eq) - Keep elements equal to a value
//!   - [`filter_lt`](PCollection::filter_lt) - Keep elements less than a value
//!   - [`filter_gt`](PCollection::filter_gt) - Keep elements greater than a value
//!   - [`filter_le`](PCollection::filter_le) - Keep elements less than or equal to a value
//!   - [`filter_ge`](PCollection::filter_ge) - Keep elements greater than or equal to a value
//!   - [`filter_range`](PCollection::filter_range) - Keep elements in a range [min, max)
//!
//! - **Computed value filtering**:
//!   - [`filter_by`](PCollection::filter_by) - Filter using a comparison on a computed value
//!
//! ## Examples
//!
//! ### Basic comparison filtering
//! ```no_run
//! use ironbeam::*;
//!
//! let p = Pipeline::default();
//!
//! // Keep numbers less than 10
//! let numbers = from_vec(&p, vec![1, 5, 10, 15, 20, 25]);
//! let small = numbers.filter_lt(&10);
//! assert_eq!(small.collect_seq().unwrap(), vec![1, 5]);
//!
//! // Keep numbers in range [10, 20)
//! let numbers = from_vec(&p, vec![1, 5, 10, 15, 20, 25]);
//! let mid_range = numbers.filter_range(&10, &20);
//! assert_eq!(mid_range.collect_seq().unwrap(), vec![10, 15]);
//! ```
//!
//! ### Filtering on struct fields
//! ```no_run
//! use ironbeam::*;
//!
//! #[derive(Clone)]
//! struct Person {
//!     name: String,
//!     age: u32,
//! }
//!
//! let p = Pipeline::default();
//! let people = from_vec(&p, vec![
//!     Person { name: "Alice".into(), age: 25 },
//!     Person { name: "Bob".into(), age: 17 },
//!     Person { name: "Carol".into(), age: 30 },
//! ]);
//!
//! // Filter adults (age >= 18) without mapping first
//! let adults = people.filter_by(|person| person.age, |age| *age >= 18);
//!
//! assert_eq!(adults.collect_seq().unwrap().len(), 2);
//! ```

use crate::{PCollection, RFBound};

impl<T: RFBound> PCollection<T> {
    /// Keep only elements equal to the given value.
    ///
    /// This is a convenience method equivalent to `filter(|elem| elem == &value)`.
    ///
    /// # Arguments
    /// - `value` - The value to compare against
    ///
    /// # Example
    /// ```no_run
    /// use ironbeam::*;
    ///
    /// let p = Pipeline::default();
    /// let numbers = from_vec(&p, vec![1, 2, 3, 2, 1]);
    /// let twos = numbers.filter_eq(&2);
    /// assert_eq!(twos.collect_seq().unwrap(), vec![2, 2]);
    /// ```
    #[must_use]
    pub fn filter_eq(self, value: &T) -> Self
    where
        T: PartialEq,
    {
        let val = value.clone();
        self.filter(move |elem| elem == &val)
    }

    /// Keep only elements not equal to the given value.
    ///
    /// This is a convenience method equivalent to `filter(|elem| elem != &value)`.
    ///
    /// # Arguments
    /// - `value` - The value to compare against
    ///
    /// # Example
    /// ```no_run
    /// use ironbeam::*;
    ///
    /// let p = Pipeline::default();
    /// let numbers = from_vec(&p, vec![1, 2, 3, 2, 1]);
    /// let not_twos = numbers.filter_ne(&2);
    /// assert_eq!(not_twos.collect_seq().unwrap(), vec![1, 3, 1]);
    /// ```
    #[must_use]
    pub fn filter_ne(self, value: &T) -> Self
    where
        T: PartialEq,
    {
        let val = value.clone();
        self.filter(move |elem| elem != &val)
    }
}

impl<T: RFBound + PartialOrd> PCollection<T> {
    /// Keep only elements less than the given value.
    ///
    /// This is a convenience method equivalent to `filter(|elem| elem < &value)`.
    ///
    /// # Arguments
    /// - `value` - The upper bound (exclusive)
    ///
    /// # Example
    /// ```no_run
    /// use ironbeam::*;
    ///
    /// let p = Pipeline::default();
    /// let numbers = from_vec(&p, vec![1, 5, 10, 15, 20]);
    /// let small = numbers.filter_lt(&10);
    /// assert_eq!(small.collect_seq().unwrap(), vec![1, 5]);
    /// ```
    #[must_use]
    pub fn filter_lt(self, value: &T) -> Self {
        let val = value.clone();
        self.filter(move |elem| elem < &val)
    }

    /// Keep only elements less than or equal to the given value.
    ///
    /// This is a convenience method equivalent to `filter(|elem| elem <= &value)`.
    ///
    /// # Arguments
    /// - `value` - The upper bound (inclusive)
    ///
    /// # Example
    /// ```no_run
    /// use ironbeam::*;
    ///
    /// let p = Pipeline::default();
    /// let numbers = from_vec(&p, vec![1, 5, 10, 15, 20]);
    /// let small = numbers.filter_le(&10);
    /// assert_eq!(small.collect_seq().unwrap(), vec![1, 5, 10]);
    /// ```
    #[must_use]
    pub fn filter_le(self, value: &T) -> Self {
        let val = value.clone();
        self.filter(move |elem| elem <= &val)
    }

    /// Keep only elements greater than the given value.
    ///
    /// This is a convenience method equivalent to `filter(|elem| elem > &value)`.
    ///
    /// # Arguments
    /// - `value` - The lower bound (exclusive)
    ///
    /// # Example
    /// ```no_run
    /// use ironbeam::*;
    ///
    /// let p = Pipeline::default();
    /// let numbers = from_vec(&p, vec![1, 5, 10, 15, 20]);
    /// let large = numbers.filter_gt(&10);
    /// assert_eq!(large.collect_seq().unwrap(), vec![15, 20]);
    /// ```
    #[must_use]
    pub fn filter_gt(self, value: &T) -> Self {
        let val = value.clone();
        self.filter(move |elem| elem > &val)
    }

    /// Keep only elements greater than or equal to the given value.
    ///
    /// This is a convenience method equivalent to `filter(|elem| elem >= &value)`.
    ///
    /// # Arguments
    /// - `value` - The lower bound (inclusive)
    ///
    /// # Example
    /// ```no_run
    /// use ironbeam::*;
    ///
    /// let p = Pipeline::default();
    /// let numbers = from_vec(&p, vec![1, 5, 10, 15, 20]);
    /// let large = numbers.filter_ge(&10);
    /// assert_eq!(large.collect_seq().unwrap(), vec![10, 15, 20]);
    /// ```
    #[must_use]
    pub fn filter_ge(self, value: &T) -> Self {
        let val = value.clone();
        self.filter(move |elem| elem >= &val)
    }

    /// Keep only elements within a range [min, max).
    ///
    /// Elements are kept if they are greater than or equal to `min` and strictly less than `max`.
    /// This is a convenience method equivalent to `filter(|elem| elem >= &min && elem < &max)`.
    ///
    /// # Arguments
    /// - `min` - The lower bound (inclusive)
    /// - `max` - The upper bound (exclusive)
    ///
    /// # Example
    /// ```no_run
    /// use ironbeam::*;
    ///
    /// let p = Pipeline::default();
    /// let numbers = from_vec(&p, vec![1, 5, 10, 15, 20, 25]);
    /// let mid = numbers.filter_range(&10, &20);
    /// assert_eq!(mid.collect_seq().unwrap(), vec![10, 15]);
    /// ```
    #[must_use]
    pub fn filter_range(self, min: &T, max: &T) -> Self {
        let min_val = min.clone();
        let max_val = max.clone();
        self.filter(move |elem| elem >= &min_val && elem < &max_val)
    }

    /// Keep only elements within an inclusive range [min, max].
    ///
    /// Elements are kept if they are greater than or equal to `min` and less than or equal to `max`.
    /// This is a convenience method equivalent to `filter(|elem| elem >= &min && elem <= &max)`.
    ///
    /// # Arguments
    /// - `min` - The lower bound (inclusive)
    /// - `max` - The upper bound (inclusive)
    ///
    /// # Example
    /// ```no_run
    /// use ironbeam::*;
    ///
    /// let p = Pipeline::default();
    /// let numbers = from_vec(&p, vec![1, 5, 10, 15, 20, 25]);
    /// let mid = numbers.filter_range_inclusive(&10, &20);
    /// assert_eq!(mid.collect_seq().unwrap(), vec![10, 15, 20]);
    /// ```
    #[must_use]
    pub fn filter_range_inclusive(self, min: &T, max: &T) -> Self {
        let min_val = min.clone();
        let max_val = max.clone();
        self.filter(move |elem| elem >= &min_val && elem <= &max_val)
    }
}

impl<T: RFBound> PCollection<T> {
    /// Filter elements by applying a comparison predicate to a computed value.
    ///
    /// This is particularly useful when working with structs where you want to filter
    /// based on a specific field without needing a separate `map` step. The `extractor`
    /// function computes a value from each element, and the `predicate` determines
    /// whether to keep the element based on that computed value.
    ///
    /// # Type Parameters
    /// - `V`: The type of value extracted by `extractor`
    /// - `E`: The extractor function type
    /// - `P`: The predicate function type
    ///
    /// # Arguments
    /// - `extractor` - Function that computes a value from each element
    /// - `predicate` - Function that tests the computed value
    ///
    /// # Example
    /// ```no_run
    /// use ironbeam::*;
    ///
    /// #[derive(Clone)]
    /// struct Product {
    ///     name: String,
    ///     price: f64,
    /// }
    ///
    /// let p = Pipeline::default();
    /// let products = from_vec(&p, vec![
    ///     Product { name: "Book".into(), price: 15.99 },
    ///     Product { name: "Pen".into(), price: 2.50 },
    ///     Product { name: "Laptop".into(), price: 899.99 },
    /// ]);
    ///
    /// // Find affordable products (price < 20)
    /// let affordable = products.filter_by(
    ///     |product| product.price,
    ///     |price| *price < 20.0
    /// );
    ///
    /// assert_eq!(affordable.collect_seq().unwrap().len(), 2);
    /// ```
    ///
    /// # Comparison with separate map + filter
    /// This method is more efficient than using separate map and filter operations.
    ///
    /// With `filter_by`, the extraction and filtering happen in a single pass,
    /// avoiding the overhead of creating intermediate tuples.
    #[must_use]
    pub fn filter_by<V, E, P>(self, extractor: E, predicate: P) -> Self
    where
        V: Clone,
        E: Fn(&T) -> V + Send + Sync + 'static,
        P: Fn(&V) -> bool + Send + Sync + 'static,
    {
        self.filter(move |elem| {
            let value = extractor(elem);
            predicate(&value)
        })
    }
}
