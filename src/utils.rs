//! Utility types and functions for Rustflow.

use std::cmp::Ordering;

/// A wrapper around f64 that implements `Ord` by using `total_cmp`.
/// This allows f64 values to be used in contexts requiring total ordering,
/// such as `BinaryHeap`, sorting, and other ordered collections.
///
/// # Examples
///
/// ```
/// use rustflow::utils::OrdF64;
/// use std::collections::BinaryHeap;
///
/// let mut heap = BinaryHeap::new();
/// heap.push(OrdF64(3.14));
/// heap.push(OrdF64(2.71));
/// heap.push(OrdF64(1.41));
///
/// assert_eq!(heap.pop().unwrap().0, 3.14);
/// ```
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct OrdF64(pub f64);

impl Eq for OrdF64 {}

impl PartialOrd for OrdF64 {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdF64 {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl From<f64> for OrdF64 {
    fn from(value: f64) -> Self {
        Self(value)
    }
}

impl From<OrdF64> for f64 {
    fn from(value: OrdF64) -> Self {
        value.0
    }
}
