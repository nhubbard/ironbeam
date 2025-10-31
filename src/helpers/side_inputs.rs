//! Side input helpers for `PCollection` transforms.
//!
//! Provides ergonomic APIs for injecting **read-only auxiliary data**--like small
//! vectors or hash maps--into map and filter operations. These inputs are captured
//! by value (cloned into `Arc`s) and broadcast read-only to all workers or threads.
//!
//! ### Use cases
//! - Constant lookups and small reference tables (`side_vec`).
//! - Keyed enrichment joins (`side_hashmap`).
//! - Conditional filters using external lists or maps.
//!
//! Side inputs are designed for **low-volume, high-fanout** data that would be
//! inefficient to materialize as a full join. They should comfortably fit in
//! memory and remain immutable during execution.
//!
//! ### Example
//! ```ignore
//! use rustflow::*;
//! use std::collections::HashMap;
//!
//! let p = Pipeline::default();
//! let nums = from_vec(&p, vec![1u32, 2, 3, 4]);
//! let primes = side_vec(vec![2u32, 3]);
//! let flagged = nums.map_with_side(primes, |n, ps| ps.contains(n));
//!
//! let map = side_hashmap(vec![("a".to_string(), 1), ("b".to_string(), 2)]);
//! let kvs = from_vec(&p, vec![("a".to_string(), 10), ("b".to_string(), 20)]);
//! let enriched = kvs.map_with_side_map(map, |(k, v), m| (k.clone(), v + m.get(k).unwrap_or(&0)));
//! ```

use crate::collection::{SideInput, SideMap};
use crate::{PCollection, RFBound};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

/// Create a read-only side input backed by a plain `Vec<T>`.
///
/// Side inputs are captured by value inside closures and are **not** distributed or mutated.
/// Use this to provide small lookup tables or configuration to map/filter steps.
///
/// # Examples
/// ```ignore
/// use rustflow::*;
///
/// let p = Pipeline::default();
/// let data = from_vec(&p, vec![1, 2, 3, 4]);
/// let primes = side_vec(vec![2u32, 3, 5, 7]);
///
/// let flagged = data
///     .map_with_side(primes, |n, ps| if ps.contains(n) { n + 100 } else { *n });
/// ```
pub fn side_vec<T: RFBound>(v: Vec<T>) -> SideInput<T> {
    SideInput(Arc::new(v))
}

impl<T: RFBound> PCollection<T> {
    /// Map with a read-only **vector** side input.
    ///
    /// The closure receives each element and a shared slice view of the side vector.
    /// The side input is cloned into an `Arc` once and reused across tasks/threads.
    ///
    /// # Type parameters
    /// - `T`: element type of the input collection
    /// - `S`: element type of the side vector
    /// - `O`: output element type
    ///
    /// # Examples
    /// ```ignore
    /// use rustflow::*;
    /// let p = Pipeline::default();
    /// let words = from_vec(&p, vec!["aa".to_string(), "abc".to_string()]);
    /// let lengths = side_vec::<usize>(vec![2, 3]);
    ///
    /// let marked = words.map_with_side(lengths, |w, ls| {
    ///     if ls.contains(&w.len()) { format!("{w}:hit") } else { w.clone() }
    /// });
    /// ```
    pub fn map_with_side<O, S, F>(self, side: SideInput<S>, f: F) -> PCollection<O>
    where
        O: RFBound,
        S: RFBound,
        F: 'static + Send + Sync + Fn(&T, &[S]) -> O,
    {
        let side_arc = side.0.clone();
        self.map(move |t: &T| f(t, &side_arc))
    }

    /// Filter with a read-only **vector** side input.
    ///
    /// The predicate receives each element and a shared slice view of the side vector.
    ///
    /// # Examples
    /// ```ignore
    /// use rustflow::*;
    /// let p = Pipeline::default();
    /// let nums = from_vec(&p, vec![1u32, 2, 3, 4, 5]);
    /// let allow = side_vec(vec![2u32, 4]);
    ///
    /// let even_whitelist = nums.filter_with_side(allow, |n, allow| allow.contains(n));
    /// ```
    pub fn filter_with_side<S, F>(self, side: SideInput<S>, pred: F) -> PCollection<T>
    where
        S: RFBound,
        F: 'static + Send + Sync + Fn(&T, &[S]) -> bool,
    {
        let side_arc = side.0.clone();
        self.filter(move |t: &T| pred(t, &side_arc))
    }
}

/// Create a read-only side input backed by a `HashMap<K, V>`.
///
/// Useful for lookup/enrichment joins that don't justify a full shuffle.
/// The map is built eagerly from the provided `pairs`.
///
/// # Examples
/// ```ignore
/// use rustflow::*;
/// use std::collections::HashMap;
///
/// let p = Pipeline::default();
/// let rows = from_vec(&p, vec![("a".to_string(), 1u32), ("b".to_string(), 2)]);
///
/// let facts = side_hashmap(vec![("a".to_string(), 10u32), ("c".to_string(), 30u32)]);
///
/// let enriched = rows.map_with_side_map(facts, |(k, v), m: &HashMap<String, u32>| {
///     let add = m.get(k).copied().unwrap_or_default();
///     (k.clone(), v + add)
/// });
/// ```
pub fn side_hashmap<K: RFBound + Eq + Hash, V: RFBound>(pairs: Vec<(K, V)>) -> SideMap<K, V> {
    SideMap(Arc::new(pairs.into_iter().collect()))
}

impl<T: RFBound> PCollection<T> {
    /// Map with a read-only **hash map** side input.
    ///
    /// The closure receives each element and an `&HashMap<K, V>` for O(1) lookups.
    /// Prefer this over `map_with_side` when key-based enrichment is needed.
    ///
    /// # Type parameters
    /// - `K`: map key (must be `Eq + Hash`)
    /// - `V`: map value
    /// - `O`: output element type
    ///
    /// # Examples
    /// ```ignore
    /// use rustflow::*;
    /// use std::collections::HashMap;
    ///
    /// let p = Pipeline::default();
    /// let users = from_vec(&p, vec![("u1".to_string(), 5u32), ("u2".to_string(), 7u32)]);
    /// let quotas = side_hashmap(vec![("u1".to_string(), 100u32)]);
    ///
    /// let with_quota = users.map_with_side_map(quotas, |(u, s), m: &HashMap<String, u32>| {
    ///     let q = m.get(u).copied().unwrap_or(0);
    ///     (u.clone(), s + q)
    /// });
    /// ```
    pub fn map_with_side_map<O, K, V, F>(self, side: SideMap<K, V>, f: F) -> PCollection<O>
    where
        O: RFBound,
        K: RFBound + Eq + Hash,
        V: RFBound,
        F: 'static + Send + Sync + Fn(&T, &HashMap<K, V>) -> O,
    {
        let side_map = side.0.clone();
        self.map(move |t: &T| f(t, &side_map))
    }
}
