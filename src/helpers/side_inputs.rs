//! Side input helpers for `PCollection` transforms.
//!
//! Provides ergonomic APIs for injecting **read-only auxiliary data**--like small
//! vectors, hash maps, singletons, or multimaps--into map and filter operations.
//! These inputs are captured by value (cloned into `Arc`s) and broadcast
//! read-only to all workers or threads.
//!
//! ### Use cases
//! - Constant lookups and small reference tables (`side_vec`).
//! - Keyed enrichment joins (`side_hashmap`, `side_multimap`).
//! - Scalar broadcast values (`side_singleton`).
//! - Conditional filters using external lists or maps.
//!
//! Side inputs are designed for **low-volume, high-fanout** data that would be
//! inefficient to materialize as a full join. They should comfortably fit in
//! memory and remain immutable during execution.
//!
//! ### Example
//! ```no_run
//! use ironbeam::*;
//! use std::collections::HashMap;
//!
//! let p = Pipeline::default();
//! let nums = from_vec(&p, vec![1u32, 2, 3, 4]);
//! let primes = side_vec(vec![2u32, 3]);
//! let flagged = nums.map_with_side(&primes, |n, ps| ps.contains(n));
//!
//! let map = side_hashmap(vec![("a".to_string(), 1), ("b".to_string(), 2)]);
//! let kvs = from_vec(&p, vec![("a".to_string(), 10u32), ("b".to_string(), 20)]);
//! let enriched = kvs.map_with_side_map(&map, |(k, v), m| (k.clone(), v + m.get(k).unwrap_or(&0)));
//!
//! // Singleton: broadcast a single threshold value
//! let threshold = side_singleton(5u32);
//! let above = from_vec(&p, vec![3u32, 6, 2, 8])
//!     .filter_with_singleton(&threshold, |x, t| x > t);
//!
//! // Multimap: one key → many values
//! let tags = side_multimap(vec![("alice", "admin"), ("alice", "user"), ("bob", "user")]);
//! let with_tags = from_vec(&p, vec!["alice", "bob", "carol"])
//!     .map_with_side_multimap(&tags, |name, m| {
//!         let ts = m.get(name).map(Vec::as_slice).unwrap_or(&[]);
//!         (*name, ts.to_vec())
//!     });
//! ```

use crate::collection::{SideInput, SideMap, SideMultimap, SideSingleton};
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
/// ```no_run
/// use ironbeam::*;
///
/// let p = Pipeline::default();
/// let data = from_vec(&p, vec![1, 2, 3, 4]);
/// let primes = side_vec(vec![2u32, 3, 5, 7]);
///
/// let flagged = data
///     .map_with_side(&primes, |n, ps| if ps.contains(n) { n + 100 } else { *n });
/// ```
#[must_use]
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
    /// ```no_run
    /// use ironbeam::*;
    /// let p = Pipeline::default();
    /// let words = from_vec(&p, vec!["aa".to_string(), "abc".to_string()]);
    /// let lengths = side_vec::<usize>(vec![2, 3]);
    ///
    /// let marked = words.map_with_side(&lengths, |w, ls| {
    ///     if ls.contains(&w.len()) { format!("{w}:hit") } else { w.clone() }
    /// });
    /// ```
    #[must_use]
    pub fn map_with_side<O, S, F>(self, side: &SideInput<S>, f: F) -> PCollection<O>
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
    /// ```no_run
    /// use ironbeam::*;
    /// let p = Pipeline::default();
    /// let nums = from_vec(&p, vec![1u32, 2, 3, 4, 5]);
    /// let allow = side_vec(vec![2u32, 4]);
    ///
    /// let even_whitelist = nums.filter_with_side(&allow, |n, allow| allow.contains(n));
    /// ```
    #[must_use]
    pub fn filter_with_side<S, F>(self, side: &SideInput<S>, pred: F) -> Self
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
/// ```no_run
/// use ironbeam::*;
/// use std::collections::HashMap;
///
/// let p = Pipeline::default();
/// let rows = from_vec(&p, vec![("a".to_string(), 1u32), ("b".to_string(), 2)]);
///
/// let facts = side_hashmap(vec![("a".to_string(), 10u32), ("c".to_string(), 30u32)]);
///
/// let enriched = rows.map_with_side_map(&facts, |(k, v), m: &HashMap<String, u32>| {
///     let add = m.get(k).copied().unwrap_or_default();
///     (k.clone(), v + add)
/// });
/// ```
#[must_use]
pub fn side_hashmap<K: RFBound + Eq + Hash, V: RFBound>(pairs: Vec<(K, V)>) -> SideMap<K, V> {
    SideMap(Arc::new(pairs.into_iter().collect()))
}

/// Create a read-only side input backed by a single scalar value.
///
/// The value is cloned into an `Arc` once and broadcast to all workers. Use this to
/// inject a threshold, factor, or any other immutable scalar into map/filter steps.
///
/// # Examples
/// ```no_run
/// use ironbeam::*;
///
/// let p = Pipeline::default();
/// let data = from_vec(&p, vec![1u32, 5, 10, 2]);
/// let threshold = side_singleton(3u32);
///
/// let above = data.filter_with_singleton(&threshold, |x, t| x > t);
/// ```
#[must_use]
pub fn side_singleton<T: RFBound>(value: T) -> SideSingleton<T> {
    SideSingleton(Arc::new(value))
}

/// Create a read-only side input backed by a `HashMap<K, Vec<V>>` (one key → many values).
///
/// Useful for one-to-many enrichment joins. Duplicate keys are grouped automatically.
///
/// # Examples
/// ```no_run
/// use ironbeam::*;
///
/// let p = Pipeline::default();
/// let users = from_vec(&p, vec!["alice".to_string(), "bob".to_string()]);
/// let tags = side_multimap(vec![
///     ("alice".to_string(), "admin".to_string()),
///     ("alice".to_string(), "user".to_string()),
///     ("bob".to_string(), "user".to_string()),
/// ]);
///
/// let with_tags = users.map_with_side_multimap(&tags, |name, m| {
///     let ts = m.get(name).map(Vec::as_slice).unwrap_or(&[]);
///     (name.clone(), ts.to_vec())
/// });
/// ```
#[must_use]
pub fn side_multimap<K: RFBound + Eq + Hash, V: RFBound>(pairs: Vec<(K, V)>) -> SideMultimap<K, V> {
    let mut map: HashMap<K, Vec<V>> = HashMap::new();
    for (k, v) in pairs {
        map.entry(k).or_default().push(v);
    }
    SideMultimap(Arc::new(map))
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
    /// ```no_run
    /// use ironbeam::*;
    /// use std::collections::HashMap;
    ///
    /// let p = Pipeline::default();
    /// let users = from_vec(&p, vec![("u1".to_string(), 5u32), ("u2".to_string(), 7u32)]);
    /// let quotas = side_hashmap(vec![("u1".to_string(), 100u32)]);
    ///
    /// let with_quota = users.map_with_side_map(&quotas, |(u, s), m: &HashMap<String, u32>| {
    ///     let q = m.get(u).copied().unwrap_or(0);
    ///     (u.clone(), s + q)
    /// });
    /// ```
    #[must_use]
    pub fn map_with_side_map<O, K, V, F>(self, side: &SideMap<K, V>, f: F) -> PCollection<O>
    where
        O: RFBound,
        K: RFBound + Eq + Hash,
        V: RFBound,
        F: 'static + Send + Sync + Fn(&T, &HashMap<K, V>) -> O,
    {
        let side_map = side.0.clone();
        self.map(move |t: &T| f(t, &side_map))
    }

    /// Filter with a read-only **hash map** side input.
    ///
    /// The predicate receives each element and an `&HashMap<K, V>` for O(1) lookups.
    ///
    /// # Examples
    /// ```no_run
    /// use ironbeam::*;
    /// use std::collections::HashMap;
    ///
    /// # fn main() -> anyhow::Result<()> {
    /// let p = Pipeline::default();
    /// let items = from_vec(&p, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    /// let allowed = side_hashmap(vec![("a".to_string(), ()), ("b".to_string(), ())]);
    ///
    /// let valid = items.filter_with_side_map(&allowed, |item, m| m.contains_key(item));
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn filter_with_side_map<K, V, F>(self, side: &SideMap<K, V>, pred: F) -> Self
    where
        K: RFBound + Eq + Hash,
        V: RFBound,
        F: 'static + Send + Sync + Fn(&T, &HashMap<K, V>) -> bool,
    {
        let side_map = side.0.clone();
        self.filter(move |t: &T| pred(t, &side_map))
    }

    /// Map with a read-only **singleton** side input.
    ///
    /// The closure receives each element and `&S`, a reference to the broadcast value.
    ///
    /// # Examples
    /// ```no_run
    /// use ironbeam::*;
    ///
    /// # fn main() -> anyhow::Result<()> {
    /// let p = Pipeline::default();
    /// let data = from_vec(&p, vec![1u32, 2, 3, 4]);
    /// let multiplier = side_singleton(10u32);
    ///
    /// let scaled = data.map_with_singleton(&multiplier, |x, m| x * m);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn map_with_singleton<O, S, F>(self, side: &SideSingleton<S>, f: F) -> PCollection<O>
    where
        O: RFBound,
        S: RFBound,
        F: 'static + Send + Sync + Fn(&T, &S) -> O,
    {
        let arc = side.0.clone();
        self.map(move |t: &T| f(t, &arc))
    }

    /// Filter with a read-only **singleton** side input.
    ///
    /// The predicate receives each element and `&S`, a reference to the broadcast value.
    ///
    /// # Examples
    /// ```no_run
    /// use ironbeam::*;
    ///
    /// # fn main() -> anyhow::Result<()> {
    /// let p = Pipeline::default();
    /// let data = from_vec(&p, vec![1u32, 5, 10, 2]);
    /// let threshold = side_singleton(3u32);
    ///
    /// let above = data.filter_with_singleton(&threshold, |x, t| x > t);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn filter_with_singleton<S, F>(self, side: &SideSingleton<S>, pred: F) -> Self
    where
        S: RFBound,
        F: 'static + Send + Sync + Fn(&T, &S) -> bool,
    {
        let arc = side.0.clone();
        self.filter(move |t: &T| pred(t, &arc))
    }

    /// Map with a read-only **multimap** side input.
    ///
    /// The closure receives each element and `&HashMap<K, Vec<V>>` for one-to-many lookups.
    ///
    /// # Examples
    /// ```no_run
    /// use ironbeam::*;
    ///
    /// # fn main() -> anyhow::Result<()> {
    /// let p = Pipeline::default();
    /// let users = from_vec(&p, vec!["alice".to_string(), "bob".to_string()]);
    /// let tags = side_multimap(vec![
    ///     ("alice".to_string(), "admin".to_string()),
    ///     ("alice".to_string(), "user".to_string()),
    ///     ("bob".to_string(), "user".to_string()),
    /// ]);
    ///
    /// let with_tags = users.map_with_side_multimap(&tags, |name, m| {
    ///     let ts = m.get(name).map(Vec::as_slice).unwrap_or(&[]);
    ///     (name.clone(), ts.to_vec())
    /// });
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn map_with_side_multimap<O, K, V, F>(
        self,
        side: &SideMultimap<K, V>,
        f: F,
    ) -> PCollection<O>
    where
        O: RFBound,
        K: RFBound + Eq + Hash,
        V: RFBound,
        F: 'static + Send + Sync + Fn(&T, &HashMap<K, Vec<V>>) -> O,
    {
        let arc = side.0.clone();
        self.map(move |t: &T| f(t, &arc))
    }

    /// Filter with a read-only **multimap** side input.
    ///
    /// The predicate receives each element and `&HashMap<K, Vec<V>>`.
    ///
    /// # Examples
    /// ```no_run
    /// use ironbeam::*;
    ///
    /// # fn main() -> anyhow::Result<()> {
    /// let p = Pipeline::default();
    /// let users = from_vec(&p, vec!["alice".to_string(), "carol".to_string()]);
    /// let approved = side_multimap(vec![("alice".to_string(), true)]);
    ///
    /// let valid = users.filter_with_side_multimap(&approved, |name, m| m.contains_key(name));
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn filter_with_side_multimap<K, V, F>(self, side: &SideMultimap<K, V>, pred: F) -> Self
    where
        K: RFBound + Eq + Hash,
        V: RFBound,
        F: 'static + Send + Sync + Fn(&T, &HashMap<K, Vec<V>>) -> bool,
    {
        let arc = side.0.clone();
        self.filter(move |t: &T| pred(t, &arc))
    }
}
