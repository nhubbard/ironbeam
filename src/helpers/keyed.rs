//! Keyed helpers: deriving keys and grouping by key.
//!
//! - [`PCollection::key_by`] maps each element to a `(K, T)` pair by deriving a key.
//! - [`PCollection<(K, V)>::group_by_key`] performs a local/merge aggregation to produce
//!   `(K, Vec<V>)` per key across the entire dataset.
//!
//! ### Notes
//! * `key_by` **clones** each element to keep ownership for the downstream collection.
//! * `group_by_key` materializes all values per key in memory as `Vec<V>`; for very
//!   large per-key fan-in, prefer a combiner that summarizes incrementally.

use crate::node::Node;
use crate::{PCollection, Partition, RFBound};
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

impl<T: RFBound> PCollection<T> {
    /// Derive a key for each element and emit `(K, T)` pairs.
    ///
    /// The provided `key_fn` runs once per element, and its result becomes the
    /// pair's key. The value is the original element (cloned).
    ///
    /// ### Types
    /// * `K`: key type; must be hashable and equatable.
    /// * `T`: original element type.
    ///
    /// ### Example
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let input = from_vec(&p, vec!["aa", "ab", "ba", "bb"].into_iter().map(String::from).collect::<Vec<_>>());
    ///
    /// // Key by first char
    /// let keyed = input.key_by(|s: &String| s.chars().next().unwrap());
    /// let out = keyed.collect_seq()?; // Vec<(char, String)>
    /// # Ok(()) }
    /// ```
    pub fn key_by<K, F>(self, key_fn: F) -> PCollection<(K, T)>
    where
        K: RFBound + Eq + Hash,
        F: 'static + Send + Sync + Fn(&T) -> K,
    {
        self.map(move |t| (key_fn(t), t.clone()))
    }

    /// Assign a constant key to all elements, useful for global grouping.
    ///
    /// This helper is especially useful when you want to group all elements
    /// together under a single key for global aggregations.
    ///
    /// ### Types
    /// * `K`: key type; must be hashable and equatable.
    /// * `T`: original element type.
    ///
    /// ### Example
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let input = from_vec(&p, vec![1u32, 2, 3, 4, 5]);
    ///
    /// // Assign constant key "all" to every element
    /// let keyed = input.with_constant_key("all");
    /// let grouped = keyed.group_by_key();
    /// let out = grouped.collect_seq()?; // [("all", vec![1, 2, 3, 4, 5])]
    /// # Ok(()) }
    /// ```
    pub fn with_constant_key<K>(self, key: K) -> PCollection<(K, T)>
    where
        K: RFBound + Eq + Hash,
    {
        self.map(move |t| (key.clone(), t.clone()))
    }

    /// Alias for [`key_by`](PCollection::key_by) for Apache Beam familiarity.
    ///
    /// This method is identical to `key_by` and is provided for users
    /// familiar with Apache Beam's `WithKeys` transform.
    ///
    /// ### Example
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let words = from_vec(&p, vec!["cat".to_string(), "dog".into(), "bird".into()]);
    ///
    /// // Key by string length
    /// let keyed = words.with_keys(|s: &String| s.len());
    /// let out = keyed.collect_seq()?; // Vec<(usize, String)>
    /// # Ok(()) }
    /// ```
    pub fn with_keys<K, F>(self, key_fn: F) -> PCollection<(K, T)>
    where
        K: RFBound + Eq + Hash,
        F: 'static + Send + Sync + Fn(&T) -> K,
    {
        self.key_by(key_fn)
    }
}

impl<K: RFBound + Eq + Hash, V: RFBound> PCollection<(K, V)> {
    /// Convert a `PCollection<(K, V)>` directly into a `HashMap<K, V>`.
    ///
    /// This is a terminal operation that collects all key-value pairs into a single
    /// `HashMap`. If there are duplicate keys, the last value encountered wins.
    ///
    /// ### Performance & memory
    /// This operation materializes all key-value pairs in memory as a `HashMap`.
    /// Use this when you need direct HashMap access to the results.
    ///
    /// ### Example
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let pairs = from_vec(&p, vec![("a".to_string(), 1u32), ("b".into(), 2), ("c".into(), 3)]);
    /// let map = pairs.to_hashmap()?; // HashMap<String, u32>
    /// assert_eq!(map.get("a"), Some(&1));
    /// # Ok(()) }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the pipeline execution fails.
    pub fn to_hashmap(self) -> anyhow::Result<HashMap<K, V>> {
        let vec = self.collect_seq()?;
        Ok(vec.into_iter().collect())
    }

    /// Group values by key, producing `(K, Vec<V>)`.
    ///
    /// This is a two-stage aggregation:
    /// 1. **Local** (per partition): collects values into `HashMap<K, Vec<V>>`.
    /// 2. **Merge** (global): merges all local maps and emits a flat `Vec<(K, Vec<V>)>`.
    ///
    /// ### Performance & memory
    /// Each key's values are materialized as a `Vec<V>`. If your next step is a
    /// summary like sum/min/max/etc., consider using a *combiner* (e.g.
    /// `combine_values` or `combine_values_lifted`) to avoid buffering every value.
    ///
    /// ### Example
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let pairs = from_vec(&p, vec![("a".to_string(), 1u32), ("a".into(), 2), ("b".into(), 3)]);
    /// let grouped = pairs.group_by_key(); // PCollection<(String, Vec<u32>)>
    /// let out = grouped.collect_seq()?;   // e.g. [("a", vec![1,2]), ("b", vec![3])]
    /// # Ok(()) }
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the input partition cannot be downcast to `Vec<(K, V)>`.
    #[must_use]
    pub fn group_by_key(self) -> PCollection<(K, Vec<V>)> {
        // Local stage: Vec<(K, V)> -> HashMap<K, Vec<V>>
        let local = Arc::new(|p: Partition| -> Partition {
            let kv = *p.downcast::<Vec<(K, V)>>().expect("GBK local: bad input");
            let mut m: HashMap<K, Vec<V>> = HashMap::new();
            for (k, v) in kv {
                m.entry(k).or_default().push(v);
            }
            Box::new(m) as Partition
        });

        // Merge stage: Vec<HashMap<K, Vec<V>>> -> Vec<(K, Vec<V>)>
        let merge = Arc::new(|parts: Vec<Partition>| -> Partition {
            let mut acc: HashMap<K, Vec<V>> = HashMap::new();
            for p in parts {
                let m = *p
                    .downcast::<HashMap<K, Vec<V>>>()
                    .expect("GBK merge: bad part");
                for (k, vs) in m {
                    acc.entry(k).or_default().extend(vs);
                }
            }
            Box::new(acc.into_iter().collect::<Vec<(K, Vec<V>)>>()) as Partition
        });

        let id = self.pipeline.insert_node(Node::GroupByKey { local, merge });
        self.pipeline.connect(self.id, id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }
}
