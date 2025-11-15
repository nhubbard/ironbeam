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
    /// # fn main() -> anyhow::Result<()> {
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
}

impl<K: RFBound + Eq + Hash, V: RFBound> PCollection<(K, V)> {
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
    /// # fn main() -> anyhow::Result<()> {
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
