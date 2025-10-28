//! Combine-by-key helpers for [`PCollection`].
//!
//! This module exposes two flavors of keyed combining:
//!
//! - [`PCollection::<(K, V)>::combine_values`] — **classic** combine-by-key over `(K, V)` pairs.
//! - [`PCollection::<(K, Vec<V>)>::combine_values_lifted`] — **lifted** combine that consumes
//!   grouped input `(K, Vec<V>)` (typically right after `group_by_key()`), allowing a
//!   [`LiftableCombiner`] to build its accumulator directly from the full group.
//!
//! Both forms ultimately produce a `(K, O)` stream by aggregating values per key. The lifted
//! variant can avoid an extra per-element pass when the combiner can initialize from a slice
//! (e.g., min/max/top-k over a group).

use crate::collection::LiftableCombiner;
use crate::node::Node;
use crate::{CombineFn, PCollection, Partition, RFBound};
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

impl<K: RFBound + Eq + Hash, V: RFBound> PCollection<(K, V)> {
    /// Combine-by-key using a user-supplied [`CombineFn`].
    ///
    /// This is the **classic** combine: the input is a stream of `(K, V)` pairs, and the combiner
    /// receives each `V` to update an accumulator `A`, which is finally turned into an output `O`.
    ///
    /// # Type Parameters
    /// - `C`: The combiner implementing `CombineFn<V, A, O>`.
    /// - `A`: Accumulator type created/merged by the combiner.
    /// - `O`: Output value produced per key.
    ///
    /// # Returns
    /// A `PCollection<(K, O)>` with one output per distinct key.
    ///
    /// # Example
    /// ```
    /// use rustflow::*;
    /// use std::collections::HashMap;
    ///
    /// // A tiny "sum" combiner for u64 values.
    /// #[derive(Clone, Default)]
    /// struct SumU64;
    /// impl CombineFn<u64, u64, u64> for SumU64 {
    ///     fn create(&self) -> u64 { 0 }
    ///     fn add_input(&self, acc: &mut u64, v: u64) { *acc += v; }
    ///     fn merge(&self, acc: &mut u64, other: u64) { *acc += other; }
    ///     fn finish(&self, acc: u64) -> u64 { acc }
    /// }
    ///
    /// let p = Pipeline::default();
    /// let kv = from_vec(&p, vec![
    ///     ("a".to_string(), 1u64),
    ///     ("a".to_string(), 2u64),
    ///     ("b".to_string(), 3u64),
    /// ]);
    ///
    /// let summed = kv.combine_values(SumU64).collect_seq_sorted().unwrap();
    /// assert_eq!(summed, vec![
    ///     ("a".to_string(), 3u64),
    ///     ("b".to_string(), 3u64),
    /// ]);
    /// ```
    pub fn combine_values<C, A, O>(self, comb: C) -> PCollection<(K, O)>
    where
        C: CombineFn<V, A, O> + 'static,
        A: Send + 'static + Sync,
        O: RFBound,
    {
        let comb = Arc::new(comb);

        // local: Vec<(K, V)> → HashMap<K, A>
        let local = {
            let comb = Arc::clone(&comb);
            Arc::new(move |p: Partition| -> Partition {
                let kv = *p
                    .downcast::<Vec<(K, V)>>()
                    .expect("combine local: bad input");
                let mut map: HashMap<K, A> = HashMap::new();
                for (k, v) in kv {
                    comb.add_input(map.entry(k).or_insert_with(|| comb.create()), v);
                }
                Box::new(map) as Partition
            })
        };

        // merge: Vec<HashMap<K, A>> → Vec<(K, O)>
        let merge = {
            let comb = Arc::clone(&comb);
            Arc::new(move |parts: Vec<Partition>| -> Partition {
                let mut accs: HashMap<K, A> = HashMap::new();
                for p in parts {
                    let m = *p
                        .downcast::<HashMap<K, A>>()
                        .expect("combine merge: bad part");
                    for (k, a) in m {
                        comb.merge(accs.entry(k).or_insert_with(|| comb.create()), a);
                    }
                }
                let out: Vec<(K, O)> = accs.into_iter().map(|(k, a)| (k, comb.finish(a))).collect();
                Box::new(out) as Partition
            })
        };

        let id = self.pipeline.insert_node(Node::CombineValues {
            local_pairs: local, // classic local for Vec<(K, V)>
            local_groups: None, // no lifted local; not grouped input
            merge,              // shared merge logic
        });
        self.pipeline.connect(self.id, id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }
}

impl<K, V> PCollection<(K, Vec<V>)>
where
    K: RFBound + Eq + Hash,
    V: RFBound,
{
    /// **Lifted combine** to be used after `group_by_key()` on `(K, Vec<V>)`.
    ///
    /// This variant lets a combiner that implements [`LiftableCombiner`] build its accumulator
    /// **once per group** from the entire slice `&[V]`, avoiding a per-element `add_input` loop in
    /// the local pass. It retains the same merge/finalization semantics as the classic form.
    ///
    /// # Type Parameters
    /// - `C`: A combiner implementing both `CombineFn<V, A, O>` and `LiftableCombiner<V, A, O>`.
    /// - `A`: Accumulator type created/merged by the combiner.
    /// - `O`: Output value produced per key.
    ///
    /// # Returns
    /// A `PCollection<(K, O)>` with one output per distinct key.
    ///
    /// # Example
    /// ```
    /// use rustflow::*;
    /// use rustflow::collection::LiftableCombiner;
    ///
    /// // A simple min combiner that can be lifted: build from an entire group.
    /// #[derive(Clone, Default)]
    /// struct MinU64;
    /// impl CombineFn<u64, Option<u64>, u64> for MinU64 {
    ///     fn create(&self) -> Option<u64> { None }
    ///     fn add_input(&self, acc: &mut Option<u64>, v: u64) {
    ///         match acc {
    ///             None => *acc = Some(v),
    ///             Some(m) => if v < *m { *m = v },
    ///         }
    ///     }
    ///     fn merge(&self, acc: &mut Option<u64>, other: Option<u64>) {
    ///         if let Some(o) = other {
    ///             self.add_input(acc, o);
    ///         }
    ///     }
    ///     fn finish(&self, acc: Option<u64>) -> u64 { acc.unwrap_or(0) }
    /// }
    /// impl LiftableCombiner<u64, Option<u64>, u64> for MinU64 {
    ///     fn build_from_group(&self, vs: &[u64]) -> Option<u64> {
    ///         vs.iter().copied().min()
    ///     }
    /// }
    ///
    /// let p = Pipeline::default();
    /// let grouped = from_vec(&p, vec![
    ///     ("a".to_string(), vec![3u64, 2u64]),
    ///     ("b".to_string(), vec![5u64]),
    /// ]);
    ///
    /// let mins = grouped.combine_values_lifted(MinU64).collect_seq_sorted().unwrap();
    /// assert_eq!(mins, vec![
    ///     ("a".to_string(), 2u64),
    ///     ("b".to_string(), 5u64),
    /// ]);
    /// ```
    pub fn combine_values_lifted<C, A, O>(self, comb: C) -> PCollection<(K, O)>
    where
        C: CombineFn<V, A, O> + LiftableCombiner<V, A, O> + 'static,
        A: Send + Sync + 'static,
        O: RFBound,
    {
        let comb = Arc::new(comb);

        // Fallback classic local for Vec<(K, V)> → HashMap<K, A> (kept for uniform node shape).
        let local_pairs = {
            let comb = Arc::clone(&comb);
            Arc::new(move |p: Partition| -> Partition {
                let kv = *p
                    .downcast::<Vec<(K, V)>>()
                    .expect("combine local_pairs: expected Vec<(K, V)>");
                let mut map: HashMap<K, A> = HashMap::new();
                for (k, v) in kv {
                    comb.add_input(map.entry(k).or_insert_with(|| comb.create()), v);
                }
                Box::new(map) as Partition
            })
        };

        // Lifted local: Vec<(K, Vec<V>)> → HashMap<K, A> using `build_from_group`
        let local_groups = {
            let comb = Arc::clone(&comb);
            Arc::new(move |p: Partition| -> Partition {
                let kvv = *p
                    .downcast::<Vec<(K, Vec<V>)>>()
                    .expect("lifted combine local: expected Vec<(K, Vec<V>)>");
                let mut map: HashMap<K, A> = HashMap::new();
                for (k, vs) in kvv {
                    let acc = comb.build_from_group(&vs);
                    map.insert(k, acc);
                }
                Box::new(map) as Partition
            })
        };

        // Merge accumulators across partitions: Vec<HashMap<K, A>> → Vec<(K, O)>
        let merge = {
            let comb = Arc::clone(&comb);
            Arc::new(move |parts: Vec<Partition>| -> Partition {
                let mut accs: HashMap<K, A> = HashMap::new();
                for p in parts {
                    let m = *p
                        .downcast::<HashMap<K, A>>()
                        .expect("lifted combine merge: expected HashMap<K, A>");
                    for (k, a) in m {
                        let entry = accs.entry(k).or_insert_with(|| comb.create());
                        comb.merge(entry, a);
                    }
                }
                let out: Vec<(K, O)> = accs.into_iter().map(|(k, a)| (k, comb.finish(a))).collect();
                Box::new(out) as Partition
            })
        };

        let id = self.pipeline.insert_node(Node::CombineValues {
            local_pairs,
            local_groups: Some(local_groups),
            merge,
        });
        self.pipeline.connect(self.id, id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }
}
