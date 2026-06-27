//! Combine-by-key helpers for [`PCollection`].
//!
//! This module exposes two flavors of keyed combining:
//!
//! - [`PCollection::<(K, V)>::combine_values`] -- **classic** combine-by-key over `(K, V)` pairs.
//! - [`PCollection::<(K, Vec<V>)>::combine_values_lifted`] -- **lifted** combine that consumes
//!   already-grouped input `(K, Vec<V>)`, building each accumulator from the full group slice
//!   via `add_input`.
//!
//! Both forms ultimately produce a `(K, O)` stream by aggregating values per key.

use crate::node::Node;
use crate::{CombineFn, Element, PCollection, Partition};
use rayon::prelude::*;
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

impl<K: Element + Eq + Hash, V: Element> PCollection<(K, V)> {
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
    /// ```no_run
    /// use ironbeam::*;
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
    ///
    /// # Panics
    ///
    /// This function panics if the downcast from `Partition` to `Vec<(K, V)>` fails.
    pub fn combine_values<C, A, O>(self, comb: C) -> PCollection<(K, O)>
    where
        C: CombineFn<V, A, O> + 'static,
        A: Send + 'static + Sync,
        O: Element,
    {
        let comb = Arc::new(comb);

        // local: Vec<(K, V)> -> HashMap<K, A>
        //
        // When the combiner is associative+commutative, group values by key first
        // then parallel-reduce each key's value list with Rayon's `reduce_with`.
        // This achieves O(log m) depth per key for keys with large fan-in.
        let local: Arc<dyn Fn(Partition) -> Partition + Send + Sync> =
            if comb.is_associative_commutative() {
                let comb = Arc::clone(&comb);
                Arc::new(move |p: Partition| -> Partition {
                    let kv = *p
                        .downcast::<Vec<(K, V)>>()
                        .expect("combine local: bad input");
                    let mut groups: HashMap<K, Vec<V>> = HashMap::new();
                    for (k, v) in kv {
                        groups.entry(k).or_default().push(v);
                    }
                    let map: HashMap<K, A> = groups
                        .into_iter()
                        .map(|(k, vals)| {
                            let acc = vals
                                .into_par_iter()
                                .map(|v| {
                                    let mut a = comb.create();
                                    comb.add_input(&mut a, v);
                                    a
                                })
                                .reduce_with(|mut a, b| {
                                    comb.merge(&mut a, b);
                                    a
                                })
                                .unwrap_or_else(|| comb.create());
                            (k, acc)
                        })
                        .collect();
                    Box::new(map) as Partition
                })
            } else {
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

        // merge: Vec<HashMap<K, A>> -> Vec<(K, O)>
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
    K: Element + Eq + Hash,
    V: Element,
{
    /// **Lifted combine** to be used on already-grouped `(K, Vec<V>)` input.
    ///
    /// Accepts input produced by `group_by_key()` or any source that already yields
    /// `(K, Vec<V>)` pairs. Each group's accumulator is built by calling `add_input`
    /// once per value, then merged across partitions and finished in the usual way.
    ///
    /// # Type Parameters
    /// - `C`: A combiner implementing `CombineFn<V, A, O>`.
    /// - `A`: Accumulator type created/merged by the combiner.
    /// - `O`: Output value produced per key.
    ///
    /// # Returns
    /// A `PCollection<(K, O)>` with one output per distinct key.
    ///
    /// # Example
    /// ```no_run
    /// use ironbeam::*;
    ///
    /// // A simple min combiner.
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
    ///         if let Some(o) = other { self.add_input(acc, o); }
    ///     }
    ///     fn finish(&self, acc: Option<u64>) -> u64 { acc.unwrap_or(0) }
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
    ///
    /// # Panics
    ///
    /// This function panics if incorrect types are used on its input.
    pub fn combine_values_lifted<C, A, O>(self, comb: C) -> PCollection<(K, O)>
    where
        C: CombineFn<V, A, O> + 'static,
        A: Send + Sync + 'static,
        O: Element,
    {
        let comb = Arc::new(comb);

        // Fallback classic local for Vec<(K, V)> -> HashMap<K, A> (kept for uniform node shape).
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

        // Lifted local: Vec<(K, Vec<V>)> -> HashMap<K, A>
        let local_groups = {
            let comb = Arc::clone(&comb);
            Arc::new(move |p: Partition| -> Partition {
                let kvv = *p
                    .downcast::<Vec<(K, Vec<V>)>>()
                    .expect("lifted combine local: expected Vec<(K, Vec<V>)>");
                let mut map: HashMap<K, A> = HashMap::new();
                for (k, vs) in kvv {
                    let mut acc = comb.create();
                    for v in vs {
                        comb.add_input(&mut acc, v);
                    }
                    map.insert(k, acc);
                }
                Box::new(map) as Partition
            })
        };

        // Merge accumulators across partitions: Vec<HashMap<K, A>> -> Vec<(K, O)>
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
