//! Core collection types and internal stateless operators.
//!
//! This module defines:
//!
//! - [`RFBound`]: the blanket trait bound we use for data elements in pipelines.
//! - [`PCollection<T>`]: the typed, logical dataset that flows through the graph.
//! - **Stateless ops** (crate-visible): internal dynamic operators used by the
//!   planner/runner for `map`, `filter`, `flat_map`, and value-only variants.
//! - **Combine traits**: [`CombineFn`] for user-supplied combiners and
//!   [`LiftableCombiner`] for optional GBK lifting.
//! - **Side inputs**: [`SideInput`] and [`SideMap`] thin wrappers for read-only vectors/maps.
//! - **Batch ops**: crate-visible operators for batch mapping (element-wise and value-only).
//!
//! Most users interact with `PCollection` via the public helpers in
//! `helpers/*` (e.g., `map`, `filter`, `group_by_key`, `combine_values`, joins,
//! windowing). This module provides the core types those helpers build upon.

use crate::node::DynOp;
use crate::pipeline::Pipeline;
use crate::type_token::Partition;
use crate::NodeId;
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

/// The standard trait bound for elements carried by a `PCollection`.
///
/// Rustflow's runtime may move elements across threads and partitions. To keep
/// this simple and predictable, all element types must be:
///
/// - `'static` (no non-'static borrows inside elements),
/// - `Send + Sync` (safe to pass/share across threads),
/// - `Clone` (many transforms duplicate values or buffer them).
///
/// This blanket impl allows any type fitting these constraints to be used.
///
/// # Example
/// ```ignore
/// use rustflow::*;
///
/// #[derive(Clone)]
/// struct MyRow { k: String, v: u64 } // Send + Sync implied for these fields
///
/// let p = Pipeline::default();
/// let _pc: PCollection<MyRow> = from_vec(&p, vec![
///     MyRow { k: "a".into(), v: 1 },
///     MyRow { k: "b".into(), v: 2 },
/// ]);
/// # anyhow::Result::<()>::Ok(())
/// ```
pub trait RFBound: 'static + Send + Sync + Clone {}
impl<T> RFBound for T where T: 'static + Send + Sync + Clone {}

/// A typed, logical dataset (the basic building block of a flow).
///
/// `PCollection<T>` is immutable and refers to a node in the pipeline graph.
/// Transform methods (e.g., `map`, `filter`, `group_by_key`) create new
/// `PCollection`s by inserting nodes and wiring edges.
///
/// You typically obtain a `PCollection` via sources like `from_vec`, or I/O
/// helpers (feature-gated), and then chain transformations.
///
/// # Example
/// ```ignore
/// use rustflow::*;
///
/// let p = Pipeline::default();
/// let words = from_vec(&p, vec!["a".to_string(), "bb".to_string()]);
/// let lengths = words.map(|w| w.len() as u64);
/// let out: Vec<u64> = lengths.collect_seq()?;
/// assert_eq!(out, vec![1, 2]);
/// # anyhow::Result::<()>::Ok(())
/// ```
#[derive(Clone)]
pub struct PCollection<T> {
    pub(crate) pipeline: Pipeline,
    pub(crate) id: NodeId,
    pub(crate) _t: PhantomData<T>,
}

// |---------------------|
// | Stateless operators |
// |---------------------|

/// Internal dynamic implementation for `map`.
pub(crate) struct MapOp<I, O, F>(pub F, pub PhantomData<(I, O)>);

impl<I, O, F> DynOp for MapOp<I, O, F>
where
    I: RFBound,
    O: RFBound,
    F: Send + Sync + Fn(&I) -> O + 'static,
{
    fn apply(&self, input: Partition) -> Partition {
        let v = *input.downcast::<Vec<I>>().expect("MapOp input type");
        let out: Vec<O> = v.iter().map(|i| self.0(i)).collect();
        Box::new(out) as Partition
    }
}

/// Internal dynamic implementation for `map_values`.
pub(crate) struct MapValuesOp<K, V, O, F>(pub F, pub PhantomData<(K, V, O)>);

impl<K, V, O, F> DynOp for MapValuesOp<K, V, O, F>
where
    K: RFBound,
    V: RFBound,
    O: RFBound,
    F: 'static + Send + Sync + Fn(&V) -> O,
{
    fn apply(&self, p: Partition) -> Partition {
        let f = &self.0;
        let kv = *p
            .downcast::<Vec<(K, V)>>()
            .expect("MapValuesOp: expected Vec<(K,V)>");
        let out: Vec<(K, O)> = kv.into_iter().map(|(k, v)| (k, f(&v))).collect();
        Box::new(out) as Partition
    }

    // Planner capability flags:
    fn key_preserving(&self) -> bool {
        true
    }
    fn value_only(&self) -> bool {
        true
    }
    fn reorder_safe_with_value_only(&self) -> bool {
        true
    }
    fn cost_hint(&self) -> u8 {
        3
    } // cheap, but keep filters before it
}

/// Internal dynamic implementation for `filter`.
pub(crate) struct FilterOp<T, P>(pub P, pub PhantomData<T>);

impl<T, P> DynOp for FilterOp<T, P>
where
    T: RFBound,
    P: Send + Sync + Fn(&T) -> bool + 'static,
{
    fn apply(&self, input: Partition) -> Partition {
        let v = *input.downcast::<Vec<T>>().expect("FilterOp input type");
        let out: Vec<T> = v.into_iter().filter(|t| self.0(t)).collect();
        Box::new(out) as Partition
    }
}

/// Internal dynamic implementation for `filter_values`.
pub(crate) struct FilterValuesOp<K, V, F>(pub F, pub PhantomData<(K, V)>);

impl<K, V, F> DynOp for FilterValuesOp<K, V, F>
where
    K: RFBound,
    V: RFBound,
    F: 'static + Send + Sync + Fn(&V) -> bool,
{
    fn apply(&self, p: Partition) -> Partition {
        let pred = &self.0;
        let kv = *p
            .downcast::<Vec<(K, V)>>()
            .expect("FilterValuesOp: expected Vec<(K,V)>");
        let out: Vec<(K, V)> = kv.into_iter().filter(|(_, v)| pred(v)).collect();
        Box::new(out) as Partition
    }

    // Planner capability flags:
    fn key_preserving(&self) -> bool {
        true
    }
    fn value_only(&self) -> bool {
        true
    }
    fn reorder_safe_with_value_only(&self) -> bool {
        true
    }
    fn cost_hint(&self) -> u8 {
        1
    } // filters are "cheapest" -> push earlier
}

/// Internal dynamic implementation for `flat_map`.
pub(crate) struct FlatMapOp<I, O, F>(pub F, pub PhantomData<(I, O)>);

impl<I, O, F> DynOp for FlatMapOp<I, O, F>
where
    I: RFBound,
    O: RFBound,
    F: Send + Sync + Fn(&I) -> Vec<O> + 'static,
{
    fn apply(&self, input: Partition) -> Partition {
        let v = *input.downcast::<Vec<I>>().expect("FlatMapOp input type");
        let mut out: Vec<O> = Vec::new();
        for i in &v {
            out.extend(self.0(i));
        }
        Box::new(out) as Partition
    }
}

// |----------------|
// | Combine traits |
// |----------------|

/// A generic *per-key* combiner used by `combine_values` and friends.
///
/// A `CombineFn<V, A, O>` processes a stream of values `V` associated with the
/// same key by:
///
/// 1. creating an accumulator `A`,
/// 2. adding inputs into it (`add_input`),
/// 3. merging partial accumulators (`merge`),
/// 4. and finishing into an output `O`.
///
/// Combiners must be associative (and preferably commutative) to support
/// parallel execution.
///
/// See also [`LiftableCombiner`] to construct accumulators from full groups of
/// values (enabling GBK lifting).
pub trait CombineFn<V, A, O>: Send + Sync + 'static {
    /// Create a fresh accumulator.
    fn create(&self) -> A;
    /// Incorporate a single value into the accumulator.
    fn add_input(&self, acc: &mut A, v: V);
    /// Merge another accumulator into `acc`.
    fn merge(&self, acc: &mut A, other: A);
    /// Finalize the accumulator into the output value.
    fn finish(&self, acc: A) -> O;
}

/// Built-in combiner that **counts** values per key.
///
/// The accumulator type is `u64`, and the output is also `u64`.
///
/// # Example
/// ```ignore
/// use rustflow::*;
///
/// let p = Pipeline::default();
/// let kv = from_vec(&p, vec![("a", 1), ("a", 2), ("b", 3)]);
/// let counts = kv.combine_values(Count).collect_seq_sorted()?;
/// // e.g., [("a", 2), ("b", 1)]
/// # let _ = counts;
/// # anyhow::Result::<()>::Ok(())
/// ```
#[derive(Clone, Default)]
pub struct Count;

impl<V> CombineFn<V, u64, u64> for Count {
    fn create(&self) -> u64 {
        0
    }
    fn add_input(&self, acc: &mut u64, _v: V) {
        *acc += 1;
    }
    fn merge(&self, acc: &mut u64, other: u64) {
        *acc += other;
    }
    fn finish(&self, acc: u64) -> u64 {
        acc
    }
}

/// The optional ability of a combiner to *construct its accumulator from an
/// entire group* of values at once.
///
/// Using `build_from_group` lets the planner skip some shuffle barriers in
/// `group_by_key().combine_values_lifted(...)`. By default, this folds the
/// slice via `add_input`; combiners can override it for more efficient logic.
///
/// # Example
/// ```ignore
/// use rustflow::*;
///
/// // Count overrides build_from_group to use values.len()
/// let p = Pipeline::default();
/// let grouped = from_vec(&p, vec![("a", 1u8), ("a", 2u8)]).group_by_key();
/// let counts = grouped.combine_values_lifted(Count).collect_seq_sorted()?;
/// # let _ = counts;
/// # anyhow::Result::<()>::Ok(())
/// ```
pub trait LiftableCombiner<V, A, O>: CombineFn<V, A, O>
where
    V: RFBound,
{
    /// Build an accumulator directly from a slice of all values in a group.
    fn build_from_group(&self, values: &[V]) -> A {
        let mut acc = self.create();
        for v in values {
            self.add_input(&mut acc, v.clone());
        }
        acc
    }
}

impl<V: RFBound> LiftableCombiner<V, u64, u64> for Count {
    fn build_from_group(&self, values: &[V]) -> u64 {
        values.len() as u64
    }
}

// |-------------|
// | Side inputs |
// |-------------|

/// A read-only vector side input.
///
/// Constructed via helper `side_vec(v)`, then consumed by APIs like
/// `map_with_side` / `filter_with_side`.
#[derive(Clone)]
pub struct SideInput<T: RFBound>(pub Arc<Vec<T>>);

/// A read-only hash map side input.
///
/// Constructed via helper `side_hashmap(pairs)`, then consumed by
/// `map_with_side_map`.
#[derive(Clone)]
pub struct SideMap<K: RFBound + Eq + Hash, V: RFBound>(pub Arc<HashMap<K, V>>);

// |---------------------|
// | Batch map operators |
// |---------------------|

/// BatchMapOp: `&[T] -> Vec<O>`, applied chunk-by-chunk over `Vec<T>`.
/// Used by `map_batches` helper.
pub struct BatchMapOp<T, O, F>(pub usize, pub F, pub PhantomData<(T, O)>)
where
    T: 'static + Send + Sync + Clone,
    O: 'static + Send + Sync + Clone,
    F: 'static + Send + Sync + Fn(&[T]) -> Vec<O>;

impl<T, O, F> DynOp for BatchMapOp<T, O, F>
where
    T: 'static + Send + Sync + Clone,
    O: 'static + Send + Sync + Clone,
    F: 'static + Send + Sync + Fn(&[T]) -> Vec<O>,
{
    fn apply(&self, input: Partition) -> Partition {
        let batch_size = self.0.max(1); // never 0
        let f = &self.1;

        // We only support Vec<T> inputs (standard collection partitions).
        let v = *input
            .downcast::<Vec<T>>()
            .expect("BatchMapOp: expected Vec<T> input");

        let mut out = Vec::with_capacity(v.len()); // heuristic: often ~1:1

        // process in chunks of &T
        for chunk in v.chunks(batch_size) {
            let mut produced = f(chunk);
            out.append(&mut produced);
        }

        Box::new(out) as Partition
    }
}

/// BatchMapValuesOp: `&[V] -> Vec<O>`, preserves keys, applies per contiguous value slice.
/// IMPORTANT: f must output exactly as many items as the input slice length.
/// Used by `map_values_batches`.
pub struct BatchMapValuesOp<K, V, O, F>(pub usize, pub F, pub PhantomData<(K, V, O)>)
where
    K: 'static + Send + Sync + Clone,
    V: 'static + Send + Sync + Clone,
    O: 'static + Send + Sync + Clone,
    F: 'static + Send + Sync + Fn(&[V]) -> Vec<O>;

impl<K, V, O, F> DynOp for BatchMapValuesOp<K, V, O, F>
where
    K: 'static + Send + Sync + Clone,
    V: 'static + Send + Sync + Clone,
    O: 'static + Send + Sync + Clone,
    F: 'static + Send + Sync + Fn(&[V]) -> Vec<O>,
{
    fn apply(&self, input: Partition) -> Partition {
        let batch = self.0.max(1);
        let f = &self.1;

        let kv = *input
            .downcast::<Vec<(K, V)>>()
            .expect("BatchMapValuesOp: expected Vec<(K,V)> input");

        let mut out = Vec::<(K, O)>::with_capacity(kv.len());

        // Traverse in value-chunks, call f(&[V]), then re-pair outputs with the same keys.
        // Enforce f(chunk).len() == chunk.len() to preserve arity per key.
        let mut idx = 0usize;
        while idx < kv.len() {
            let end = (idx + batch).min(kv.len());
            // Gather values for the chunk (clone to build a contiguous &[V])
            let vals: Vec<V> = kv[idx..end].iter().map(|(_, v)| v.clone()).collect();
            let produced = f(&vals);
            assert_eq!(
                produced.len(),
                vals.len(),
                "BatchMapValuesOp: f(chunk) must return same length as the chunk ({} != {})",
                produced.len(),
                vals.len()
            );

            // Re-pair with the original keys in order
            for (j, o) in produced.into_iter().enumerate() {
                let k = kv[idx + j].0.clone();
                out.push((k, o));
            }

            idx = end;
        }

        Box::new(out) as Partition
    }

    // Planner hints: key-preserving + value-only + safe to reorder with other value-only ops.
    fn key_preserving(&self) -> bool {
        true
    }
    fn value_only(&self) -> bool {
        true
    }
    fn reorder_safe_with_value_only(&self) -> bool {
        true
    }
    fn cost_hint(&self) -> u8 {
        2
    }
}
