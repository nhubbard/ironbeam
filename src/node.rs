//! Execution graph "nodes" and the dynamic operator trait.
//!
//! This module defines:
//! - [`DynOp`]: the trait for **stateless, per-partition** operators (`map`, `filter`,
//!   `flat_map`, value-only transforms, batching, etc.). These are scheduled and
//!   fused by the planner.
//! - [`Node`]: the **typed execution IR** that the planner/runner interprets.
//!   Nodes include sources, chains of stateless ops, keyed barriers
//!   ([`Node::GroupByKey`], [`Node::CombineValues`]), binary co-groups for joins, and
//!   pre-materialized payloads.
//!
//! The planner uses the capability flags on [`DynOp`] to reorder/fuse stateless
//! ops safely and cheaply:
//! - [`DynOp::key_preserving`] -- the op keeps `(K, _)` keys unchanged.
//! - [`DynOp::value_only`] -- the op touches only the value side of `(K, V)`.
//! - [`DynOp::reorder_safe_with_value_only`] -- the op can be reordered across
//!   other `value_only` ops without changing semantics.
//! - [`DynOp::cost_hint`] -- tiny integer used to bias local ordering (smaller
//!   tends to run earlier).
//!
//! # Notes
//! * Nodes are **type-erased** at runtime via `Partition` (a boxed `Any`), but
//!   every node closure we build is typed, so downcasts are safe where used.
//! * Barriers like [`Node::GroupByKey`] and [`Node::CombineValues`] intentionally break
//!   partition parallelism to enforce global grouping/merge semantics.
//! * [`Node::CoGroup`] executes two **subplans** (left/right) and then invokes a
//!   typed closure to produce joined results; it is the building block for
//!   `join_inner`, `join_left`, `join_right`, and `join_full`.

use crate::type_token::{Partition, TypeTag, VecOps};
use std::any::Any;
use std::sync::Arc;

/// Trait for **stateless, per-partition** operators.
///
/// Implementors receive a single `Partition` (typically a `Vec<T>` or `Vec<(K,V)>`)
/// and must return a transformed `Partition`. These ops can be planned/fused
/// aggressively because they do not require cross-partition coordination.
///
/// The optional capability flags inform the planner how to reorder/fuse ops:
/// - [`Self::key_preserving`] -- returns `true` if keys are unchanged for `(K, V)` inputs.
/// - [`Self::value_only`] -- returns `true` if the op only inspects/modifies `V`.
/// - [`Self::reorder_safe_with_value_only`] -- returns `true` if reordering across
///   other `value_only` ops is semantics-preserving.
/// - [`Self::cost_hint`] -- small heuristic cost (smaller often scheduled earlier).
pub trait DynOp: Send + Sync {
    /// Apply the operator to a single partition.
    fn apply(&self, input: Partition) -> Partition;

    /// True if the op preserves the key in `(K, V)` rows.
    fn key_preserving(&self) -> bool {
        false
    }

    /// True if the op reads/writes only the value part in `(K, V)`.
    fn value_only(&self) -> bool {
        false
    }

    /// True if the op may be safely reordered with other `value_only` ops.
    fn reorder_safe_with_value_only(&self) -> bool {
        false
    }

    /// Small cost hint to help local op ordering (lower is "cheaper").
    fn cost_hint(&self) -> u8 {
        10
    }
}

/// A node in the compiled execution plan.
///
/// The runner interprets a linearized chain of nodes:
/// - A plan **must** start with a [`Node::Source`].
/// - The planner may fuse zero or more [`Node::Stateless`] segments.
/// - Barriers like [`Node::GroupByKey`] and [`Node::CombineValues`] materialize/merge partitions.
/// - [`Node::CoGroup`] executes two subplans (for joins) and then a typed exec closure.
/// - [`Node::Materialized`] anchors a pre-existing typed payload for terminal reads.
#[derive(Clone)]
pub enum Node {
    /// Start of a plan; holds the payload and the vector operations used to split/clone it.
    ///
    /// * `payload`: an `Arc<dyn Any + Send + Sync>` that stores the actual data source
    ///   (e.g., `Vec<T>`, `JsonlShards`, `CsvShards`, `ParquetShards`, …).
    /// * `vec_ops`: a type-erased strategy (`VecOps`) that can `len/split/clone_any` the payload.
    /// * `elem_tag`: a lightweight type tag used by the planner for sanity/dispatch.
    Source {
        payload: Arc<dyn Any + Send + Sync>,
        vec_ops: Arc<dyn VecOps>,
        elem_tag: TypeTag,
    },

    /// A planner-fused sequence of stateless operators (`DynOp`) to be applied to the input partition.
    Stateless(Vec<Arc<dyn DynOp>>),

    /// Combine-by-key barrier.
    ///
    /// The runner selects the appropriate `local` closure depending on whether the
    /// input is still `(K, V)` ("pairs") or already grouped as `(K, Vec<V>)` ("groups").
    ///
    /// - `local_pairs`: consumes `Vec<(K, V)>` and builds `HashMap<K, A>` (per-partition).
    /// - `local_groups`: optional lifted path that consumes `Vec<(K, Vec<V>)>` and builds `HashMap<K, A>`.
    /// - `merge`: merges all per-partition `HashMap<K, A>` into a single `Vec<(K, O)>`.
    CombineValues {
        local_pairs: Arc<dyn Fn(Partition) -> Partition + Send + Sync>,
        local_groups: Option<Arc<dyn Fn(Partition) -> Partition + Send + Sync>>,
        merge: Arc<dyn Fn(Vec<Partition>) -> Partition + Send + Sync>,
    },

    /// Group-by-key barrier.
    ///
    /// - `local`: partitions of `Vec<(K, V)>` -> `HashMap<K, Vec<V>>`
    /// - `merge`: merges `Vec<HashMap<K, Vec<V>>>` -> `Vec<(K, Vec<V>)>`
    GroupByKey {
        local: Arc<dyn Fn(Partition) -> Partition + Send + Sync>,
        merge: Arc<dyn Fn(Vec<Partition>) -> Partition + Send + Sync>,
    },

    /// Binary co-group (building block for joins).
    ///
    /// The planner injects a tiny dummy `Source` so the outer plan still begins with a `Source`.
    /// Each side is a full subplan (a cloned chain of [`Node`]s) that must ultimately
    /// produce `Vec<(K, V)>` (left) and `Vec<(K, W)>` (right).
    ///
    /// **Fields**
    /// - `left_chain`, `right_chain`: subplans to execute to materialization.
    /// - `coalesce_left`, `coalesce_right`: merge per-partition outputs on each side into single `Vec<(K, V)>`/`Vec<(K, W)>`.
    /// - `exec`: typed closure that takes the coalesced left & right partitions and returns a joined partition.
    ///
    /// **Typical `exec` outputs**
    /// - Inner join: `Vec<(K, (V, W))>`
    /// - Left/Right/Full outer: `Vec<(K, (Option<V>, Option<W>))>` with appropriate `None`s.
    CoGroup {
        left_chain: Arc<Vec<Node>>,
        right_chain: Arc<Vec<Node>>,
        coalesce_left: Arc<dyn Fn(Vec<Partition>) -> Partition + Send + Sync>,
        coalesce_right: Arc<dyn Fn(Vec<Partition>) -> Partition + Send + Sync>,
        exec: Arc<dyn Fn(Partition, Partition) -> Partition + Send + Sync>,
    },

    /// Global (non-keyed) combine:
    /// - `local`: consumes `Vec<T>` -> `A` (accumulator)
    /// - `merge`: merges `Vec<A>` -> `A`
    /// - `finish`: converts `A` -> `Vec<O>` (typically a singleton)
    /// - `fanout`: optional breadth limit for multi-round parallel reduction
    CombineGlobal {
        local: Arc<dyn Fn(Partition) -> Partition + Send + Sync>,
        merge: Arc<dyn Fn(Vec<Partition>) -> Partition + Send + Sync>,
        finish: Arc<dyn Fn(Partition) -> Partition + Send + Sync>,
        fanout: Option<usize>,
    },

    /// Pre-materialized payload (type-erased).
    ///
    /// Used by some tests/terminals to anchor a typed vector that the runner
    /// should return without further transformation.
    Materialized(Arc<dyn Any + Send + Sync>),
}
