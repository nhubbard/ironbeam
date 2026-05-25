//! Data-flow dependency barrier for [`PCollection`].
//!
//! [`PCollection::wait_on`] is the Ironbeam equivalent of Apache Beam's
//! `Wait.on`: it introduces a *signal-only* dependency between two
//! collections so that downstream processing of one collection is held until
//! a separate signal collection has fully drained — **without** consuming
//! the signal's data in the primary path.
//!
//! The canonical use case is sequencing a side-effecting branch (e.g. a
//! database write) before a downstream branch that depends on those side
//! effects existing. For example:
//!
//! ```no_run
//! # use anyhow::Result;
//! # use ironbeam::*;
//! # fn main() -> Result<()> {
//! # let p = Pipeline::default();
//! # let raw = from_vec(&p, vec![1u32]);
//! # fn write_rows(c: PCollection<u32>) -> PCollection<()> { c.map(|_| ()) }
//! # fn dependent_work(c: PCollection<u32>) -> PCollection<u32> { c }
//! // Branch A performs a side effect that produces a "done" signal.
//! let write_done = write_rows(raw.clone());
//! // Branch B must not start until A has fully drained.
//! let safe_to_use = dependent_work(raw).wait_on(&write_done);
//! let _out = safe_to_use.collect_seq()?;
//! # Ok(()) }
//! ```
//!
//! ## How the dependency is enforced
//!
//! Internally, `wait_on(&signal)` builds a [`Node::Flatten`] whose two
//! subchains are:
//!
//! 1. The signal collection's full subchain, followed by an internal
//!    discard-and-emit-empty stateless op that ignores whatever partition
//!    the signal produced and substitutes an empty `Vec<T>` (matching the
//!    data side's element type). The signal therefore *executes to
//!    completion* but contributes zero elements to the merged output.
//! 2. The data collection's full subchain, untouched.
//!
//! The runner already treats `Flatten` as a barrier — every subchain must
//! finish before the merge step runs — so anything downstream of the
//! `wait_on` result is automatically held until the signal drains. No new
//! planner pass or graph-edge concept is required.
//!
//! The signal's original element type is irrelevant to the contract: the
//! tail op is type-erased on the input side and only emits a `Vec<T>::new()`
//! of the data type. The downstream collection's element type, ordering,
//! and content are exactly the data collection's.

use crate::collection::RFBound;
use crate::node::{DynOp, Node};
use crate::type_token::{Partition, TypeTag, vec_ops_for};
use crate::{NodeId, PCollection, Pipeline};
use std::marker::PhantomData;
use std::sync::Arc;

/// Internal [`DynOp`] that discards its input partition and emits an empty
/// `Vec<T>`.
///
/// Used as the trailing op on the signal subchain inside
/// [`PCollection::wait_on`]: whatever the signal's last node produced (any
/// type) is consumed by the `Partition` parameter being dropped, and a new
/// `Vec<T>` (matching the data side) is returned. Because the op never
/// downcasts the input, the signal's element type is completely
/// type-erased away.
pub(crate) struct DiscardAndEmitEmptyOp<T>(PhantomData<T>);

impl<T> DiscardAndEmitEmptyOp<T> {
    pub(crate) const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T: RFBound> DynOp for DiscardAndEmitEmptyOp<T> {
    fn apply(&self, _input: Partition) -> Partition {
        Box::new(Vec::<T>::new()) as Partition
    }
}

/// Walk the pipeline back into a linear chain ending at `terminal`.
///
/// Mirrors [`crate::helpers::flatten`]'s private helper of the same name;
/// kept local to this module to avoid cross-module visibility churn. The
/// loop terminates naturally when either the terminal has no predecessor
/// or — defensively — its node id isn't found in the snapshot (which can
/// only happen for a corrupt pipeline graph and yields an empty chain).
fn chain_from(p: &Pipeline, terminal: NodeId) -> Vec<Node> {
    let (mut nodes, edges) = p.snapshot();
    let mut chain = Vec::<Node>::new();
    let mut cur = terminal;
    while let Some(n) = nodes.remove(&cur) {
        chain.push(n);
        if let Some((from, _)) = edges.iter().find(|(_, to)| *to == cur).copied() {
            cur = from;
        } else {
            break;
        }
    }
    chain.reverse();
    chain
}

/// Insert a tiny dummy `Source` so the outer plan always begins with a
/// `Source` node (the runner contract).
fn insert_dummy_source(p: &Pipeline) -> NodeId {
    p.insert_node(Node::Source {
        payload: Arc::new(vec![0u8]),
        vec_ops: vec_ops_for::<u8>(),
        elem_tag: TypeTag::of::<u8>(),
    })
}

impl<T: RFBound> PCollection<T> {
    /// Return a new `PCollection<T>` containing exactly the elements of
    /// `self`, but whose downstream consumers are blocked until `signal`
    /// has fully drained.
    ///
    /// This is the Ironbeam equivalent of Apache Beam's `Wait.on`. The
    /// signal collection's data is *not* observable on the primary path —
    /// only its execution is sequenced before downstream processing.
    /// `signal`'s element type may differ from `self`'s; only its
    /// completion matters.
    ///
    /// Composes with itself: `data.wait_on(&a).wait_on(&b)` blocks until
    /// both `a` and `b` (in their respective dependency layers) have
    /// drained.
    ///
    /// # Behavioural contract
    /// - The returned collection's contents and order match `self`
    ///   exactly. Naming a [`wait_on`](PCollection::wait_on) call via
    ///   `.with_name(...)` labels the Flatten barrier node it inserts.
    /// - If `signal` panics or returns an error during execution, the
    ///   error propagates up — downstream consumers of the returned
    ///   collection never see any data.
    /// - If `signal` is empty (or yields no elements after its own
    ///   transforms), the dependency still holds — its chain still
    ///   executes to coalesce before merge.
    ///
    /// # Example
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let data = from_vec(&p, vec![1u32, 2, 3]);
    /// let signal = from_vec(&p, vec!["audit".to_string()]);
    ///
    /// let gated = data.wait_on(&signal);
    /// let out = gated.collect_seq()?;
    /// assert_eq!(out, vec![1u32, 2, 3]); // unchanged
    /// # Ok(()) }
    /// ```
    ///
    /// # Panics
    ///
    /// At execution time, the internal `coalesce`/`merge` closures panic if
    /// any subchain produces a partition whose runtime type is not
    /// `Vec<T>`. The signal subchain is guaranteed to satisfy this by the
    /// discard-and-emit-empty tail; the data subchain is the caller's
    /// `PCollection<T>` chain, which the builder API guarantees terminates
    /// in `Vec<T>`.
    #[must_use]
    pub fn wait_on<S: RFBound>(self, signal: &PCollection<S>) -> Self {
        let data_chain = chain_from(&self.pipeline, self.id);

        // Build the signal subchain with the discard-and-emit-empty tail so
        // it terminates in a `Vec<T>` (always empty), letting Flatten's
        // typed coalesce/merge close over every subchain.
        let mut signal_chain = chain_from(&signal.pipeline, signal.id);
        signal_chain.push(Node::Stateless(vec![Arc::new(
            DiscardAndEmitEmptyOp::<T>::new(),
        )]));

        // Decide subchain layout:
        //
        // - If `self`'s terminal is already a `Flatten` — e.g. a prior
        //   `wait_on` call, or a user-level `flatten(&[...])` — unwrap its
        //   existing subchains and append the new signal alongside them.
        //   This keeps the resulting Flatten **flat** (no nesting) so the
        //   runner's subplan executor never has to recurse into another
        //   Flatten, which it currently refuses to do.
        // - Otherwise, treat the data chain as a single subchain. (Note:
        //   if the data chain contains a `Flatten` or `CoGroup` *in the
        //   middle* — i.e. there are intermediate transforms after the
        //   previous multi-input node — the runner will still bail. The
        //   recommended workaround is to insert `wait_on` calls before
        //   such intermediate transforms.)
        //
        // Peek at the tail via `.last()` and, when it's a Flatten, clone
        // its inner chains directly off the reference. The outer
        // `data_chain` (including the inner Flatten's dummy source) is
        // discarded with the falling-out-of-scope binding.
        let chains: Vec<Vec<Node>> = match data_chain.last() {
            Some(Node::Flatten {
                chains: inner_chains,
                ..
            }) => {
                let mut out: Vec<Vec<Node>> = (**inner_chains).clone();
                out.push(signal_chain);
                out
            }
            _ => vec![data_chain, signal_chain],
        };

        let coalesce = Arc::new(|parts: Vec<Partition>| -> Partition {
            let mut out: Vec<T> = Vec::new();
            for p in parts {
                let mut v = *p.downcast::<Vec<T>>().expect("coalesce: wrong type");
                out.append(&mut v);
            }
            Box::new(out) as Partition
        });

        let merge = Arc::new(|coalesced_inputs: Vec<Partition>| -> Partition {
            let mut result: Vec<T> = Vec::new();
            for p in coalesced_inputs {
                let mut v = *p.downcast::<Vec<T>>().expect("merge: wrong type");
                result.append(&mut v);
            }
            Box::new(result) as Partition
        });

        let pipeline = self.pipeline;
        let source_id = insert_dummy_source(&pipeline);
        let id = pipeline.insert_node(Node::Flatten {
            chains: Arc::new(chains),
            coalesce,
            merge,
        });
        pipeline.connect(source_id, id);

        Self {
            pipeline,
            id,
            _t: PhantomData,
        }
    }
}
