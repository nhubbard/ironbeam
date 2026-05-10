//! Execution engine.
//!
//! The `Runner` executes an optimized, linearized plan produced by the planner.
//! It supports both **sequential** and **parallel** execution modes:
//!
//! - **Sequential** walks the node chain in a single thread, materializing one
//!   partition buffer at a time.
//! - **Parallel** uses `rayon` to evaluate partition-local work in parallel,
//!   coalescing at barriers such as `GroupByKey`, `CombineValues`, and `CoGroup`.
//!
//! Determinism: within a single partition, stateless transforms preserve element
//! order. Parallel execution may interleave partitions; callers that require a
//! stable final order can use the `collect_*_sorted` helpers after the collection
//! is complete.

use crate::NodeId;
use crate::node::Node;
use crate::pipeline::Pipeline;
use crate::planner::{build_plan, find_deepest_fanout_ancestor};
use crate::type_token::{Partition, TypeTag, vec_ops_for};
use anyhow::{Result, anyhow, bail};
use ordered_float::NotNan;
use rayon::ThreadPoolBuilder;
use rayon::prelude::*;
use std::any::Any;
use std::collections::{BinaryHeap, HashMap};
use std::sync::{Arc, Mutex};

#[cfg(feature = "checkpointing")]
use crate::checkpoint::CheckpointConfig;

/// A shared cache for Common Subexpression Elimination (CSE).
///
/// Maps a [`NodeId`] to the type-erased `Vec<T>` result materialized at that node.
/// Pass the **same** `SharedCSECache` to multiple [`Runner::run_collect_cached`] calls
/// that share a common pipeline prefix so the shared work executes only once.
///
/// The cache is correct for the lifetime of the owning [`Pipeline`]: pipelines are
/// append-only (nodes and edges are never removed), so cached results remain valid.
///
/// # Type invariant
///
/// Each entry stores `Arc<Vec<T>>` for the *exact* `T` produced by that node.
/// [`Runner::run_collect_cached`] downcasts on retrieval; a mismatch returns an error.
pub type SharedCSECache = Arc<Mutex<HashMap<NodeId, Arc<dyn Any + Send + Sync>>>>;

/// Execution mode for a plan.
///
/// - `Sequential` runs in a single thread.
/// - `Parallel` runs with optional thread count and partition count hints.
///   If `threads` is `Some(n)`, a global rayon thread pool with `n` threads
///   is installed for this process (first one wins; later calls are no-ops).
///   If `partitions` is `None`, the planner's suggestion (if any) is used,
///   otherwise `Runner::default_partitions`.
#[derive(Clone, Copy, Debug)]
pub enum ExecMode {
    /// Single-threaded execution.
    Sequential,
    /// Parallel execution using rayon.
    Parallel {
        /// Optional rayon worker thread count.
        threads: Option<usize>,
        /// Optional number of source partitions.
        partitions: Option<usize>,
    },
}

/// Executes a pipeline produced by the builder API.
///
/// Construct a `Runner` and call [`Runner::run_collect`] with a pipeline and
/// terminal node id. See `helpers` for higher-level `collect_*` convenience
/// methods that build a `Runner` for you.
pub struct Runner {
    /// Selected execution mode.
    pub mode: ExecMode,
    /// Default partition count when neither the caller nor the planner suggests one.
    pub default_partitions: usize,
    /// Optional checkpoint configuration for fault tolerance.
    #[cfg(feature = "checkpointing")]
    pub checkpoint_config: Option<CheckpointConfig>,
}

impl Default for Runner {
    fn default() -> Self {
        Self {
            mode: ExecMode::Parallel {
                threads: None,
                partitions: None,
            },
            // Heuristic default: 2× hardware threads (min 2)
            default_partitions: 2 * num_cpus::get().max(2),
            #[cfg(feature = "checkpointing")]
            checkpoint_config: None,
        }
    }
}

impl Runner {
    /// Execute the pipeline ending at `terminal`, collecting the terminal
    /// vector as `Vec<T>`.
    ///
    /// This function:
    /// 1. Builds an optimized plan with the planner.
    /// 2. Chooses sequential or parallel engine based on `self.mode`.
    /// 3. Honors planner's suggested partitioning unless overridden.
    ///
    /// # Errors
    /// An error is returned if the plan is malformed (e.g., a missing source),
    /// if a node encounters an unexpected input type, or if the terminal
    /// materialized type does not match `T`.
    ///
    /// # Panics
    ///
    /// If the pipeline is in an inconsistent state, such as during concurrent modifications.
    pub fn run_collect<T: 'static + Send + Sync + Clone>(
        &self,
        p: &Pipeline,
        terminal: NodeId,
    ) -> Result<Vec<T>> {
        #[cfg(feature = "metrics")]
        p.record_metrics_start();

        let plan = build_plan(p, terminal)?;
        let chain = plan.chain;
        let suggested_parts = plan.suggested_partitions;
        let limit = plan.limit;

        #[cfg(feature = "checkpointing")]
        let checkpoint_enabled = self.checkpoint_config.as_ref().is_some_and(|c| c.enabled);

        #[cfg(feature = "checkpointing")]
        let result = if checkpoint_enabled {
            let config = self.checkpoint_config.as_ref().unwrap().clone();
            match self.mode {
                ExecMode::Sequential => exec_seq_with_checkpointing::<T>(chain, config),
                ExecMode::Parallel {
                    threads,
                    partitions,
                } => {
                    if let Some(t) = threads {
                        ThreadPoolBuilder::new().num_threads(t).build_global().ok();
                    }
                    let parts = partitions
                        .or(suggested_parts)
                        .unwrap_or(self.default_partitions);
                    exec_par_with_checkpointing::<T>(&chain, parts, config)
                }
            }
        } else {
            match self.mode {
                ExecMode::Sequential => exec_seq::<T>(chain),
                ExecMode::Parallel {
                    threads,
                    partitions,
                } => {
                    if let Some(t) = threads {
                        ThreadPoolBuilder::new().num_threads(t).build_global().ok();
                    }
                    let parts = partitions
                        .or(suggested_parts)
                        .unwrap_or(self.default_partitions);
                    exec_par::<T>(&chain, parts, limit)
                }
            }
        };

        #[cfg(not(feature = "checkpointing"))]
        let result = match self.mode {
            ExecMode::Sequential => exec_seq::<T>(chain),
            ExecMode::Parallel {
                threads,
                partitions,
            } => {
                if let Some(t) = threads {
                    // Best-effort: first builder to install wins globally.
                    ThreadPoolBuilder::new().num_threads(t).build_global().ok();
                }
                let parts = partitions
                    .or(suggested_parts)
                    .unwrap_or(self.default_partitions);
                exec_par::<T>(chain, parts, limit)
            }
        };

        #[cfg(feature = "metrics")]
        p.record_metrics_end();

        result
    }

    /// Execute the pipeline ending at `terminal` with Common Subexpression Elimination.
    ///
    /// Identical to [`Runner::run_collect`] for pipelines with no shared prefix.  When
    /// multiple `PCollection`s share a common upstream subgraph (a *fan-out* node), this
    /// method materializes and caches the result of the deepest shared ancestor the first
    /// time it is reached, then reuses it for every subsequent call that walks through
    /// the same ancestor.
    ///
    /// Pass the **same** `cache` instance across all calls that should share work:
    ///
    /// ```no_run
    /// use ironbeam::{Pipeline, Runner, SharedCSECache, from_vec};
    /// use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let shared = from_vec(&p, vec![1u32, 2, 3]).map(|x: &u32| x + 10);
    /// let a = shared.clone().map(|x: &u32| x * 2);
    /// let b = shared.map(|x: &u32| x + 1);
    ///
    /// let cache = SharedCSECache::default();
    /// let runner = Runner::default();
    /// let out_a = runner.run_collect_cached::<u32>(&p, a.node_id(), &cache)?;
    /// let out_b = runner.run_collect_cached::<u32>(&p, b.node_id(), &cache)?;
    /// // The `+10` map ran only 3 times total, not 6.
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Type invariant
    ///
    /// The fan-out ancestor node must produce `Vec<T>`.  If the shared prefix ends with
    /// a different intermediate type, the cache insertion fails and an error is returned.
    /// In that case use [`Runner::run_collect`] directly.
    ///
    /// # Errors
    ///
    /// Same as [`Runner::run_collect`], plus a type-mismatch error if the fan-out
    /// ancestor does not produce `Vec<T>`.
    ///
    /// # Panics
    ///
    /// If the pipeline or cache mutex is in a poisoned state.
    pub fn run_collect_cached<T: 'static + Send + Sync + Clone>(
        &self,
        p: &Pipeline,
        terminal: NodeId,
        cache: &SharedCSECache,
    ) -> Result<Vec<T>> {
        let (nodes, edges) = p.snapshot();

        let Some(fanout_id) = find_deepest_fanout_ancestor(&edges, terminal) else {
            return self.run_collect::<T>(p, terminal);
        };

        let cached_vec: Vec<T> = {
            let maybe_arc = cache.lock().unwrap().get(&fanout_id).cloned();
            if let Some(arc) = maybe_arc {
                arc.downcast::<Vec<T>>()
                    .map(|a| (*a).clone())
                    .map_err(|_| anyhow!("CSE: cached type mismatch at node {fanout_id:?}"))?
            } else {
                let prefix_result = self.run_collect::<T>(p, fanout_id)?;
                cache
                    .lock()
                    .unwrap()
                    .insert(fanout_id, Arc::new(prefix_result.clone()));
                prefix_result
            }
        };

        run_collect_suffix(self, terminal, fanout_id, cached_vec, &nodes, &edges)
    }
}

/// Build and execute the suffix chain from just after `fanout_id` to `terminal`,
/// seeding it with `cached` as the initial source data.
///
/// A fresh mini-pipeline is assembled so the standard planner passes (fusion,
/// predicate pushdown, etc.) are applied to the suffix just as they would be to
/// a normal top-level plan.
fn run_collect_suffix<T: 'static + Send + Sync + Clone>(
    runner: &Runner,
    terminal: NodeId,
    fanout_id: NodeId,
    cached: Vec<T>,
    nodes: &HashMap<NodeId, Node>,
    edges: &[(NodeId, NodeId)],
) -> Result<Vec<T>> {
    // Collect the ordered list of node IDs on the path [fanout_id+1 .. terminal].
    let mut suffix_ids = Vec::new();
    let mut cur = terminal;
    while cur != fanout_id {
        suffix_ids.push(cur);
        let (from, _) = edges
            .iter()
            .find(|(_, to)| *to == cur)
            .copied()
            .ok_or_else(|| anyhow!("CSE: no predecessor for node {cur:?} while building suffix"))?;
        cur = from;
    }
    suffix_ids.reverse(); // source-to-terminal order

    // Build a mini-pipeline: Source(cached) → [suffix nodes...]
    let new_p = Pipeline::default();
    let source_id = new_p.insert_node(Node::Source {
        payload: Arc::new(cached),
        vec_ops: vec_ops_for::<T>(),
        elem_tag: TypeTag::of::<T>(),
    });

    let mut prev_id = source_id;
    for &orig_id in &suffix_ids {
        let node = nodes
            .get(&orig_id)
            .ok_or_else(|| anyhow!("CSE: missing node {orig_id:?} in suffix build"))?
            .clone();
        let new_id = new_p.insert_node(node);
        new_p.connect(prev_id, new_id);
        prev_id = new_id;
    }

    runner.run_collect::<T>(&new_p, prev_id)
}

/// Execute a fully linearized chain **sequentially**, collecting `Vec<T>`.
///
/// Internal helper used by [`Runner::run_collect`]. Walks the chain left->right,
/// maintaining a single opaque `Partition` buffer.
#[allow(clippy::too_many_lines)]
fn exec_seq<T: 'static + Send + Sync + Clone>(chain: Vec<Node>) -> Result<Vec<T>> {
    let mut buf: Option<Partition> = None;

    let run_subplan_seq = |chain: Vec<Node>| -> Result<Vec<Partition>> {
        let mut curr: Option<Partition> = None;
        for node in chain {
            curr = Some(match node {
                Node::Source {
                    payload, vec_ops, ..
                } => vec_ops
                    .clone_any(payload.as_ref())
                    .ok_or_else(|| anyhow!("unsupported source vec type"))?,
                Node::Stateless(ops) => ops
                    .into_iter()
                    .fold(curr.take().unwrap(), |acc, op| op.apply(acc)),
                Node::GroupByKey { local, merge } => {
                    let mid = local(curr.take().unwrap());
                    merge(vec![mid])
                }
                Node::CombineValues {
                    local_pairs,
                    local_groups,
                    merge,
                } => {
                    // choose which local to run based on the presence of local_groups
                    let local = local_groups.map_or(local_pairs, |lg| lg);
                    let mid = local(curr.take().unwrap());
                    merge(vec![mid])
                }
                Node::Materialized(p) => Box::new(p) as Partition,
                Node::Flatten { .. } => bail!("nested Flatten not supported in subplan"),
                Node::CoGroup { .. } => bail!("nested CoGroup not supported in subplan"),
                Node::Reshuffle { .. } => {
                    bail!("Reshuffle not supported in CoGroup/Flatten subplans")
                }
                Node::CombineGlobal {
                    local,
                    merge,
                    finish,
                    ..
                } => {
                    let mid = local(curr.take().unwrap());
                    let acc = merge(vec![mid]);
                    if let Some(h) = acc.downcast_ref::<BinaryHeap<NotNan<f64>>>() {
                        eprintln!("DEBUG: KMV heap len = {}", h.len()); // should be <= k
                    }
                    finish(acc)
                }
            });
        }
        Ok(vec![curr.unwrap()])
    };

    for node in chain {
        buf = Some(match node {
            Node::Flatten {
                chains,
                coalesce,
                merge,
            } => {
                let mut coalesced_inputs: Vec<Partition> = Vec::new();
                for chain in chains.iter() {
                    let mut parts = run_subplan_seq(chain.clone())?;
                    let single: Partition = if parts.len() == 1 {
                        parts.pop().unwrap()
                    } else {
                        coalesce(parts)
                    };
                    coalesced_inputs.push(single);
                }
                merge(coalesced_inputs)
            }
            Node::CoGroup {
                left_chain,
                right_chain,
                coalesce_left,
                coalesce_right,
                exec,
                ..
            } => {
                let mut left_parts = run_subplan_seq((*left_chain).clone())?;
                let mut right_parts = run_subplan_seq((*right_chain).clone())?;

                let left_single: Partition = if left_parts.len() == 1 {
                    left_parts.pop().unwrap()
                } else {
                    coalesce_left(left_parts)
                };
                let right_single: Partition = if right_parts.len() == 1 {
                    right_parts.pop().unwrap()
                } else {
                    coalesce_right(right_parts)
                };

                exec(left_single, right_single)
            }
            Node::Source {
                payload, vec_ops, ..
            } => vec_ops
                .clone_any(payload.as_ref())
                .ok_or_else(|| anyhow!("unsupported source vec type"))?,
            Node::Stateless(ops) => ops
                .into_iter()
                .fold(buf.take().unwrap(), |acc, op| op.apply(acc)),
            Node::GroupByKey { local, merge } => {
                let mid = local(buf.take().unwrap());
                merge(vec![mid])
            }
            Node::CombineValues {
                local_pairs,
                local_groups,
                merge,
            } => {
                let local = local_groups.map_or(local_pairs, |lg| lg);
                let mid = local(buf.take().unwrap());
                merge(vec![mid])
            }
            Node::Materialized(p) => Box::new(
                p.downcast_ref::<Vec<T>>()
                    .cloned()
                    .ok_or_else(|| anyhow!("terminal type mismatch"))?,
            ) as Partition,
            Node::CombineGlobal {
                local,
                merge,
                finish,
                ..
            } => {
                let mid_acc = local(buf.take().unwrap());
                let acc = merge(vec![mid_acc]);
                finish(acc)
            }
            Node::Reshuffle { reshuffle } => reshuffle(vec![buf.take().unwrap()], 1)
                .into_iter()
                .next()
                .expect("Reshuffle returned empty vec in sequential mode"),
        });
    }

    let out = buf.unwrap();
    let v = *out
        .downcast::<Vec<T>>()
        .map_err(|_| anyhow!("terminal type mismatch"))?;
    Ok(v)
}

/// Execute a fully linearized chain **in parallel**, collecting `Vec<T>`.
///
/// Internal helper used by [`Runner::run_collect`]. Partitions the head source
/// and applies stateless runs with rayon. Barriers (`GroupByKey`, `CombineValues`,
/// `CoGroup`) perform a parallel local phase followed by a global merge.
///
/// When `limit` is `Some(n)` the final merge step stops accumulating elements as
/// soon as `n` total have been collected — providing early termination for
/// pipelines that end with `take(n)` / `first()`.
#[allow(clippy::too_many_lines)]
fn exec_par<T: 'static + Send + Sync + Clone>(
    chain: &[Node],
    partitions: usize,
    limit: Option<usize>,
) -> Result<Vec<T>> {
    /// Run a nested subplan (used by `CoGroup`) in parallel, returning a vector
    /// of partitions. The subplan must start with a `Source`. Nested `CoGroup`
    /// inside a subplan is not supported.
    fn run_subplan_par(chain: &[Node], partitions: usize) -> Result<Vec<Partition>> {
        let (payload, vec_ops, rest) = match &chain[0] {
            Node::Source {
                payload, vec_ops, ..
            } => (payload.clone(), vec_ops.clone(), &chain[1..]),
            _ => bail!("subplan must start with a Source"),
        };
        let total_len = vec_ops.len(payload.as_ref()).unwrap_or(0);
        let parts = partitions.max(1).min(total_len.max(1));
        let mut curr = vec_ops.split(payload.as_ref(), parts).unwrap_or_else(|| {
            vec![
                vec_ops
                    .clone_any(payload.as_ref())
                    .expect("cloneable source"),
            ]
        });

        let mut i = 0usize;
        while i < rest.len() {
            match &rest[i] {
                Node::Stateless(_) => {
                    let mut ops = Vec::new();
                    while i < rest.len() {
                        if let Node::Stateless(more) = &rest[i] {
                            ops.extend(more.iter().cloned());
                            i += 1;
                        } else {
                            break;
                        }
                    }
                    curr = curr
                        .into_par_iter()
                        .map(|p| ops.iter().fold(p, |acc, op| op.apply(acc)))
                        .collect();
                }
                Node::GroupByKey { local, merge } => {
                    let mids: Vec<Partition> = curr.into_par_iter().map(|p| local(p)).collect();
                    curr = vec![merge(mids)];
                    i += 1;
                }
                Node::CombineValues {
                    local_pairs,
                    local_groups,
                    merge,
                } => {
                    let local = local_groups
                        .as_ref()
                        .map_or_else(|| local_pairs.clone(), |lg| lg.clone());
                    let mids: Vec<Partition> = curr.into_par_iter().map(|p| local(p)).collect();
                    curr = vec![merge(mids)];
                    i += 1;
                }
                Node::Source { .. } | Node::Materialized(_) => {
                    bail!("unexpected source/materialized in subplan")
                }
                Node::Flatten { .. } => bail!("nested Flatten not supported in subplan"),
                Node::CoGroup { .. } => bail!("nested CoGroup not supported in subplan"),
                Node::Reshuffle { .. } => {
                    bail!("Reshuffle not supported in CoGroup/Flatten subplans")
                }
                Node::CombineGlobal {
                    local,
                    merge,
                    finish,
                    fanout,
                    tree_reduce,
                } => {
                    let accs: Vec<Partition> = curr.into_par_iter().map(|p| local(p)).collect();

                    let acc = if *tree_reduce {
                        let merge = Arc::clone(merge);
                        accs.into_par_iter()
                            .reduce_with(|a, b| merge(vec![a, b]))
                            .unwrap_or_else(|| merge(Vec::new()))
                    } else {
                        let mut accs = accs;
                        let f = fanout.unwrap_or(usize::MAX).max(1);
                        while accs.len() > 1 {
                            if f == usize::MAX {
                                accs = vec![merge(accs)];
                                break;
                            }
                            let mut next: Vec<Partition> =
                                Vec::with_capacity(accs.len().div_ceil(f));
                            let mut it = accs.into_iter();
                            loop {
                                let mut group: Vec<Partition> = Vec::with_capacity(f);
                                for _ in 0..f {
                                    if let Some(p) = it.next() {
                                        group.push(p);
                                    } else {
                                        break;
                                    }
                                }
                                if group.is_empty() {
                                    break;
                                }
                                next.push(merge(group));
                            }
                            accs = next;
                        }
                        accs.into_iter().next().unwrap_or_else(|| merge(Vec::new()))
                    };

                    if let Some(h) = acc.downcast_ref::<BinaryHeap<NotNan<f64>>>() {
                        eprintln!("DEBUG: KMV heap len = {}", h.len()); // should be <= k
                    }
                    curr = vec![finish(acc)];
                    i += 1;
                }
            }
        }
        Ok(curr)
    }

    let (payload, vec_ops, rest) = match &chain[0] {
        Node::Source {
            payload, vec_ops, ..
        } => (Arc::clone(payload), Arc::clone(vec_ops), &chain[1..]),
        _ => bail!("execution plan must start with a Source node"),
    };

    let total_len = vec_ops.len(payload.as_ref()).unwrap_or(0);
    let parts = partitions.max(1).min(total_len.max(1));
    let mut curr = vec_ops.split(payload.as_ref(), parts).unwrap_or_else(|| {
        vec![
            vec_ops
                .clone_any(payload.as_ref())
                .expect("cloneable source"),
        ]
    });

    let mut i = 0usize;
    while i < rest.len() {
        match &rest[i] {
            Node::Stateless(_) => {
                let mut ops = Vec::new();
                while i < rest.len() {
                    if let Node::Stateless(more) = &rest[i] {
                        ops.extend(more.iter().cloned());
                        i += 1;
                    } else {
                        break;
                    }
                }
                curr = curr
                    .into_par_iter()
                    .map(|p| ops.iter().fold(p, |acc, op| op.apply(acc)))
                    .collect();
            }
            Node::GroupByKey { local, merge } => {
                let mids: Vec<Partition> = curr.into_par_iter().map(|p| local(p)).collect();
                curr = vec![merge(mids)];
                i += 1;
            }
            Node::CombineValues {
                local_pairs,
                local_groups,
                merge,
            } => {
                let local = local_groups
                    .as_ref()
                    .map_or_else(|| local_pairs.clone(), |lg| lg.clone());
                let mids: Vec<Partition> = curr.into_par_iter().map(|p| local(p)).collect();
                curr = vec![merge(mids)];
                i += 1;
            }
            Node::Flatten {
                chains,
                coalesce,
                merge,
            } => {
                // Run each branch concurrently via Rayon; Result propagation via collect.
                let coalesced_inputs: Vec<Partition> = chains
                    .par_iter()
                    .map(|chain| {
                        let parts = run_subplan_par(chain, partitions)?;
                        Ok(if parts.len() == 1 {
                            parts.into_iter().next().unwrap()
                        } else {
                            coalesce(parts)
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                curr = vec![merge(coalesced_inputs)];
                i += 1;
            }
            Node::CoGroup {
                left_chain,
                right_chain,
                coalesce_left,
                coalesce_right,
                exec,
                ..
            } => {
                // Run both subplans concurrently via rayon::join (same thread pool,
                // no oversubscription). Results are propagated after the join.
                let lc = (**left_chain).clone();
                let rc = (**right_chain).clone();
                let (left_result, right_result) = rayon::join(
                    || run_subplan_par(&lc, partitions),
                    || run_subplan_par(&rc, partitions),
                );
                let left_parts = left_result?;
                let right_parts = right_result?;

                let left_single = if left_parts.len() == 1 {
                    left_parts.into_iter().next().unwrap()
                } else {
                    coalesce_left(left_parts)
                };
                let right_single = if right_parts.len() == 1 {
                    right_parts.into_iter().next().unwrap()
                } else {
                    coalesce_right(right_parts)
                };

                curr = vec![exec(left_single, right_single)];
                i += 1;
            }
            Node::Source { .. } | Node::Materialized(_) => {
                bail!("unexpected additional source/materialized")
            }
            Node::CombineGlobal {
                local,
                merge,
                finish,
                fanout,
                tree_reduce,
            } => {
                let accs: Vec<Partition> = curr.into_par_iter().map(|p| local(p)).collect();

                let acc = if *tree_reduce {
                    // O(log n) parallel tree reduction: Rayon's work-stealing
                    // `reduce_with` processes accumulators in a binary fan-in
                    // pattern, achieving O(log n) critical-path depth.
                    let merge = Arc::clone(merge);
                    accs.into_par_iter()
                        .reduce_with(|a, b| merge(vec![a, b]))
                        .unwrap_or_else(|| merge(Vec::new()))
                } else {
                    // Fanout-based sequential merge loop (original strategy).
                    let mut accs = accs;
                    let f = fanout.unwrap_or(usize::MAX).max(1);
                    while accs.len() > 1 {
                        if f == usize::MAX {
                            accs = vec![merge(accs)];
                            break;
                        }
                        let mut next: Vec<Partition> = Vec::with_capacity(accs.len().div_ceil(f));
                        let mut it = accs.into_iter();
                        loop {
                            let mut group: Vec<Partition> = Vec::with_capacity(f);
                            for _ in 0..f {
                                if let Some(p) = it.next() {
                                    group.push(p);
                                } else {
                                    break;
                                }
                            }
                            if group.is_empty() {
                                break;
                            }
                            next.push(merge(group));
                        }
                        accs = next;
                    }
                    accs.into_iter().next().unwrap_or_else(|| merge(Vec::new()))
                };

                if let Some(h) = acc.downcast_ref::<BinaryHeap<NotNan<f64>>>() {
                    eprintln!("DEBUG: KMV heap len = {}", h.len()); // should be <= k
                }
                curr = vec![finish(acc)];
                i += 1;
            }
            Node::Reshuffle { reshuffle } => {
                // Parallel: collect all partitions, re-distribute across `partitions` lanes.
                // This re-expands parallelism so downstream stateless ops run on all cores.
                curr = reshuffle(curr, partitions);
                i += 1;
            }
        }
    }

    if curr.len() == 1 {
        let one = curr.into_iter().next().unwrap();
        let mut v = *one
            .downcast::<Vec<T>>()
            .map_err(|_| anyhow!("terminal type mismatch"))?;
        if let Some(n) = limit {
            v.truncate(n);
        }
        Ok(v)
    } else {
        let mut out = Vec::<T>::new();
        for part in curr {
            let v = *part
                .downcast::<Vec<T>>()
                .map_err(|_| anyhow!("terminal type mismatch"))?;
            if let Some(n) = limit {
                let remaining = n.saturating_sub(out.len());
                if remaining == 0 {
                    break;
                }
                out.extend(v.into_iter().take(remaining));
                if out.len() >= n {
                    break;
                }
            } else {
                out.extend(v);
            }
        }
        Ok(out)
    }
}

/// Execute a fully linearized chain **sequentially** with checkpointing support.
///
/// This version saves checkpoint state at configurable intervals and can resume
/// from the last checkpoint on failure. Note: Due to type-erasure, we cannot
/// serialize intermediate partition state, so checkpoints track progress only.
/// On recovery, the pipeline re-executes from the start but logs progress.
#[cfg(feature = "checkpointing")]
#[allow(clippy::too_many_lines)]
fn exec_seq_with_checkpointing<T: 'static + Send + Sync + Clone>(
    chain: Vec<Node>,
    config: CheckpointConfig,
) -> Result<Vec<T>> {
    use crate::checkpoint::{
        CheckpointManager, CheckpointMetadata, CheckpointState, compute_checksum,
        current_timestamp_ms, generate_pipeline_id,
    };

    let total_nodes = chain.len();
    let mut manager = CheckpointManager::new(config)?;

    let pipeline_id = generate_pipeline_id(&format!("{:?}", chain.len()));

    if manager.config.auto_recover
        && let Some(checkpoint_path) = manager.find_latest_checkpoint(&pipeline_id)?
    {
        eprintln!("[Checkpoint] Found existing checkpoint, attempting recovery...");
        match manager.load_checkpoint(&checkpoint_path) {
            Ok(state) => {
                eprintln!(
                    "[Checkpoint] Recovered from node {} ({:.0}% complete)",
                    state.completed_node_index, state.metadata.progress_percent
                );
                // Type-erasure prevents restoring partition state; we re-execute from the start.
            }
            Err(e) => {
                eprintln!("[Checkpoint] Failed to load checkpoint: {e}");
            }
        }
    }

    let mut buf: Option<Partition> = None;

    for (idx, node) in chain.into_iter().enumerate() {
        let is_barrier = matches!(
            node,
            Node::GroupByKey { .. }
                | Node::CombineValues { .. }
                | Node::Flatten { .. }
                | Node::CoGroup { .. }
                | Node::CombineGlobal { .. }
                | Node::Reshuffle { .. }
        );

        let node_type = match &node {
            Node::Source { .. } => "Source",
            Node::Stateless(_) => "Stateless",
            Node::GroupByKey { .. } => "GroupByKey",
            Node::CombineValues { .. } => "CombineValues",
            Node::Flatten { .. } => "Flatten",
            Node::CoGroup { .. } => "CoGroup",
            Node::Materialized(_) => "Materialized",
            Node::CombineGlobal { .. } => "CombineGlobal",
            Node::Reshuffle { .. } => "Reshuffle",
        };

        buf = Some(match node {
            Node::Source {
                payload, vec_ops, ..
            } => vec_ops
                .clone_any(payload.as_ref())
                .ok_or_else(|| anyhow!("unsupported source vec type"))?,
            Node::Stateless(ops) => ops
                .into_iter()
                .fold(buf.take().unwrap(), |acc, op| op.apply(acc)),
            Node::GroupByKey { local, merge } => {
                let mid = local(buf.take().unwrap());
                merge(vec![mid])
            }
            Node::CombineValues {
                local_pairs,
                local_groups,
                merge,
            } => {
                let local = local_groups.map_or(local_pairs, |lg| lg);
                let mid = local(buf.take().unwrap());
                merge(vec![mid])
            }
            Node::Materialized(p) => Box::new(
                p.downcast_ref::<Vec<T>>()
                    .cloned()
                    .ok_or_else(|| anyhow!("terminal type mismatch"))?,
            ) as Partition,
            Node::Flatten { .. } => bail!("Flatten requires subplan execution"),
            Node::CoGroup { .. } => bail!("CoGroup requires subplan execution"),
            Node::CombineGlobal {
                local,
                merge,
                finish,
                ..
            } => {
                let mid_acc = local(buf.take().unwrap());
                let acc = merge(vec![mid_acc]);
                finish(acc)
            }
            Node::Reshuffle { reshuffle } => reshuffle(vec![buf.take().unwrap()], 1)
                .into_iter()
                .next()
                .expect("Reshuffle returned empty vec in sequential mode"),
        });

        if manager.should_checkpoint(idx, is_barrier, total_nodes) {
            let timestamp = current_timestamp_ms();
            #[allow(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                clippy::cast_precision_loss
            )]
            let progress_percent = ((idx as f64 / total_nodes as f64) * 100.0) as u8;
            let metadata_str = format!("{pipeline_id}:{idx}:{timestamp}:1");
            let checksum = compute_checksum(metadata_str.as_bytes());

            let state = CheckpointState {
                pipeline_id: pipeline_id.clone(),
                completed_node_index: idx,
                timestamp,
                partition_count: 1,
                checksum,
                exec_mode: "sequential".to_string(),
                metadata: CheckpointMetadata {
                    total_nodes,
                    last_node_type: node_type.to_string(),
                    progress_percent,
                },
            };

            match manager.save_checkpoint(&state) {
                Ok(path) => {
                    eprintln!(
                        "[Checkpoint] Saved checkpoint at node {idx} ({:.0}% complete) to {:?}",
                        progress_percent,
                        path.display()
                    );
                }
                Err(e) => {
                    eprintln!("[Checkpoint] Warning: Failed to save checkpoint: {e}");
                }
            }
        }
    }

    let out = buf.unwrap();
    let v = *out
        .downcast::<Vec<T>>()
        .map_err(|_| anyhow!("terminal type mismatch"))?;

    manager.clear_checkpoints(&pipeline_id).ok();
    eprintln!("[Checkpoint] Pipeline completed successfully, checkpoints cleared");

    Ok(v)
}

/// Execute a fully linearized chain **in parallel** with checkpointing support.
#[cfg(feature = "checkpointing")]
fn exec_par_with_checkpointing<T: 'static + Send + Sync + Clone>(
    chain: &[Node],
    partitions: usize,
    config: CheckpointConfig,
) -> Result<Vec<T>> {
    use crate::checkpoint::{
        CheckpointManager, CheckpointMetadata, CheckpointState, compute_checksum,
        current_timestamp_ms, generate_pipeline_id,
    };

    let total_nodes = chain.len();
    let mut manager = CheckpointManager::new(config)?;

    let pipeline_id = generate_pipeline_id(&format!("{:?}:{}", chain.len(), partitions));

    if manager.config.auto_recover
        && let Some(checkpoint_path) = manager.find_latest_checkpoint(&pipeline_id)?
    {
        eprintln!("[Checkpoint] Found existing checkpoint, attempting recovery...");
        match manager.load_checkpoint(&checkpoint_path) {
            Ok(state) => {
                eprintln!(
                    "[Checkpoint] Recovered from node {} ({:.0}% complete)",
                    state.completed_node_index, state.metadata.progress_percent
                );
            }
            Err(e) => {
                eprintln!("[Checkpoint] Failed to load checkpoint: {e}");
            }
        }
    }

    // Execute with checkpointing (simplified: checkpoint after major barriers only)
    // Due to parallel execution complexity, we use the standard exec_par and checkpoint
    // at coarser granularity. No limit is passed here because checkpointing pipelines
    // do not currently support early termination.
    let result = exec_par::<T>(chain, partitions, None);

    if result.is_ok() {
        manager.clear_checkpoints(&pipeline_id).ok();
        eprintln!("[Checkpoint] Pipeline completed successfully, checkpoints cleared");
    } else {
        let timestamp = current_timestamp_ms();
        let metadata_str = format!("{pipeline_id}:0:{timestamp}:{partitions}");
        let checksum = compute_checksum(metadata_str.as_bytes());
        let state = CheckpointState {
            pipeline_id,
            completed_node_index: 0,
            timestamp,
            partition_count: partitions,
            checksum,
            exec_mode: format!("parallel:{partitions}"),
            metadata: CheckpointMetadata {
                total_nodes,
                last_node_type: "Failed".to_string(),
                progress_percent: 0,
            },
        };
        manager.save_checkpoint(&state).ok();
    }

    result
}
