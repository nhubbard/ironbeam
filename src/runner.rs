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

use crate::node::Node;
use crate::pipeline::Pipeline;
use crate::planner::build_plan;
use crate::type_token::Partition;
use crate::NodeId;
use anyhow::{anyhow, bail, Result};
use ordered_float::NotNan;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::BinaryHeap;
use std::sync::Arc;

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
    pub fn run_collect<T: 'static + Send + Sync + Clone>(
        &self,
        p: &Pipeline,
        terminal: NodeId,
    ) -> Result<Vec<T>> {
        // Record start time in metrics
        #[cfg(feature = "metrics")]
        p.record_metrics_start();

        // Get the optimized plan
        let plan = build_plan(p, terminal)?;
        let chain = plan.chain;
        let suggested_parts = plan.suggested_partitions;

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
                exec_par::<T>(chain, parts)
            }
        };

        // Record end time in metrics
        #[cfg(feature = "metrics")]
        p.record_metrics_end();

        result
    }
}

/// Execute a fully linearized chain **sequentially**, collecting `Vec<T>`.
///
/// Internal helper used by [`Runner::run_collect`]. Walks the chain left->right,
/// maintaining a single opaque `Partition` buffer.
fn exec_seq<T: 'static + Send + Sync + Clone>(chain: Vec<Node>) -> Result<Vec<T>> {
    let mut buf: Option<Partition> = None;

    let run_subplan_seq = |mut chain: Vec<Node>| -> Result<Vec<Partition>> {
        let mut curr: Option<Partition> = None;
        for node in chain.drain(..) {
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
                    let local = if let Some(lg) = local_groups {
                        lg
                    } else {
                        local_pairs
                    };
                    let mid = local(curr.take().unwrap());
                    merge(vec![mid])
                }
                Node::Materialized(p) => Box::new(p) as Partition,
                Node::CoGroup { .. } => bail!("nested CoGroup not supported in subplan"),
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
            Node::CoGroup {
                left_chain,
                right_chain,
                coalesce_left,
                coalesce_right,
                exec,
            } => {
                // Execute left/right subplans and coalesce if they produced multiple partitions.
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
                let local = if let Some(lg) = local_groups {
                    lg
                } else {
                    local_pairs
                };
                let mid = local(buf.take().unwrap());
                merge(vec![mid])
            }
            // Terminal: type-check and materialize as Vec<T>
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
                if let Some(h) = acc.downcast_ref::<BinaryHeap<NotNan<f64>>>() {
                    eprintln!("DEBUG: KMV heap len = {}", h.len()); // should be <= k
                }
                finish(acc)
            }
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
fn exec_par<T: 'static + Send + Sync + Clone>(
    chain: Vec<Node>,
    partitions: usize,
) -> Result<Vec<T>> {
    /// Run a nested subplan (used by `CoGroup`) in parallel, returning a vector
    /// of partitions. The subplan must start with a `Source`. Nested `CoGroup`
    /// inside a subplan is not supported.
    fn run_subplan_par(chain: Vec<Node>, partitions: usize) -> Result<Vec<Partition>> {
        // must start with a source
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
                    let local = if let Some(lg) = local_groups {
                        lg.clone()
                    } else {
                        local_pairs.clone()
                    };
                    let mids: Vec<Partition> = curr.into_par_iter().map(|p| local(p)).collect();
                    curr = vec![merge(mids)];
                    i += 1;
                }
                Node::Source { .. } | Node::Materialized(_) => {
                    bail!("unexpected source/materialized in subplan")
                }
                Node::CoGroup { .. } => bail!("nested CoGroup not supported in subplan"),
                Node::CombineGlobal {
                    local,
                    merge,
                    finish,
                    fanout,
                } => {
                    // local on each partition -> Vec<A> (type-erased)
                    let mut accs: Vec<Partition> = curr.into_par_iter().map(|p| local(p)).collect();

                    // multi-round merge with optional fanout, no cloning
                    let f = fanout.unwrap_or(usize::MAX).max(1);
                    while accs.len() > 1 {
                        if f == usize::MAX {
                            accs = vec![merge(accs)];
                            break;
                        } else {
                            let mut next: Vec<Partition> =
                                Vec::with_capacity(accs.len().div_ceil(f));
                            let mut it = accs.into_iter(); // take ownership to avoid clones
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
                    }

                    let acc = accs.pop().unwrap_or_else(|| merge(Vec::new()));
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

    // Original head Source
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
                let local = if let Some(lg) = local_groups {
                    lg.clone()
                } else {
                    local_pairs.clone()
                };
                let mids: Vec<Partition> = curr.into_par_iter().map(|p| local(p)).collect();
                curr = vec![merge(mids)];
                i += 1;
            }
            Node::CoGroup {
                left_chain,
                right_chain,
                coalesce_left,
                coalesce_right,
                exec,
            } => {
                // Execute left/right subplans in parallel; coalesce when necessary.
                let left_parts = run_subplan_par((**left_chain).clone(), partitions)?;
                let right_parts = run_subplan_par((**right_chain).clone(), partitions)?;

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
            } => {
                let mut accs: Vec<Partition> = curr.into_par_iter().map(|p| local(p)).collect();

                let f = fanout.unwrap_or(usize::MAX).max(1);
                while accs.len() > 1 {
                    if f == usize::MAX {
                        accs = vec![merge(accs)];
                        break;
                    } else {
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
                }

                let acc = accs.pop().unwrap_or_else(|| merge(Vec::new()));
                if let Some(h) = acc.downcast_ref::<BinaryHeap<NotNan<f64>>>() {
                    eprintln!("DEBUG: KMV heap len = {}", h.len()); // should be <= k
                }
                curr = vec![finish(acc)];
                i += 1;
            }
        }
    }

    // Terminal collection
    if curr.len() == 1 {
        let one = curr.into_iter().next().unwrap();
        let v = *one
            .downcast::<Vec<T>>()
            .map_err(|_| anyhow!("terminal type mismatch"))?;
        Ok(v)
    } else {
        let mut out = Vec::<T>::new();
        for part in curr {
            let v = *part
                .downcast::<Vec<T>>()
                .map_err(|_| anyhow!("terminal type mismatch"))?;
            out.extend(v);
        }
        Ok(out)
    }
}
