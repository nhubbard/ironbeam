//! Query planner and optimizer passes.
//!
//! The planner converts the pipeline graph into a single **linear execution chain**
//! and applies a few lightweight, semantics-preserving rewrites:
//!
//! 1. **Fuse stateless ops** -- adjacent `Node::Stateless` blocks are concatenated.
//! 2. **Reorder value-only runs** -- within a stateless block where *all* ops are
//!    key-preserving and value-only, put cheaper/filters first using `cost_hint`.
//! 3. **Lift GBK→Combine** -- if a `GroupByKey` is immediately followed by a
//!    `CombineValues` that also has a lifted local (`local_groups.is_some()`),
//!    drop the `GroupByKey` and keep the combine, switching it to consume
//!    `(K, V)` pairs via `local_pairs`.
//! 4. **Drop mid-materialized** -- only keep a `Materialized` node if it is the final
//!    terminal in the chain.
//!
//! The planner also provides a heuristic **partition suggestion** that the runner
//! may use to size parallel execution.

use crate::node::{DynOp, Node};
use crate::{NodeId, Pipeline};
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::sync::Arc;

/// A finalized execution plan: a linearized chain and an optional partition hint.
pub struct Plan {
    /// Linear list of nodes to execute from source → terminal.
    pub chain: Vec<Node>,
    /// Optional suggested partition count (runner may override).
    pub suggested_partitions: Option<usize>,
}

/// Build a linear plan from `terminal`, then apply optimizer passes and produce
/// a partitioning hint.
///
/// The pass order is intentional:
/// 1) backwalk graph → chain
/// 2) fuse stateless
/// 3) reorder value-only ops (requires fused blocks)
/// 4) lift GBK→Combine (structure-changing)
/// 5) drop mid-materialized (cleanup)
pub fn build_plan(p: &Pipeline, terminal: NodeId) -> Result<Plan> {
    let (nodes, edges) = p.snapshot();
    let mut chain = backwalk_linear(nodes, edges, terminal)?;
    let len_hint = estimate_source_len(&chain);

    chain = fuse_stateless(chain);
    chain = reorder_value_only_runs(chain);
    chain = lift_gbk_then_combine(chain);
    chain = drop_mid_materialized(chain);

    let suggested = suggest_partitions(len_hint);
    Ok(Plan {
        chain,
        suggested_partitions: suggested,
    })
}

/// Walk the pipeline graph **backwards** from `terminal` following single-predecessor
/// edges and return a **forward** (source→terminal) linear chain.
///
/// # Errors
/// An error is returned if a referenced node is missing from the snapshot.
fn backwalk_linear(
    mut nodes: HashMap<NodeId, Node>,
    edges: Vec<(NodeId, NodeId)>,
    terminal: NodeId,
) -> Result<Vec<Node>> {
    let mut chain = Vec::<Node>::new();
    let mut cur = terminal;
    loop {
        let n = nodes
            .remove(&cur)
            .ok_or_else(|| anyhow!("planner: missing node {cur:?}"))?;
        chain.push(n);
        if let Some((from, _)) = edges.iter().find(|(_, to)| *to == cur).cloned() {
            cur = from;
        } else {
            break;
        }
    }
    chain.reverse();
    Ok(chain)
}

/* ---------- Simple stateless fusion ---------- */

/// Merge adjacent `Node::Stateless` blocks into a single block by concatenating
/// their op vectors. Preserves overall order.
fn fuse_stateless(chain: Vec<Node>) -> Vec<Node> {
    if chain.is_empty() {
        return chain;
    }
    let mut out = Vec::<Node>::with_capacity(chain.len());
    let mut i = 0usize;
    while i < chain.len() {
        match &chain[i] {
            Node::Stateless(first_ops) => {
                let mut fused = first_ops.clone();
                let mut j = i + 1;
                while j < chain.len() {
                    if let Node::Stateless(more) = &chain[j] {
                        fused.extend(more.iter().cloned());
                        j += 1;
                    } else {
                        break;
                    }
                }
                out.push(Node::Stateless(fused));
                i = j;
            }
            n => {
                out.push(n.clone());
                i += 1;
            }
        }
    }
    out
}

/* ---------- NEW: reorder value-only runs ---------- */

/// Within each `Node::Stateless`, if **all** ops are key-preserving, value-only,
/// and declare `reorder_safe_with_value_only() == true`, reorder the ops:
/// - "Cheapest" filters (we tag with `cost_hint() == 1`) first,
/// - then the rest by ascending `cost_hint()`.
///
/// This is a local, stable improvement that can reduce intermediate sizes
/// without changing semantics.
fn reorder_value_only_runs(chain: Vec<Node>) -> Vec<Node> {
    let mut out = Vec::with_capacity(chain.len());
    for n in chain.into_iter() {
        if let Node::Stateless(ops) = n {
            // reorder only when ALL ops meet the capability contract
            let all_vo = ops.iter().all(|op| {
                op.value_only() && op.key_preserving() && op.reorder_safe_with_value_only()
            });
            if all_vo {
                let mut ops_owned: Vec<Arc<dyn DynOp>> = ops;
                ops_owned.sort_by_key(|op| {
                    // promote "filters" (cost==1) first, then by cost
                    let is_filter_first = if op.cost_hint() == 1 { 0 } else { 1 };
                    (is_filter_first, op.cost_hint())
                });
                out.push(Node::Stateless(ops_owned));
            } else {
                out.push(Node::Stateless(ops));
            }
        } else {
            out.push(n);
        }
    }
    out
}

/* ---------- NEW: GBK → Combine lifting ---------- */

/// If a `GroupByKey` is immediately followed by a `CombineValues` **with a lifted local**
/// (`local_groups.is_some()`), remove the `GroupByKey` and keep the `CombineValues`
/// with `local_groups` disabled so the runner uses `local_pairs` on `(K, V)` input.
///
/// This preserves results while skipping the grouping barrier.
fn lift_gbk_then_combine(chain: Vec<Node>) -> Vec<Node> {
    if chain.len() < 2 {
        return chain;
    }
    let mut out = Vec::with_capacity(chain.len());
    let mut i = 0usize;

    while i < chain.len() {
        if i + 1 < chain.len() {
            match (&chain[i], &chain[i + 1]) {
                (
                    Node::GroupByKey { .. },
                    Node::CombineValues {
                        local_pairs,
                        local_groups,
                        merge,
                    },
                ) if local_groups.is_some() => {
                    // Drop GBK; run CombineValues directly on (K, V) via local_pairs.
                    out.push(Node::CombineValues {
                        local_pairs: local_pairs.clone(),
                        local_groups: None,
                        merge: merge.clone(),
                    });
                    i += 2;
                    continue;
                }
                _ => {}
            }
        }
        out.push(chain[i].clone());
        i += 1;
    }
    out
}

/* ---------- Keep only terminal Materialized ---------- */

/// Remove any `Materialized` nodes that are **not** the last node in the chain.
/// The runner expects only a terminal materialization, if any.
fn drop_mid_materialized(chain: Vec<Node>) -> Vec<Node> {
    if chain.len() <= 1 {
        return chain;
    }
    let last = chain.len() - 1;
    chain
        .into_iter()
        .enumerate()
        .filter_map(|(i, n)| match (i, &n) {
            (idx, Node::Materialized(_)) if idx != last => None,
            _ => Some(n),
        })
        .collect()
}

/* ---------- Adaptive partitions ---------- */

/// If the first node is a `Source`, ask its `VecOps` for a length hint.
/// Returns `None` when not available.
fn estimate_source_len(chain: &[Node]) -> Option<usize> {
    if let Some(Node::Source {
        payload, vec_ops, ..
    }) = chain.first()
    {
        vec_ops.len(payload.as_ref())
    } else {
        None
    }
}

/// Suggest a parallelism level from an optional input length hint.
/// Heuristic target ≈ 64k rows/partition, then clamped between
/// `[num_cpus, 8*num_cpus]`.
fn suggest_partitions(len_hint: Option<usize>) -> Option<usize> {
    let n = len_hint?;
    let target_rows_per_part = 64_000usize;
    let mut parts = n.div_ceil(target_rows_per_part);
    let hw = num_cpus::get().max(2);
    parts = parts.clamp(hw, hw * 8);
    Some(parts)
}
