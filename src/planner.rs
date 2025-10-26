use crate::node::Node;
use crate::{NodeId, Pipeline};
use anyhow::{anyhow, Result};
use std::collections::HashMap;

pub struct Plan {
    pub chain: Vec<Node>,
    pub suggested_partitions: Option<usize>,
}

pub fn build_plan(p: &Pipeline, terminal: NodeId) -> Result<Plan> {
    let (nodes, edges) = p.snapshot();
    let mut chain = backwalk_linear(nodes, edges, terminal)?;
    let len_hint = estimate_source_len(&chain);

    chain = fuse_stateless(chain);
    chain = reorder_value_only_runs(chain); // NEW: push filters early, cheap-first
    chain = lift_gbk_then_combine(chain); // NEW: drop GBK if combine has lifted local
    chain = drop_mid_materialized(chain);

    let suggested = suggest_partitions(len_hint);
    Ok(Plan {
        chain,
        suggested_partitions: suggested,
    })
}

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

/* ---------- Simple stateless fusion (kept) ---------- */
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
fn reorder_value_only_runs(chain: Vec<Node>) -> Vec<Node> {
    use std::sync::Arc;
    let mut out = Vec::with_capacity(chain.len());
    for n in chain.into_iter() {
        if let Node::Stateless(ops) = n {
            // split by capability: only reorder if ALL are value-only & key-preserving
            let all_vo = ops.iter().all(|op| {
                op.value_only() && op.key_preserving() && op.reorder_safe_with_value_only()
            });
            if all_vo {
                // stable partition: filters first, then rest by cost
                let mut ops_owned: Vec<Arc<dyn crate::node::DynOp>> = ops;
                ops_owned.sort_by_key(|op| {
                    let is_filter_first = if op.cost_hint() == 1 { 0 } else { 1 }; // we tagged filters with cost 1
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
// src/planner.rs
fn lift_gbk_then_combine(chain: Vec<Node>) -> Vec<Node> {
    use crate::node::Node;
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
                    // Drop GBK and keep CombineValues BUT disable local_groups so runner uses local_pairs.
                    out.push(Node::CombineValues {
                        local_pairs: local_pairs.clone(),
                        local_groups: None, // we removed GBK, so input is (K,V)
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

fn suggest_partitions(len_hint: Option<usize>) -> Option<usize> {
    let n = len_hint?;
    // simple heuristic: ~64k rows per partition, clamped
    let target_rows_per_part = 64_000usize;
    let mut parts = n.div_ceil(target_rows_per_part);
    let hw = num_cpus::get().max(2);
    parts = parts.clamp(hw, hw * 8);
    Some(parts)
}
