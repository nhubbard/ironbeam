use crate::node::Node;
use crate::{NodeId, Pipeline};
use anyhow::{anyhow, Result};
use std::collections::HashMap;

/// Build an executable, optimized linear chain for a single terminal node.
///
/// Passes (in order):
///  1) Backwalk terminal -> source (prunes dead graph).
///  2) Fuse adjacent Stateless nodes.
///  3) Drop non-terminal Materialized passthroughs.
pub fn build_plan(p: &Pipeline, terminal: NodeId) -> Result<Vec<Node>> {
    let (nodes, edges) = p.snapshot();
    let chain = backwalk_linear(nodes, edges, terminal)?;
    let chain = fuse_stateless(chain);
    let chain = drop_mid_materialized(chain);
    Ok(chain)
}

/// Linear backwalk: follow the unique upstream edge until a Source (or start).
/// Fails if an unexpected additional source/materialized appears mid-chain.
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

/// Fuse consecutive Stateless nodes: [ .., S([a]), S([b,c]), .. ] -> [ .., S([a,b,c]), .. ]
fn fuse_stateless(chain: Vec<Node>) -> Vec<Node> {
    if chain.is_empty() {
        return chain;
    }
    let mut out = Vec::<Node>::with_capacity(chain.len());
    let mut i = 0usize;

    while i < chain.len() {
        match &chain[i] {
            Node::Stateless(first_ops) => {
                // Start gathering a run
                let mut fused = first_ops.clone();
                let mut j = i + 1;
                while j < chain.len() {
                    match &chain[j] {
                        Node::Stateless(more_ops) => {
                            fused.extend(more_ops.iter().cloned());
                            j += 1;
                        }
                        _ => break,
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

/// Remove Materialized nodes that appear in the middle of a chain.
/// Keep Materialized only if it is the terminal node.
fn drop_mid_materialized(chain: Vec<Node>) -> Vec<Node> {
    if chain.len() <= 1 {
        return chain;
    }
    let last_idx = chain.len() - 1;
    let mut out = Vec::<Node>::with_capacity(chain.len());
    for (idx, n) in chain.into_iter().enumerate() {
        match (idx, &n) {
            (i, Node::Materialized(_)) if i != last_idx => {
                // Drop mid-chain materialized; execution will read the upstream instead.
                continue;
            }
            _ => out.push(n),
        }
    }
    out
}