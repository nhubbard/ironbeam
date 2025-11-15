//! Query planner and optimizer passes.
//!
//! The planner converts the pipeline graph into a single **linear execution chain**
//! and applies a few lightweight, semantics-preserving rewrites:
//!
//! 1. **Fuse stateless ops** -- adjacent `Node::Stateless` blocks are concatenated.
//! 2. **Reorder value-only runs** -- within a stateless block where *all* ops are
//!    key-preserving and value-only, put cheaper/filters first using `cost_hint`.
//! 3. **Lift GBK->Combine** -- if a `GroupByKey` is immediately followed by a
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
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::fmt::{Display, Formatter, Result as FormatResult};
use std::sync::Arc;

/// A finalized execution plan: a linearized chain and an optional partition hint.
pub struct Plan {
    /// Linear list of nodes to execute from source -> terminal.
    pub chain: Vec<Node>,
    /// Optional suggested partition count (runner may override).
    pub suggested_partitions: Option<usize>,
    /// Optimization decisions made during planning.
    pub optimizations: Vec<OptimizationDecision>,
}

/// Represents an optimization decision made by the planner.
#[derive(Debug, Clone)]
pub enum OptimizationDecision {
    /// Adjacent stateless operations were fused together.
    FusedStateless {
        /// Number of stateless blocks before fusion.
        blocks_before: usize,
        /// Number of stateless blocks after fusion.
        blocks_after: usize,
        /// Total number of operations fused.
        ops_count: usize,
    },
    /// Value-only operations were reordered for efficiency.
    ReorderedValueOps {
        /// Number of operations reordered.
        ops_count: usize,
        /// Operations were sorted by cost hint.
        by_cost: bool,
    },
    /// `GroupByKey` followed by `CombineValues` was lifted.
    LiftedGBKCombine {
        /// The optimization removes the `GroupByKey` barrier.
        removed_barrier: bool,
    },
    /// Mid-pipeline materialized nodes were dropped.
    DroppedMidMaterialized {
        /// Number of materialized nodes removed.
        count: usize,
    },
    /// Partition count suggestion.
    PartitionSuggestion {
        /// Estimated source length.
        source_len: Option<usize>,
        /// Suggested partition count.
        partitions: usize,
    },
}

/// Detailed explanation of an execution plan including cost estimates and optimizations.
#[derive(Debug, Clone)]
pub struct ExecutionExplanation {
    /// The linearized execution chain.
    pub steps: Vec<ExplainStep>,
    /// Cost estimates for the entire plan.
    pub cost_estimate: CostEstimate,
    /// List of optimization decisions made by the planner.
    pub optimizations: Vec<OptimizationDecision>,
    /// Suggested partition count.
    pub suggested_partitions: Option<usize>,
}

impl Display for ExecutionExplanation {
    #[allow(clippy::too_many_lines)]
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatResult {
        writeln!(
            f,
            "╔═══════════════════════════════════════════════════════════════╗"
        )?;
        writeln!(
            f,
            "║              EXECUTION PLAN EXPLANATION                       ║"
        )?;
        writeln!(
            f,
            "╚═══════════════════════════════════════════════════════════════╝"
        )?;
        writeln!(f)?;

        // Cost Summary
        writeln!(
            f,
            "┌─ COST ESTIMATES ─────────────────────────────────────────────┐"
        )?;
        writeln!(
            f,
            "│ Source Size:       {:>10}",
            self.cost_estimate
                .source_size
                .map_or_else(|| "Unknown".to_string(), |s| s.to_string())
        )?;
        writeln!(
            f,
            "│ Total Operations:  {:>10}",
            self.cost_estimate.total_ops
        )?;
        writeln!(
            f,
            "│ Stateless Ops:     {:>10}",
            self.cost_estimate.stateless_ops
        )?;
        writeln!(
            f,
            "│ Barrier Ops:       {:>10}",
            self.cost_estimate.barriers
        )?;
        if let Some(parts) = self.suggested_partitions {
            writeln!(f, "│ Suggested Parts:   {parts:>10}")?;
        }
        writeln!(
            f,
            "└──────────────────────────────────────────────────────────────┘"
        )?;
        writeln!(f)?;

        // Execution Steps
        writeln!(
            f,
            "┌─ EXECUTION STEPS ────────────────────────────────────────────┐"
        )?;
        for step in &self.steps {
            let barrier_marker = if step.is_barrier { " [BARRIER]" } else { "" };
            writeln!(f, "│")?;
            writeln!(
                f,
                "│ Step {}: {}{}",
                step.step, step.node_type, barrier_marker
            )?;
            writeln!(f, "│   {}", step.description)?;
            writeln!(f, "│   Cost: {}", step.cost_hint)?;
        }
        writeln!(f, "│")?;
        writeln!(
            f,
            "└──────────────────────────────────────────────────────────────┘"
        )?;

        // Optimizations
        if !self.optimizations.is_empty() {
            writeln!(f)?;
            writeln!(
                f,
                "┌─ OPTIMIZATIONS APPLIED ──────────────────────────────────────┐"
            )?;
            for opt in &self.optimizations {
                match opt {
                    OptimizationDecision::FusedStateless {
                        blocks_before,
                        blocks_after,
                        ops_count,
                    } => {
                        writeln!(f, "│ • Fused Stateless Operations")?;
                        writeln!(
                            f,
                            "│   Reduced {blocks_before} blocks → {blocks_after} blocks ({ops_count} ops total)"
                        )?;
                    }
                    OptimizationDecision::ReorderedValueOps { ops_count, by_cost } => {
                        writeln!(f, "│ • Reordered Value-Only Operations")?;
                        writeln!(
                            f,
                            "│   {} operations sorted by {}",
                            ops_count,
                            if *by_cost {
                                "cost hint"
                            } else {
                                "default order"
                            }
                        )?;
                    }
                    OptimizationDecision::LiftedGBKCombine { removed_barrier } => {
                        writeln!(f, "│ • Lifted GroupByKey→CombineValues")?;
                        if *removed_barrier {
                            writeln!(f, "│   Removed GroupByKey barrier for efficiency")?;
                        }
                    }
                    OptimizationDecision::DroppedMidMaterialized { count } => {
                        writeln!(f, "│ • Dropped Mid-Pipeline Materialization")?;
                        writeln!(f, "│   Removed {count} unnecessary materialized node(s)")?;
                    }
                    OptimizationDecision::PartitionSuggestion {
                        source_len,
                        partitions,
                    } => {
                        writeln!(f, "│ • Partition Count Suggestion")?;
                        if let Some(len) = source_len {
                            writeln!(
                                f,
                                "│   Based on source size {len}, suggest {partitions} partitions"
                            )?;
                        } else {
                            writeln!(f, "│   Suggest {partitions} partitions")?;
                        }
                    }
                }
            }
            writeln!(
                f,
                "└──────────────────────────────────────────────────────────────┘"
            )?;
        }

        Ok(())
    }
}

/// A single step in the execution plan with cost information.
#[derive(Debug, Clone)]
pub struct ExplainStep {
    /// Step number in the execution sequence.
    pub step: usize,
    /// Type of node being executed.
    pub node_type: String,
    /// Human-readable description of the operation.
    pub description: String,
    /// Whether this operation is a barrier (requires collecting all partitions).
    pub is_barrier: bool,
    /// Cost hint for this step.
    pub cost_hint: u64,
}

/// Cost estimates for the execution plan.
#[derive(Debug, Clone)]
pub struct CostEstimate {
    /// Estimated number of barrier operations.
    pub barriers: usize,
    /// Estimated total operation count.
    pub total_ops: usize,
    /// Estimated number of stateless operations.
    pub stateless_ops: usize,
    /// Estimated source size hint.
    pub source_size: Option<usize>,
}

impl Plan {
    /// Generate a detailed explanation of the execution plan.
    ///
    /// Returns an [`ExecutionExplanation`] containing:
    /// - Step-by-step execution sequence
    /// - Cost estimates (barriers, operations, source size)
    /// - Optimization decisions made by the planner
    /// - Suggested partition count
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn explain(&self) -> ExecutionExplanation {
        let mut steps = Vec::new();
        let mut barriers = 0;
        let mut total_ops = 0;
        let mut stateless_ops = 0;
        let mut source_size = None;

        for (idx, node) in self.chain.iter().enumerate() {
            let (node_type, description, is_barrier, cost) = match node {
                Node::Source {
                    vec_ops, payload, ..
                } => {
                    source_size = vec_ops.len(payload.as_ref());
                    let size_str = source_size
                        .map_or_else(|| "unknown size".to_string(), |s| format!("{s} elements"));
                    ("Source", format!("Read data source ({size_str})"), false, 1)
                }
                Node::Stateless(ops) => {
                    stateless_ops += ops.len();
                    total_ops += ops.len();
                    let ops_list = ops
                        .iter()
                        .map(|op| format!("op(cost={})", op.cost_hint()))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let total_cost: u64 = ops.iter().map(|op| u64::from(op.cost_hint())).sum();
                    (
                        "Stateless",
                        format!("Apply {} operations: [{}]", ops.len(), ops_list),
                        false,
                        total_cost,
                    )
                }
                Node::GroupByKey { .. } => {
                    barriers += 1;
                    total_ops += 1;
                    (
                        "GroupByKey",
                        "Group elements by key (BARRIER)".to_string(),
                        true,
                        100,
                    )
                }
                Node::CombineValues { local_groups, .. } => {
                    barriers += 1;
                    total_ops += 1;
                    let mode = if local_groups.is_some() {
                        "with local pre-aggregation"
                    } else {
                        "on pairs"
                    };
                    (
                        "CombineValues",
                        format!("Combine values per key {mode} (BARRIER)"),
                        true,
                        80,
                    )
                }
                Node::CoGroup { .. } => {
                    barriers += 1;
                    total_ops += 1;
                    (
                        "CoGroup",
                        "Co-group two collections (BARRIER)".to_string(),
                        true,
                        150,
                    )
                }
                Node::CombineGlobal { fanout, .. } => {
                    barriers += 1;
                    total_ops += 1;
                    let fanout_str =
                        fanout.map_or_else(|| "unbounded".to_string(), |f| f.to_string());
                    (
                        "CombineGlobal",
                        format!("Global aggregation with fanout={fanout_str} (BARRIER)"),
                        true,
                        90,
                    )
                }
                Node::Materialized(_) => {
                    total_ops += 1;
                    ("Materialized", "Materialize results".to_string(), false, 1)
                }
            };

            steps.push(ExplainStep {
                step: idx + 1,
                node_type: node_type.to_string(),
                description,
                is_barrier,
                cost_hint: cost,
            });
        }

        ExecutionExplanation {
            steps,
            cost_estimate: CostEstimate {
                barriers,
                total_ops,
                stateless_ops,
                source_size,
            },
            optimizations: self.optimizations.clone(),
            suggested_partitions: self.suggested_partitions,
        }
    }
}

/// Build a linear plan from `terminal`, then apply optimizer passes and produce
/// a partitioning hint.
///
/// The pass order is intentional:
/// 1) backwalk graph -> chain
/// 2) fuse stateless
/// 3) reorder value-only ops (requires fused blocks)
/// 4) lift GBK->Combine (structure-changing)
/// 5) drop mid-materialized (cleanup)
///
/// # Errors
///
/// If any of the optimizer passes fail or the pipeline is in an inconsistent state.
pub fn build_plan(p: &Pipeline, terminal: NodeId) -> Result<Plan> {
    let (nodes, edges) = p.snapshot();
    let mut chain = backwalk_linear(nodes, &edges, terminal)?;
    let len_hint = estimate_source_len(&chain);

    let mut optimizations = Vec::new();

    // Track optimization decisions
    let (new_chain, fusion_opt) = fuse_stateless_tracked(chain);
    chain = new_chain;
    if let Some(opt) = fusion_opt {
        optimizations.push(opt);
    }

    let (new_chain, reorder_opt) = reorder_value_only_runs_tracked(chain);
    chain = new_chain;
    optimizations.extend(reorder_opt);

    let (new_chain, lift_opt) = lift_gbk_then_combine_tracked(chain);
    chain = new_chain;
    if let Some(opt) = lift_opt {
        optimizations.push(opt);
    }

    let (new_chain, drop_opt) = drop_mid_materialized_tracked(chain);
    chain = new_chain;
    if let Some(opt) = drop_opt {
        optimizations.push(opt);
    }

    let suggested = suggest_partitions(len_hint);
    if let Some(parts) = suggested {
        optimizations.push(OptimizationDecision::PartitionSuggestion {
            source_len: len_hint,
            partitions: parts,
        });
    }

    Ok(Plan {
        chain,
        suggested_partitions: suggested,
        optimizations,
    })
}

/// Walk the pipeline graph **backwards** from `terminal` following single-predecessor
/// edges and return a **forward** (source->terminal) linear chain.
///
/// # Errors
///
/// An error is returned if a referenced node is missing from the snapshot.
fn backwalk_linear(
    mut nodes: HashMap<NodeId, Node>,
    edges: &[(NodeId, NodeId)],
    terminal: NodeId,
) -> Result<Vec<Node>> {
    let mut chain = Vec::<Node>::new();
    let mut cur = terminal;
    loop {
        let n = nodes
            .remove(&cur)
            .ok_or_else(|| anyhow!("planner: missing node {cur:?}"))?;
        chain.push(n);
        if let Some((from, _)) = edges.iter().find(|(_, to)| *to == cur).copied() {
            cur = from;
        } else {
            break;
        }
    }
    chain.reverse();
    Ok(chain)
}

/* ---------- Simple stateless fusion ---------- */

/// Merge adjacent `Node::Stateless` blocks and track optimization decisions.
fn fuse_stateless_tracked(chain: Vec<Node>) -> (Vec<Node>, Option<OptimizationDecision>) {
    if chain.is_empty() {
        return (chain, None);
    }
    let mut out = Vec::<Node>::with_capacity(chain.len());
    let mut i = 0usize;
    let mut blocks_before = 0;
    let mut total_ops = 0;

    while i < chain.len() {
        match &chain[i] {
            Node::Stateless(first_ops) => {
                blocks_before += 1;
                let mut fused = first_ops.clone();
                total_ops += first_ops.len();
                let mut j = i + 1;
                while j < chain.len() {
                    if let Node::Stateless(more) = &chain[j] {
                        blocks_before += 1;
                        total_ops += more.len();
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

    let blocks_after = out
        .iter()
        .filter(|n| matches!(n, Node::Stateless(_)))
        .count();
    let optimization = if blocks_before > blocks_after {
        Some(OptimizationDecision::FusedStateless {
            blocks_before,
            blocks_after,
            ops_count: total_ops,
        })
    } else {
        None
    };

    (out, optimization)
}

/* ---------- NEW: reorder value-only runs ---------- */

/// Reorder value-only operations and track optimization decisions.
fn reorder_value_only_runs_tracked(chain: Vec<Node>) -> (Vec<Node>, Vec<OptimizationDecision>) {
    let mut out = Vec::with_capacity(chain.len());
    let mut optimizations = Vec::new();

    for n in chain {
        if let Node::Stateless(ops) = n {
            // reorder only when ALL ops meet the capability contract
            let all_vo = ops.iter().all(|op| {
                op.value_only() && op.key_preserving() && op.reorder_safe_with_value_only()
            });
            if all_vo && ops.len() > 1 {
                let mut ops_owned: Vec<Arc<dyn DynOp>> = ops;
                ops_owned.sort_by_key(|op| {
                    // promote "filters" (cost==1) first, then by cost
                    let is_filter_first = i32::from(op.cost_hint() != 1);
                    (is_filter_first, op.cost_hint())
                });
                optimizations.push(OptimizationDecision::ReorderedValueOps {
                    ops_count: ops_owned.len(),
                    by_cost: true,
                });
                out.push(Node::Stateless(ops_owned));
            } else {
                out.push(Node::Stateless(ops));
            }
        } else {
            out.push(n);
        }
    }
    (out, optimizations)
}

/* ---------- NEW: GBK -> Combine lifting ---------- */

/// Lift GBK->Combine pattern and track optimization decisions.
fn lift_gbk_then_combine_tracked(chain: Vec<Node>) -> (Vec<Node>, Option<OptimizationDecision>) {
    if chain.len() < 2 {
        return (chain, None);
    }
    let mut out = Vec::with_capacity(chain.len());
    let mut i = 0usize;
    let mut lifted = false;

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
                    lifted = true;
                    i += 2;
                    continue;
                }
                _ => {}
            }
        }
        out.push(chain[i].clone());
        i += 1;
    }

    let optimization = if lifted {
        Some(OptimizationDecision::LiftedGBKCombine {
            removed_barrier: true,
        })
    } else {
        None
    };

    (out, optimization)
}

/* ---------- Keep only terminal Materialized ---------- */

/// Drop mid-materialized nodes and track optimization decisions.
fn drop_mid_materialized_tracked(chain: Vec<Node>) -> (Vec<Node>, Option<OptimizationDecision>) {
    if chain.len() <= 1 {
        return (chain, None);
    }
    let last = chain.len() - 1;
    let mut dropped_count = 0;

    let result: Vec<Node> = chain
        .into_iter()
        .enumerate()
        .filter_map(|(i, n)| match (i, &n) {
            (idx, Node::Materialized(_)) if idx != last => {
                dropped_count += 1;
                None
            }
            _ => Some(n),
        })
        .collect();

    let optimization = if dropped_count > 0 {
        Some(OptimizationDecision::DroppedMidMaterialized {
            count: dropped_count,
        })
    } else {
        None
    };

    (result, optimization)
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
