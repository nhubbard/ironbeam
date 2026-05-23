//! Query planner and optimizer pass.
//!
//! The planner converts the pipeline graph into a single **linear execution chain**
//! and applies a few lightweight, semantics-preserving rewrites:
//!
//! 0. **Dead subtree elimination** (pre-pass) -- before extracting the linear chain,
//!    nodes that have no forward path to the target terminal are pruned from the graph.
//!    This is most impactful in multi-terminal graphs (e.g. `partition!`, tee patterns)
//!    where building the plan for terminal A should not include branches leading only to
//!    terminal B.  Running this before `backwalk_linear` also prevents ambiguous
//!    predecessor selection when dead branches introduce extra incoming edges at a shared
//!    node.
//! 1. **Fuse stateless ops** -- adjacent `Node::Stateless` blocks are concatenated.
//! 2. **`CoGroup` input reordering** -- input subchains of every `Flatten` node are
//!    sorted by estimated cardinality (ascending).  `cogroup_by_key!` implements N-way
//!    grouping as a `Flatten` (one subplan per input collection) followed by a
//!    `GroupByKey`; placing smaller subchains first reduces peak intermediate memory in
//!    sequential execution.  Subchains with unknown cardinality are moved to the end.
//! 3. **Predicate pushdown before barriers** -- within a fused `Stateless` block immediately
//!    before a `GroupByKey` *or* `Reshuffle`, ops that are `key_preserving + value_only +
//!    cardinality_reducing` (e.g. `filter_values`) are split into their own earlier block when
//!    doing so is type-safe and cost-beneficial (cost-hint gate).  Because `Reshuffle` never
//!    alters element content or count, the same pushdown rationale that applies to `GroupByKey`
//!    applies equally, reducing the volume of elements that flow into the redistribution step.
//! 4. **Predicate pushdown into Flatten subplans** -- `value_only + cardinality_reducing` ops
//!    that immediately follow a `Flatten` are cloned into the tail of every Flatten input
//!    subplan and removed from the post-Flatten block.  Because each subplan produces the same
//!    element type that the merge function expects, pushing a filter *before* the fan-in reduces
//!    the volume of elements that flow into the merge step.
//! 5. **Reorder value-only runs** -- within a stateless block where *all* ops are
//!    key-preserving and value-only, put cheaper/filters first using `cost_hint`.
//! 6. **Lift GBK->Combine** -- if a `GroupByKey` is immediately followed by a
//!    `CombineValues` that also has a lifted local (`local_groups.is_some()`),
//!    drop the `GroupByKey` and keep the combine, switching it to consume
//!    `(K, V)` pairs via `local_pairs`.
//! 7. **Eliminate redundant Reshuffle** -- a `Reshuffle` immediately before a shuffle
//!    barrier (`GroupByKey`, `CombineValues`, `CoGroup`, `Flatten`) is a no-op because
//!    the barrier already redistributes all elements.  Two consecutive `Reshuffle` nodes
//!    reduce to one for the same reason.  Runs after pass 6 so that lifted combiners
//!    (which remove the `GroupByKey`) are visible as `CombineValues` targets.
//! 8. **Drop mid-materialized** -- only keep a `Materialized` node if it is the final
//!    terminal in the chain.
//!
//! The planner also provides a heuristic **partition suggestion** that the runner
//! may use to size parallel execution.

use crate::node::{DynOp, Node};
use crate::{NodeId, Pipeline};
use anyhow::{Result, anyhow};
use std::collections::{HashMap, HashSet, VecDeque};
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
    /// If the terminal stateless block ends with a `take(N)` / `first()` operator,
    /// this is `Some(N)` — the runner uses it to stop collecting elements as soon
    /// as `N` total have been gathered across all partitions.
    pub limit: Option<usize>,
    /// True when the source is known to be empty (`VecOps::len() == 0`) and the
    /// chain does not contain a [`Node::CombineGlobal`].
    ///
    /// The runner short-circuits immediately, returning `Vec::new()` without
    /// entering the execution engine. Not set for `CombineGlobal` pipelines because
    /// a combiner always emits the identity value for an empty input.
    pub is_empty: bool,
    /// True when the source contains exactly one element (`VecOps::len() == 1`) and the
    /// chain does not contain a [`Node::Flatten`] or [`Node::CoGroup`].
    ///
    /// The runner overrides the execution mode to sequential regardless of any
    /// parallelism hint, since partitioning a single element across N workers
    /// adds scheduler overhead with zero benefit.
    ///
    /// Flatten and `CoGroup` pipelines always use a 1-element dummy source as a graph
    /// anchor; `is_singleton` intentionally excludes them so their subchains execute via
    /// the parallel `run_subplan_par` path when parallel mode is selected.
    pub is_singleton: bool,
    /// Snapshot of human-readable node names taken from the source [`Pipeline`] at
    /// plan-build time.
    ///
    /// Populated by [`build_plan`] from
    /// [`Pipeline::node_names_snapshot`](crate::Pipeline::node_names_snapshot). The
    /// planner itself does not consume this map — it is forwarded to the
    /// [`ExecutionExplanation`] so the local explain view and external backends
    /// (e.g. the Google Dataflow translator) can render meaningful labels next to
    /// generic op categories like `Stateless` / `GroupByKey`.
    pub node_names: HashMap<NodeId, String>,
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
    /// Cardinality-reducing ops were confirmed (and where beneficial, split out) before
    /// a shuffle barrier (`GroupByKey` or `Reshuffle`), shrinking the barrier's input.
    ///
    /// Because neither `GroupByKey` nor `Reshuffle` alters element content or count, a
    /// `key_preserving + value_only + cardinality_reducing` filter is equally beneficial
    /// before either one.
    PushedDownPredicates {
        /// The number of operations confirmed as pre-barrier predicates.
        ops_pushed: usize,
    },
    /// `value_only + cardinality_reducing` ops were cloned into every `Flatten` input
    /// subplan and removed from the post-`Flatten` `Stateless` block, reducing the volume
    /// of elements that flow into the fan-in merge step.
    ///
    /// Only ops that satisfy both `value_only` and `cardinality_reducing` are eligible:
    /// `value_only` guarantees the element type is preserved so the subplan output still
    /// matches the `coalesce`/`merge` closures; `cardinality_reducing` guarantees no new
    /// elements are introduced.
    PushedDownIntoFlattenSubplans {
        /// Number of operations cloned into each subplan.
        ops_pushed: usize,
        /// Total number of subplans that received the cloned ops
        /// (sum over all `Flatten` nodes processed in the chain).
        subplan_count: usize,
    },
    /// Partition count suggestion.
    PartitionSuggestion {
        /// Estimated source length.
        source_len: Option<usize>,
        /// Suggested partition count.
        partitions: usize,
    },
    /// Redundant `Reshuffle` nodes were removed from the plan.
    ///
    /// A `Reshuffle` is redundant when it immediately precedes a shuffle barrier
    /// (`GroupByKey`, `CombineValues`, `CoGroup`, or `Flatten`) that already
    /// redistributes all elements, or when it immediately precedes another `Reshuffle`.
    EliminatedReshuffle {
        /// Number of `Reshuffle` nodes removed.
        count: usize,
    },
    /// Dead-subtree nodes were pruned from the pipeline graph before chain extraction.
    ///
    /// Any node that has no forward path to the target terminal is unreachable from
    /// the perspective of the plan being built, and can be safely removed.  This
    /// optimization is most impactful in multi-terminal graphs (e.g. `partition!`,
    /// tee patterns) where building the plan for terminal A should not pay the cost
    /// of evaluating branches that lead exclusively to terminal B.
    PrunedDeadSubtrees {
        /// Number of nodes removed from the pipeline graph.
        nodes_pruned: usize,
    },
    /// Input subchains of a `Flatten` node were reordered by estimated cardinality so
    /// that smaller inputs are processed before larger ones.
    ///
    /// In sequential execution the runner replays subchains one-at-a-time; placing
    /// cheaper (smaller) subchains first reduces peak intermediate memory and can
    /// allow downstream combiners to see useful data sooner.  Subchains whose
    /// cardinality cannot be estimated (no `Source` with a known length) are sorted
    /// to the end as a conservative default.
    ///
    /// `original_order[i]` is the original zero-based index of the chain that was
    /// at position `i` before sorting.  `new_order[i]` is the original index of the
    /// chain that occupies position `i` after sorting.
    ReorderedCoGroupInputs {
        /// Original subchain ordering (always `[0, 1, 2, …, n-1]`).
        original_order: Vec<usize>,
        /// New ordering: `new_order[i]` is the original index now at position `i`.
        new_order: Vec<usize>,
    },
    /// One or more `CombineGlobal` nodes will use O(log n) parallel tree reduction.
    ///
    /// When a combiner declares [`crate::collection::CombineFn::is_associative_commutative`] `= true`,
    /// the parallel runner replaces the sequential fanout merge loop with Rayon's
    /// `reduce_with`, which processes accumulators in a binary fan-in pattern.
    /// This halves the critical-path merge depth on each doubling of input size
    /// (O(log n) instead of O(n)).
    TreeReduction {
        /// Number of `CombineGlobal` nodes that use tree reduction.
        global_count: usize,
    },
    /// The terminal stateless block ends with a `take(N)` / `first()` operator.
    ///
    /// The runner will stop collecting elements across partitions as soon as `N`
    /// total have been gathered, providing early termination without executing
    /// the full pipeline.  Each partition is also individually capped at `N`
    /// elements by the internal `TakeOp` stateless operator.
    LimitPushdown {
        /// The maximum number of elements to collect.
        n: usize,
    },

    /// The source is known to be empty at plan time (`VecOps::len() == 0`).
    ///
    /// The runner will skip the execution engine entirely and return an empty
    /// `Vec` immediately. No stateless or barrier ops are evaluated.
    ///
    /// This optimization does **not** fire when the chain contains a
    /// [`Node::CombineGlobal`], which always emits exactly one output (the
    /// combiner identity value) regardless of input cardinality.
    EmptySourceShortCircuit,

    /// The source contains exactly one element at plan time (`VecOps::len() == 1`).
    ///
    /// The runner overrides any parallelism hint and executes the plan sequentially.
    /// Splitting a single-element source across N workers adds scheduler overhead
    /// with no throughput benefit.
    SingletonSourceShortCircuit,

    /// The runner will adaptively rescale `suggested_partitions` between barrier stages.
    ///
    /// After each barrier in the plan chain, the runner multiplies the current partition
    /// count by a barrier-specific output-to-input cardinality ratio:
    /// - `GroupByKey` / `CombineValues` → ratio `0.1` (key deduplication shrinks output)
    /// - `CombineGlobal` → ratio `f64::EPSILON` (collapses to a single value → 1 partition)
    /// - `Flatten` with N input chains → ratio `N` (fan-in expansion)
    /// - `CoGroup` → ratio `0.5` (join eliminates non-matching pairs)
    /// - `Reshuffle` → ratio `1.0` (no cardinality change, but uses updated count)
    ///
    /// The result is clamped to `[1, suggested_partitions]` to prevent runaway growth.
    AdaptivePartitionCount {
        /// Number of barrier stages that will apply adaptive rescaling.
        barrier_count: usize,
    },

    /// A [`Node::CoGroup`] binary join node uses a Bloom semi-join pre-filter.
    ///
    /// Before the hash-join phase, the `exec` closure builds a Bloom filter from the
    /// keys of the smaller (or semantically-required) join side and discards elements
    /// from the other side whose key is definitively absent.  Elements eliminated this
    /// way never reach the hash-map construction step, reducing both peak memory and
    /// CPU cost for sparse joins.
    ///
    /// `smaller_side` identifies which side's keys are used to build the filter:
    /// - **Inner join**: the side with smaller estimated cardinality (or `"left"` /
    ///   `"right"` when one or both cardinalities are unknown).
    /// - **Left outer join**: always `"left"` (only the right side is filtered).
    /// - **Right outer join**: always `"right"` (only the left side is filtered).
    ///
    /// `estimated_reduction_pct` is a planner-time upper-bound estimate of the fraction
    /// of the *filtered* side's elements that will be discarded, expressed as a
    /// percentage `0–100`.  Computed as
    /// `max(0, ⌊(|larger| − |smaller|) / |larger| × 100⌋)` when both cardinalities are
    /// available; `0` otherwise.
    BloomSemiJoin {
        /// Which side's keys are used to build the Bloom filter (`"left"` or `"right"`).
        smaller_side: String,
        /// Estimated upper-bound percentage of elements filtered from the probe side.
        estimated_reduction_pct: u8,
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
    /// Human-readable labels attached to individual nodes via
    /// [`PCollection::with_name`](crate::PCollection::with_name).
    ///
    /// Snapshot of the source [`Pipeline`]'s name map at plan-build time
    /// (see [`Plan::node_names`]).  Empty when no node has been named.  The
    /// [`Display`] impl renders these as a "NAMED OPERATIONS" footer block
    /// when non-empty; external backends consult the same data when
    /// translating the graph to a remote runner.
    pub node_names: HashMap<NodeId, String>,
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
                    OptimizationDecision::PushedDownPredicates { ops_pushed } => {
                        writeln!(f, "│ • Predicate Pushdown Before Shuffle Barrier")?;
                        writeln!(
                            f,
                            "│   {ops_pushed} cardinality-reducing op(s) confirmed pre-barrier (GroupByKey or Reshuffle)"
                        )?;
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
                    OptimizationDecision::PushedDownIntoFlattenSubplans {
                        ops_pushed,
                        subplan_count,
                    } => {
                        writeln!(f, "│ • Predicate Pushdown Into Flatten Subplans")?;
                        writeln!(
                            f,
                            "│   {ops_pushed} op(s) cloned into {subplan_count} subplan(s) before fan-in"
                        )?;
                    }
                    OptimizationDecision::EliminatedReshuffle { count } => {
                        writeln!(f, "│ • Eliminated Redundant Reshuffle")?;
                        writeln!(
                            f,
                            "│   Removed {count} redundant Reshuffle node(s) before shuffle barriers or consecutive pairs"
                        )?;
                    }
                    OptimizationDecision::PrunedDeadSubtrees { nodes_pruned } => {
                        writeln!(f, "│ • Dead Subtree Elimination")?;
                        writeln!(
                            f,
                            "│   Pruned {nodes_pruned} unreachable node(s) before chain extraction"
                        )?;
                    }
                    OptimizationDecision::ReorderedCoGroupInputs {
                        original_order,
                        new_order,
                    } => {
                        writeln!(f, "│ • `CoGroup` Input Reordering")?;
                        writeln!(
                            f,
                            "│   Reordered {} subchain(s) by estimated cardinality: {:?} → {:?}",
                            new_order.len(),
                            original_order,
                            new_order
                        )?;
                    }
                    OptimizationDecision::TreeReduction { global_count } => {
                        writeln!(f, "│ • Tree Reduction for Associative Combiners")?;
                        writeln!(
                            f,
                            "│   {global_count} CombineGlobal node(s) use O(log n) parallel tree reduction"
                        )?;
                    }
                    OptimizationDecision::LimitPushdown { n } => {
                        writeln!(f, "│ • Early Termination / Limit Pushdown")?;
                        writeln!(
                            f,
                            "│   Terminal take({n}): runner stops after collecting {n} element(s)"
                        )?;
                    }
                    OptimizationDecision::BloomSemiJoin {
                        smaller_side,
                        estimated_reduction_pct,
                    } => {
                        writeln!(f, "│ • Bloom Semi-Join Pre-Filter")?;
                        writeln!(
                            f,
                            "│   Build side: {smaller_side}; estimated probe-side reduction: {estimated_reduction_pct}%"
                        )?;
                    }
                    OptimizationDecision::AdaptivePartitionCount { barrier_count } => {
                        writeln!(f, "│ • Adaptive Inter-Stage Partition Count")?;
                        writeln!(
                            f,
                            "│   {barrier_count} barrier stage(s) will rescale partition count by cardinality ratio"
                        )?;
                    }
                    OptimizationDecision::EmptySourceShortCircuit => {
                        writeln!(f, "│ • Empty Source Short-Circuit")?;
                        writeln!(
                            f,
                            "│   Source has 0 elements; runner returns Vec::new() without executing"
                        )?;
                    }
                    OptimizationDecision::SingletonSourceShortCircuit => {
                        writeln!(f, "│ • Singleton Source Short-Circuit")?;
                        writeln!(
                            f,
                            "│   Source has 1 element; runner forces sequential execution to avoid partition overhead"
                        )?;
                    }
                }
            }
            writeln!(
                f,
                "└──────────────────────────────────────────────────────────────┘"
            )?;
        }

        if !self.node_names.is_empty() {
            writeln!(f)?;
            writeln!(
                f,
                "┌─ NAMED OPERATIONS ───────────────────────────────────────────┐"
            )?;
            // Sort by NodeId so the output is deterministic regardless of the
            // underlying HashMap's iteration order.
            let mut entries: Vec<(&NodeId, &String)> = self.node_names.iter().collect();
            entries.sort_by_key(|(id, _)| id.raw());
            for (id, name) in entries {
                writeln!(f, "│ • {id:?}: {name}")?;
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
                Node::Flatten { chains, .. } => {
                    barriers += 1;
                    total_ops += 1;
                    (
                        "Flatten",
                        format!("Flatten {} collections (BARRIER)", chains.len()),
                        true,
                        120,
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
                Node::CombineGlobal {
                    fanout,
                    tree_reduce,
                    ..
                } => {
                    barriers += 1;
                    total_ops += 1;
                    let fanout_str =
                        fanout.map_or_else(|| "unbounded".to_string(), |f| f.to_string());
                    let mode = if *tree_reduce {
                        "tree-reduce"
                    } else {
                        "fanout-merge"
                    };
                    (
                        "CombineGlobal",
                        format!("Global aggregation [{mode}] fanout={fanout_str} (BARRIER)"),
                        true,
                        90,
                    )
                }
                Node::Materialized(_) => {
                    total_ops += 1;
                    ("Materialized", "Materialize results".to_string(), false, 1)
                }
                Node::Reshuffle { .. } => {
                    barriers += 1;
                    total_ops += 1;
                    (
                        "Reshuffle",
                        "Collect all partitions and redistribute elements evenly (BARRIER)"
                            .to_string(),
                        true,
                        100,
                    )
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
            node_names: self.node_names.clone(),
        }
    }
}

/// Build a linear plan from `terminal`, apply optimizer passes, and produce
/// a partitioning hint.
///
/// The pass order is intentional:
/// 0) dead subtree elimination (pre-pass before chain extraction — operates on the raw graph)
/// 1) backwalk graph -> chain
/// 2) fuse stateless
/// 3) `CoGroup` input reordering — sort Flatten subchains by estimated cardinality ascending
/// 4) predicate pushdown before shuffle barriers — `GroupByKey` and `Reshuffle` — (requires fused
///    blocks; may split one Stateless into two)
/// 5) predicate pushdown into Flatten subplans (clones qualifying ops into each subplan tail)
/// 6) reorder value-only ops (works on the blocks produced by steps 4–5)
/// 7) lift GBK->Combine (structure-changing; GBK must still be present)
/// 8) eliminate redundant Reshuffle (runs after lift so lifted `CombineValues` is visible as a target)
/// 9) drop mid-materialized (cleanup)
///
/// # Errors
///
/// If any of the optimizer passes fail, or the pipeline is in an inconsistent state.
#[allow(clippy::too_many_lines)]
pub fn build_plan(p: &Pipeline, terminal: NodeId) -> Result<Plan> {
    let (nodes, edges) = p.snapshot();

    let mut optimizations = Vec::new();

    // Pre-pass 0: dead subtree elimination — remove nodes with no forward path to terminal.
    let (nodes, edges, nodes_pruned) = prune_dead_subtrees(nodes, edges, terminal);
    if nodes_pruned > 0 {
        optimizations.push(OptimizationDecision::PrunedDeadSubtrees { nodes_pruned });
    }

    let mut chain = backwalk_linear(nodes, &edges, terminal)?;
    let len_hint = estimate_source_len(&chain);

    let (new_chain, fusion_opt) = fuse_stateless_tracked(chain);
    chain = new_chain;
    if let Some(opt) = fusion_opt {
        optimizations.push(opt);
    }

    let (new_chain, cogroup_order_opts) = reorder_cogroup_inputs_pass(chain);
    chain = new_chain;
    optimizations.extend(cogroup_order_opts);

    let (new_chain, pushdown_opt) = push_down_before_barrier_pass(chain);
    chain = new_chain;
    if let Some(opt) = pushdown_opt {
        optimizations.push(opt);
    }

    let (new_chain, flatten_pushdown_opt) = push_down_into_flatten_pass(chain);
    chain = new_chain;
    if let Some(opt) = flatten_pushdown_opt {
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

    let (new_chain, reshuffle_opt) = eliminate_reshuffle_pass(chain);
    chain = new_chain;
    if let Some(opt) = reshuffle_opt {
        optimizations.push(opt);
    }

    let (new_chain, drop_opt) = drop_mid_materialized_tracked(chain);
    chain = new_chain;
    if let Some(opt) = drop_opt {
        optimizations.push(opt);
    }

    // Post-pass: count CombineGlobal nodes with tree reduction enabled.
    let tree_reduce_count = chain
        .iter()
        .filter(|n| {
            matches!(
                n,
                Node::CombineGlobal {
                    tree_reduce: true,
                    ..
                }
            )
        })
        .count();
    if tree_reduce_count > 0 {
        optimizations.push(OptimizationDecision::TreeReduction {
            global_count: tree_reduce_count,
        });
    }

    // Post-pass: record BloomSemiJoin optimization decisions for CoGroup nodes.
    let bloom_opts = bloom_semi_join_pass(&chain);
    optimizations.extend(bloom_opts);

    // Post-pass: adaptive inter-stage partition count — count barrier stages.
    let adaptive_barriers = count_adaptive_barriers(&chain);
    if adaptive_barriers > 0 {
        optimizations.push(OptimizationDecision::AdaptivePartitionCount {
            barrier_count: adaptive_barriers,
        });
    }

    // Post-pass: detect a terminal take(N) / first() for early-termination support.
    // The limit is encoded as a TakeOp whose `limit_n()` returns `Some(N)`.
    let limit = chain.last().and_then(|node| {
        if let Node::Stateless(ops) = node {
            ops.last().and_then(|op| op.limit_n())
        } else {
            None
        }
    });
    if let Some(n) = limit {
        optimizations.push(OptimizationDecision::LimitPushdown { n });
    }

    // Post-pass: empty / singleton source short-circuits.
    // `CombineGlobal` always emits exactly one output element (the identity value) even
    // for an empty input, so the empty short-circuit must NOT fire when one is present.
    //
    // `Flatten` and `CoGroup` pipelines use a 1-element dummy source as a graph anchor;
    // `is_singleton` must not fire for them because the real data lives in the subchains,
    // and we want their subchains to run via `exec_par` (with `run_subplan_par`) when
    // parallel mode is selected.
    let has_combine_global = chain
        .iter()
        .any(|n| matches!(n, Node::CombineGlobal { .. }));
    let has_flatten_or_cogroup = chain
        .iter()
        .any(|n| matches!(n, Node::Flatten { .. } | Node::CoGroup { .. }));
    let is_empty = !has_combine_global && len_hint == Some(0);
    let is_singleton = !has_flatten_or_cogroup && len_hint == Some(1);
    if is_empty {
        optimizations.push(OptimizationDecision::EmptySourceShortCircuit);
    } else if is_singleton {
        optimizations.push(OptimizationDecision::SingletonSourceShortCircuit);
    }

    // For a singleton source, override the partition suggestion to 1.
    let suggested = if is_singleton {
        Some(1)
    } else {
        suggest_partitions(len_hint)
    };
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
        limit,
        is_empty,
        is_singleton,
        node_names: p.node_names_snapshot(),
    })
}

/* ---------- CoGroup input reordering ---------- */

/// Estimate the cardinality of a subchain by inspecting its first node.
///
/// Returns `Some(n)` when the chain starts with a `Source` node whose `VecOps`
/// implementation reports a known length; otherwise returns `None`.
fn estimate_subchain_cardinality(chain: &[Node]) -> Option<usize> {
    if let Some(Node::Source {
        payload, vec_ops, ..
    }) = chain.first()
    {
        vec_ops.len(payload.as_ref())
    } else {
        None
    }
}

/// Sort the input subchains of every `Flatten` node by estimated cardinality (ascending).
///
/// For each `Flatten { chains, … }` in the chain with two or more input subplans:
///
/// 1. Estimates the cardinality of each subchain using [`estimate_subchain_cardinality`]
///    (looks for a leading `Source` node with a known length).
/// 2. Builds a sort permutation: subchains with a known cardinality are ordered ascending
///    by that count; subchains with an **unknown** cardinality are moved to the end
///    (treated conservatively as large).
/// 3. If the sorted permutation differs from the original order, applies the sort and
///    emits a [`OptimizationDecision::ReorderedCoGroupInputs`] recording both the
///    original and new ordering.
///
/// **Why `Flatten` rather than `Node::CoGroup`?**  The `cogroup_by_key!` macro
/// implements N-way grouping as a `Flatten` (one subplan per input collection) followed
/// by a `GroupByKey`, **not** as a binary `CoGroup` join tree.  All subchains in such a
/// `Flatten` produce the same tagged element type — making reordering type-safe.  The
/// `Node::CoGroup` binary join has distinct left/right value types baked into its
/// `exec` closure, so swapping those chains would produce a runtime type mismatch.
///
/// **Performance rationale:** In sequential execution the runner replays subchains
/// one-at-a-time.  Scheduling cheaper (smaller) subchains first reduces the peak
/// number of intermediate elements held in memory before the downstream `GroupByKey`
/// or merge step can consume them, and can surface data to later pipeline stages
/// sooner.  In parallel execution (rayon) all subplans run concurrently, so ordering
/// has no effect on wall-clock time but also incurs no overhead.
fn reorder_cogroup_inputs_pass(chain: Vec<Node>) -> (Vec<Node>, Vec<OptimizationDecision>) {
    let mut out = Vec::with_capacity(chain.len());
    let mut decisions = Vec::new();

    for node in chain {
        let Node::Flatten {
            chains,
            coalesce,
            merge,
        } = node
        else {
            out.push(node);
            continue;
        };

        let old_chains: Vec<Vec<Node>> =
            Arc::try_unwrap(chains).unwrap_or_else(|arc| (*arc).clone());
        let n = old_chains.len();

        if n <= 1 {
            out.push(Node::Flatten {
                chains: Arc::new(old_chains),
                coalesce,
                merge,
            });
            continue;
        }

        // Estimate cardinality of each subchain.
        let cardinalities: Vec<Option<usize>> = old_chains
            .iter()
            .map(|c| estimate_subchain_cardinality(c))
            .collect();

        // Build a sorted permutation: known sizes ascending first, unknowns at the end.
        let mut indices: Vec<usize> = (0..n).collect();
        indices.sort_by_key(|&i| cardinalities[i].map_or((1u8, 0), |len| (0u8, len)));

        let original_order: Vec<usize> = (0..n).collect();
        if indices == original_order {
            out.push(Node::Flatten {
                chains: Arc::new(old_chains),
                coalesce,
                merge,
            });
            continue;
        }

        let new_order = indices.clone();
        let new_chains: Vec<Vec<Node>> =
            indices.into_iter().map(|i| old_chains[i].clone()).collect();
        decisions.push(OptimizationDecision::ReorderedCoGroupInputs {
            original_order,
            new_order,
        });
        out.push(Node::Flatten {
            chains: Arc::new(new_chains),
            coalesce,
            merge,
        });
    }

    (out, decisions)
}

/* ---------- Bloom semi-join pass ---------- */

/// Scan the chain for [`Node::CoGroup`] nodes that use a Bloom semi-join pre-filter
/// and emit a [`OptimizationDecision::BloomSemiJoin`] for each one.
///
/// Uses the same cardinality estimation as [`reorder_cogroup_inputs_pass`] to determine
/// which side is "smaller" (the build side) and to compute an upper-bound estimate of
/// how many elements on the probe side will be discarded.
///
/// ## `smaller_side` semantics
///
/// | Estimated cardinalities  | `smaller_side` |
/// |--------------------------|----------------|
/// | Both known, left < right | `"left"`        |
/// | Both known, right ≤ left | `"right"`       |
/// | Only left known          | `"left"`        |
/// | Only right known         | `"right"`       |
/// | Neither known            | `"left"` (default) |
///
/// ## `estimated_reduction_pct`
///
/// `max(0, ⌊(|probe| − |build|) / |probe| × 100⌋)` when both cardinalities are
/// available; `0` when either is unknown.
fn bloom_semi_join_pass(chain: &[Node]) -> Vec<OptimizationDecision> {
    let mut decisions = Vec::new();

    for node in chain {
        let Node::CoGroup {
            left_chain,
            right_chain,
            uses_bloom_semi_join,
            ..
        } = node
        else {
            continue;
        };

        if !uses_bloom_semi_join {
            continue;
        }

        let left_card = estimate_subchain_cardinality(left_chain);
        let right_card = estimate_subchain_cardinality(right_chain);

        let (smaller_side, estimated_reduction_pct) = match (left_card, right_card) {
            (Some(l), Some(r)) => {
                if l <= r {
                    // Build from left, probe right.
                    #[allow(
                        clippy::cast_precision_loss,
                        clippy::cast_sign_loss,
                        clippy::cast_possible_truncation
                    )]
                    let pct = if r > 0 {
                        (r.saturating_sub(l) as f64 / r as f64 * 100.0) as u8
                    } else {
                        0u8
                    };
                    ("left".to_string(), pct)
                } else {
                    // Build from right, probe left.
                    #[allow(
                        clippy::cast_precision_loss,
                        clippy::cast_sign_loss,
                        clippy::cast_possible_truncation
                    )]
                    let pct = if l > 0 {
                        (l.saturating_sub(r) as f64 / l as f64 * 100.0) as u8
                    } else {
                        0u8
                    };
                    ("right".to_string(), pct)
                }
            }
            (Some(_) | None, None) => ("left".to_string(), 0u8),
            (None, Some(_)) => ("right".to_string(), 0u8),
        };

        decisions.push(OptimizationDecision::BloomSemiJoin {
            smaller_side,
            estimated_reduction_pct,
        });
    }

    decisions
}

/* ---------- Adaptive inter-stage partition count ---------- */

/// Count the number of barrier stages in the plan chain that will trigger
/// adaptive partition rescaling at runtime.
///
/// A barrier stage is any node that collapses or reshapes partitions:
/// `GroupByKey`, `CombineValues`, `CombineGlobal`, `Flatten`, `CoGroup`, `Reshuffle`.
///
/// The runner uses per-barrier cardinality ratios to adaptively scale the current
/// partition count after each such stage, keeping downstream Reshuffle splits in proportion
/// to the actual data volume rather than the original source-based suggestion.
fn count_adaptive_barriers(chain: &[Node]) -> usize {
    chain
        .iter()
        .filter(|n| {
            matches!(
                n,
                Node::GroupByKey { .. }
                    | Node::CombineValues { .. }
                    | Node::CombineGlobal { .. }
                    | Node::Flatten { .. }
                    | Node::CoGroup { .. }
                    | Node::Reshuffle { .. }
            )
        })
        .count()
}

/* ---------- Dead subtree elimination ---------- */

/// Remove nodes that have no forward path to `terminal` before chain extraction.
///
/// Performs a backward BFS from `terminal`, following edges in reverse (`to → from`).
/// Every node reachable by this traversal is an ancestor of `terminal` — i.e. it can
/// reach `terminal` by following edges forward.  Nodes that are *not* reached are dead:
/// they belong to branches of the graph that lead exclusively to some other terminal and
/// can be safely pruned before `backwalk_linear` is called.
///
/// **Why run this before `backwalk_linear`?**  In a multi-terminal graph (e.g. after
/// `partition!` or a tee), a shared source node has out-degree > 1 — one edge per
/// branch.  `backwalk_linear` uses `edges.iter().find(|(_, to)| *to == cur)`, which
/// picks the *first* matching edge.  If a dead-branch edge happens to appear first in
/// the slice, `backwalk_linear` would follow the wrong predecessor.  Pruning dead nodes
/// (and their edges) first guarantees that only one in-edge survives per node on the
/// live path, making predecessor selection unambiguous.
///
/// Returns `(live_nodes, live_edges, count_of_nodes_removed)`.
fn prune_dead_subtrees(
    mut nodes: HashMap<NodeId, Node>,
    mut edges: Vec<(NodeId, NodeId)>,
    terminal: NodeId,
) -> (HashMap<NodeId, Node>, Vec<(NodeId, NodeId)>, usize) {
    let mut reachable: HashSet<NodeId> = HashSet::new();
    let mut queue: VecDeque<NodeId> = VecDeque::new();
    reachable.insert(terminal);
    queue.push_back(terminal);

    while let Some(cur) = queue.pop_front() {
        for &(from, to) in &edges {
            if to == cur && reachable.insert(from) {
                queue.push_back(from);
            }
        }
    }

    let before = nodes.len();
    nodes.retain(|id, _| reachable.contains(id));
    edges.retain(|(from, to)| reachable.contains(from) && reachable.contains(to));
    let nodes_pruned = before - nodes.len();

    (nodes, edges, nodes_pruned)
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

/* ---------- Predicate pushdown into Flatten subplans ---------- */

/// Push `value_only + cardinality_reducing` ops from the post-`Flatten` `Stateless` block
/// into the tail of every `Flatten` input subplan, then remove them from the outer chain.
///
/// For each consecutive `[Flatten { chains, … }, Stateless(ops)]` pair:
///
/// 1. Splits `ops` into `pushable` (both `value_only` and `cardinality_reducing` are true)
///    and `remaining` (everything else).
/// 2. Clones each `pushable` op and appends them as a new trailing `Stateless` block to
///    **every** input subplan inside `Flatten`.  Because `Flatten` executes each subplan
///    independently, the filter runs once per subplan, reducing the elements that reach
///    the coalesce/merge step.
/// 3. Reconstructs the `Flatten` node with the augmented subplans.
/// 4. Keeps the post-`Flatten` `Stateless` block only if it still has `remaining` ops;
///    drops it entirely when all ops were pushed.
/// 5. Records a [`OptimizationDecision::PushedDownIntoFlattenSubplans`] when any ops are
///    pushed, so the explain output reflects the optimization.
///
/// **Safety invariant:** only `value_only` ops are eligible because `value_only` guarantees
/// the element type is preserved — the subplan output remains the same type that the
/// `coalesce` and `merge` closures expect.  A non-`value_only` op (e.g. `map_values`) could
/// silently change the element type and cause a runtime downcast failure inside the runner.
fn push_down_into_flatten_pass(chain: Vec<Node>) -> (Vec<Node>, Option<OptimizationDecision>) {
    if chain.len() < 2 {
        return (chain, None);
    }

    let mut out = Vec::with_capacity(chain.len());
    let mut total_ops_pushed = 0usize;
    let mut total_subplans_affected = 0usize;

    let is_pushable = |op: &Arc<dyn DynOp>| op.value_only() && op.cardinality_reducing();

    let mut iter = chain.into_iter().peekable();
    while let Some(node) = iter.next() {
        let Node::Flatten {
            chains,
            coalesce,
            merge,
        } = node
        else {
            out.push(node);
            continue;
        };

        // Only apply the pattern when the very next node is a Stateless block.
        if !iter
            .peek()
            .is_some_and(|nx| matches!(nx, Node::Stateless(_)))
        {
            out.push(Node::Flatten {
                chains,
                coalesce,
                merge,
            });
            continue;
        }
        let Node::Stateless(ops) = iter.next().expect("peeked Some") else {
            unreachable!("matched Stateless above");
        };

        let (pushable, remaining): (Vec<_>, Vec<_>) = ops.iter().cloned().partition(is_pushable);

        if pushable.is_empty() {
            // No eligible ops — pass through unchanged.
            out.push(Node::Flatten {
                chains,
                coalesce,
                merge,
            });
            out.push(Node::Stateless(ops));
            continue;
        }

        // Clone each subplan, appending the pushable ops to its tail.
        let old_chains: Vec<Vec<Node>> =
            Arc::try_unwrap(chains).unwrap_or_else(|arc| (*arc).clone());

        let subplan_count = old_chains.len();
        let new_chains: Vec<Vec<Node>> = old_chains
            .into_iter()
            .map(|mut subchain| {
                subchain.push(Node::Stateless(pushable.clone()));
                subchain
            })
            .collect();

        total_ops_pushed += pushable.len();
        total_subplans_affected += subplan_count;

        out.push(Node::Flatten {
            chains: Arc::new(new_chains),
            coalesce,
            merge,
        });

        // Keep remaining ops in the outer chain; drop the block if nothing is left.
        if !remaining.is_empty() {
            out.push(Node::Stateless(remaining));
        }
    }

    let opt =
        (total_ops_pushed > 0).then_some(OptimizationDecision::PushedDownIntoFlattenSubplans {
            ops_pushed: total_ops_pushed,
            subplan_count: total_subplans_affected,
        });
    (out, opt)
}

/* ---------- Reorder value-only runs ---------- */

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

/* ---------- Predicate pushdown before shuffle barriers ---------- */

/// Hoist cardinality-reducing ops to run as early as possible before a shuffle barrier.
///
/// Treated barriers: [`Node::GroupByKey`] and [`Node::Reshuffle`].  Both redistribute
/// all elements but neither alters element content or count, so a filter that would be
/// beneficial to run before a `GroupByKey` is equally beneficial before a `Reshuffle`.
///
/// For each consecutive `[Stateless(ops), GroupByKey | Reshuffle]` pair in the chain the pass:
///
/// 1. Splits `ops` into `pushable` (all three of `key_preserving`, `value_only`,
///    `cardinality_reducing` are true) and `remaining` (everything else).
/// 2. **Records** an [`OptimizationDecision::PushedDownPredicates`] whenever any pushable
///    ops are found, so the explain output reflects that predicates are in the ideal
///    pre-barrier position.
/// 3. **Structurally splits** the Stateless block — putting pushable ops in their own
///    earlier Stateless node — only when two conditions both hold:
///    - **Type-safe**: every `remaining` op that currently precedes the first pushable op
///      is itself `value_only + key_preserving` (guaranteeing the `(K, V)` type is
///      intact at the point we insert the filter block).
///    - **Cost-beneficial** (the cost-hint gate): at least one such preceding remaining op
///      has a higher `cost_hint` than the cheapest pushable op.  If the pushable ops are
///      already first, or the preceding ops are equally cheap, splitting would add node
///      dispatch overhead with no throughput benefit.
///
/// No ops are ever moved past the barrier; `remaining` always stays pre-barrier.
fn push_down_before_barrier_pass(chain: Vec<Node>) -> (Vec<Node>, Option<OptimizationDecision>) {
    let mut out = Vec::with_capacity(chain.len() + 1);
    let mut pushed_count = 0usize;

    let is_pushable =
        |op: &Arc<dyn DynOp>| op.key_preserving() && op.value_only() && op.cardinality_reducing();

    let mut iter = chain.into_iter().peekable();
    while let Some(node) = iter.next() {
        let Node::Stateless(ops) = node else {
            out.push(node);
            continue;
        };

        // Only apply the pattern when the very next node is a GroupByKey or Reshuffle.
        // Both barriers redistribute elements without altering content or count, so a
        // cardinality-reducing filter is equally beneficial before either one.
        if !iter
            .peek()
            .is_some_and(|nx| matches!(nx, Node::GroupByKey { .. } | Node::Reshuffle { .. }))
        {
            out.push(Node::Stateless(ops));
            continue;
        }
        let barrier = iter.next().expect("peeked Some");

        let (pushable, remaining): (Vec<_>, Vec<_>) = ops.iter().cloned().partition(is_pushable);

        if pushable.is_empty() {
            // No pushable ops in this block — pass through unchanged.
            out.push(Node::Stateless(ops));
            out.push(barrier);
            continue;
        }

        pushed_count += pushable.len();

        // Cost-hint gate: only perform a structural split when it is both
        // type-safe and cheaper than the ops it would leapfrog.
        let min_pushable_cost = pushable
            .iter()
            .map(|op| op.cost_hint())
            .min()
            .unwrap_or(u8::MAX);
        let first_pushable_pos = ops.iter().position(is_pushable).unwrap_or(ops.len());
        let pre_pushable = &ops[..first_pushable_pos];

        // Type-safety: all ops that precede the first pushable op must be
        // value_only + key_preserving — meaning the partition is already in
        // (K, V) form at the insertion point.
        let type_safe = pre_pushable
            .iter()
            .all(|op| op.value_only() && op.key_preserving());

        // Benefit: at least one preceding op is strictly more expensive than
        // the cheapest pushable op.  Equal-cost shuffling adds dispatch
        // overhead with no throughput gain.
        let beneficial = pre_pushable
            .iter()
            .any(|op| op.cost_hint() > min_pushable_cost);

        if !remaining.is_empty() && type_safe && beneficial {
            // Structural split: filters first, remaining pre-barrier ops second.
            out.push(Node::Stateless(pushable));
            out.push(Node::Stateless(remaining));
        } else {
            // Already optimal or unsafe to split; keep the fused block as-is.
            out.push(Node::Stateless(ops));
        }
        out.push(barrier);
    }

    let opt = (pushed_count > 0).then_some(OptimizationDecision::PushedDownPredicates {
        ops_pushed: pushed_count,
    });
    (out, opt)
}

/* ---------- GBK -> Combine lifting ---------- */

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

/* ---------- Reshuffle elimination ---------- */

/// Remove redundant [`Node::Reshuffle`] nodes and track the decision.
///
/// A `Reshuffle` is redundant in two cases, both of which are pure no-ops:
///
/// 1. It immediately precedes a shuffle barrier — [`Node::GroupByKey`],
///    [`Node::CombineValues`], [`Node::CoGroup`], or [`Node::Flatten`] — each of
///    which already materializes and redistributes all elements across partitions.
///    Reshuffling immediately before such a barrier is therefore a wasted O(N) pass.
/// 2. It immediately precedes another [`Node::Reshuffle`].  After the first pass
///    elements are already evenly distributed; a second pass produces the same
///    distribution.  The leading (first) `Reshuffle` is dropped, keeping the
///    trailing one so the redistribution still occurs once.
///
/// The pass scans the chain left-to-right and skips (drops) the current node
/// whenever it is a `Reshuffle` whose successor matches one of the above patterns.
/// Greedy left-to-right scanning handles chains of three or more consecutive
/// reshuffles in a single pass.
fn eliminate_reshuffle_pass(chain: Vec<Node>) -> (Vec<Node>, Option<OptimizationDecision>) {
    if chain.len() < 2 {
        return (chain, None);
    }
    let mut out = Vec::with_capacity(chain.len());
    let mut i = 0usize;
    let mut eliminated = 0usize;

    while i < chain.len() {
        if i + 1 < chain.len() && matches!(chain[i], Node::Reshuffle { .. }) {
            let successor_absorbs = matches!(
                chain[i + 1],
                Node::GroupByKey { .. }
                    | Node::CombineValues { .. }
                    | Node::CoGroup { .. }
                    | Node::Flatten { .. }
                    | Node::Reshuffle { .. }
            );
            if successor_absorbs {
                eliminated += 1;
                i += 1; // skip the leading Reshuffle; process successor on next iteration
                continue;
            }
        }
        out.push(chain[i].clone());
        i += 1;
    }

    let opt =
        (eliminated > 0).then_some(OptimizationDecision::EliminatedReshuffle { count: eliminated });
    (out, opt)
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

/* ---------- CSE helpers ---------- */

/// DFS post-order traversal from `source` through the forward edge graph.
///
/// Nodes are appended to `order` after all their successors have been visited
/// (post-order).  Reversing the result yields *reverse post-order* (RPO), which
/// Cooper's dominance algorithm requires.
fn dfs_postorder(
    node: NodeId,
    succs: &HashMap<NodeId, Vec<NodeId>>,
    visited: &mut HashSet<NodeId>,
    order: &mut Vec<NodeId>,
) {
    if !visited.insert(node) {
        return;
    }
    for &child in succs.get(&node).map_or(&[] as &[NodeId], Vec::as_slice) {
        dfs_postorder(child, succs, visited, order);
    }
    order.push(node);
}

/// Walk up the dominator tree from `b1` and `b2` until they meet.
///
/// This is the *intersect* subroutine of Cooper's dominance algorithm.  Both
/// fingers advance toward the root along the `idom` chain, guided by the RPO
/// numbering so the deeper finger always moves first.
fn dominator_intersect(
    mut b1: NodeId,
    mut b2: NodeId,
    idom: &HashMap<NodeId, NodeId>,
    rpo: &HashMap<NodeId, usize>,
) -> NodeId {
    while b1 != b2 {
        while rpo.get(&b1).copied().unwrap_or(usize::MAX)
            > rpo.get(&b2).copied().unwrap_or(usize::MAX)
        {
            b1 = idom[&b1];
        }
        while rpo.get(&b2).copied().unwrap_or(usize::MAX)
            > rpo.get(&b1).copied().unwrap_or(usize::MAX)
        {
            b2 = idom[&b2];
        }
    }
    b1
}

/// Compute the immediate-dominator map for all nodes reachable from `source`.
///
/// Uses Cooper's simple dominance algorithm (PLDI 2001), which is O(N²) in
/// the worst case but converges in one or two passes for the acyclic pipeline
/// DAGs that Ironbeam produces.
///
/// A node `d` **dominates** node `n` when every directed path from `source` to
/// `n` passes through `d`.  The *immediate* dominator of `n` is the deepest
/// such `d ≠ n` in the dominator tree.
///
/// # Returns
///
/// A `HashMap<NodeId, NodeId>` where each key maps to its immediate dominator.
/// The `source` node maps to itself as a sentinel.  Nodes unreachable from
/// `source` are absent from the map.
fn build_dominator_tree(edges: &[(NodeId, NodeId)], source: NodeId) -> HashMap<NodeId, NodeId> {
    let mut succs: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
    let mut preds: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
    for &(from, to) in edges {
        succs.entry(from).or_default().push(to);
        preds.entry(to).or_default().push(from);
    }

    // DFS post-order, then reverse → RPO.
    let mut order = Vec::new();
    let mut visited = HashSet::new();
    dfs_postorder(source, &succs, &mut visited, &mut order);
    order.reverse();

    let rpo: HashMap<NodeId, usize> = order.iter().enumerate().map(|(i, &n)| (n, i)).collect();

    let mut idom: HashMap<NodeId, NodeId> = HashMap::new();
    idom.insert(source, source);

    let mut changed = true;
    while changed {
        changed = false;
        for &b in order.iter().skip(1) {
            let processed_preds: Vec<NodeId> = preds
                .get(&b)
                .map(|ps| {
                    ps.iter()
                        .filter(|&&p| idom.contains_key(&p))
                        .copied()
                        .collect()
                })
                .unwrap_or_default();
            if processed_preds.is_empty() {
                continue;
            }
            let mut new_idom = processed_preds[0];
            for &p in &processed_preds[1..] {
                new_idom = dominator_intersect(p, new_idom, &idom, &rpo);
            }
            if idom.get(&b) != Some(&new_idom) {
                idom.insert(b, new_idom);
                changed = true;
            }
        }
    }

    idom
}

/// Identify the CSE materialization node for `terminal` using dominator-tree analysis.
///
/// This replaces the earlier heuristic (`find_deepest_fanout_ancestor`) with a
/// principled approach: the **immediate dominator** of `terminal` in the pipeline
/// DAG is the deepest node that *every* source-to-terminal path passes through,
/// making it the correct and maximally-deep cache point for Common Subexpression
/// Elimination.
///
/// Improvements over the fan-out heuristic:
///
/// - **Diamond patterns** — when two input branches merge at a join node (e.g. a
///   `Flatten`), the join node is the immediate dominator of any terminal beyond
///   it.  The old heuristic walked backward and found the *fan-out* node (the
///   fork, not the join), yielding a shallow, near-trivial cache point.  The
///   dominator approach correctly identifies the *join* node, caching the full
///   merged prefix.
/// - **Linear pipelines** — for a single chain with no branching, the old
///   heuristic returned `None` (no caching).  The dominator approach returns the
///   node immediately before `terminal`, enabling caching on repeated calls for
///   the same terminal.
///
/// Returns `None` when:
/// - `edges` is empty.
/// - `terminal` is the source itself (no prefix to cache).
/// - `idom(terminal)` equals the source (the only shared ancestor is the root,
///   meaning there is no useful shared prefix deeper in the graph).
pub(crate) fn find_cache_node_via_dominators(
    edges: &[(NodeId, NodeId)],
    terminal: NodeId,
) -> Option<NodeId> {
    if edges.is_empty() {
        return None;
    }

    // Source = node that appears as `from` but never as `to`.
    let has_incoming: HashSet<NodeId> = edges.iter().map(|&(_, to)| to).collect();
    let source = edges
        .iter()
        .map(|&(from, _)| from)
        .find(|n| !has_incoming.contains(n))?;

    if source == terminal {
        return None;
    }

    let idom = build_dominator_tree(edges, source);
    let cache_node = *idom.get(&terminal)?;

    if cache_node == source {
        None
    } else {
        Some(cache_node)
    }
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

#[cfg(test)]
mod dominator_tests {
    use super::*;

    fn n(v: u64) -> NodeId {
        NodeId::new(v)
    }

    // ── build_dominator_tree ───────────────────────────────────────────────

    #[test]
    fn dominator_linear_chain() {
        // 0 → 1 → 2 → 3
        let edges = vec![(n(0), n(1)), (n(1), n(2)), (n(2), n(3))];
        let idom = build_dominator_tree(&edges, n(0));
        assert_eq!(idom[&n(0)], n(0)); // source maps to itself
        assert_eq!(idom[&n(1)], n(0));
        assert_eq!(idom[&n(2)], n(1));
        assert_eq!(idom[&n(3)], n(2));
    }

    #[test]
    fn dominator_simple_fanout() {
        // 0 → 1 → 2
        //       ↘ 3
        let edges = vec![(n(0), n(1)), (n(1), n(2)), (n(1), n(3))];
        let idom = build_dominator_tree(&edges, n(0));
        assert_eq!(idom[&n(1)], n(0));
        assert_eq!(idom[&n(2)], n(1));
        assert_eq!(idom[&n(3)], n(1));
    }

    #[test]
    fn dominator_diamond_join_point() {
        // 0 → 1 → 3 → 4
        //   ↘ 2 ↗
        // dom(3) = {0, 3} → idom(3) = 0 (source is the only shared ancestor)
        // dom(4) = {0, 3, 4} → idom(4) = 3
        let edges = vec![
            (n(0), n(1)),
            (n(0), n(2)),
            (n(1), n(3)),
            (n(2), n(3)),
            (n(3), n(4)),
        ];
        let idom = build_dominator_tree(&edges, n(0));
        assert_eq!(idom[&n(1)], n(0));
        assert_eq!(idom[&n(2)], n(0));
        assert_eq!(idom[&n(3)], n(0)); // join node: idom = source
        assert_eq!(idom[&n(4)], n(3)); // post-join terminal: idom = join
    }

    #[test]
    fn dominator_double_diamond() {
        // Two back-to-back diamonds:
        // 0 → 1 → 3 → 4 → 6 → 7
        //   ↘ 2 ↗   ↘ 5 ↗
        let edges = vec![
            (n(0), n(1)),
            (n(0), n(2)),
            (n(1), n(3)),
            (n(2), n(3)),
            (n(3), n(4)),
            (n(3), n(5)),
            (n(4), n(6)),
            (n(5), n(6)),
            (n(6), n(7)),
        ];
        let idom = build_dominator_tree(&edges, n(0));
        assert_eq!(idom[&n(3)], n(0)); // first join: idom = source
        assert_eq!(idom[&n(6)], n(3)); // second join: idom = first join
        assert_eq!(idom[&n(7)], n(6)); // terminal: idom = second join
    }

    // ── find_cache_node_via_dominators ────────────────────────────────────

    #[test]
    fn cache_node_empty_edges() {
        assert_eq!(find_cache_node_via_dominators(&[], n(5)), None);
    }

    #[test]
    fn cache_node_terminal_is_source() {
        let edges = vec![(n(0), n(1))];
        assert_eq!(find_cache_node_via_dominators(&edges, n(0)), None);
    }

    #[test]
    fn cache_node_idom_is_source_returns_none() {
        // Diamond: 0 → 1 → 3 and 0 → 2 → 3. idom(3) = 0 = source → None.
        let edges = vec![(n(0), n(1)), (n(0), n(2)), (n(1), n(3)), (n(2), n(3))];
        assert_eq!(find_cache_node_via_dominators(&edges, n(3)), None);
    }

    #[test]
    fn cache_node_linear_pipeline() {
        // 0 → 1 → 2: idom(2) = 1 (not source) → Some(1)
        let edges = vec![(n(0), n(1)), (n(1), n(2))];
        assert_eq!(find_cache_node_via_dominators(&edges, n(2)), Some(n(1)));
    }

    #[test]
    fn cache_node_fanout_returns_shared_ancestor() {
        // 0 → 1 → 2 and 0 → 1 → 3: idom(2) = idom(3) = 1 → same cache key.
        let edges = vec![(n(0), n(1)), (n(1), n(2)), (n(1), n(3))];
        assert_eq!(find_cache_node_via_dominators(&edges, n(2)), Some(n(1)));
        assert_eq!(find_cache_node_via_dominators(&edges, n(3)), Some(n(1)));
    }

    #[test]
    fn cache_node_diamond_returns_join_not_fork() {
        // Diamond followed by terminal:
        // 0 → 1 → 3 → 4  and  0 → 2 → 3 → 4
        // Old fan-out heuristic would return source (0); dominator returns join (3).
        let edges = vec![
            (n(0), n(1)),
            (n(0), n(2)),
            (n(1), n(3)),
            (n(2), n(3)),
            (n(3), n(4)),
        ];
        assert_eq!(find_cache_node_via_dominators(&edges, n(4)), Some(n(3)));
    }
}
