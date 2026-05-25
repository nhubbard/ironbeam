//! In-memory representation of a dataflow pipeline graph.
//!
//! The [`Pipeline`] acts as the central registry for all execution nodes
//! ([`Node`]) and their directed connections. It is lightweight,
//! cloneable, and thread-safe via internal `Arc<Mutex<_>>` wrapping, allowing
//! concurrent construction and inspection from different builder contexts.
//!
//! # Overview
//! - Each transformation on a [`PCollection`](crate::PCollection) inserts a new [`Node`].
//! - Edges are stored as `(from, to)` pairs of [`NodeId`]s.
//! - The planner and runner take a *snapshot* of the current graph state before execution.
//!
//! The graph is intentionally simple--no complex dependency tracking--since
//! execution occurs in topologically sorted linear chains rather than arbitrary DAGs.

use crate::NodeId;
use crate::node::Node;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[cfg(feature = "metrics")]
use crate::metrics::MetricsCollector;

/// Thread-safe pipeline graph structure holding all nodes and edges.
///
/// Each pipeline is essentially a shared, mutable graph:
/// ```text
///  Source -> Stateless -> GroupByKey -> CombineValues -> ...
/// ```
///
/// The `Pipeline` itself is cheaply cloneable; all clones share the same
/// underlying `PipelineInner`.
pub struct Pipeline {
    /// Shared reference to the internal graph data.
    pub(crate) inner: Arc<Mutex<PipelineInner>>,
}

/// Inner mutable graph state for a [`Pipeline`].
///
/// This struct tracks:
/// - `next_id`: incremental counter for node IDs.
/// - `nodes`: map of [`NodeId`] -> [`Node`](Node).
/// - `edges`: ordered list of `(from, to)` directed edges.
/// - `node_names`: optional human-readable labels for individual nodes, populated by
///   [`PCollection::with_name`](crate::PCollection::with_name); see
///   [`Pipeline::set_node_name`] and [`Pipeline::node_name`] for the public accessors.
/// - `scope_stack`: stack of active [`ScopeFrame`]s for [`Pipeline::named_scope`].
///   The active scope path is `scope_stack.iter().map(|f| &f.name).join("/")`;
///   newly inserted nodes inside a scope get an auto-generated name of
///   `"<path>/<counter>"` (per-frame counter), and
///   [`PCollection::with_name`](crate::PCollection::with_name) prepends the
///   active path to user-supplied labels.
/// - `metrics`: optional metrics collector for tracking execution statistics.
///
/// The parent synchronizes access to the data in the [`Pipeline`].
pub(crate) struct PipelineInner {
    pub next_id: u64,
    pub nodes: HashMap<NodeId, Node>,
    pub edges: Vec<(NodeId, NodeId)>,
    pub node_names: HashMap<NodeId, String>,
    pub scope_stack: Vec<ScopeFrame>,
    #[cfg(feature = "metrics")]
    pub metrics: Option<MetricsCollector>,
}

/// One frame of the active scope stack used by [`Pipeline::named_scope`].
///
/// Each frame carries its own monotonic auto-numbering counter, so nested
/// scopes do not interleave indices with their parents (outer counters
/// are never incremented by inner-scope inserts).
pub(crate) struct ScopeFrame {
    pub name: String,
    pub counter: u64,
}

impl Default for Pipeline {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(PipelineInner {
                next_id: 0,
                nodes: HashMap::new(),
                edges: vec![],
                node_names: HashMap::new(),
                scope_stack: Vec::new(),
                #[cfg(feature = "metrics")]
                metrics: None,
            })),
        }
    }
}

impl Clone for Pipeline {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Pipeline {
    /// Insert a new node into the graph and return its [`NodeId`].
    ///
    /// This is typically called by transformation builders like
    /// [`map`](crate::PCollection::map) or [`group_by_key`](crate::PCollection::group_by_key).
    ///
    /// When an active [`named_scope`](Self::named_scope) is on the stack, the
    /// newly inserted node is automatically labeled `"<path>/<counter>"`,
    /// where `<path>` is the `"/"`-joined scope frame names and `<counter>`
    /// is the (then-incremented) counter on the innermost frame. Subsequent
    /// [`PCollection::with_name`](crate::PCollection::with_name) /
    /// [`set_node_name`](Self::set_node_name) calls overwrite this
    /// auto-generated label.
    pub(crate) fn insert_node(&self, node: Node) -> NodeId {
        let mut g = self.inner.lock().unwrap();
        let id = NodeId::new(g.next_id);
        g.next_id += 1;
        g.nodes.insert(id, node);
        if !g.scope_stack.is_empty() {
            let path = g
                .scope_stack
                .iter()
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>()
                .join("/");
            // Safe: scope_stack is non-empty per the check above.
            let top = g.scope_stack.last_mut().expect("scope_stack non-empty");
            let counter = top.counter;
            top.counter += 1;
            g.node_names.insert(id, format!("{path}/{counter}"));
        }
        drop(g);
        id
    }

    /// Connect two nodes by their IDs, forming a directed edge `(from -> to)`.
    ///
    /// Used to chain together consecutive transforms within the same pipeline.
    pub(crate) fn connect(&self, from: NodeId, to: NodeId) {
        self.inner.lock().unwrap().edges.push((from, to));
    }

    /// Return a **snapshot** of the current pipeline graph (nodes and edges).
    ///
    /// This is a deep clone of all node and edge data, used by the planner and runner
    /// to analyze or execute the pipeline without mutating the original.
    ///
    /// # Panics
    ///
    /// If the pipeline is in an inconsistent state, such as during concurrent modifications.
    #[must_use]
    pub fn snapshot(&self) -> (HashMap<NodeId, Node>, Vec<(NodeId, NodeId)>) {
        let g = self.inner.lock().unwrap();
        (g.nodes.clone(), g.edges.clone())
    }

    /// Attach a human-readable name to the node identified by `id`.
    ///
    /// Names are pure metadata — they do not influence planning or execution.
    /// They are exposed back to backends and translators (see
    /// [`node_name`](Self::node_name) /
    /// [`node_names_snapshot`](Self::node_names_snapshot)) and surfaced in
    /// [`Plan`](crate::planner::Plan) /
    /// [`ExecutionExplanation`](crate::planner::ExecutionExplanation) output so
    /// that external runners and the local `explain` view can render
    /// meaningful identifiers instead of generic op categories.
    ///
    /// Calling this method twice for the same node overwrites the previous
    /// name (last-write-wins).
    ///
    /// Most user code attaches names via the fluent
    /// [`PCollection::with_name`](crate::PCollection::with_name) helper rather
    /// than calling this method directly; the explicit form is provided for
    /// advanced use cases such as labeling nodes built from raw
    /// [`NodeId`]s.
    ///
    /// # Panics
    ///
    /// If the pipeline mutex is poisoned by a concurrent panic.
    pub fn set_node_name(&self, id: NodeId, name: impl Into<String>) {
        let mut g = self.inner.lock().unwrap();
        g.node_names.insert(id, name.into());
    }

    /// Return the human-readable name attached to `id`, if any.
    ///
    /// Returns `None` for nodes that have never been named via
    /// [`set_node_name`](Self::set_node_name) or
    /// [`PCollection::with_name`](crate::PCollection::with_name).
    ///
    /// # Panics
    ///
    /// If the pipeline mutex is poisoned by a concurrent panic.
    #[must_use]
    pub fn node_name(&self, id: NodeId) -> Option<String> {
        let g = self.inner.lock().unwrap();
        g.node_names.get(&id).cloned()
    }

    /// Return a deep clone of the entire `NodeId -> name` mapping.
    ///
    /// Intended for external translators (e.g., the community Google Dataflow
    /// backend) and other consumers that need a stable view of every named
    /// node in a single call rather than per-id lookups.
    ///
    /// # Panics
    ///
    /// If the pipeline mutex is poisoned by a concurrent panic.
    #[must_use]
    pub fn node_names_snapshot(&self) -> HashMap<NodeId, String> {
        let g = self.inner.lock().unwrap();
        g.node_names.clone()
    }

    /// Run `f` inside a named scope, returning whatever the closure returns.
    ///
    /// While the closure is executing, the supplied `name` is pushed onto an
    /// internal scope stack. This has two effects:
    ///
    /// 1. Every node inserted into the graph during the closure is
    ///    automatically labeled `"<path>/<counter>"`, where `<path>` is the
    ///    `"/"`-joined names of all active scope frames and `<counter>` is
    ///    a per-frame monotonic counter (starting at `0` for each frame).
    ///    Per-frame counters mean nested scopes do not perturb each other —
    ///    the outer frame's counter only advances for nodes inserted
    ///    directly inside the outer scope, never for inner-scope inserts.
    /// 2. [`PCollection::with_name`](crate::PCollection::with_name) calls
    ///    inside the closure produce
    ///    `"<path>/<user-supplied>"`, replacing the auto-generated label
    ///    with the user's identifier while preserving the scope hierarchy.
    ///
    /// `named_scope` is the Ironbeam analogue of Beam's "composite
    /// `PTransform`": it gives a sub-graph a logical name that external
    /// runners and the local `explain` view can display as a single
    /// hierarchical path.
    ///
    /// The scope is popped via a [`Drop`] guard, so it is **panic-safe**:
    /// if `f` panics, the scope is still popped before the panic
    /// propagates and the pipeline is left in a consistent state.
    ///
    /// # Example
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let counts = p.named_scope("WordCount", |p| {
    ///     from_vec(p, vec!["a b c".to_string()])
    ///         .flat_map(|s: &String| s.split_whitespace().map(String::from).collect())
    ///         .with_name("Split")               // -> "WordCount/Split"
    ///         .key_by(|w: &String| w.clone())
    ///         .map_values(|_| 1u64)
    ///         .combine_values(Sum::<u64>::default())
    ///         .with_name("Sum")                 // -> "WordCount/Sum"
    /// });
    /// assert_eq!(p.node_name(counts.node_id()).as_deref(), Some("WordCount/Sum"));
    /// # Ok(()) }
    /// ```
    ///
    /// # Panics
    ///
    /// If the pipeline mutex is poisoned by a concurrent panic.
    pub fn named_scope<R, F>(&self, name: impl Into<String>, f: F) -> R
    where
        F: FnOnce(&Self) -> R,
    {
        struct ScopeGuard<'a>(&'a Pipeline);
        impl Drop for ScopeGuard<'_> {
            fn drop(&mut self) {
                let mut g = self
                    .0
                    .inner
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                g.scope_stack.pop();
            }
        }

        {
            let mut g = self.inner.lock().unwrap();
            g.scope_stack.push(ScopeFrame {
                name: name.into(),
                counter: 0,
            });
        }
        let _guard = ScopeGuard(self);
        f(self)
    }

    /// Prepend the active [`named_scope`](Self::named_scope) path (if any)
    /// to `name`.
    ///
    /// Used internally by
    /// [`PCollection::with_name`](crate::PCollection::with_name) so the
    /// fluent setter composes correctly with scopes:
    /// `with_name("Filter")` inside `p.named_scope("WordCount", …)` yields
    /// `"WordCount/Filter"`. Outside any scope it returns `name` unchanged.
    pub(crate) fn qualify_with_scope(&self, name: impl Into<String>) -> String {
        let name = name.into();
        let g = self.inner.lock().unwrap();
        if g.scope_stack.is_empty() {
            return name;
        }
        let path = g
            .scope_stack
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>()
            .join("/");
        drop(g);
        format!("{path}/{name}")
    }

    /// Set the metrics collector for this pipeline.
    ///
    /// This enables collecting metrics during pipeline execution. Metrics can be
    /// retrieved after execution using [`take_metrics`](Self::take_metrics).
    ///
    /// # Panics
    ///
    /// If the pipeline is in an inconsistent state, such as during concurrent modifications.
    #[cfg(feature = "metrics")]
    pub fn set_metrics(&self, metrics: MetricsCollector) {
        let mut g = self.inner.lock().unwrap();
        g.metrics = Some(metrics);
    }

    /// Take the metrics collector from this pipeline, leaving `None` in its place.
    ///
    /// This is typically called after pipeline execution to retrieve and report metrics.
    ///
    /// # Panics
    ///
    /// If the pipeline is in an inconsistent state, such as during concurrent modifications.
    #[cfg(feature = "metrics")]
    #[must_use]
    pub fn take_metrics(&self) -> Option<MetricsCollector> {
        let mut g = self.inner.lock().unwrap();
        g.metrics.take()
    }

    /// Get a clone of the metrics collector, if present.
    ///
    /// # Panics
    ///
    /// If the pipeline is in an inconsistent state, such as during concurrent modifications.
    #[cfg(feature = "metrics")]
    #[must_use]
    pub fn get_metrics(&self) -> Option<MetricsCollector> {
        let g = self.inner.lock().unwrap();
        g.metrics.clone()
    }

    /// Record the start of pipeline execution in metrics.
    ///
    /// # Panics
    ///
    /// If the pipeline is in an inconsistent state, such as during concurrent modifications.
    #[cfg(feature = "metrics")]
    pub fn record_metrics_start(&self) {
        let g = self.inner.lock().unwrap();
        if let Some(ref metrics) = g.metrics {
            metrics.record_start();
        }
    }

    /// Record the end of pipeline execution in metrics.
    ///
    /// # Panics
    ///
    /// If the pipeline is in an inconsistent state, such as during concurrent modifications.
    #[cfg(feature = "metrics")]
    pub fn record_metrics_end(&self) {
        let g = self.inner.lock().unwrap();
        if let Some(ref metrics) = g.metrics {
            metrics.record_end();
        }
    }
}
