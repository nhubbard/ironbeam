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

#[cfg(feature = "serde-coders")]
use crate::coders::{BincodeCoder, BincodeKvCoder, ElementCoder};
#[cfg(feature = "serde-coders")]
use crate::collection::RFBound;

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
/// - `metrics`: optional metrics collector for tracking execution statistics.
///
/// The parent synchronizes access to the data in the [`Pipeline`].
pub(crate) struct PipelineInner {
    pub next_id: u64,
    pub nodes: HashMap<NodeId, Node>,
    pub edges: Vec<(NodeId, NodeId)>,
    /// Per-node element coder, keyed by output [`NodeId`]. Populated by the
    /// combinators when `serde-coders` is on; consumed by wire backends via
    /// [`Pipeline::snapshot_coders`].
    #[cfg(feature = "serde-coders")]
    pub coders: HashMap<NodeId, Arc<dyn ElementCoder>>,
    #[cfg(feature = "metrics")]
    pub metrics: Option<MetricsCollector>,
}

impl Default for Pipeline {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(PipelineInner {
                next_id: 0,
                nodes: HashMap::new(),
                edges: vec![],
                #[cfg(feature = "serde-coders")]
                coders: HashMap::new(),
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
    pub(crate) fn insert_node(&self, node: Node) -> NodeId {
        let mut g = self.inner.lock().unwrap();
        let id = NodeId::new(g.next_id);
        g.next_id += 1;
        g.nodes.insert(id, node);
        id
    }

    /// Connect two nodes by their IDs, forming a directed edge `(from -> to)`.
    ///
    /// Used to chain together consecutive transforms within the same pipeline.
    pub(crate) fn connect(&self, from: NodeId, to: NodeId) {
        self.inner.lock().unwrap().edges.push((from, to));
    }

    /// Attach the default bincode coder for output type `T` to `id`.
    ///
    /// Combinators call this unconditionally right after `insert_node`; without
    /// the `serde-coders` feature it compiles to a no-op so the call sites stay
    /// feature-agnostic.
    #[cfg(feature = "serde-coders")]
    pub(crate) fn set_coder<T: RFBound>(&self, id: NodeId) {
        self.inner
            .lock()
            .unwrap()
            .coders
            .insert(id, Arc::new(BincodeCoder::<T>::new()));
    }

    #[cfg(not(feature = "serde-coders"))]
    pub(crate) fn set_coder<T>(&self, _id: NodeId) {}

    /// Upgrade `id` to a KV-aware coder. Called by `group_by_key` on its
    /// predecessor so the pre-GBK edge can emit each `(K, V)` as two
    /// independently length-prefixed bincode halves (Beam's `kv<lp, lp>`).
    #[cfg(feature = "serde-coders")]
    pub(crate) fn set_kv_coder<K: RFBound, V: RFBound>(&self, id: NodeId) {
        self.inner
            .lock()
            .unwrap()
            .coders
            .insert(id, Arc::new(BincodeKvCoder::<K, V>::new()));
    }

    #[cfg(not(feature = "serde-coders"))]
    pub(crate) fn set_kv_coder<K, V>(&self, _id: NodeId) {}

    /// Override the coder attached to `id` with a hand-built one. Escape hatch
    /// for custom `DynOp`s whose output partition is not the declared `Vec<O>`,
    /// or for non-default wire coders.
    #[cfg(feature = "serde-coders")]
    pub fn set_coder_override(&self, id: NodeId, coder: Arc<dyn ElementCoder>) {
        self.inner.lock().unwrap().coders.insert(id, coder);
    }

    /// Snapshot the per-node coder map. A deep clone (the coders are `Arc`),
    /// taken alongside [`snapshot`](Self::snapshot) by wire backends.
    ///
    /// # Panics
    /// If the pipeline lock is poisoned by a panicking concurrent builder.
    #[cfg(feature = "serde-coders")]
    #[must_use]
    pub fn snapshot_coders(&self) -> HashMap<NodeId, Arc<dyn ElementCoder>> {
        self.inner.lock().unwrap().coders.clone()
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

    /// Set the metrics collector for this pipeline.
    ///
    /// This enables metrics collection during pipeline execution. Metrics can be
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
