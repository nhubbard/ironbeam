//! In-memory representation of a dataflow pipeline graph.
//!
//! The [`Pipeline`] acts as the central registry for all execution nodes
//! ([`Node`](Node)) and their directed connections. It is lightweight,
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

use crate::node::Node;
use crate::NodeId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Thread-safe pipeline graph structure holding all nodes and edges.
///
/// Each pipeline is essentially a shared, mutable graph:
/// ```text
///  Source → Stateless → GroupByKey → CombineValues → ...
/// ```
///
/// The `Pipeline` itself is cheaply cloneable; all clones share the same
/// underlying [`PipelineInner`].
pub struct Pipeline {
    /// Shared reference to the internal graph data.
    pub(crate) inner: Arc<Mutex<PipelineInner>>,
}

/// Inner mutable graph state for a [`Pipeline`].
///
/// This struct tracks:
/// - `next_id`: incremental counter for node IDs.
/// - `nodes`: map of [`NodeId`] → [`Node`](Node).
/// - `edges`: ordered list of `(from, to)` directed edges.
///
/// Access to this data is synchronized by the parent [`Pipeline`].
pub(crate) struct PipelineInner {
    pub next_id: u64,
    pub nodes: HashMap<NodeId, Node>,
    pub edges: Vec<(NodeId, NodeId)>,
}

impl Default for Pipeline {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(PipelineInner {
                next_id: 0,
                nodes: HashMap::new(),
                edges: vec![],
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

    /// Connect two nodes by their IDs, forming a directed edge `(from → to)`.
    ///
    /// Used to chain together consecutive transforms within the same pipeline.
    pub(crate) fn connect(&self, from: NodeId, to: NodeId) {
        self.inner.lock().unwrap().edges.push((from, to));
    }

    /// Return a **snapshot** of the current pipeline graph (nodes and edges).
    ///
    /// This is a deep clone of all node and edge data, used by the planner and runner
    /// to analyze or execute the pipeline without mutating the original.
    pub(crate) fn snapshot(&self) -> (HashMap<NodeId, Node>, Vec<(NodeId, NodeId)>) {
        let g = self.inner.lock().unwrap();
        (g.nodes.clone(), g.edges.clone())
    }
}