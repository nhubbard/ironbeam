//! Lightweight unique identifier for nodes within a [`Pipeline`](crate::pipeline::Pipeline).
//!
//! Each [`Node`](crate::node::Node) inserted into the pipeline graph is assigned
//! a sequential `NodeId`. These are opaque handles—only the planner and runner
//! inspect them directly.
//!
//! They’re small, `Copy`, and hashable, so they can be used efficiently as keys
//! in maps or sets when snapshotting or traversing the plan.

/// Unique numeric identifier for a node in a pipeline graph.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct NodeId(u64);

impl NodeId {
    /// Create a new `NodeId` (used internally by the pipeline).
    pub(crate) fn new(v: u64) -> Self {
        Self(v)
    }

    /// Return the underlying numeric value.
    ///
    /// Useful mainly for debugging or serialization.
    pub fn raw(&self) -> u64 {
        self.0
    }
}
