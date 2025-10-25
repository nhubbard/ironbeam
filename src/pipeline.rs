use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use anyhow::{bail, Context};
use crate::node::Node;
use crate::node_id::NodeId;

/// -------- Pipeline + nodes --------
/// We keep a tiny graph so the public API doesn't change later.
pub struct Pipeline {
    pub(crate) inner: Arc<Mutex<PipelineInner>>,
}

pub struct PipelineInner {
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
                edges: Vec::new(),
            })),
        }
    }
}

/// Allow `Pipeline` cloning.
impl Clone for Pipeline {
    fn clone(&self) -> Self {
        Pipeline { inner: Arc::clone(&self.inner) }
    }
}

impl Pipeline {
    pub(crate) fn insert_node(&self, node: Node) -> NodeId {
        let mut g = self.inner.lock().unwrap();
        let id = NodeId::new(g.next_id);
        g.next_id += 1;
        g.nodes.insert(id, node);
        id
    }

    pub(crate) fn connect(&self, from: NodeId, to: NodeId) {
        let mut g = self.inner.lock().unwrap();
        g.edges.push((from, to));
    }

    //noinspection RsUnwrap
    pub(crate) fn read_vec<T: 'static + Send + Sync + Clone>(&self, id: NodeId) -> anyhow::Result<Vec<T>> {
        let g = self.inner.lock().unwrap();
        let Some(Node::Materialized(payload)) = g.nodes.get(&id) else {
            bail!("node {id:?} not found");
        };
        // Downcast safely; don't use expect—propagate a nice error instead.
        payload
            .downcast_ref::<Vec<T>>()
            .cloned()
            .with_context(|| format!("type mismatch reading node {:?}", id))
    }

    pub(crate) fn write_vec<T: 'static + Send + Sync>(&self, data: Vec<T>) -> NodeId {
        self.insert_node(Node::Materialized(Box::new(data)))
    }
}