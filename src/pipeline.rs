use crate::{Node, NodeId};
use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct Pipeline {
    pub(crate) inner: Arc<Mutex<PipelineInner>>,
}

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
                edges: Vec::new(),
            })),
        }
    }
}

impl Clone for Pipeline {
    fn clone(&self) -> Self { Self { inner: Arc::clone(&self.inner) } }
}

impl Pipeline {
    pub(crate) fn insert_node(&self, node: Node) -> NodeId {
        let mut g = self.inner.lock().unwrap();
        let id = NodeId::new(g.next_id); g.next_id += 1;
        g.nodes.insert(id, node);
        id
    }

    pub(crate) fn connect(&self, from: NodeId, to: NodeId) {
        self.inner.lock().unwrap().edges.push((from, to));
    }

    //noinspection RsUnwrap
    pub(crate) fn read_vec<T: 'static + Send + Sync + Clone>(&self, id: NodeId) -> Result<Vec<T>> {
        let g = self.inner.lock().unwrap();
        let Some(Node::Materialized(payload)) = g.nodes.get(&id) else {
            bail!("node {id:?} not found or not materialized");
        };
        payload
            .downcast_ref::<Vec<T>>()
            .cloned()
            .with_context(|| format!("type mismatch reading node {:?}", id))
    }

    pub(crate) fn write_materialized<T: 'static + Send + Sync>(&self, data: Vec<T>) -> NodeId {
        self.insert_node(Node::Materialized(Arc::new(data)))
    }
    
    pub(crate) fn add_source<T: 'static + Send + Sync>(&self, data: Vec<T>) -> NodeId {
        self.insert_node(Node::Source(Arc::new(data)))
    }
}