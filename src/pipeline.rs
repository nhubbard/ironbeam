use crate::node::Node;
use crate::NodeId;
use anyhow::{bail, Result};
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
    pub(crate) fn insert_node(&self, node: Node) -> NodeId {
        let mut g = self.inner.lock().unwrap();
        let id = NodeId::new(g.next_id);
        g.next_id += 1;
        g.nodes.insert(id, node);
        id
    }
    pub(crate) fn connect(&self, from: NodeId, to: NodeId) {
        self.inner.lock().unwrap().edges.push((from, to));
    }
    pub(crate) fn snapshot(&self) -> (HashMap<NodeId, Node>, Vec<(NodeId, NodeId)>) {
        let g = self.inner.lock().unwrap();
        (g.nodes.clone(), g.edges.clone())
    }

    #[allow(dead_code)]
    pub(crate) fn read_vec<T: 'static + Send + Sync + Clone>(&self, id: NodeId) -> Result<Vec<T>> {
        let g = self.inner.lock().unwrap();
        let Some(Node::Materialized(p)) = g.nodes.get(&id) else {
            bail!("not materialized");
        };
        Ok(p.downcast_ref::<Vec<T>>().unwrap().clone())
    }
    #[allow(dead_code)]
    pub(crate) fn write_materialized<T: 'static + Send + Sync>(&self, data: Vec<T>) -> NodeId {
        self.insert_node(Node::Materialized(Arc::new(data)))
    }
}
