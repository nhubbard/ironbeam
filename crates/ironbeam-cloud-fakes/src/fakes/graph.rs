use super::*;

// ============================================================================
// FakeGraphIO
// ============================================================================

#[derive(Clone)]
pub struct FakeGraphIO {
    nodes: Arc<Mutex<HashMap<String, GraphNode>>>,
    edges: Arc<Mutex<HashMap<String, GraphEdge>>>,
    node_counter: Arc<Mutex<u64>>,
    edge_counter: Arc<Mutex<u64>>,
}

impl FakeGraphIO {
    #[must_use]
    /// Create an empty in-memory graph service for tests.
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(Mutex::new(HashMap::new())),
            edges: Arc::new(Mutex::new(HashMap::new())),
            node_counter: Arc::new(Mutex::new(0)),
            edge_counter: Arc::new(Mutex::new(0)),
        }
    }

    fn next_node_id(&self) -> String {
        let mut counter = self
            .node_counter
            .lock()
            .expect("node_counter mutex poisoned");
        *counter += 1;
        let id = *counter;
        drop(counter);
        format!("node-{id}")
    }

    fn next_edge_id(&self) -> String {
        let mut counter = self
            .edge_counter
            .lock()
            .expect("edge_counter mutex poisoned");
        *counter += 1;
        let id = *counter;
        drop(counter);
        format!("edge-{id}")
    }
}

impl Default for FakeGraphIO {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphIO for FakeGraphIO {
    fn add_node(
        &self,
        labels: Vec<String>,
        properties: HashMap<String, String>,
    ) -> CloudResult<String> {
        let id = self.next_node_id();
        let node = GraphNode {
            id: id.clone(),
            labels,
            properties,
        };
        self.nodes
            .lock()
            .expect("nodes mutex poisoned")
            .insert(id.clone(), node);
        Ok(id)
    }

    fn get_node(&self, id: &str) -> CloudResult<Option<GraphNode>> {
        Ok(self
            .nodes
            .lock()
            .expect("nodes mutex poisoned")
            .get(id)
            .cloned())
    }

    fn update_node(&self, id: &str, properties: HashMap<String, String>) -> CloudResult<()> {
        let mut nodes = self.nodes.lock().expect("nodes mutex poisoned");
        if let Some(node) = nodes.get_mut(id) {
            node.properties.extend(properties);
            Ok(())
        } else {
            Err(CloudIOError::new(
                ErrorKind::NotFound,
                format!("Node {id} not found"),
            ))
        }
    }

    fn delete_node(&self, id: &str) -> CloudResult<()> {
        self.nodes.lock().expect("nodes mutex poisoned").remove(id);
        Ok(())
    }

    fn add_edge(
        &self,
        from: &str,
        to: &str,
        label: &str,
        properties: HashMap<String, String>,
    ) -> CloudResult<String> {
        let id = self.next_edge_id();
        let edge = GraphEdge {
            id: id.clone(),
            label: label.to_string(),
            from_node: from.to_string(),
            to_node: to.to_string(),
            properties,
        };
        self.edges
            .lock()
            .expect("edges mutex poisoned")
            .insert(id.clone(), edge);
        Ok(id)
    }

    fn get_edge(&self, id: &str) -> CloudResult<Option<GraphEdge>> {
        Ok(self
            .edges
            .lock()
            .expect("edges mutex poisoned")
            .get(id)
            .cloned())
    }

    fn delete_edge(&self, id: &str) -> CloudResult<()> {
        self.edges.lock().expect("edges mutex poisoned").remove(id);
        Ok(())
    }

    fn query(
        &self,
        _query: &str,
        _params: HashMap<String, String>,
    ) -> CloudResult<Vec<HashMap<String, String>>> {
        // Simple fake - return empty results
        Ok(Vec::new())
    }

    fn get_neighbors(
        &self,
        node_id: &str,
        direction: EdgeDirection,
    ) -> CloudResult<Vec<GraphNode>> {
        let edges = self.edges.lock().expect("edges mutex poisoned");

        let neighbor_ids: Vec<String> = edges
            .values()
            .filter_map(|edge| match direction {
                EdgeDirection::Outgoing => {
                    if edge.from_node == node_id {
                        Some(edge.to_node.clone())
                    } else {
                        None
                    }
                }
                EdgeDirection::Incoming => {
                    if edge.to_node == node_id {
                        Some(edge.from_node.clone())
                    } else {
                        None
                    }
                }
                EdgeDirection::Both => {
                    if edge.from_node == node_id {
                        Some(edge.to_node.clone())
                    } else if edge.to_node == node_id {
                        Some(edge.from_node.clone())
                    } else {
                        None
                    }
                }
            })
            .collect();

        drop(edges);

        let nodes = self.nodes.lock().expect("nodes mutex poisoned");
        Ok(neighbor_ids
            .iter()
            .filter_map(|id| nodes.get(id).cloned())
            .collect())
    }
}
