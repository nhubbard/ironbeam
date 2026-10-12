use super::{CloudResult, HashMap};

// ============================================================================
// GraphIO - Graph Databases
// ============================================================================

/// A node in a graph
#[derive(Debug, Clone)]
pub struct GraphNode {
    pub id: String,
    pub labels: Vec<String>,
    pub properties: HashMap<String, String>,
}

/// An edge in a graph
#[derive(Debug, Clone)]
pub struct GraphEdge {
    pub id: String,
    pub label: String,
    pub from_node: String,
    pub to_node: String,
    pub properties: HashMap<String, String>,
}

/// Trait for graph database operations
pub trait GraphIO: Send + Sync {
    /// Add a node to the graph
    ///
    /// # Errors
    ///
    /// Returns an error if permissions are not enough or the operation fails
    fn add_node(
        &self,
        labels: Vec<String>,
        properties: HashMap<String, String>,
    ) -> CloudResult<String>;

    /// Get a node by ID
    ///
    /// # Errors
    ///
    /// Returns an error if permissions are not enough or the operation fails
    fn get_node(&self, id: &str) -> CloudResult<Option<GraphNode>>;

    /// Update node properties
    ///
    /// # Errors
    ///
    /// Returns an error if the node doesn't exist, permissions are not enough, or the update fails
    fn update_node(&self, id: &str, properties: HashMap<String, String>) -> CloudResult<()>;

    /// Delete a node
    ///
    /// # Errors
    ///
    /// Returns an error if the node doesn't exist, permissions are not enough, or the deletion fails
    fn delete_node(&self, id: &str) -> CloudResult<()>;

    /// Add an edge between nodes
    ///
    /// # Errors
    ///
    /// Returns an error if either node doesn't exist, permissions are not enough, or the operation fails
    fn add_edge(
        &self,
        from: &str,
        to: &str,
        label: &str,
        properties: HashMap<String, String>,
    ) -> CloudResult<String>;

    /// Get an edge by ID
    ///
    /// # Errors
    ///
    /// Returns an error if permissions are not enough or the operation fails
    fn get_edge(&self, id: &str) -> CloudResult<Option<GraphEdge>>;

    /// Delete an edge
    ///
    /// # Errors
    ///
    /// Returns an error if the edge doesn't exist, permissions are not enough, or the deletion fails
    fn delete_edge(&self, id: &str) -> CloudResult<()>;

    /// Query the graph (using provider-specific query language)
    ///
    /// # Errors
    ///
    /// Returns an error if the query is invalid, execution fails, or there's a connection issue
    fn query(
        &self,
        query: &str,
        params: HashMap<String, String>,
    ) -> CloudResult<Vec<HashMap<String, String>>>;

    /// Find neighbors of a node
    ///
    /// # Errors
    ///
    /// Returns an error if the node doesn't exist, permissions are not enough, or the operation fails
    fn get_neighbors(&self, node_id: &str, direction: EdgeDirection)
    -> CloudResult<Vec<GraphNode>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeDirection {
    Outgoing,
    Incoming,
    Both,
}
