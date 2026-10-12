use super::{CloudResult, HashMap};

// ============================================================================
// SearchIO - Search and Log Services
// ============================================================================

/// A search result hit
#[derive(Debug, Clone)]
pub struct SearchHit {
    pub id: String,
    pub score: f64,
    pub fields: HashMap<String, String>,
}

/// Search query parameters
#[derive(Debug, Clone)]
pub struct SearchQuery {
    pub query: String,
    pub filters: HashMap<String, String>,
    pub limit: u32,
    pub offset: u32,
}

/// Trait for search and log operations
pub trait SearchIO: Send + Sync {
    /// Index a document
    ///
    /// # Errors
    ///
    /// Returns an error if the index doesn't exist, permissions are not enough, or indexing fails
    fn index(&self, index: &str, id: &str, document: HashMap<String, String>) -> CloudResult<()>;

    /// Batch index documents
    ///
    /// # Errors
    ///
    /// Returns an error if the index doesn't exist, permissions are not enough, or indexing fails
    fn batch_index(
        &self,
        index: &str,
        documents: Vec<(String, HashMap<String, String>)>,
    ) -> CloudResult<()>;

    /// Search documents
    ///
    /// # Errors
    ///
    /// Returns an error if the index doesn't exist, the query is invalid, or search fails
    fn search(&self, index: &str, query: SearchQuery) -> CloudResult<Vec<SearchHit>>;

    /// Delete a document from the index
    ///
    /// # Errors
    ///
    /// Returns an error if the index doesn't exist, permissions are not enough, or deletion fails
    fn delete(&self, index: &str, id: &str) -> CloudResult<()>;

    /// Get a document by ID
    ///
    /// # Errors
    ///
    /// Returns an error if the index doesn't exist, permissions are not enough, or the operation fails
    fn get(&self, index: &str, id: &str) -> CloudResult<Option<HashMap<String, String>>>;

    /// Check if an index exists
    ///
    /// # Errors
    ///
    /// Returns an error if there's a connection issue or permissions are not enough
    fn index_exists(&self, index: &str) -> CloudResult<bool>;
}
