use super::{CloudResult, HashMap};

// ============================================================================
// KeyValueIO - Document/NoSQL Databases
// ============================================================================

/// A document in a key-value store
#[derive(Debug, Clone)]
pub struct Document {
    pub key: String,
    pub data: HashMap<String, String>,
    pub version: Option<String>, // For optimistic locking
}

/// Trait for key-value/document store operations
pub trait KeyValueIO: Send + Sync {
    /// Put a document
    ///
    /// # Errors
    ///
    /// Returns an error if the collection doesn't exist, permissions are not enough, or the operation fails
    fn put(&self, collection: &str, key: &str, data: HashMap<String, String>) -> CloudResult<()>;

    /// Get a document
    ///
    /// # Errors
    ///
    /// Returns an error if the collection doesn't exist, permissions are not enough, or the operation fails
    fn get(&self, collection: &str, key: &str) -> CloudResult<Option<Document>>;

    /// Delete a document
    ///
    /// # Errors
    ///
    /// Returns an error if the collection doesn't exist, permissions are not enough, or the deletion fails
    fn delete(&self, collection: &str, key: &str) -> CloudResult<()>;

    /// Query documents (simple key-value filtering)
    ///
    /// # Errors
    ///
    /// Returns an error if the collection doesn't exist, permissions are not enough, or the query fails
    fn query(
        &self,
        collection: &str,
        filters: HashMap<String, String>,
    ) -> CloudResult<Vec<Document>>;

    /// Get multiple documents in a batch
    ///
    /// # Errors
    ///
    /// Returns an error if the collection doesn't exist, permissions are not enough, or the operation fails
    fn batch_get(&self, collection: &str, keys: Vec<String>) -> CloudResult<Vec<Option<Document>>>;

    /// Batch put multiple documents
    ///
    /// # Errors
    ///
    /// Returns an error if the collection doesn't exist, permissions are not enough, or the operation fails
    fn batch_put(
        &self,
        collection: &str,
        documents: Vec<(String, HashMap<String, String>)>,
    ) -> CloudResult<()>;

    /// Check if a document exists
    ///
    /// # Errors
    ///
    /// Returns an error if the collection doesn't exist, permissions are not enough, or the check fails
    fn exists(&self, collection: &str, key: &str) -> CloudResult<bool>;
}
