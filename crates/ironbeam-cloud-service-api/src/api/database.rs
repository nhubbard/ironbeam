use super::{CloudResult, HashMap};

// ============================================================================
// DatabaseIO - Relational Databases
// ============================================================================

/// A row from a database query
pub type Row = HashMap<String, String>;

/// Trait for relational database operations
pub trait DatabaseIO: Send + Sync {
    /// Execute a query and return results
    ///
    /// # Errors
    ///
    /// Returns an error if the query is invalid, execution fails, or there's a connection issue
    fn query(&self, sql: &str, params: Vec<String>) -> CloudResult<Vec<Row>>;

    /// Execute a statement without returning results (INSERT, UPDATE, DELETE)
    ///
    /// # Errors
    ///
    /// Returns an error if the statement is invalid, execution fails, or there's a connection issue
    fn execute(&self, sql: &str, params: Vec<String>) -> CloudResult<u64>;

    /// Begin a transaction
    ///
    /// # Errors
    ///
    /// Returns an error if transaction creation fails or there's a connection issue
    fn begin_transaction(&self) -> CloudResult<Box<dyn Transaction>>;

    /// Check if a table exists
    ///
    /// # Errors
    ///
    /// Returns an error if there's a connection issue or permissions are not enough
    fn table_exists(&self, table: &str) -> CloudResult<bool>;

    /// Get table schema
    ///
    /// # Errors
    ///
    /// Returns an error if the table doesn't exist, there's a connection issue, or permissions are not enough
    fn get_schema(&self, table: &str) -> CloudResult<Vec<(String, String)>>;
}

/// Trait for database transactions
pub trait Transaction: Send + Sync {
    /// Execute a query within the transaction
    ///
    /// # Errors
    ///
    /// Returns an error if the query is invalid, execution fails, or the transaction has been rolled back
    fn query(&mut self, sql: &str, params: Vec<String>) -> CloudResult<Vec<Row>>;

    /// Execute a statement within the transaction
    ///
    /// # Errors
    ///
    /// Returns an error if the statement is invalid, execution fails, or the transaction has been rolled back
    fn execute(&mut self, sql: &str, params: Vec<String>) -> CloudResult<u64>;

    /// Commit the transaction
    ///
    /// # Errors
    ///
    /// Returns an error if the commit fails or there's a connection issue
    fn commit(self: Box<Self>) -> CloudResult<()>;

    /// Roll back the transaction
    ///
    /// # Errors
    ///
    /// Returns an error if the rollback fails or there's a connection issue
    fn rollback(self: Box<Self>) -> CloudResult<()>;
}
