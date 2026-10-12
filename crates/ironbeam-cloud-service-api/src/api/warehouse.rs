use super::{CloudResult, HashMap};

// ============================================================================
// WarehouseIO - Analytical Databases
// ============================================================================

/// Query result from a warehouse
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub row_count: usize,
}

/// Trait for analytical data warehouse operations
pub trait WarehouseIO: Send + Sync {
    /// Execute a query and return results
    ///
    /// # Errors
    ///
    /// Returns an error if the query is invalid, execution fails, or there's a connection issue
    fn query(&self, sql: &str) -> CloudResult<QueryResult>;

    /// Execute a query without returning results (for DDL/DML)
    ///
    /// # Errors
    ///
    /// Returns an error if the query is invalid, execution fails, or there's a connection issue
    fn execute(&self, sql: &str) -> CloudResult<()>;

    /// Load data from a source (e.g., cloud storage) into a table
    ///
    /// # Errors
    ///
    /// Returns an error if the source is inaccessible, the table doesn't exist, or the data format is invalid
    fn load_data(
        &self,
        table: &str,
        source_uri: &str,
        options: HashMap<String, String>,
    ) -> CloudResult<()>;

    /// Export data from a query to a destination
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails, the destination is inaccessible, or permissions are not enough
    fn export_data(
        &self,
        sql: &str,
        destination_uri: &str,
        options: HashMap<String, String>,
    ) -> CloudResult<()>;

    /// Check if a table exists
    ///
    /// # Errors
    ///
    /// Returns an error if there's a connection issue or insufficient permissions
    fn table_exists(&self, table: &str) -> CloudResult<bool>;

    /// Get table schema
    ///
    /// # Errors
    ///
    /// Returns an error if the table doesn't exist, there's a connection issue, or permissions are not enough
    fn get_schema(&self, table: &str) -> CloudResult<Vec<(String, String)>>;
}
