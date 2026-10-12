use super::*;

// ============================================================================
// CacheIO - In-Memory Caching
// ============================================================================

/// Trait for cache operations
pub trait CacheIO: Send + Sync {
    /// Get a value from the cache
    ///
    /// # Errors
    ///
    /// Returns an error if there's a connection issue or the operation fails
    fn get(&self, key: &str) -> CloudResult<Option<Vec<u8>>>;

    /// Set a value in the cache with optional TTL in seconds
    ///
    /// # Errors
    ///
    /// Returns an error if there's a connection issue or the operation fails
    fn set(&self, key: &str, value: &[u8], ttl_secs: Option<u64>) -> CloudResult<()>;

    /// Delete a value from the cache
    ///
    /// # Errors
    ///
    /// Returns an error if there's a connection issue or the deletion fails
    fn delete(&self, key: &str) -> CloudResult<()>;

    /// Check if a key exists
    ///
    /// # Errors
    ///
    /// Returns an error if there's a connection issue or the check fails
    fn exists(&self, key: &str) -> CloudResult<bool>;

    /// Get multiple values
    ///
    /// # Errors
    ///
    /// Returns an error if there's a connection issue or the operation fails
    fn get_batch(&self, keys: Vec<String>) -> CloudResult<Vec<Option<Vec<u8>>>>;

    /// Set multiple values
    ///
    /// # Errors
    ///
    /// Returns an error if there's a connection issue or the operation fails
    fn set_batch(&self, items: Vec<(String, Vec<u8>, Option<u64>)>) -> CloudResult<()>;

    /// Increment a counter
    ///
    /// # Errors
    ///
    /// Returns an error if the key doesn't exist, isn't a numeric value, or there's a connection issue
    fn increment(&self, key: &str, delta: i64) -> CloudResult<i64>;

    /// Flush all data (use with caution!)
    ///
    /// # Errors
    ///
    /// Returns an error if there's a connection issue or permissions are not enough
    fn flush(&self) -> CloudResult<()>;
}
