use super::CloudResult;

// ============================================================================
// ConfigIO - Configuration Services
// ============================================================================

/// A configuration value
#[derive(Debug, Clone)]
pub struct ConfigValue {
    pub key: String,
    pub value: String,
    pub version: Option<String>,
    pub is_secret: bool,
}

/// Trait for configuration service operations
pub trait ConfigIO: Send + Sync {
    /// Get a configuration value
    ///
    /// # Errors
    ///
    /// Returns an error if the key doesn't exist, permissions are not enough, or the operation fails
    fn get(&self, key: &str) -> CloudResult<ConfigValue>;

    /// Set a configuration value
    ///
    /// # Errors
    ///
    /// Returns an error if permissions are not enough, the value is invalid, or the operation fails
    fn set(&self, key: &str, value: &str, is_secret: bool) -> CloudResult<()>;

    /// Delete a configuration value
    ///
    /// # Errors
    ///
    /// Returns an error if the key doesn't exist, permissions are not enough, or the deletion fails
    fn delete(&self, key: &str) -> CloudResult<()>;

    /// List all configuration keys with a prefix
    ///
    /// # Errors
    ///
    /// Returns an error if permissions are not enough or the listing fails
    fn list(&self, prefix: Option<&str>) -> CloudResult<Vec<String>>;

    /// Get multiple configuration values
    ///
    /// # Errors
    ///
    /// Returns an error if permissions are not enough or the operation fails
    fn batch_get(&self, keys: Vec<String>) -> CloudResult<Vec<Option<ConfigValue>>>;
}
