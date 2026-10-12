use super::{CloudResult, HashMap};

// ============================================================================
// Credential and Configuration Traits
// ============================================================================

/// Trait for cloud service credentials
pub trait CloudCredentials: Send + Sync {
    /// Returns a unique identifier for this credential (e.g., access key ID, service account email)
    fn identifier(&self) -> &str;

    /// Returns the credential type (e.g., `"api_key"`, `"oauth2"`, `"iam_role"`)
    fn credential_type(&self) -> &str;

    /// Validates the credentials (returns Ok if valid)
    ///
    /// # Errors
    ///
    /// Returns an error if the credentials are invalid, expired, or cannot be validated
    fn validate(&self) -> CloudResult<()>;

    /// Returns any additional metadata about the credentials
    fn metadata(&self) -> HashMap<String, String> {
        HashMap::new()
    }
}

/// Trait for cloud service configuration
pub trait CloudConfig: Send + Sync {
    /// Returns the region or location for the service
    fn region(&self) -> Option<&str> {
        None
    }

    /// Returns the endpoint URL (if a custom endpoint is used)
    fn endpoint(&self) -> Option<&str> {
        None
    }

    /// Returns the timeout in seconds for operations
    fn timeout_secs(&self) -> u64 {
        30
    }

    /// Returns the maximum number of retry attempts
    fn max_retries(&self) -> u32 {
        3
    }

    /// Returns additional provider-specific configuration
    fn extra(&self) -> HashMap<String, String> {
        HashMap::new()
    }
}
