use super::*;

// ============================================================================
// ComputeIO - Serverless Compute
// ============================================================================

/// Result of a compute invocation
#[derive(Debug, Clone)]
pub struct ComputeResult {
    pub status_code: u16,
    pub output: Vec<u8>,
    pub logs: Option<String>,
    pub execution_time_ms: u64,
}

/// Trait for serverless compute operations
pub trait ComputeIO: Send + Sync {
    /// Invoke a function synchronously
    ///
    /// # Errors
    ///
    /// Returns an error if the function doesn't exist, permissions are not enough, or invocation fails
    fn invoke(&self, function_name: &str, payload: &[u8]) -> CloudResult<ComputeResult>;

    /// Invoke a function asynchronously (fire and forget)
    ///
    /// # Errors
    ///
    /// Returns an error if the function doesn't exist, permissions are not enough, or invocation fails
    fn invoke_async(&self, function_name: &str, payload: &[u8]) -> CloudResult<String>;

    /// Get the status of an async invocation
    ///
    /// # Errors
    ///
    /// Returns an error if the invocation ID is invalid, permissions are not enough, or the operation fails
    fn get_invocation_status(&self, invocation_id: &str) -> CloudResult<InvocationStatus>;

    /// List the available functions
    ///
    /// # Errors
    ///
    /// Returns an error if permissions are not enough or the listing fails
    fn list_functions(&self) -> CloudResult<Vec<String>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvocationStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    TimedOut,
}
