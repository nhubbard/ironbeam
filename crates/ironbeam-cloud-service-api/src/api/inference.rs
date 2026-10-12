use super::*;

// ============================================================================
// IntelligenceIO - ML Model Inference
// ============================================================================

/// Input for model inference
#[derive(Debug, Clone)]
pub struct InferenceInput {
    pub data: Vec<u8>,
    pub content_type: String, // e.g., "application/json", "text/plain"
}

/// Output from model inference
#[derive(Debug, Clone)]
pub struct InferenceOutput {
    pub data: Vec<u8>,
    pub content_type: String,
    pub model_version: Option<String>,
    pub inference_time_ms: u64,
}

/// Trait for ML model inference operations
pub trait IntelligenceIO: Send + Sync {
    /// Invoke a model for inference
    ///
    /// # Errors
    ///
    /// Returns an error if the model doesn't exist, the input is invalid, or inference fails
    fn predict(&self, model_name: &str, input: InferenceInput) -> CloudResult<InferenceOutput>;

    /// Batch prediction
    ///
    /// # Errors
    ///
    /// Returns an error if the model doesn't exist, any input is invalid, or inference fails
    fn predict_batch(
        &self,
        model_name: &str,
        inputs: Vec<InferenceInput>,
    ) -> CloudResult<Vec<InferenceOutput>>;

    /// List available models
    ///
    /// # Errors
    ///
    /// Returns an error if permissions are not enough or the listing fails
    fn list_models(&self) -> CloudResult<Vec<String>>;

    /// Get model metadata
    ///
    /// # Errors
    ///
    /// Returns an error if the model doesn't exist, permissions are not enough, or the operation fails
    fn get_model_info(&self, model_name: &str) -> CloudResult<HashMap<String, String>>;
}
