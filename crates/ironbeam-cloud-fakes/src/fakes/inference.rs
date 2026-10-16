use super::*;

// ============================================================================
// FakeIntelligenceIO
// ============================================================================

#[derive(Clone)]
/// In-memory inference service for tests.
pub struct FakeIntelligenceIO {
    models: FunctionMap,
}

impl FakeIntelligenceIO {
    #[must_use]
    /// Create an empty in-memory inference service for tests.
    pub fn new() -> Self {
        Self {
            models: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Registers a model handler for testing.
    ///
    /// # Panics
    ///
    /// Panics if the mutex protecting the models is poisoned.
    pub fn register_model<F>(&self, name: &str, model: F)
    where
        F: Fn(&[u8]) -> Vec<u8> + Send + Sync + 'static,
    {
        self.models
            .lock()
            .expect("models mutex poisoned")
            .insert(name.to_string(), Box::new(model));
    }
}

impl Default for FakeIntelligenceIO {
    fn default() -> Self {
        Self::new()
    }
}

impl IntelligenceIO for FakeIntelligenceIO {
    fn predict(&self, model_name: &str, input: InferenceInput) -> CloudResult<InferenceOutput> {
        let models = self.models.lock().expect("models mutex poisoned");
        let model = models.get(model_name).ok_or_else(|| {
            CloudIOError::new(ErrorKind::NotFound, format!("Model {model_name} not found"))
        })?;

        let output_data = model(&input.data);
        drop(models);

        Ok(InferenceOutput {
            data: output_data,
            content_type: "application/json".to_string(),
            model_version: Some("v1".to_string()),
            inference_time_ms: 50,
        })
    }

    fn predict_batch(
        &self,
        model_name: &str,
        inputs: Vec<InferenceInput>,
    ) -> CloudResult<Vec<InferenceOutput>> {
        inputs
            .into_iter()
            .map(|input| self.predict(model_name, input))
            .collect()
    }

    fn list_models(&self) -> CloudResult<Vec<String>> {
        let models = self.models.lock().expect("models mutex poisoned");
        let mut names: Vec<String> = models.keys().cloned().collect();
        drop(models);
        names.sort();
        Ok(names)
    }

    fn get_model_info(&self, model_name: &str) -> CloudResult<HashMap<String, String>> {
        let models = self.models.lock().expect("models mutex poisoned");
        if models.contains_key(model_name) {
            let mut info = HashMap::new();
            info.insert("name".to_string(), model_name.to_string());
            info.insert("version".to_string(), "v1".to_string());
            Ok(info)
        } else {
            Err(CloudIOError::new(
                ErrorKind::NotFound,
                format!("Model {model_name} not found"),
            ))
        }
    }
}
