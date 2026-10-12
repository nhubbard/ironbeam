use super::*;

// ============================================================================
// FakeComputeIO
// ============================================================================

#[derive(Clone)]
pub struct FakeComputeIO {
    functions: FunctionMap,
    invocation_counter: Arc<Mutex<u64>>,
}

impl FakeComputeIO {
    #[must_use]
    pub fn new() -> Self {
        Self {
            functions: Arc::new(Mutex::new(HashMap::new())),
            invocation_counter: Arc::new(Mutex::new(0)),
        }
    }

    /// Registers a function handler for testing.
    ///
    /// # Panics
    ///
    /// Panics if the mutex protecting the functions is poisoned.
    pub fn register_function<F>(&self, name: &str, func: F)
    where
        F: Fn(&[u8]) -> Vec<u8> + Send + Sync + 'static,
    {
        self.functions
            .lock()
            .expect("functions mutex poisoned")
            .insert(name.to_string(), Box::new(func));
    }

    fn next_invocation_id(&self) -> String {
        let mut counter = self
            .invocation_counter
            .lock()
            .expect("invocation_counter mutex poisoned");
        *counter += 1;
        let id = *counter;
        drop(counter);
        format!("inv-{id}")
    }
}

impl Default for FakeComputeIO {
    fn default() -> Self {
        Self::new()
    }
}

impl ComputeIO for FakeComputeIO {
    fn invoke(&self, function_name: &str, payload: &[u8]) -> CloudResult<ComputeResult> {
        let functions = self.functions.lock().expect("functions mutex poisoned");
        let func = functions.get(function_name).ok_or_else(|| {
            CloudIOError::new(
                ErrorKind::NotFound,
                format!("Function {function_name} not found"),
            )
        })?;

        let output = func(payload);
        drop(functions);

        Ok(ComputeResult {
            status_code: 200,
            output,
            logs: Some("Fake function executed".to_string()),
            execution_time_ms: 10,
        })
    }

    fn invoke_async(&self, _function_name: &str, _payload: &[u8]) -> CloudResult<String> {
        Ok(self.next_invocation_id())
    }

    fn get_invocation_status(&self, _invocation_id: &str) -> CloudResult<InvocationStatus> {
        Ok(InvocationStatus::Succeeded)
    }

    fn list_functions(&self) -> CloudResult<Vec<String>> {
        let functions = self.functions.lock().expect("functions mutex poisoned");
        let mut names: Vec<String> = functions.keys().cloned().collect();
        drop(functions);
        names.sort();
        Ok(names)
    }
}
