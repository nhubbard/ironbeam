use super::*;

// ============================================================================
// FakeConfigIO
// ============================================================================

#[derive(Clone)]
pub struct FakeConfigIO {
    config: Arc<Mutex<HashMap<String, ConfigValue>>>,
}

impl FakeConfigIO {
    #[must_use]
    /// Create an empty in-memory config service for tests.
    pub fn new() -> Self {
        Self {
            config: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for FakeConfigIO {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigIO for FakeConfigIO {
    fn get(&self, key: &str) -> CloudResult<ConfigValue> {
        self.config
            .lock()
            .expect("config mutex poisoned")
            .get(key)
            .cloned()
            .ok_or_else(|| {
                CloudIOError::new(ErrorKind::NotFound, format!("Config key {key} not found"))
            })
    }

    fn set(&self, key: &str, value: &str, is_secret: bool) -> CloudResult<()> {
        self.config.lock().expect("config mutex poisoned").insert(
            key.to_string(),
            ConfigValue {
                key: key.to_string(),
                value: value.to_string(),
                version: Some("v1".to_string()),
                is_secret,
            },
        );
        Ok(())
    }

    fn delete(&self, key: &str) -> CloudResult<()> {
        self.config
            .lock()
            .expect("config mutex poisoned")
            .remove(key);
        Ok(())
    }

    fn list(&self, prefix: Option<&str>) -> CloudResult<Vec<String>> {
        let config = self.config.lock().expect("config mutex poisoned");
        let mut keys: Vec<String> = config
            .keys()
            .filter(|k| prefix.is_none_or(|p| k.starts_with(p)))
            .cloned()
            .collect();
        drop(config);
        keys.sort();
        Ok(keys)
    }

    fn batch_get(&self, keys: Vec<String>) -> CloudResult<Vec<Option<ConfigValue>>> {
        let config = self.config.lock().expect("config mutex poisoned");
        Ok(keys.into_iter().map(|k| config.get(&k).cloned()).collect())
    }
}
