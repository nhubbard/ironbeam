use super::*;

// ============================================================================
// FakeCacheIO
// ============================================================================

#[derive(Clone)]
/// In-memory cache service for deterministic tests.
pub struct FakeCacheIO {
    cache: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl FakeCacheIO {
    #[must_use]
    /// Create an empty in-memory cache service for tests.
    pub fn new() -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for FakeCacheIO {
    fn default() -> Self {
        Self::new()
    }
}

impl CacheIO for FakeCacheIO {
    fn get(&self, key: &str) -> CloudResult<Option<Vec<u8>>> {
        Ok(self
            .cache
            .lock()
            .expect("cache mutex poisoned")
            .get(key)
            .cloned())
    }

    fn set(&self, key: &str, value: &[u8], _ttl_secs: Option<u64>) -> CloudResult<()> {
        self.cache
            .lock()
            .expect("cache mutex poisoned")
            .insert(key.to_string(), value.to_vec());
        Ok(())
    }

    fn delete(&self, key: &str) -> CloudResult<()> {
        self.cache.lock().expect("cache mutex poisoned").remove(key);
        Ok(())
    }

    fn exists(&self, key: &str) -> CloudResult<bool> {
        Ok(self
            .cache
            .lock()
            .expect("cache mutex poisoned")
            .contains_key(key))
    }

    fn get_batch(&self, keys: Vec<String>) -> CloudResult<Vec<Option<Vec<u8>>>> {
        let cache = self.cache.lock().expect("cache mutex poisoned");
        Ok(keys.into_iter().map(|k| cache.get(&k).cloned()).collect())
    }

    fn set_batch(&self, items: Vec<(String, Vec<u8>, Option<u64>)>) -> CloudResult<()> {
        let mut cache = self.cache.lock().expect("cache mutex poisoned");
        for (key, value, _ttl) in items {
            cache.insert(key, value);
        }
        drop(cache);
        Ok(())
    }

    fn increment(&self, key: &str, delta: i64) -> CloudResult<i64> {
        let mut cache = self.cache.lock().expect("cache mutex poisoned");
        let current = cache
            .get(key)
            .and_then(|v| String::from_utf8(v.clone()).ok())
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        let new_value = current + delta;
        cache.insert(key.to_string(), new_value.to_string().into_bytes());
        drop(cache);
        Ok(new_value)
    }

    fn flush(&self) -> CloudResult<()> {
        self.cache.lock().expect("cache mutex poisoned").clear();
        Ok(())
    }
}
