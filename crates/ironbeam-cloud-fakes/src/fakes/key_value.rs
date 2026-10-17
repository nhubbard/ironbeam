use super::*;

// ============================================================================
// FakeKeyValueIO
// ============================================================================

#[derive(Clone)]
/// In-memory key-value service for tests.
pub struct FakeKeyValueIO {
    collections: CollectionStorage,
}

impl FakeKeyValueIO {
    #[must_use]
    /// Create an empty in-memory key value service for tests.
    pub fn new() -> Self {
        Self {
            collections: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for FakeKeyValueIO {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyValueIO for FakeKeyValueIO {
    fn put(&self, collection: &str, key: &str, data: HashMap<String, String>) -> CloudResult<()> {
        let mut collections = self.collections.lock().expect("collections mutex poisoned");
        let coll = collections.entry(collection.to_string()).or_default();
        coll.insert(
            key.to_string(),
            Document {
                key: key.to_string(),
                data,
                version: Some("v1".to_string()),
            },
        );
        drop(collections);
        Ok(())
    }

    fn get(&self, collection: &str, key: &str) -> CloudResult<Option<Document>> {
        let collections = self.collections.lock().expect("collections mutex poisoned");
        Ok(collections
            .get(collection)
            .and_then(|c| c.get(key))
            .cloned())
    }

    fn delete(&self, collection: &str, key: &str) -> CloudResult<()> {
        if let Some(coll) = self
            .collections
            .lock()
            .expect("collections mutex poisoned")
            .get_mut(collection)
        {
            coll.remove(key);
        }
        Ok(())
    }

    fn query(
        &self,
        collection: &str,
        filters: HashMap<String, String>,
    ) -> CloudResult<Vec<Document>> {
        let collections = self.collections.lock().expect("collections mutex poisoned");
        let coll = collections.get(collection).ok_or_else(|| {
            CloudIOError::new(
                ErrorKind::NotFound,
                format!("Collection {collection} not found"),
            )
        })?;

        let results: Vec<Document> = coll
            .values()
            .filter(|doc| filters.iter().all(|(k, v)| doc.data.get(k) == Some(v)))
            .cloned()
            .collect();

        drop(collections);
        Ok(results)
    }

    fn batch_get(&self, collection: &str, keys: Vec<String>) -> CloudResult<Vec<Option<Document>>> {
        keys.into_iter().map(|k| self.get(collection, &k)).collect()
    }

    fn batch_put(
        &self,
        collection: &str,
        documents: Vec<(String, HashMap<String, String>)>,
    ) -> CloudResult<()> {
        for (key, data) in documents {
            self.put(collection, &key, data)?;
        }
        Ok(())
    }

    fn exists(&self, collection: &str, key: &str) -> CloudResult<bool> {
        let collections = self.collections.lock().expect("collections mutex poisoned");
        Ok(collections
            .get(collection)
            .is_some_and(|c| c.contains_key(key)))
    }
}
