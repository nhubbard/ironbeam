use super::*;

// ============================================================================
// FakeObjectIO
// ============================================================================

#[derive(Clone)]
pub struct FakeObjectIO {
    storage: BucketStorage,
}

impl FakeObjectIO {
    #[must_use]
    pub fn new() -> Self {
        Self {
            storage: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for FakeObjectIO {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectIO for FakeObjectIO {
    fn put_object(&self, bucket: &str, key: &str, data: &[u8]) -> CloudResult<()> {
        self.storage
            .lock()
            .expect("storage mutex poisoned")
            .entry(bucket.to_string())
            .or_default()
            .insert(key.to_string(), data.to_vec());
        Ok(())
    }

    fn get_object(&self, bucket: &str, key: &str) -> CloudResult<Vec<u8>> {
        let storage = self.storage.lock().expect("storage mutex poisoned");
        storage
            .get(bucket)
            .and_then(|b| b.get(key))
            .cloned()
            .ok_or_else(|| {
                CloudIOError::new(
                    ErrorKind::NotFound,
                    format!("Object {bucket}/{key} not found"),
                )
            })
    }

    fn delete_object(&self, bucket: &str, key: &str) -> CloudResult<()> {
        if let Some(bucket_map) = self
            .storage
            .lock()
            .expect("storage mutex poisoned")
            .get_mut(bucket)
        {
            bucket_map.remove(key);
        }
        Ok(())
    }

    fn list_objects(&self, bucket: &str, prefix: Option<&str>) -> CloudResult<Vec<ObjectMetadata>> {
        let storage = self.storage.lock().expect("storage mutex poisoned");
        let bucket_map = storage.get(bucket).ok_or_else(|| {
            CloudIOError::new(ErrorKind::NotFound, format!("Bucket {bucket} not found"))
        })?;

        let mut objects: Vec<ObjectMetadata> = bucket_map
            .iter()
            .filter(|(key, _)| prefix.is_none_or(|p| key.starts_with(p)))
            .map(|(key, data)| ObjectMetadata {
                key: key.clone(),
                size: data.len() as u64,
                content_type: Some("application/octet-stream".to_string()),
                last_modified: Some(0),
                etag: Some(format!("etag-{key}")),
                custom_metadata: HashMap::new(),
            })
            .collect();

        drop(storage);
        objects.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(objects)
    }

    fn object_exists(&self, bucket: &str, key: &str) -> CloudResult<bool> {
        let storage = self.storage.lock().expect("storage mutex poisoned");
        Ok(storage.get(bucket).is_some_and(|b| b.contains_key(key)))
    }

    fn get_metadata(&self, bucket: &str, key: &str) -> CloudResult<ObjectMetadata> {
        let storage = self.storage.lock().expect("storage mutex poisoned");
        storage
            .get(bucket)
            .and_then(|b| b.get(key))
            .map(|data| ObjectMetadata {
                key: key.to_string(),
                size: data.len() as u64,
                content_type: Some("application/octet-stream".to_string()),
                last_modified: Some(0),
                etag: Some(format!("etag-{key}")),
                custom_metadata: HashMap::new(),
            })
            .ok_or_else(|| {
                CloudIOError::new(
                    ErrorKind::NotFound,
                    format!("Object {bucket}/{key} not found"),
                )
            })
    }

    fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
    ) -> CloudResult<()> {
        let data = self.get_object(src_bucket, src_key)?;
        self.put_object(dst_bucket, dst_key, &data)
    }
}
