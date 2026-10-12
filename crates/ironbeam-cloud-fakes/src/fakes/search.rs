use super::*;

// ============================================================================
// FakeSearchIO
// ============================================================================

#[derive(Clone)]
pub struct FakeSearchIO {
    indices: IndexStorage,
}

impl FakeSearchIO {
    #[must_use]
    pub fn new() -> Self {
        Self {
            indices: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for FakeSearchIO {
    fn default() -> Self {
        Self::new()
    }
}

impl SearchIO for FakeSearchIO {
    fn index(&self, index: &str, id: &str, document: HashMap<String, String>) -> CloudResult<()> {
        self.indices
            .lock()
            .expect("indices mutex poisoned")
            .entry(index.to_string())
            .or_default()
            .insert(id.to_string(), document);
        Ok(())
    }

    fn batch_index(
        &self,
        index: &str,
        documents: Vec<(String, HashMap<String, String>)>,
    ) -> CloudResult<()> {
        for (id, doc) in documents {
            self.index(index, &id, doc)?;
        }
        Ok(())
    }

    fn search(&self, index: &str, query: SearchQuery) -> CloudResult<Vec<SearchHit>> {
        let indices = self.indices.lock().expect("indices mutex poisoned");
        let idx = indices.get(index).ok_or_else(|| {
            CloudIOError::new(ErrorKind::NotFound, format!("Index {index} not found"))
        })?;

        let mut hits: Vec<SearchHit> = idx
            .iter()
            .filter(|(_, doc)| {
                // Simple filtering - check if the query string appears in any field
                doc.values().any(|v| v.contains(&query.query))
                    && query.filters.iter().all(|(k, v)| doc.get(k) == Some(v))
            })
            .map(|(id, fields)| SearchHit {
                id: id.clone(),
                score: 1.0,
                fields: fields.clone(),
            })
            .collect();

        drop(indices);

        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .expect("NaN score encountered")
        });

        let start = query.offset as usize;
        let end = std::cmp::min(start + query.limit as usize, hits.len());

        Ok(hits[start..end].to_vec())
    }

    fn delete(&self, index: &str, id: &str) -> CloudResult<()> {
        if let Some(idx) = self
            .indices
            .lock()
            .expect("indices mutex poisoned")
            .get_mut(index)
        {
            idx.remove(id);
        }
        Ok(())
    }

    fn get(&self, index: &str, id: &str) -> CloudResult<Option<HashMap<String, String>>> {
        let indices = self.indices.lock().expect("indices mutex poisoned");
        Ok(indices.get(index).and_then(|idx| idx.get(id)).cloned())
    }

    fn index_exists(&self, index: &str) -> CloudResult<bool> {
        Ok(self
            .indices
            .lock()
            .expect("indices mutex poisoned")
            .contains_key(index))
    }
}
