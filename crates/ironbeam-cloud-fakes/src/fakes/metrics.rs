use super::*;

// ============================================================================
// FakeMetricIO
// ============================================================================

#[derive(Clone)]
pub struct FakeMetricIO {
    metrics: Arc<Mutex<HashMap<String, Vec<MetricPoint>>>>,
}

impl FakeMetricIO {
    #[must_use]
    /// Create an empty in-memory metrics service for tests.
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for FakeMetricIO {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricIO for FakeMetricIO {
    fn put_metric(&self, namespace: &str, metric: MetricPoint) -> CloudResult<()> {
        self.metrics
            .lock()
            .expect("metrics mutex poisoned")
            .entry(namespace.to_string())
            .or_default()
            .push(metric);
        Ok(())
    }

    fn put_metrics(&self, namespace: &str, metrics: Vec<MetricPoint>) -> CloudResult<()> {
        for metric in metrics {
            self.put_metric(namespace, metric)?;
        }
        Ok(())
    }

    fn query_metrics(&self, namespace: &str, query: MetricQuery) -> CloudResult<Vec<MetricPoint>> {
        let metrics = self.metrics.lock().expect("metrics mutex poisoned");
        let ns_metrics = metrics.get(namespace).ok_or_else(|| {
            CloudIOError::new(
                ErrorKind::NotFound,
                format!("Namespace {namespace} not found"),
            )
        })?;

        let results: Vec<MetricPoint> = ns_metrics
            .iter()
            .filter(|m| {
                m.name == query.metric_name
                    && m.timestamp >= query.start_time
                    && m.timestamp <= query.end_time
                    && query.tags.iter().all(|(k, v)| m.tags.get(k) == Some(v))
            })
            .cloned()
            .collect();

        drop(metrics);
        Ok(results)
    }

    fn list_metrics(&self, namespace: &str) -> CloudResult<Vec<String>> {
        let metrics = self.metrics.lock().expect("metrics mutex poisoned");
        let ns_metrics = metrics.get(namespace).ok_or_else(|| {
            CloudIOError::new(
                ErrorKind::NotFound,
                format!("Namespace {namespace} not found"),
            )
        })?;

        let mut names: Vec<String> = ns_metrics.iter().map(|m| m.name.clone()).collect();
        drop(metrics);
        names.sort();
        names.dedup();
        Ok(names)
    }
}
