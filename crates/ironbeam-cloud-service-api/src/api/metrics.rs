use super::*;

// ============================================================================
// MetricIO - Metrics and Monitoring
// ============================================================================

/// A metric data point
#[derive(Debug, Clone)]
pub struct MetricPoint {
    pub name: String,
    pub value: f64,
    pub timestamp: i64, // Unix timestamp
    pub tags: HashMap<String, String>,
}

/// Metric query parameters
#[derive(Debug, Clone)]
pub struct MetricQuery {
    pub metric_name: String,
    pub start_time: i64,
    pub end_time: i64,
    pub aggregation: Option<String>, // avg, sum, min, max, etc.
    pub tags: HashMap<String, String>,
}

/// Trait for metrics operations
pub trait MetricIO: Send + Sync {
    /// Put a single metric
    ///
    /// # Errors
    ///
    /// Returns an error if the namespace doesn't exist, permissions are not enough, or the operation fails
    fn put_metric(&self, namespace: &str, metric: MetricPoint) -> CloudResult<()>;

    /// Put multiple metrics
    ///
    /// # Errors
    ///
    /// Returns an error if the namespace doesn't exist, permissions are not enough, or the operation fails
    fn put_metrics(&self, namespace: &str, metrics: Vec<MetricPoint>) -> CloudResult<()>;

    /// Query metrics
    ///
    /// # Errors
    ///
    /// Returns an error if the namespace doesn't exist, the query is invalid, or the operation fails
    fn query_metrics(&self, namespace: &str, query: MetricQuery) -> CloudResult<Vec<MetricPoint>>;

    /// List available metrics in a namespace
    ///
    /// # Errors
    ///
    /// Returns an error if the namespace doesn't exist, permissions are not enough, or the listing fails
    fn list_metrics(&self, namespace: &str) -> CloudResult<Vec<String>>;
}
