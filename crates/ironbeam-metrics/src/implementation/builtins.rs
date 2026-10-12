impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for MetricsCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f.debug_struct("MetricsCollector")
            .field("metric_count", &inner.metrics.len())
            .finish()
    }
}

// ========== Built-in Metrics ==========

/// A simple counter metric.
pub struct CounterMetric {
    name: String,
    count: u64,
}

impl CounterMetric {
    /// Create a new counter metric with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            count: 0,
        }
    }

    /// Create a counter metric with an initial value.
    pub fn with_value(name: impl Into<String>, count: u64) -> Self {
        Self {
            name: name.into(),
            count,
        }
    }
}

impl Metric for CounterMetric {
    fn name(&self) -> &str {
        &self.name
    }

    fn value(&self) -> Value {
        json!(self.count)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A gauge metric that holds a single numeric value.
pub struct GaugeMetric {
    name: String,
    value: f64,
    description: Option<String>,
}

impl GaugeMetric {
    /// Create a new gauge metric.
    pub fn new(name: impl Into<String>, value: f64) -> Self {
        Self {
            name: name.into(),
            value,
            description: None,
        }
    }

    /// Set a description for this gauge.
    #[must_use]
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
}

impl Metric for GaugeMetric {
    fn name(&self) -> &str {
        &self.name
    }

    fn value(&self) -> Value {
        json!(self.value)
    }

    fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A histogram metric that tracks value distribution.
pub struct HistogramMetric {
    name: String,
    values: Vec<f64>,
    description: Option<String>,
}

impl HistogramMetric {
    /// Create a new histogram metric.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            values: Vec::new(),
            description: None,
        }
    }

    /// Create a histogram with initial values.
    pub fn with_values(name: impl Into<String>, values: Vec<f64>) -> Self {
        Self {
            name: name.into(),
            values,
            description: None,
        }
    }

    /// Set a description for this histogram.
    #[must_use]
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Record a value in the histogram.
    pub fn record(&mut self, value: f64) {
        self.values.push(value);
    }

    /// Get statistics from the histogram.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn stats(&self) -> HistogramStats {
        if self.values.is_empty() {
            return HistogramStats::default();
        }

        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

        let count = sorted.len();
        let sum: f64 = sorted.iter().sum();
        let mean = sum / count as f64;
        let min = sorted[0];
        let max = sorted[count - 1];

        let p50 = sorted[count / 2];
        let p95 = sorted[(count * 95) / 100];
        let p99 = sorted[(count * 99) / 100];

        HistogramStats {
            count,
            sum,
            mean,
            min,
            max,
            p50,
            p95,
            p99,
        }
    }
}

impl Metric for HistogramMetric {
    fn name(&self) -> &str {
        &self.name
    }

    fn value(&self) -> Value {
        let stats = self.stats();
        json!({
            "count": stats.count,
            "sum": stats.sum,
            "mean": stats.mean,
            "min": stats.min,
            "max": stats.max,
            "p50": stats.p50,
            "p95": stats.p95,
            "p99": stats.p99,
        })
    }

    fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Statistics computed from a histogram.
#[derive(Debug, Clone)]
pub struct HistogramStats {
    pub count: usize,
    pub sum: f64,
    pub mean: f64,
    pub min: f64,
    pub max: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
}

impl Default for HistogramStats {
    fn default() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            mean: 0.0,
            min: 0.0,
            max: 0.0,
            p50: 0.0,
            p95: 0.0,
            p99: 0.0,
        }
    }
}
