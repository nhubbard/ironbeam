use anyhow::Result;
use serde_json::{Map, Value, json, to_string_pretty};
use std::any::Any;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Trait for custom metrics.
///
/// Implement this trait to define your own metrics that can be tracked
/// during pipeline execution.
pub trait Metric: Send + Sync + Any {
    /// The name of this metric (e.g., `element_count`, `processing_time_ms`).
    fn name(&self) -> &str;

    /// The current value of this metric as a JSON value.
    fn value(&self) -> Value;

    /// Optional description of what this metric measures.
    fn description(&self) -> Option<&str> {
        None
    }

    /// Cast to Any for downcasting.
    fn as_any(&self) -> &dyn Any;
}

/// Thread-safe container for collecting pipeline execution metrics.
///
/// The `MetricsCollector` allows you to register custom metrics and built-in
/// metrics, then retrieve them after pipeline execution for reporting.
#[derive(Clone)]
pub struct MetricsCollector {
    inner: Arc<Mutex<MetricsCollectorInner>>,
}

struct MetricsCollectorInner {
    metrics: HashMap<String, Box<dyn Metric>>,
    start_time: Option<Instant>,
    end_time: Option<Instant>,
}

impl MetricsCollector {
    /// Create an empty custom metrics collector for one job.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MetricsCollectorInner {
                metrics: HashMap::new(),
                start_time: None,
                end_time: None,
            })),
        }
    }

    /// Alias for [`Self::new`].
    #[must_use]
    pub fn empty() -> Self {
        Self::new()
    }

    /// Register a custom metric.
    ///
    /// If a metric with the same name already exists, it will be replaced.
    ///
    /// # Panics
    ///
    /// Panics if the internal metrics mutex is poisoned.
    pub fn register(&mut self, metric: Box<dyn Metric>) {
        let mut inner = self.inner.lock().unwrap();
        inner.metrics.insert(metric.name().to_string(), metric);
    }

    /// Register multiple metrics at once.
    pub fn register_all(&mut self, metrics: Vec<Box<dyn Metric>>) {
        for metric in metrics {
            self.register(metric);
        }
    }

    /// Record the start time of job execution.
    ///
    /// # Panics
    ///
    /// Panics if the internal metrics mutex is poisoned.
    pub fn record_start(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.start_time = Some(Instant::now());
        inner.end_time = None;
    }

    /// Record the end time of job execution once.
    ///
    /// # Panics
    ///
    /// Panics if the internal metrics mutex is poisoned.
    pub fn record_end(&self) {
        let mut inner = self.inner.lock().unwrap();
        if inner.end_time.is_none() {
            inner.end_time = Some(Instant::now());
        }
    }

    /// Get the elapsed execution time, if available.
    ///
    /// # Panics
    ///
    /// Panics if the internal metrics mutex is poisoned.
    #[must_use]
    pub fn elapsed(&self) -> Option<Duration> {
        let inner = self.inner.lock().unwrap();
        match (inner.start_time, inner.end_time) {
            (Some(start), Some(end)) => Some(end.duration_since(start)),
            _ => None,
        }
    }

    /// Increment a counter metric by name.
    ///
    /// If the metric doesn't exist, it will be created as a `CounterMetric`.
    ///
    /// # Panics
    ///
    /// Panics if the internal metrics mutex is poisoned.
    pub fn increment_counter(&self, name: &str, value: u64) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(metric) = inner.metrics.get_mut(name) {
            if let Some(counter) = metric.as_any().downcast_ref::<CounterMetric>() {
                let new_count = counter.count.saturating_add(value);
                inner.metrics.insert(
                    name.to_string(),
                    Box::new(CounterMetric::with_value(name, new_count)),
                );
            }
        } else {
            inner.metrics.insert(
                name.to_string(),
                Box::new(CounterMetric {
                    name: name.to_string(),
                    count: value,
                }),
            );
        }
    }

    /// Set a counter metric to a specific value.
    ///
    /// # Panics
    ///
    /// Panics if the internal metrics mutex is poisoned.
    pub fn set_counter(&self, name: &str, value: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.metrics.insert(
            name.to_string(),
            Box::new(CounterMetric {
                name: name.to_string(),
                count: value,
            }),
        );
    }

    /// Get all metrics as a JSON object.
    ///
    /// # Panics
    ///
    /// Panics if the internal metrics mutex is poisoned.
    #[must_use]
    pub fn to_json(&self) -> Value {
        let inner = self.inner.lock().unwrap();
        let mut metrics_json = Map::new();

        for (name, metric) in &inner.metrics {
            let mut metric_obj = Map::new();
            metric_obj.insert("value".to_string(), metric.value());
            if let Some(desc) = metric.description() {
                metric_obj.insert("description".to_string(), json!(desc));
            }
            metrics_json.insert(name.clone(), Value::Object(metric_obj));
        }

        if let (Some(start), Some(end)) = (inner.start_time, inner.end_time) {
            let elapsed_ms = end.duration_since(start).as_millis();
            let mut time_obj = Map::new();
            time_obj.insert("value".to_string(), json!(elapsed_ms));
            time_obj.insert(
                "description".to_string(),
                json!("Total pipeline execution time in milliseconds"),
            );
            metrics_json.insert("execution_time_ms".to_string(), Value::Object(time_obj));
        }
        drop(inner);
        json!(metrics_json)
    }

    /// Print all metrics to stdout in a human-readable format.
    ///
    /// # Panics
    ///
    /// Panics if the internal metrics mutex is poisoned.
    pub fn print(&self) {
        println!("\n========== Pipeline Metrics ==========");

        let inner = self.inner.lock().unwrap();

        if let (Some(start), Some(end)) = (inner.start_time, inner.end_time) {
            let elapsed = end.duration_since(start);
            println!(
                "Execution Time: {:.3}s ({} ms)",
                elapsed.as_secs_f64(),
                elapsed.as_millis()
            );
            println!("--------------------------------------");
        }

        let mut sorted_metrics: Vec<_> = inner.metrics.iter().collect();
        sorted_metrics.sort_by_key(|(name, _)| *name);
        for (name, metric) in sorted_metrics {
            if let Some(desc) = metric.description() {
                println!("{}: {} ({})", name, metric.value(), desc);
            } else {
                println!("{}: {}", name, metric.value());
            }
        }
        drop(inner);
        println!("======================================\n");
    }

    /// Save all metrics to a JSON file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created or written to.
    pub fn save_to_file(&self, path: &str) -> Result<()> {
        let json = self.to_json();
        let mut file = File::create(path)?;
        let formatted = to_string_pretty(&json)?;
        file.write_all(formatted.as_bytes())?;
        Ok(())
    }

    /// Get a snapshot of all metric names and values.
    ///
    /// # Panics
    ///
    /// Panics if the internal metrics mutex is poisoned.
    #[must_use]
    pub fn snapshot(&self) -> HashMap<String, Value> {
        let inner = self.inner.lock().unwrap();
        inner
            .metrics
            .iter()
            .map(|(name, metric)| (name.clone(), metric.value()))
            .collect()
    }
}
