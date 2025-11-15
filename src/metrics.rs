//! Metrics collection and reporting for pipeline execution.
//!
//! The metrics module provides an extensible API for tracking pipeline execution statistics.
//! Users can register custom metrics alongside built-in metrics and optionally print or
//! save metrics to a file at the end of pipeline execution.
//!
//! # Overview
//!
//! - [`Metric`] trait defines the interface for custom metrics
//! - [`MetricsCollector`] manages metric registration and collection
//! - Built-in metrics track common execution statistics
//! - Metrics can be printed to stdout or saved to a JSON file
//!
//! # Example
//!
//! ```no_run
//! use ironbeam::*;
//! use ironbeam::metrics::{MetricsCollector, Metric};
//! use serde_json::Value;
//!
//! // Create a custom metric
//! struct MyCustomMetric {
//!     count: usize,
//! }
//!
//! impl Metric for MyCustomMetric {
//!     fn name(&self) -> &str {
//!         "custom_count"
//!     }
//!
//!     fn value(&self) -> Value {
//!         serde_json::json!(self.count)
//!     }
//!
//!     fn as_any(&self) -> &dyn std::any::Any {
//!         self
//!     }
//! }
//!
//! # fn main() -> anyhow::Result<()> {
//! let p = Pipeline::default();
//!
//! // Enable metrics collection
//! let mut metrics = MetricsCollector::new();
//! metrics.register(Box::new(MyCustomMetric { count: 42 }));
//! p.set_metrics(metrics);
//!
//! // Build and execute the pipeline
//! let data = from_vec(&p, vec![1, 2, 3, 4, 5]);
//! let result = data.map(|x: &i32| x * 2).collect_seq()?;
//!
//! // Print metrics to stdout
//! if let Some(metrics) = p.take_metrics() {
//!     metrics.print();
//!     // Or save to file
//!     metrics.save_to_file("metrics.json")?;
//! }
//! # Ok(())
//! # }
//! ```

use anyhow::Result;
use serde_json::{json, Value};
use std::any::Any;
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
    /// Create a new metrics collector with built-in metrics enabled by default.
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

    /// Create a new metrics collector without any built-in metrics.
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
    /// Returns a `MetricsError` if the metric name is invalid or already exists.
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

    /// Record the start time of pipeline execution.
    ///
    /// # Panics
    ///
    /// Returns a `MetricsError` if the metric name is invalid or already exists.
    pub fn record_start(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.start_time = Some(Instant::now());
    }

    /// Record the end time of pipeline execution.
    ///
    /// # Panics
    ///
    /// Returns a `MetricsError` if the metric name is invalid or already exists.
    pub fn record_end(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.end_time = Some(Instant::now());
    }

    /// Get the elapsed execution time, if available.
    ///
    /// # Panics
    ///
    /// Returns a `MetricsError` if the metric name is invalid or already exists.
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
    /// Returns a `MetricsError` if the metric name is invalid or already exists.
    pub fn increment_counter(&self, name: &str, value: u64) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(metric) = inner.metrics.get_mut(name) {
            // Try to downcast to CounterMetric and increment
            if let Some(counter) = metric.as_any().downcast_ref::<CounterMetric>() {
                // We can't mutate through the trait object, so we need to replace it
                let new_count = counter.count + value;
                drop(inner);
                self.set_counter(name, new_count);
            }
        } else {
            // Create a new counter
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
    /// Panics if the metric name is invalid or already exists.
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
    /// Returns a `MetricsError` if the metric name is invalid or already exists.
    #[must_use]
    pub fn to_json(&self) -> Value {
        let inner = self.inner.lock().unwrap();
        let mut metrics_json = serde_json::Map::new();

        for (name, metric) in &inner.metrics {
            let mut metric_obj = serde_json::Map::new();
            metric_obj.insert("value".to_string(), metric.value());
            if let Some(desc) = metric.description() {
                metric_obj.insert("description".to_string(), json!(desc));
            }
            metrics_json.insert(name.clone(), Value::Object(metric_obj));
        }

        // Add execution time if available
        if let (Some(start), Some(end)) = (inner.start_time, inner.end_time) {
            let elapsed_ms = end.duration_since(start).as_millis();
            let mut time_obj = serde_json::Map::new();
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
    /// Panics if the metric name is invalid or already exists.
    pub fn print(&self) {
        println!("\n========== Pipeline Metrics ==========");

        let inner = self.inner.lock().unwrap();

        // Print execution time first if available
        if let (Some(start), Some(end)) = (inner.start_time, inner.end_time) {
            let elapsed = end.duration_since(start);
            println!(
                "Execution Time: {:.3}s ({} ms)",
                elapsed.as_secs_f64(),
                elapsed.as_millis()
            );
            println!("--------------------------------------");
        }

        // Print all metrics
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
    /// Returns a `MetricsError` if the file cannot be created or written to.
    pub fn save_to_file(&self, path: &str) -> Result<()> {
        let json = self.to_json();
        let mut file = File::create(path)?;
        let formatted = serde_json::to_string_pretty(&json)?;
        file.write_all(formatted.as_bytes())?;
        Ok(())
    }

    /// Get a snapshot of all metric names and values.
    ///
    /// # Panics
    ///
    /// Returns a `MetricsError` if the metric name is invalid or already exists.
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

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
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
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

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
