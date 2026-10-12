//! Optional custom metrics and reporting for Ironbeam 5 jobs.
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
//! # use anyhow::Result;
//! use ironbeam_metrics::{MetricsCollector, Metric};
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
//! # fn main() -> Result<()> {
//! let mut metrics = MetricsCollector::new();
//! metrics.register(Box::new(MyCustomMetric { count: 42 }));
//! metrics.increment_counter("rows", 5);
//!
//! // Print metrics to stdout or save them to a JSON file.
//! metrics.print();
//! metrics.save_to_file("metrics.json")?;
//! # Ok(())
//! # }
//! ```

mod implementation;
pub use implementation::*;
