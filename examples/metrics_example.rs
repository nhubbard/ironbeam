//! Example demonstrating the metrics API.
//!
//! This example shows how to:
//! - Create a metrics collector
//! - Register built-in and custom metrics
//! - Track pipeline execution
//! - Print metrics to stdout
//! - Save metrics to a JSON file

use anyhow::Result;
use ironbeam::{Pipeline, from_vec};
use serde_json::{Value, json};
use std::any::Any;

#[cfg(feature = "metrics")]
use ironbeam::metrics::{CounterMetric, GaugeMetric, HistogramMetric, Metric, MetricsCollector};

#[cfg(feature = "metrics")]
struct CustomMetric {
    name: String,
    items_processed: usize,
}

#[cfg(feature = "metrics")]
impl Metric for CustomMetric {
    fn name(&self) -> &str {
        &self.name
    }

    fn value(&self) -> Value {
        json!({
            "items_processed": self.items_processed,
            "status": "completed"
        })
    }

    fn description(&self) -> Option<&str> {
        Some("Custom processing metric")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn main() -> Result<()> {
    #[cfg(feature = "metrics")]
    {
        println!("=== Ironbeam Metrics Example ===\n");

        // Create a pipeline
        let p = Pipeline::default();

        // Create a metrics collector and register metrics
        let mut metrics = MetricsCollector::new();

        // Register built-in metrics
        metrics.register(Box::new(CounterMetric::with_value("input_records", 1000)));
        metrics.register(Box::new(GaugeMetric::new("avg_record_size", 256.5)));
        metrics.register(Box::new(
            HistogramMetric::with_values("processing_times_ms", vec![10.5, 12.3, 11.8, 9.7, 13.2])
                .with_description("Per-record processing times"),
        ));

        // Register a custom metric
        metrics.register(Box::new(CustomMetric {
            name: "custom_processing".to_string(),
            items_processed: 1000,
        }));

        // Attach metrics to the pipeline
        p.set_metrics(metrics);

        // Build and execute a simple pipeline
        println!("Running pipeline...\n");
        let data = from_vec(&p, (0..1000).collect::<Vec<i32>>());

        let result = data
            .filter(|x: &i32| x % 2 == 0) // Keep even numbers
            .map(|x: &i32| x * 2) // Double them
            .collect_seq()?;

        println!("Pipeline completed. Processed {} records.\n", result.len());

        // Retrieve and display metrics
        if let Some(metrics) = p.take_metrics() {
            // Print metrics to stdout
            metrics.print();

            // Save metrics to a JSON file
            metrics.save_to_file("pipeline_metrics.json")?;
            println!("Metrics saved to: pipeline_metrics.json\n");

            // You can also get metrics programmatically
            let snapshot = metrics.snapshot();
            println!("Metrics snapshot:");
            for (name, value) in snapshot {
                println!("  {name}: {value}");
            }
        }
    }

    #[cfg(not(feature = "metrics"))]
    {
        println!("Metrics feature is not enabled. Rebuild with --features metrics");
    }

    Ok(())
}
