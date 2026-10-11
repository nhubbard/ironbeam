//! Built-in Direct job metrics and optional custom metric reporting.
//!
//! Run with `cargo run --example metrics_example --features v5-metrics`.

use anyhow::Result;
use ironbeam::v5::{
    JobConfig, Pipeline,
    metrics::{CounterMetric, GaugeMetric, HistogramMetric, Metric, MetricsCollector},
    prelude::*,
};
use serde_json::{Value, json};
use std::any::Any;

struct CustomMetric {
    name: String,
    items_processed: usize,
}

impl Metric for CustomMetric {
    fn name(&self) -> &str {
        &self.name
    }
    fn value(&self) -> Value {
        json!({ "items_processed": self.items_processed, "status": "completed" })
    }
    fn description(&self) -> Option<&str> {
        Some("Custom processing metric")
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn main() -> Result<()> {
    let pipeline = Pipeline::default();
    let mut custom = MetricsCollector::new();
    custom.register(Box::new(CounterMetric::with_value("input_records", 1000)));
    custom.register(Box::new(GaugeMetric::new("avg_record_size", 256.5)));
    custom.register(Box::new(
        HistogramMetric::with_values("processing_times_ms", vec![10.5, 12.3, 11.8, 9.7, 13.2])
            .with_description("Sample processing times"),
    ));
    custom.register(Box::new(CustomMetric {
        name: "custom_processing".into(),
        items_processed: 1000,
    }));

    let updates = custom.clone();
    let output = pipeline
        .from_vec("records", (0..1000_i32).collect())?
        .filter_named("even", |value| value % 2 == 0)?
        .map_named("double", move |value| {
            updates.increment_counter("doubled_records", 1);
            value * 2
        })?;
    let (values, job) = output.collect_with(&JobConfig {
        metrics_collector: Some(custom),
        ..JobConfig::default()
    })?;
    assert_eq!(values.len(), 500);
    assert_eq!(job.metrics().source_elements, 1000);
    assert_eq!(job.metrics().filtered_elements, 500);
    assert_eq!(job.metrics().output_elements, 500);

    let custom = job.custom_metrics().expect("configured metrics collector");
    assert_eq!(custom.snapshot()["doubled_records"], json!(500));
    assert!(custom.elapsed().is_some());
    custom.print();
    let path = tempfile::NamedTempFile::new()?;
    custom.save_to_file(path.path().to_str().expect("UTF-8 temporary path"))?;
    let saved: Value = serde_json::from_slice(&std::fs::read(path.path())?)?;
    assert_eq!(saved["doubled_records"]["value"], json!(500));
    println!("Direct job {} processed {} records", job.id(), values.len());
    Ok(())
}
