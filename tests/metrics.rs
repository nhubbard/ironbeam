//! Tests for the metrics module.

use rustflow::metrics::{CounterMetric, GaugeMetric, HistogramMetric, Metric, MetricsCollector};
use serde_json::json;

#[macro_use]
mod macros;

#[test]
fn test_counter_metric() {
    let mut collector = MetricsCollector::new();
    collector.register(Box::new(CounterMetric::with_value("test_counter", 5)));

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.get("test_counter").unwrap(), &json!(5));
}

#[test]
fn test_gauge_metric() {
    let mut collector = MetricsCollector::new();
    collector.register(Box::new(
        GaugeMetric::new("test_gauge", 42.5).with_description("Test gauge"),
    ));

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.get("test_gauge").unwrap(), &json!(42.5));
}

#[test]
fn test_histogram_metric() {
    let mut collector = MetricsCollector::new();
    let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    collector.register(Box::new(HistogramMetric::with_values("test_hist", values)));

    let snapshot = collector.snapshot();
    let hist_value = snapshot.get("test_hist").unwrap();
    assert_eq!(hist_value["count"], json!(5));
    assert_eq!(hist_value["mean"], json!(3.0));
}

#[test]
fn test_increment_counter() {
    let collector = MetricsCollector::new();
    collector.increment_counter("requests", 1);
    collector.increment_counter("requests", 5);

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.get("requests").unwrap(), &json!(6));
}

#[test]
fn test_set_counter() {
    let collector = MetricsCollector::new();
    collector.set_counter("operations", 100);

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.get("operations").unwrap(), &json!(100));

    // Overwrite with new value
    collector.set_counter("operations", 200);
    let snapshot2 = collector.snapshot();
    assert_eq!(snapshot2.get("operations").unwrap(), &json!(200));
}

#[test]
fn test_register_all() {
    let mut collector = MetricsCollector::new();
    let metrics: Vec<Box<dyn Metric>> = vec![
        Box::new(CounterMetric::with_value("counter1", 10)),
        Box::new(CounterMetric::with_value("counter2", 20)),
        Box::new(GaugeMetric::new("gauge1", std::f64::consts::PI)),
    ];

    collector.register_all(metrics);

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.get("counter1").unwrap(), &json!(10));
    assert_eq!(snapshot.get("counter2").unwrap(), &json!(20));
    assert_eq!(snapshot.get("gauge1").unwrap(), &json!(std::f64::consts::PI));
}

#[test]
fn test_metrics_collector_default() {
    let collector = MetricsCollector::default();
    let snapshot = collector.snapshot();
    assert!(snapshot.is_empty());
}

#[test]
fn test_metrics_collector_empty() {
    let collector = MetricsCollector::empty();
    let snapshot = collector.snapshot();
    assert!(snapshot.is_empty());
}

#[test]
fn test_elapsed_time() {
    use std::thread;
    use std::time::Duration;

    let collector = MetricsCollector::new();

    // No time recorded yet
    assert!(collector.elapsed().is_none());

    // Record start and end
    collector.record_start();
    thread::sleep(Duration::from_millis(50));
    collector.record_end();

    // Should have elapsed time
    let elapsed = collector.elapsed();
    assert!(elapsed.is_some());
    assert!(elapsed.unwrap().as_millis() >= 50);
}

#[test]
fn test_to_json_with_execution_time() {
    use std::thread;
    use std::time::Duration;

    let mut collector = MetricsCollector::new();
    collector.register(Box::new(CounterMetric::with_value("test", 42)));

    collector.record_start();
    thread::sleep(Duration::from_millis(10));
    collector.record_end();

    let json = collector.to_json();
    assert_eq!(json["test"]["value"], json!(42));
    assert!(json["execution_time_ms"]["value"].is_number());
    assert!(
        json["execution_time_ms"]["description"]
            .as_str()
            .unwrap()
            .contains("execution time")
    );
}

#[test]
fn test_to_json_without_execution_time() {
    let mut collector = MetricsCollector::new();
    collector.register(Box::new(CounterMetric::with_value("count", 100)));

    let json = collector.to_json();
    assert_eq!(json["count"]["value"], json!(100));
    assert!(json.get("execution_time_ms").is_none());
}

#[test]
fn test_print_metrics() {
    let mut collector = MetricsCollector::new();
    collector.register(Box::new(
        GaugeMetric::new("temp", 98.6).with_description("Temperature"),
    ));

    // Just ensure it doesn't panic
    collector.print();
}

#[test]
fn test_save_to_file() {
    use std::fs;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("metrics.json");

    let mut collector = MetricsCollector::new();
    collector.register(Box::new(CounterMetric::with_value("saved", 123)));

    collector.save_to_file(file_path.to_str().unwrap()).unwrap();

    // Verify the file exists and contains valid JSON
    let contents = fs::read_to_string(&file_path).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&contents).unwrap();
    assert_eq!(parsed["saved"]["value"], json!(123));
}

#[test]
fn test_counter_metric_new() {
    let counter = CounterMetric::new("new_counter");
    assert_eq!(counter.name(), "new_counter");
    assert_eq!(counter.value(), json!(0));
}

#[test]
fn test_counter_metric_with_value() {
    let counter = CounterMetric::with_value("init_counter", 50);
    assert_eq!(counter.name(), "init_counter");
    assert_eq!(counter.value(), json!(50));
}

#[test]
fn test_gauge_metric_new() {
    let gauge = GaugeMetric::new("cpu_usage", 85.5);
    assert_eq!(gauge.name(), "cpu_usage");
    assert_eq!(gauge.value(), json!(85.5));
    assert!(gauge.description().is_none());
}

#[test]
fn test_gauge_metric_with_description() {
    let gauge = GaugeMetric::new("memory", 1024.0).with_description("Memory usage in MB");
    assert_eq!(gauge.name(), "memory");
    assert_eq!(gauge.value(), json!(1024.0));
    assert_eq!(gauge.description(), Some("Memory usage in MB"));
}

#[test]
fn test_histogram_metric_new() {
    let hist = HistogramMetric::new("latency");
    assert_eq!(hist.name(), "latency");

    let stats = hist.stats();
    assert_eq!(stats.count, 0);
    assert_approx_eq!(stats.sum, 0.0);
}

#[test]
fn test_histogram_metric_with_values() {
    let values = vec![10.0, 20.0, 30.0, 40.0, 50.0];
    let hist = HistogramMetric::with_values("response_time", values);

    let stats = hist.stats();
    assert_eq!(stats.count, 5);
    assert_approx_eq!(stats.sum, 150.0);
    assert_approx_eq!(stats.mean, 30.0);
    assert_approx_eq!(stats.min, 10.0);
    assert_approx_eq!(stats.max, 50.0);
    assert_approx_eq!(stats.p50, 30.0);
}

#[test]
fn test_histogram_record() {
    let mut hist = HistogramMetric::new("values");
    hist.record(1.0);
    hist.record(2.0);
    hist.record(3.0);

    let stats = hist.stats();
    assert_eq!(stats.count, 3);
    assert_approx_eq!(stats.mean, 2.0);
}

#[test]
fn test_histogram_percentiles() {
    let values: Vec<f64> = (1..=100).map(f64::from).collect();
    let hist = HistogramMetric::with_values("percentile_test", values);

    let stats = hist.stats();
    assert_eq!(stats.count, 100);
    assert_approx_eq!(stats.min, 1.0);
    assert_approx_eq!(stats.max, 100.0);
    assert!(stats.p50 >= 49.0 && stats.p50 <= 51.0);
    assert!(stats.p95 >= 94.0 && stats.p95 <= 96.0);
    assert!(stats.p99 >= 98.0 && stats.p99 <= 100.0);
}

#[test]
fn test_histogram_with_description() {
    let hist = HistogramMetric::new("request_latency").with_description("API request latency");
    assert_eq!(hist.name(), "request_latency");
    assert_eq!(hist.description(), Some("API request latency"));
}

#[test]
fn test_histogram_stats_default() {
    let stats = rustflow::metrics::HistogramStats::default();
    assert_eq!(stats.count, 0);
    assert_approx_eq!(stats.sum, 0.0);
    assert_approx_eq!(stats.mean, 0.0);
}

#[test]
fn test_increment_nonexistent_counter() {
    let collector = MetricsCollector::new();

    // Incrementing a non-existent counter should create it
    collector.increment_counter("new_counter", 10);

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.get("new_counter").unwrap(), &json!(10));
}

#[test]
fn test_metric_replacement() {
    let mut collector = MetricsCollector::new();

    // Register a counter
    collector.register(Box::new(CounterMetric::with_value("metric", 10)));
    let snapshot1 = collector.snapshot();
    assert_eq!(snapshot1.get("metric").unwrap(), &json!(10));

    // Replace counter with a gauge of the same name
    collector.register(Box::new(GaugeMetric::new("metric", 99.9)));
    let snapshot2 = collector.snapshot();
    assert_eq!(snapshot2.get("metric").unwrap(), &json!(99.9));
}

#[test]
fn test_metrics_with_special_characters_in_name() {
    let mut collector = MetricsCollector::new();
    collector.register(Box::new(CounterMetric::with_value(
        "metric-with-dashes",
        100,
    )));
    collector.register(Box::new(CounterMetric::with_value(
        "metric_with_underscores",
        200,
    )));

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.get("metric-with-dashes").unwrap(), &json!(100));
    assert_eq!(
        snapshot.get("metric_with_underscores").unwrap(),
        &json!(200)
    );
}

#[test]
fn test_histogram_single_value() {
    let hist = HistogramMetric::with_values("single", vec![42.0]);

    let stats = hist.stats();
    assert_eq!(stats.count, 1);
    assert_approx_eq!(stats.sum, 42.0);
    assert_approx_eq!(stats.mean, 42.0);
    assert_approx_eq!(stats.min, 42.0);
    assert_approx_eq!(stats.max, 42.0);
    assert_approx_eq!(stats.p50, 42.0);
    assert_approx_eq!(stats.p95, 42.0);
    assert_approx_eq!(stats.p99, 42.0);
}

#[test]
fn test_histogram_unsorted_values() {
    let values = vec![50.0, 10.0, 30.0, 20.0, 40.0];
    let hist = HistogramMetric::with_values("unsorted", values);

    let stats = hist.stats();
    assert_eq!(stats.count, 5);
    assert_approx_eq!(stats.min, 10.0);
    assert_approx_eq!(stats.max, 50.0);
    assert_approx_eq!(stats.mean, 30.0);
}

#[test]
fn test_metrics_collector_clone() {
    let mut collector1 = MetricsCollector::new();
    collector1.register(Box::new(CounterMetric::with_value("shared", 42)));

    let collector2 = collector1.clone();
    let snapshot = collector2.snapshot();
    assert_eq!(snapshot.get("shared").unwrap(), &json!(42));
}

#[test]
fn test_elapsed_with_only_start() {
    let collector = MetricsCollector::new();
    collector.record_start();

    // Without end time, elapsed should be None
    assert!(collector.elapsed().is_none());
}

#[test]
fn test_json_sorted_metrics() {
    let mut collector = MetricsCollector::new();
    collector.register(Box::new(CounterMetric::with_value("z_last", 1)));
    collector.register(Box::new(CounterMetric::with_value("a_first", 2)));
    collector.register(Box::new(CounterMetric::with_value("m_middle", 3)));

    // Just verify all metrics are present in JSON
    let json = collector.to_json();
    assert_eq!(json["z_last"]["value"], json!(1));
    assert_eq!(json["a_first"]["value"], json!(2));
    assert_eq!(json["m_middle"]["value"], json!(3));
}
