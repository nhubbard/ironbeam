use ironbeam_metrics::{CounterMetric, GaugeMetric, HistogramMetric, Metric, MetricsCollector};
use serde_json::json;

#[test]
fn counters_are_atomic_across_captured_worker_clones() {
    let collector = MetricsCollector::new();
    let workers: Vec<_> = (0..4)
        .map(|_| {
            let collector = collector.clone();
            std::thread::spawn(move || {
                for _ in 0..1_000 {
                    collector.increment_counter("processed", 1);
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(collector.snapshot()["processed"], json!(4_000));
    collector.set_counter("processed", u64::MAX);
    collector.increment_counter("processed", 1);
    assert_eq!(collector.snapshot()["processed"], json!(u64::MAX));
}

#[test]
fn snapshots_include_custom_shapes_and_completed_job_time() {
    let mut collector = MetricsCollector::new();
    collector.register(Box::new(CounterMetric::with_value("input", 10)));
    collector.register(Box::new(
        GaugeMetric::new("load", 2.5).with_description("load gauge"),
    ));
    collector.register(Box::new(HistogramMetric::with_values(
        "latency",
        vec![1.0, 3.0, 2.0],
    )));
    collector.record_start();
    collector.record_end();
    let elapsed = collector.elapsed().unwrap();
    collector.record_end();
    assert_eq!(collector.elapsed(), Some(elapsed));
    let report = collector.to_json();
    assert_eq!(report["input"]["value"], json!(10));
    assert_eq!(report["load"]["description"], json!("load gauge"));
    assert_eq!(report["latency"]["value"]["count"], json!(3));
    assert!(report["execution_time_ms"]["value"].is_number());
}

#[test]
fn histogram_keeps_v4_percentile_and_empty_shapes() {
    let empty = HistogramMetric::new("empty");
    assert_eq!(empty.value()["count"], json!(0));
    let values = HistogramMetric::with_values("latency", vec![9.0, 1.0, 5.0, 3.0, 7.0]);
    let stats = values.stats();
    assert_eq!(
        (stats.min, stats.p50, stats.p95, stats.max),
        (1.0, 5.0, 9.0, 9.0)
    );
}

#[test]
fn collector_reports_registered_metrics_and_saves_json() {
    let mut collector = MetricsCollector::empty();
    assert!(collector.elapsed().is_none());
    collector.print();
    collector.register_all(vec![
        Box::new(CounterMetric::new("fresh")) as Box<dyn Metric>,
        Box::new(GaugeMetric::new("load", 1.5).with_description("machine load")),
    ]);
    collector.increment_counter("fresh", 2);
    collector.increment_counter("load", 1);
    assert_eq!(collector.snapshot()["fresh"], json!(2));
    assert_eq!(collector.snapshot()["load"], json!(1.5));
    assert!(format!("{collector:?}").contains("metric_count: 2"));
    collector.record_start();
    collector.record_end();
    collector.print();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("metrics.json");
    collector.save_to_file(path.to_str().unwrap()).unwrap();
    let saved: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(saved["fresh"]["value"], json!(2));
    assert_eq!(saved["load"]["description"], json!("machine load"));
    assert!(
        collector
            .save_to_file(dir.path().join("missing/metrics.json").to_str().unwrap())
            .is_err()
    );

    let mut histogram = HistogramMetric::new("samples").with_description("sample values");
    histogram.record(3.0);
    histogram.record(1.0);
    assert_eq!(histogram.value()["p50"], json!(3.0));
    assert_eq!(histogram.description(), Some("sample values"));
    assert!(histogram.as_any().is::<HistogramMetric>());
    assert!(GaugeMetric::new("gauge", 1.0).as_any().is::<GaugeMetric>());
    assert_eq!(MetricsCollector::default().snapshot().len(), 0);
}
