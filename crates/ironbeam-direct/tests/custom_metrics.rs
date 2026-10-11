#![cfg(feature = "metrics")]

use ironbeam_core::{Error, ErrorKind, Pipeline, Replay, SinkMode, SinkSpec, TypedSink};
use ironbeam_direct::{DirectCollectExt, DirectRunner, JobConfig, JobStatus};
use ironbeam_metrics::MetricsCollector;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[test]
fn custom_metrics_follow_a_successful_direct_job() {
    let collector = MetricsCollector::new();
    let updates = collector.clone();
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", (0..100_i32).collect())
        .unwrap()
        .map_named("count", move |value| {
            updates.increment_counter("mapped", 1);
            value * 2
        })
        .unwrap();
    let (values, job) = output
        .collect_with(&JobConfig {
            metrics_collector: Some(collector.clone()),
            ..JobConfig::default()
        })
        .unwrap();
    assert_eq!(values.len(), 100);
    assert_eq!(job.status(), JobStatus::Succeeded);
    assert_eq!(job.metrics().source_elements, 100);
    assert_eq!(
        job.custom_metrics().unwrap().snapshot()["mapped"],
        json!(100)
    );
    assert!(collector.elapsed().is_some());
    assert!(collector.to_json()["execution_time_ms"]["value"].is_number());
}

#[test]
fn failed_direct_job_still_closes_its_custom_metrics_interval() {
    let collector = MetricsCollector::new();
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", vec![1_i32, 2, 3])
        .unwrap()
        .map_named("failure", |value| {
            assert!(value != 2, "expected failure");
            value
        })
        .unwrap();
    let error = output
        .collect_with(&JobConfig {
            metrics_collector: Some(collector.clone()),
            ..JobConfig::default()
        })
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert!(collector.elapsed().is_some());
}

#[test]
fn custom_metrics_follow_a_committed_sink_job() {
    struct CountingSink(Arc<AtomicUsize>);
    impl TypedSink<i32> for CountingSink {
        fn write(&mut self, _: i32) -> Result<(), Error> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }
    let written = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .from_vec("source", vec![1_i32, 2, 3])
        .unwrap()
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::FinalResult,
                replay: Replay::Idempotent,
                commit: true,
            },
            {
                let written = Arc::clone(&written);
                move || Ok(Box::new(CountingSink(Arc::clone(&written))))
            },
        )
        .unwrap();
    let collector = MetricsCollector::new();
    let job = DirectRunner
        .run_sink(
            &sink,
            &JobConfig {
                metrics_collector: Some(collector.clone()),
                ..JobConfig::default()
            },
        )
        .unwrap();
    assert_eq!(job.status(), JobStatus::Succeeded);
    assert_eq!(written.load(Ordering::Relaxed), 3);
    assert!(collector.elapsed().is_some());
}
