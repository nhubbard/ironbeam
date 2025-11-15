//! Tests for pipeline graph functionality.

use ironbeam::testing::*;
use ironbeam::*;

#[test]
fn test_pipeline_default() {
    let p = TestPipeline::new();
    let (nodes, edges) = p.snapshot();

    assert!(nodes.is_empty());
    assert!(edges.is_empty());
}

#[test]
fn test_pipeline_clone() {
    let p1 = TestPipeline::new();
    let _data = from_vec(&p1, vec![1, 2, 3]);

    let p2 = p1.clone();
    let (nodes1, edges1) = p1.snapshot();
    let (nodes2, edges2) = p2.snapshot();

    // Both should see the same nodes since they share the inner state
    assert_eq!(nodes1.len(), nodes2.len());
    assert_eq!(edges1.len(), edges2.len());
}

#[test]
fn test_pipeline_node_insertion() {
    let p = TestPipeline::new();

    let data = from_vec(&p, vec![1, 2, 3, 4, 5]);
    let _mapped = data.map(|x: &i32| x * 2);

    let (nodes, _) = p.snapshot();

    // Should have at least 2 nodes: source and map
    assert!(nodes.len() >= 2);
}

#[test]
fn test_pipeline_edges() {
    let p = TestPipeline::new();

    let data = from_vec(&p, vec![1, 2, 3]);
    let mapped = data.map(|x: &i32| x * 2);
    let _filtered = mapped.filter(|x: &i32| *x > 2);

    let (_, edges) = p.snapshot();

    // Should have edges connecting the nodes
    assert!(!edges.is_empty());
}

#[test]
fn test_pipeline_complex_graph() {
    let p = TestPipeline::new();

    let data = from_vec(&p, vec![1, 2, 3, 4, 5]);
    let mapped = data.map(|x: &i32| x * 2);
    let keyed = mapped.key_by(|x: &i32| format!("k{}", x % 2));
    let _grouped = keyed.group_by_key();

    let (nodes, edges) = p.snapshot();

    // Should have multiple nodes and edges
    assert!(nodes.len() >= 4);
    assert!(edges.len() >= 3);
}

#[test]
fn test_multiple_sources() {
    let p = TestPipeline::new();

    let _data1 = from_vec(&p, vec![1, 2, 3]);
    let _data2 = from_vec(&p, vec![4, 5, 6]);
    let _data3 = from_vec(&p, vec![7, 8, 9]);

    let (nodes, _) = p.snapshot();

    // Should have multiple source nodes
    assert!(nodes.len() >= 3);
}

#[test]
fn test_pipeline_join_creates_edges() {
    let p = TestPipeline::new();

    let left = from_vec(&p, vec![("a".to_string(), 1), ("b".to_string(), 2)]);
    let right = from_vec(&p, vec![("a".to_string(), 10), ("c".to_string(), 30)]);
    let _joined = left.join_inner(&right);

    let (nodes, edges) = p.snapshot();

    // Join should create additional nodes and edges
    assert!(nodes.len() >= 3);
    assert!(!edges.is_empty());
}

#[cfg(feature = "metrics")]
mod metrics_tests {
    use super::*;
    use ironbeam::metrics::{CounterMetric, MetricsCollector};

    #[test]
    fn test_pipeline_metrics_set_and_get() {
        let p = TestPipeline::new();

        let mut metrics = MetricsCollector::new();
        metrics.register(Box::new(CounterMetric::with_value("test", 42)));

        p.set_metrics(metrics);

        let retrieved = p.get_metrics();
        assert!(retrieved.is_some());

        let snapshot = retrieved.unwrap().snapshot();
        assert_eq!(snapshot.get("test").unwrap(), &serde_json::json!(42));
    }

    #[test]
    fn test_pipeline_metrics_take() {
        let p = TestPipeline::new();

        let mut metrics = MetricsCollector::new();
        metrics.register(Box::new(CounterMetric::with_value("counter", 100)));

        p.set_metrics(metrics);

        // First take should succeed
        let taken = p.take_metrics();
        assert!(taken.is_some());

        // Second take should return None
        let taken_again = p.take_metrics();
        assert!(taken_again.is_none());
    }

    #[test]
    fn test_pipeline_record_metrics() {
        use std::thread;
        use std::time::Duration;

        let p = TestPipeline::new();
        let metrics = MetricsCollector::new();
        p.set_metrics(metrics);

        // Simulate pipeline execution
        p.record_metrics_start();
        thread::sleep(Duration::from_millis(10));
        p.record_metrics_end();

        let retrieved = p.get_metrics().unwrap();
        let elapsed = retrieved.elapsed();
        assert!(elapsed.is_some());
        assert!(elapsed.unwrap().as_millis() >= 10);
    }

    #[test]
    fn test_pipeline_metrics_integration() {
        let p = TestPipeline::new();
        let metrics = MetricsCollector::new();
        p.set_metrics(metrics);

        let data = from_vec(&p, vec![1, 2, 3, 4, 5]);
        let result = data.collect_seq().unwrap();

        assert_collections_equal(&result, &[1, 2, 3, 4, 5]);

        let metrics = p.take_metrics();
        assert!(metrics.is_some());
    }

    #[test]
    fn test_pipeline_without_metrics() {
        let p = TestPipeline::new();

        // Should work fine without metrics
        let data = from_vec(&p, vec![1, 2, 3]);
        let result = data.collect_seq().unwrap();

        assert_collections_equal(&result, &[1, 2, 3]);
        assert!(p.take_metrics().is_none());
    }

    #[test]
    fn test_metrics_record_without_set() {
        let p = TestPipeline::new();

        // These should not panic even if metrics are not set
        p.record_metrics_start();
        p.record_metrics_end();

        assert!(p.get_metrics().is_none());
    }

    #[test]
    fn test_pipeline_metrics_clone() {
        let p1 = TestPipeline::new();
        let mut metrics = MetricsCollector::new();
        metrics.register(Box::new(CounterMetric::with_value("shared", 999)));
        p1.set_metrics(metrics);

        let p2 = p1.clone();

        // Both should see the same metrics
        let m1 = p1.get_metrics().unwrap();
        let m2 = p2.get_metrics().unwrap();

        assert_eq!(
            m1.snapshot().get("shared").unwrap(),
            &serde_json::json!(999)
        );
        assert_eq!(
            m2.snapshot().get("shared").unwrap(),
            &serde_json::json!(999)
        );
    }
}

#[test]
fn test_pipeline_snapshot_is_independent() {
    let p = TestPipeline::new();
    let _data = from_vec(&p, vec![1, 2, 3]);

    let (nodes1, edges1) = p.snapshot();

    // Add more nodes
    let data2 = from_vec(&p, vec![4, 5, 6]);
    let _mapped = data2.map(|x: &i32| x * 2);

    let (nodes2, edges2) = p.snapshot();

    // Second snapshot should have more nodes
    assert!(nodes2.len() > nodes1.len());
    assert!(edges2.len() >= edges1.len());
}

#[test]
fn test_pipeline_node_ids_increment() {
    let p = TestPipeline::new();

    let d1 = from_vec(&p, vec![1]);
    let d2 = from_vec(&p, vec![2]);
    let d3 = from_vec(&p, vec![3]);

    // Node IDs should be different and incrementing
    assert_ne!(d1.node_id(), d2.node_id());
    assert_ne!(d2.node_id(), d3.node_id());
    assert_ne!(d1.node_id(), d3.node_id());
}

#[test]
fn test_pipeline_large_graph() {
    let p = TestPipeline::new();

    let mut current = from_vec(&p, vec![1, 2, 3, 4, 5]);

    // Build a chain of 100 transformations
    for i in 0..100 {
        current = current.map(move |x: &i32| x + i);
    }

    let (nodes, edges) = p.snapshot();

    assert!(nodes.len() >= 100);
    assert!(edges.len() >= 99); // At least n-1 edges for a chain
}

#[test]
fn test_pipeline_parallel_branches() {
    let p = TestPipeline::new();

    let source = from_vec(&p, vec![1, 2, 3, 4, 5]);

    // Create two independent branches
    let branch1 = source.clone().map(|x: &i32| x * 2);
    let branch2 = source.map(|x: &i32| x * 3);

    let _result1 = branch1.collect_seq().unwrap();
    let _result2 = branch2.collect_seq().unwrap();

    let (nodes, _) = p.snapshot();

    // Should have nodes for source + both branches
    assert!(nodes.len() >= 3);
}
