// Unit tests for the multi-source route parser in src/stream.rs.
use super::multi_wait_stream_route;
use ironbeam_core::{Graph, Operation, Pipeline, TestStreamEvent};

fn wait_graph() -> Graph {
    let pipeline = Pipeline::default();
    let main = pipeline
        .test_stream_source::<i32>("main", vec![TestStreamEvent::AdvanceWatermark(10)])
        .unwrap()
        .fixed_windows("main-window", 10)
        .unwrap();
    let first = pipeline
        .test_stream_source::<i32>("first", vec![TestStreamEvent::AdvanceWatermark(10)])
        .unwrap()
        .fixed_windows("first-window", 10)
        .unwrap();
    let second = pipeline
        .test_stream_source::<i32>("second", vec![TestStreamEvent::AdvanceWatermark(10)])
        .unwrap()
        .fixed_windows("second-window", 10)
        .unwrap();
    main.wait_on_all_windowed_named("wait", &[first, second])
        .unwrap()
        .output("result")
        .unwrap();
    pipeline.snapshot().unwrap().0
}

#[test]
fn independent_wait_route_keeps_each_source_and_rejects_unsafe_shapes() {
    let graph = wait_graph();
    let route = multi_wait_stream_route(&graph, "result").unwrap();
    assert_eq!(
        route
            .sources
            .iter()
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>(),
        ["main", "first", "second"]
    );

    let mut repeated = graph.clone();
    repeated
        .nodes
        .iter_mut()
        .find(|node| node.key == "wait")
        .unwrap()
        .additional_inputs[1]
        .key = "first-window".into();
    assert!(multi_wait_stream_route(&repeated, "result").is_none());

    let mut missing = graph.clone();
    missing
        .nodes
        .iter_mut()
        .find(|node| node.key == "wait")
        .unwrap()
        .additional_inputs
        .pop();
    assert!(multi_wait_stream_route(&missing, "result").is_none());

    let mut downstream = graph;
    downstream
        .nodes
        .iter_mut()
        .find(|node| node.key == "result")
        .unwrap()
        .operation = Operation::Reshuffle;
    assert!(multi_wait_stream_route(&downstream, "result").is_none());
}
