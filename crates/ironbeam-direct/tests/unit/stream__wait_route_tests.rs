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

    for operation in [
        Operation::Map,
        Operation::Filter,
        Operation::FlatMap,
        Operation::AssignTimestamp,
        Operation::FixedWindow,
        Operation::SlidingWindow,
    ] {
        let mut downstream = graph.clone();
        downstream
            .nodes
            .iter_mut()
            .find(|node| node.key == "result")
            .unwrap()
            .operation = operation;
        assert_eq!(
            multi_wait_stream_route(&downstream, "result")
                .unwrap()
                .after_wait,
            ["result"]
        );
    }

    let mut two_downstream_stages = graph.clone();
    let mut stage = two_downstream_stages
        .nodes
        .iter()
        .find(|node| node.key == "result")
        .unwrap()
        .clone();
    stage.key = "after-map".into();
    stage.operation = Operation::Map;
    two_downstream_stages
        .nodes
        .iter_mut()
        .find(|node| node.key == "result")
        .unwrap()
        .input = Some("after-map".into());
    two_downstream_stages.nodes.push(stage);
    assert_eq!(
        multi_wait_stream_route(&two_downstream_stages, "result")
            .unwrap()
            .after_wait,
        ["after-map"]
    );

    let mut extra_downstream_input = graph.clone();
    let extra_edge = extra_downstream_input
        .nodes
        .iter()
        .find(|node| node.key == "wait")
        .unwrap()
        .additional_inputs[0]
        .clone();
    extra_downstream_input
        .nodes
        .iter_mut()
        .find(|node| node.key == "result")
        .unwrap()
        .additional_inputs
        .push(extra_edge);
    assert!(multi_wait_stream_route(&extra_downstream_input, "result").is_none());

    assert!(multi_wait_stream_route(&graph, "missing-result").is_none());

    let mut no_downstream_input = graph.clone();
    no_downstream_input
        .nodes
        .iter_mut()
        .find(|node| node.key == "result")
        .unwrap()
        .input = None;
    assert!(multi_wait_stream_route(&no_downstream_input, "result").is_none());

    let mut no_main = graph.clone();
    no_main
        .nodes
        .iter_mut()
        .find(|node| node.key == "wait")
        .unwrap()
        .input = None;
    assert!(multi_wait_stream_route(&no_main, "result").is_none());

    let mut unknown_side = graph.clone();
    unknown_side
        .nodes
        .iter_mut()
        .find(|node| node.key == "wait")
        .unwrap()
        .additional_inputs[0]
        .key = "missing-side".into();
    assert!(multi_wait_stream_route(&unknown_side, "result").is_none());

    let mut repeated_main = graph.clone();
    repeated_main
        .nodes
        .iter_mut()
        .find(|node| node.key == "wait")
        .unwrap()
        .additional_inputs[0]
        .key = "main-window".into();
    assert!(multi_wait_stream_route(&repeated_main, "result").is_none());

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
