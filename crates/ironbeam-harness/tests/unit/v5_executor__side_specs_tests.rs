use super::*;

#[test]
fn worker_rejects_a_mapped_side_spec_that_differs_from_its_graph() {
    let pipeline = Pipeline::default();
    let main = pipeline
        .from_vec("main-source", vec![1_i32])
        .unwrap()
        .fixed_windows("main-windows", 4_000_000)
        .unwrap();
    let side = pipeline
        .from_vec("side-source", vec![2_i32])
        .unwrap()
        .sliding_windows("side-windows", 10_000_000, 5_000_000, 0)
        .unwrap();
    main.map_with_mapped_window_side_named("mapped", &side, |value, values| {
        value + values.iter().sum::<i32>()
    })
    .unwrap();

    let worker = V5BundleExecutor::new(&pipeline, "side-spec-test").unwrap();
    let node = worker.graph.node("mapped").unwrap();
    let expected = expected_side_input_specs(&worker.graph, node).unwrap();
    let main_pc = collection_id("main-windows");
    let output_pc = collection_id("mapped");
    let plan = BundlePlan {
        steps: vec![
            Step::Source {
                transform_id: "runner-source".into(),
                port: Default::default(),
                output_pc: main_pc.clone(),
                wire: SourceWire::Opaque,
            },
            Step::V5ParDo {
                transform_id: "mapped-transform".into(),
                node_ref: ironbeam_graphx::v5::V5NodeRef {
                    key: "mapped".into(),
                    identity: worker.identity.clone(),
                    co_group_input: None,
                },
                input_pc: main_pc,
                output_pc: output_pc.clone(),
                stateful_value: false,
            },
            Step::Sink {
                transform_id: "runner-sink".into(),
                port: Default::default(),
                input_pc: output_pc,
                wire: SinkWire::Opaque,
            },
        ],
        pbd_id: "mapped-side-spec".into(),
        state_endpoint: Some("http://runner-state".into()),
        side_inputs: HashMap::from([(
            "mapped".into(),
            HashMap::from([("side".into(), collection_id("side-windows"))]),
        )]),
        side_specs: HashMap::from([("mapped".into(), expected)]),
    };
    assert!(worker.validate(&plan).is_ok());

    let mut tampered = plan;
    let mapping = tampered.side_specs.get_mut("mapped").unwrap().get_mut("side").unwrap()
        .window_mapping_fn.as_mut().unwrap();
    mapping.payload = ironbeam_graphx::windowing::sliding_window_fn(
        20_000_000,
        5_000_000,
        0,
    ).unwrap().payload;
    let error = worker.validate(&tampered).unwrap_err();
    assert!(error.to_string().contains("side input mapped differs"), "{error}");
}
