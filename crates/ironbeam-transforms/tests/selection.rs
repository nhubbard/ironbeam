use ironbeam_core::Pipeline;
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_transforms::{KeyedSelectionExt, SelectionExt};

#[test]
fn top_and_bottom_k_match_v4_order_and_edge_cases() {
    let pipeline = Pipeline::default();
    let values = pipeline
        .from_vec("values", vec![5_i32, 2, 8, 1, 9, 3])
        .unwrap();
    let top = values
        .top_k_globally_named("top", 3)
        .unwrap()
        .output("top-out")
        .unwrap();
    let bottom = values
        .bottom_k_globally_named("bottom", 3)
        .unwrap()
        .output("bottom-out")
        .unwrap();
    let zero = values
        .top_k_globally_named("zero", 0)
        .unwrap()
        .output("zero-out")
        .unwrap();
    for parallelism in [1, 3] {
        let cfg = JobConfig {
            parallelism,
            ..JobConfig::default()
        };
        assert_eq!(
            DirectRunner.run_collect(&top, &cfg).unwrap().0,
            [vec![9, 8, 5]]
        );
        assert_eq!(
            DirectRunner.run_collect(&bottom, &cfg).unwrap().0,
            [vec![1, 2, 3]]
        );
        assert_eq!(
            DirectRunner.run_collect(&zero, &cfg).unwrap().0,
            [Vec::<i32>::new()]
        );
    }
}

#[test]
fn keyed_top_k_matches_v4_per_key_values() {
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec(
            "input",
            vec![
                ("a".to_string(), 5_i32),
                ("a".into(), 2),
                ("b".into(), 10),
                ("a".into(), 8),
                ("b".into(), 7),
            ],
        )
        .unwrap();
    let top = input
        .top_k_per_key_named("top-group", "top", 2)
        .unwrap()
        .output("top-out")
        .unwrap();
    let bottom = input
        .bottom_k_per_key_named("bottom-group", "bottom", 2)
        .unwrap()
        .output("bottom-out")
        .unwrap();
    let cfg = JobConfig::default();
    assert_eq!(
        DirectRunner.run_collect(&top, &cfg).unwrap().0,
        [("a".to_string(), vec![8, 5]), ("b".into(), vec![10, 7])]
    );
    assert_eq!(
        DirectRunner.run_collect(&bottom, &cfg).unwrap().0,
        [("a".to_string(), vec![2, 5]), ("b".into(), vec![7, 10])]
    );
}

#[test]
fn top_k_streams_beyond_materialization_budget() {
    let pipeline = Pipeline::default();
    let values = pipeline
        .from_vec("large", (0_i32..10_000).collect())
        .unwrap();
    let top = values
        .top_k_globally_named("streamed-top", 4)
        .unwrap()
        .output("streamed-top-out")
        .unwrap();
    let keyed = values
        .map_named("streamed-keys", |value| (value % 2, value))
        .unwrap()
        .bottom_k_per_key_named("streamed-key-fold", "streamed-key-identity", 2)
        .unwrap()
        .output("streamed-key-out")
        .unwrap();
    let config = JobConfig {
        memory_budget_bytes: 256,
        buffer_budget_elements: 13,
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner.run_collect(&top, &config).unwrap().0,
        vec![vec![9999, 9998, 9997, 9996]]
    );
    assert_eq!(
        DirectRunner.run_collect(&keyed, &config).unwrap().0,
        vec![(0, vec![0, 2]), (1, vec![1, 3])]
    );
}
