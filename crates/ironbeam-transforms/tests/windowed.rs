use ironbeam_core::{Pipeline, WindowInterval};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_transforms::{KeyedWindowedExt, WindowedExt};

#[test]
fn windowed_numeric_helpers_match_v4_shapes() {
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec("values", vec![1_i32, 2, 11])
        .unwrap()
        .assign_timestamps_named("times", |value| i64::from(*value))
        .unwrap();
    let sum = input
        .sum_per_window_named("sum", 10, 0)
        .unwrap()
        .output("sum-out")
        .unwrap();
    let count = input
        .count_per_window_named("count", 10, 0)
        .unwrap()
        .output("count-out")
        .unwrap();
    let min = input
        .min_per_window_named("min", 10, 0)
        .unwrap()
        .output("min-out")
        .unwrap();
    let max = input
        .max_per_window_named("max", 10, 0)
        .unwrap()
        .output("max-out")
        .unwrap();
    let average = input
        .average_per_window_named("average", 10, 0)
        .unwrap()
        .output("average-out")
        .unwrap();
    let keyed = input
        .map_named("keys", |value| (value.rem_euclid(2), value))
        .unwrap();
    let keyed_sum = keyed
        .sum_per_key_and_window_named("keyed-sum", 10, 0)
        .unwrap()
        .output("keyed-sum-out")
        .unwrap();
    let keyed_count = keyed
        .count_per_key_and_window_named("keyed-count", 10, 0)
        .unwrap()
        .output("keyed-count-out")
        .unwrap();
    let keyed_min = keyed
        .min_per_key_and_window_named("keyed-min", 10, 0)
        .unwrap()
        .output("keyed-min-out")
        .unwrap();
    let keyed_max = keyed
        .max_per_key_and_window_named("keyed-max", 10, 0)
        .unwrap()
        .output("keyed-max-out")
        .unwrap();
    let keyed_average = keyed
        .average_per_key_and_window_named("keyed-average", 10, 0)
        .unwrap()
        .output("keyed-average-out")
        .unwrap();
    let first = WindowInterval {
        start_ns: 0,
        end_ns: 10,
    };
    let second = WindowInterval {
        start_ns: 10,
        end_ns: 20,
    };
    for parallelism in [1, 3] {
        let config = JobConfig {
            parallelism,
            ..JobConfig::default()
        };
        assert_eq!(
            DirectRunner.run_collect(&sum, &config).unwrap().0,
            vec![(first, 3), (second, 11)]
        );
        assert_eq!(
            DirectRunner.run_collect(&count, &config).unwrap().0,
            vec![(first, 2), (second, 1)]
        );
        assert_eq!(
            DirectRunner.run_collect(&min, &config).unwrap().0,
            vec![(first, 1), (second, 11)]
        );
        assert_eq!(
            DirectRunner.run_collect(&max, &config).unwrap().0,
            vec![(first, 2), (second, 11)]
        );
        assert_eq!(
            DirectRunner.run_collect(&average, &config).unwrap().0,
            vec![(first, 1.5), (second, 11.0)]
        );
        assert_eq!(
            DirectRunner.run_collect(&keyed_sum, &config).unwrap().0,
            vec![((1, first), 1), ((0, first), 2), ((1, second), 11)]
        );
        assert_eq!(
            DirectRunner.run_collect(&keyed_count, &config).unwrap().0,
            vec![((1, first), 1), ((0, first), 1), ((1, second), 1)]
        );
        assert_eq!(
            DirectRunner.run_collect(&keyed_min, &config).unwrap().0,
            vec![((1, first), 1), ((0, first), 2), ((1, second), 11)]
        );
        assert_eq!(
            DirectRunner.run_collect(&keyed_max, &config).unwrap().0,
            vec![((1, first), 1), ((0, first), 2), ((1, second), 11)]
        );
        assert_eq!(
            DirectRunner.run_collect(&keyed_average, &config).unwrap().0,
            vec![((1, first), 1.0), ((0, first), 2.0), ((1, second), 11.0)]
        );
    }
    assert!(input.sum_per_window_named("invalid", 0, 0).is_err());
}
