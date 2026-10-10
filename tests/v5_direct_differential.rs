//! First Phase 3 differential case: the same bounded elementwise corpus on v4 and v5.
#![cfg(feature = "v5-preview")]

use ironbeam::v5::{DirectRunner, JobConfig, Pipeline as V5Pipeline};
use ironbeam::{Pipeline as V4Pipeline, from_vec};

#[test]
fn bounded_elementwise_matches_v4_in_sequential_and_parallel_modes() {
    for input in [vec![], vec![1], vec![1, 2, 3, 4, 5, 6]] {
        let v4 = V4Pipeline::default();
        let expected = from_vec(&v4, input.clone())
            .map(|value| value * 2)
            .filter(|value| value % 4 == 0)
            .collect_seq()
            .unwrap();

        let v5 = V5Pipeline::default();
        let output = v5
            .from_vec("source", input.clone())
            .unwrap()
            .map_named("double/v1", |value| value * 2)
            .unwrap()
            .filter_named("divisible-by-four/v1", |value| value % 4 == 0)
            .unwrap()
            .output("result")
            .unwrap();
        let (actual, _) = DirectRunner
            .run_collect(
                &output,
                &JobConfig {
                    parallelism: 1,
                    ..JobConfig::default()
                },
            )
            .unwrap();
        assert_eq!(actual, expected);

        let v4 = V4Pipeline::default();
        let mut expected_parallel = from_vec(&v4, input)
            .map(|value| value * 2)
            .filter(|value| value % 4 == 0)
            .collect_par(Some(3), Some(2))
            .unwrap();
        expected_parallel.sort_unstable();
        let (mut actual_parallel, _) = DirectRunner
            .run_collect(
                &output,
                &JobConfig {
                    parallelism: 3,
                    ..JobConfig::default()
                },
            )
            .unwrap();
        actual_parallel.sort_unstable();
        assert_eq!(actual_parallel, expected_parallel);
    }
}
