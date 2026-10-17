//! Bounded v4/v5 differential corpus against published Ironbeam 4.1.1.

use ironbeam_compat::prelude::*;
use ironbeam_core::Pipeline as V5Pipeline;
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig, SpillConfig};
use ironbeam_v4::{Pipeline as V4Pipeline, from_vec};

#[test]
fn bounded_elementwise_matches_v4_in_sequential_and_parallel_modes() {
    for input in [vec![], vec![1], vec![1, 2, 3, 4, 5, 6], (0..257).collect()] {
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
                    buffer_budget_elements: 7,
                    ..JobConfig::default()
                },
            )
            .unwrap();
        actual_parallel.sort_unstable();
        assert_eq!(actual_parallel, expected_parallel);
    }
}

#[test]
fn string_type_change_spill_and_checkpoint_match_v4() {
    let input = vec![
        "a".to_string(),
        "bb".to_string(),
        "ccc".to_string(),
        "dddd".to_string(),
    ];
    let v4 = V4Pipeline::default();
    let expected = from_vec(&v4, input.clone())
        .map(|value| i32::try_from(value.len()).unwrap())
        .filter(|length| *length % 2 == 0)
        .collect_seq()
        .unwrap();
    let v5 = V5Pipeline::default();
    let output = v5
        .from_vec("source", input)
        .unwrap()
        .map_named("length/v1", |value| i32::try_from(value.len()).unwrap())
        .unwrap()
        .filter_named("even/v1", |length| length % 2 == 0)
        .unwrap()
        .output("out")
        .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let config = JobConfig {
        parallelism: 3,
        buffer_budget_elements: 2,
        memory_budget_bytes: 4,
        spill: Some(SpillConfig {
            directory: temp.path().join("spill"),
            threshold_bytes: 4,
        }),
        checkpoint: Some(CheckpointConfig {
            directory: temp.path().join("checkpoint"),
            binary_build_id: "differential-build-1".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let (actual, first) = DirectRunner.run_collect(&output, &config).unwrap();
    assert_eq!(actual, expected);
    assert!(first.metrics().spill_files > 0);
    assert!(first.metrics().checkpoints > 0);
    let (recovered, second) = DirectRunner.run_collect(&output, &config).unwrap();
    assert_eq!(recovered, expected);
    assert_eq!(second.metrics().recovered_elements, expected.len() as u64);
}

#[test]
fn grouping_and_join_multisets_match_v4_across_bounded_inputs() {
    for seed in 0_i32..24 {
        let left_rows: Vec<(i32, i32)> = (0..(seed % 11))
            .map(|index| ((index * 7 + seed) % 5, index % 4))
            .collect();
        let right_rows: Vec<(i32, i32)> = (0..((seed * 3) % 13))
            .map(|index| ((index * 3 + seed) % 5, index % 3))
            .collect();

        let v4 = V4Pipeline::default();
        let left = from_vec(&v4, left_rows.clone());
        let right = from_vec(&v4, right_rows.clone());
        let mut expected_groups = left.clone().group_by_key().collect_seq().unwrap();
        for (_, values) in &mut expected_groups {
            values.sort_unstable();
        }
        expected_groups.sort_unstable();
        let mut expected_inner = left.join_inner(&right).collect_seq().unwrap();
        let mut expected_left = left.join_left(&right).collect_seq().unwrap();
        let mut expected_right = left.join_right(&right).collect_seq().unwrap();
        let mut expected_full = left.join_full(&right).collect_seq().unwrap();
        expected_inner.sort_unstable();
        expected_left.sort_unstable();
        expected_right.sort_unstable();
        expected_full.sort_unstable();

        let v5 = V5Pipeline::default();
        let left = v5.from_vec("left", left_rows).unwrap();
        let right = v5.from_vec("right", right_rows).unwrap();
        let grouped = left
            .group_by_key_named("group")
            .unwrap()
            .output("groups")
            .unwrap();
        let inner = left
            .join_inner_named("inner", &right)
            .unwrap()
            .output("inner-out")
            .unwrap();
        let left_join = left
            .join_left_named("left-join", &right)
            .unwrap()
            .output("left-out")
            .unwrap();
        let right_join = left
            .join_right_named("right-join", &right)
            .unwrap()
            .output("right-out")
            .unwrap();
        let full = left
            .join_full_named("full", &right)
            .unwrap()
            .output("full-out")
            .unwrap();
        for parallelism in [1, 3] {
            let config = JobConfig {
                parallelism,
                buffer_budget_elements: 3,
                ..JobConfig::default()
            };
            let mut groups = DirectRunner.run_collect(&grouped, &config).unwrap().0;
            for (_, values) in &mut groups {
                values.sort_unstable();
            }
            groups.sort_unstable();
            assert_eq!(
                groups, expected_groups,
                "grouping seed {seed}, parallelism {parallelism}"
            );
            let mut actual = DirectRunner.run_collect(&inner, &config).unwrap().0;
            actual.sort_unstable();
            assert_eq!(
                actual, expected_inner,
                "inner seed {seed}, parallelism {parallelism}"
            );
            let mut actual = DirectRunner.run_collect(&left_join, &config).unwrap().0;
            actual.sort_unstable();
            assert_eq!(
                actual, expected_left,
                "left seed {seed}, parallelism {parallelism}"
            );
            let mut actual = DirectRunner.run_collect(&right_join, &config).unwrap().0;
            actual.sort_unstable();
            assert_eq!(
                actual, expected_right,
                "right seed {seed}, parallelism {parallelism}"
            );
            let mut actual = DirectRunner.run_collect(&full, &config).unwrap().0;
            actual.sort_unstable();
            assert_eq!(
                actual, expected_full,
                "full seed {seed}, parallelism {parallelism}"
            );
        }
    }
}

#[test]
fn bounded_selection_and_count_helpers_match_v4() {
    for seed in 0_i32..18 {
        let input = (0..(seed % 12))
            .map(|index| (index * 7 + seed) % 5)
            .collect::<Vec<_>>();
        let limit = usize::try_from(seed % 7).unwrap();

        let v4 = V4Pipeline::default();
        let source = from_vec(&v4, input.clone());
        let expected_take = source.clone().take(limit).collect_seq().unwrap();
        let expected_first = source.clone().first().collect_seq().unwrap();
        let mut expected_distinct = source.clone().distinct().collect_seq().unwrap();
        let mut expected_counts = source.clone().count_per_element().collect_seq().unwrap();
        let expected_unique_count = source.distinct_count_globally().collect_seq().unwrap();
        expected_distinct.sort_unstable();
        expected_counts.sort_unstable();

        let v5 = V5Pipeline::default();
        let source = v5.from_vec("source", input).unwrap();
        let take = source
            .take_named("take", limit)
            .unwrap()
            .output("take-out")
            .unwrap();
        let first = source
            .first_named("first")
            .unwrap()
            .output("first-out")
            .unwrap();
        let distinct = source
            .distinct_named("distinct")
            .unwrap()
            .output("distinct-out")
            .unwrap();
        let counts = source
            .count_per_element_named("counts")
            .unwrap()
            .output("counts-out")
            .unwrap();
        let unique_count = source
            .distinct_count_globally_named("unique-count")
            .unwrap()
            .output("unique-count-out")
            .unwrap();

        let sequential = JobConfig {
            parallelism: 1,
            ..JobConfig::default()
        };
        assert_eq!(
            DirectRunner.run_collect(&take, &sequential).unwrap().0,
            expected_take,
            "take seed {seed}"
        );
        assert_eq!(
            DirectRunner.run_collect(&first, &sequential).unwrap().0,
            expected_first,
            "first seed {seed}"
        );
        for parallelism in [1, 3] {
            let config = JobConfig {
                parallelism,
                buffer_budget_elements: 3,
                ..JobConfig::default()
            };
            let mut actual_distinct = DirectRunner.run_collect(&distinct, &config).unwrap().0;
            actual_distinct.sort_unstable();
            assert_eq!(
                actual_distinct, expected_distinct,
                "distinct seed {seed}, parallelism {parallelism}"
            );
            let mut actual_counts = DirectRunner.run_collect(&counts, &config).unwrap().0;
            actual_counts.sort_unstable();
            assert_eq!(
                actual_counts, expected_counts,
                "counts seed {seed}, parallelism {parallelism}"
            );
            assert_eq!(
                DirectRunner.run_collect(&unique_count, &config).unwrap().0,
                expected_unique_count,
                "unique-count seed {seed}, parallelism {parallelism}"
            );
        }
    }
}
