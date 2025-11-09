use anyhow::Result;
use rustflow::collection::Count;
use rustflow::runner::{ExecMode, Runner};
use rustflow::testing::*;
use rustflow::from_vec;

fn sorted<T: Ord>(mut v: Vec<T>) -> Vec<T> {
    v.sort();
    v
}

/// Test CoGroup operations in sequential mode - this exercises the run_subplan_seq closure
#[test]
fn cogroup_sequential_mode() -> Result<()> {
    let p = TestPipeline::new();

    let left = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("b".to_string(), 2),
            ("a".to_string(), 3),
        ],
    );

    let right = from_vec(
        &p,
        vec![
            ("a".to_string(), 10i32),
            ("c".to_string(), 30),
            ("a".to_string(), 40),
        ],
    );

    let joined = left.join_inner(&right);

    // Use collect_seq to force sequential execution
    let result = sorted(joined.collect_seq()?);
    let expected = sorted(vec![
        ("a".to_string(), (1u32, 10i32)),
        ("a".to_string(), (1u32, 40i32)),
        ("a".to_string(), (3u32, 10i32)),
        ("a".to_string(), (3u32, 40i32)),
    ]);

    assert_kv_collections_equal(result, expected);
    Ok(())
}

/// Test CoGroup with stateless operations in the subplan (sequential)
#[test]
fn cogroup_with_stateless_ops_sequential() -> Result<()> {
    let p = TestPipeline::new();

    let left = from_vec(&p, vec![1u32, 2, 3, 4, 5])
        .map(|x: &u32| x * 2)
        .key_by(|x: &u32| format!("key_{}", x % 2));

    let right = from_vec(&p, vec![10u32, 20, 30])
        .filter(|x: &u32| *x > 10)
        .key_by(|x: &u32| format!("key_{}", x % 2));

    let joined = left.join_inner(&right);
    let result = joined.collect_seq()?;
    assert!(!result.is_empty());
    Ok(())
}

/// Test join with GroupByKey in the subplan (sequential)
#[test]
fn join_with_groupby_in_subplan_sequential() -> Result<()> {
    let p = TestPipeline::new();

    // Test pipeline with GroupByKey before join
    let left = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("a".to_string(), 2u32),
            ("b".to_string(), 3u32),
        ],
    )
    .group_by_key()
    .map_values(|v: &Vec<u32>| v.iter().sum::<u32>());

    let right = from_vec(&p, vec![("a".to_string(), 10u32), ("b".to_string(), 20u32)]);

    let joined = left.join_inner(&right);
    let result = joined.collect_seq()?;
    assert!(!result.is_empty());
    Ok(())
}

/// Test join with CombineValues in the subplan (sequential)
#[test]
fn join_with_combine_values_in_subplan_sequential() -> Result<()> {
    let p = TestPipeline::new();

    let left = from_vec(
        &p,
        vec![
            ("x".to_string(), 1u32),
            ("x".to_string(), 2u32),
            ("y".to_string(), 3u32),
        ],
    )
    .combine_values(Count);

    let right = from_vec(
        &p,
        vec![("x".to_string(), 100u32), ("y".to_string(), 200u32)],
    )
    .combine_values(Count);

    let joined = left.join_inner(&right);
    let result = joined.collect_seq()?;
    assert!(!result.is_empty());
    Ok(())
}

/// Test CoGroup operations in parallel mode - exercises run_subplan_par
#[test]
fn cogroup_parallel_mode() -> Result<()> {
    let p = TestPipeline::new();

    let left = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("b".to_string(), 2),
            ("a".to_string(), 3),
            ("c".to_string(), 4),
        ],
    );

    let right = from_vec(
        &p,
        vec![
            ("a".to_string(), 10i32),
            ("b".to_string(), 20),
            ("c".to_string(), 30),
        ],
    );

    let joined = left.join_inner(&right);

    // Use collect_par with specific partition count
    let result = sorted(joined.collect_par(Some(2), Some(4))?);
    let expected = sorted(vec![
        ("a".to_string(), (1u32, 10i32)),
        ("a".to_string(), (3u32, 10i32)),
        ("b".to_string(), (2u32, 20i32)),
        ("c".to_string(), (4u32, 30i32)),
    ]);

    assert_eq!(result, expected);
    Ok(())
}

/// Test CoGroup with stateless operations in parallel subplan
#[test]
fn cogroup_with_stateless_ops_parallel() -> Result<()> {
    let p = TestPipeline::new();

    let left = from_vec(&p, vec![1u32, 2, 3, 4, 5, 6, 7, 8])
        .map(|x: &u32| x * 2)
        .filter(|x: &u32| *x > 5)
        .key_by(|x: &u32| format!("k{}", x % 3));

    let right = from_vec(&p, vec![10u32, 20, 30, 40])
        .map(|x: &u32| x / 2)
        .key_by(|x: &u32| format!("k{}", x % 3));

    let joined = left.join_full(&right);
    let result = joined.collect_par(None, Some(3))?;
    assert!(!result.is_empty());
    Ok(())
}

/// Test join with GroupByKey in parallel subplan
#[test]
fn join_with_groupby_in_subplan_parallel() -> Result<()> {
    let p = TestPipeline::new();

    let left = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("a".to_string(), 2u32),
            ("b".to_string(), 3u32),
            ("b".to_string(), 4u32),
        ],
    )
    .group_by_key()
    .map_values(|v: &Vec<u32>| v.len() as u32);

    let right = from_vec(
        &p,
        vec![
            ("a".to_string(), 10u32),
            ("b".to_string(), 20u32),
            ("c".to_string(), 30u32),
        ],
    );

    let joined = left.join_inner(&right);
    let result = joined.collect_par(None, Some(2))?;
    assert!(!result.is_empty());
    Ok(())
}

/// Test join with CombineValues in parallel subplan
#[test]
fn join_with_combine_values_in_subplan_parallel() -> Result<()> {
    let p = TestPipeline::new();

    let left = from_vec(
        &p,
        vec![
            ("x".to_string(), 1u32),
            ("x".to_string(), 2u32),
            ("y".to_string(), 3u32),
            ("z".to_string(), 4u32),
        ],
    )
    .combine_values(Count);

    let right = from_vec(
        &p,
        vec![
            ("x".to_string(), 100u32),
            ("y".to_string(), 200u32),
            ("z".to_string(), 300u32),
        ],
    )
    .combine_values(Count);

    let joined = left.join_inner(&right);
    let result = joined.collect_par(None, Some(3))?;
    assert!(!result.is_empty());
    Ok(())
}

/// Test explicit sequential execution mode
#[test]
fn explicit_sequential_mode() -> Result<()> {
    let p = TestPipeline::new();
    let data = from_vec(&p, vec![1u32, 2, 3, 4, 5]);
    let mapped = data.map(|x: &u32| x * 2);

    let result = mapped.collect_seq()?;
    assert_eq!(result, vec![2, 4, 6, 8, 10]);
    Ok(())
}

/// Test sequential mode with GroupByKey
#[test]
fn sequential_mode_with_groupby() -> Result<()> {
    let p = TestPipeline::new();
    let data = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("b".to_string(), 2),
            ("a".to_string(), 3),
        ],
    );
    let grouped = data.group_by_key();

    let mut result = grouped.collect_seq()?;
    result.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(result.len(), 2);
    Ok(())
}

/// Test sequential mode with CombineValues
#[test]
fn sequential_mode_with_combine_values() -> Result<()> {
    let p = TestPipeline::new();
    let data = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("b".to_string(), 2),
            ("a".to_string(), 3),
        ],
    );
    let combined = data.combine_values(Count);

    let mut result = combined.collect_seq()?;
    result.sort();

    assert_eq!(result, vec![("a".to_string(), 2), ("b".to_string(), 1)]);
    Ok(())
}

/// Test CombineGlobal with custom fanout in parallel mode
#[test]
fn combine_global_with_custom_fanout() -> Result<()> {
    let p = TestPipeline::new();

    // Create a large dataset to trigger multi-round merge
    let data: Vec<u32> = (1..=100).collect();
    let pcoll = from_vec(&p, data);

    // Use combine_globally with fanout
    let combined = pcoll.combine_globally(Count, Some(3));

    let result = combined.collect_par(None, Some(8))?;
    assert_eq!(result, vec![100]);
    Ok(())
}

/// Test CombineGlobal with fanout=2 (binary merge tree)
#[test]
fn combine_global_with_fanout_two() -> Result<()> {
    let p = TestPipeline::new();

    let data: Vec<u32> = (1..=50).collect();
    let pcoll = from_vec(&p, data);
    let combined = pcoll.combine_globally(Count, Some(2));

    let result = combined.collect_par(None, Some(8))?;
    assert_eq!(result, vec![50]);
    Ok(())
}

/// Test CombineGlobal with larger fanout
#[test]
fn combine_global_with_large_fanout() -> Result<()> {
    let p = TestPipeline::new();

    let data: Vec<u32> = (1..=1000).collect();
    let pcoll = from_vec(&p, data);
    let combined = pcoll.combine_globally(Count, Some(10));

    let result = combined.collect_par(None, Some(16))?;
    assert_eq!(result, vec![1000]);
    Ok(())
}

/// Test parallel mode with multi-partition output collection
#[test]
fn parallel_multi_partition_collection() -> Result<()> {
    let p = TestPipeline::new();

    // Create enough data to result in multiple partitions at terminal
    let data: Vec<u32> = (1..=100).collect();
    let pcoll = from_vec(&p, data).map(|x: &u32| x * 2);

    let result = pcoll.collect_par(None, Some(5))?;
    assert_eq!(result.len(), 100);

    // Verify all values are doubled
    let mut sorted_result = result;
    sorted_result.sort();
    assert_eq!(sorted_result[0], 2);
    assert_eq!(sorted_result[99], 200);
    Ok(())
}

/// Test Runner::default() uses parallel mode
#[test]
fn runner_default_is_parallel() {
    let runner = Runner::default();
    match runner.mode {
        ExecMode::Parallel { .. } => (),
        _ => panic!("Default runner should use parallel mode"),
    }
    assert!(runner.default_partitions >= 2);
}

/// Test parallel execution with explicit thread count
#[test]
fn parallel_with_explicit_threads() -> Result<()> {
    let p = TestPipeline::new();
    let data = from_vec(&p, vec![1u32, 2, 3, 4, 5]);
    let mapped = data.map(|x: &u32| x * 3);

    let mut result = mapped.collect_par(Some(2), Some(2))?;
    result.sort();
    assert_eq!(result, vec![3, 6, 9, 12, 15]);
    Ok(())
}

/// Test that planner's suggested partitions are honored
#[test]
fn honors_planner_suggested_partitions() -> Result<()> {
    let p = TestPipeline::new();
    let data = from_vec(&p, vec![1u32, 2, 3, 4, 5]);
    let mapped = data.map(|x: &u32| x + 1);

    // Use None for partitions to let planner suggest or use default
    let mut result = mapped.collect_par(None, None)?;
    result.sort();
    assert_eq!(result, vec![2, 3, 4, 5, 6]);
    Ok(())
}

/// Test empty source collection
#[test]
fn empty_source_sequential() -> Result<()> {
    let p = TestPipeline::new();
    let data: Vec<u32> = vec![];
    let pcoll = from_vec(&p, data);

    let result = pcoll.collect_seq()?;
    assert_eq!(result, Vec::<u32>::new());
    Ok(())
}

/// Test empty source collection in parallel
#[test]
fn empty_source_parallel() -> Result<()> {
    let p = TestPipeline::new();
    let data: Vec<u32> = vec![];
    let pcoll = from_vec(&p, data);

    let result = pcoll.collect_par(None, Some(4))?;
    assert_eq!(result, Vec::<u32>::new());
    Ok(())
}

/// Test complex pipeline with multiple stages (sequential)
#[test]
fn complex_pipeline_sequential() -> Result<()> {
    let p = TestPipeline::new();

    let data = from_vec(&p, vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let result = data
        .filter(|x: &u32| *x % 2 == 0)
        .map(|x: &u32| x * 2)
        .key_by(|x: &u32| format!("group_{}", x % 3))
        .group_by_key()
        .map_values(|v: &Vec<u32>| v.len() as u32);

    let output = result.collect_seq()?;
    assert!(!output.is_empty());
    Ok(())
}

/// Test complex pipeline with multiple stages (parallel)
#[test]
fn complex_pipeline_parallel() -> Result<()> {
    let p = TestPipeline::new();

    let data = from_vec(&p, vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let result = data
        .filter(|x: &u32| *x % 2 == 0)
        .map(|x: &u32| x * 2)
        .key_by(|x: &u32| format!("group_{}", x % 3))
        .combine_values(Count);

    let output = result.collect_par(None, Some(4))?;
    assert!(!output.is_empty());
    Ok(())
}

/// Test single element source
#[test]
fn single_element_source() -> Result<()> {
    let p = TestPipeline::new();
    let data = from_vec(&p, vec![42u32]);

    let result = data.collect_par(None, Some(10))?;
    assert_eq!(result, vec![42]);
    Ok(())
}

/// Test CombineGlobal in sequential mode
#[test]
fn combine_global_sequential() -> Result<()> {
    let p = TestPipeline::new();
    let data: Vec<u32> = (1..=20).collect();
    let pcoll = from_vec(&p, data);
    let combined = pcoll.combine_globally(Count, None);

    let result = combined.collect_seq()?;
    assert_eq!(result, vec![20]);
    Ok(())
}

/// Test CombineGlobal with fanout in sequential mode
#[test]
fn combine_global_sequential_with_fanout() -> Result<()> {
    let p = TestPipeline::new();
    let data: Vec<u32> = (1..=50).collect();
    let pcoll = from_vec(&p, data);
    let combined = pcoll.combine_globally(Count, Some(4));

    let result = combined.collect_seq()?;
    assert_eq!(result, vec![50]);
    Ok(())
}

/// Test CombineGlobal with unlimited fanout (usize::MAX)
#[test]
fn combine_global_unlimited_fanout() -> Result<()> {
    let p = TestPipeline::new();
    let data: Vec<u32> = (1..=100).collect();
    let pcoll = from_vec(&p, data);
    // None translates to usize::MAX fanout in the runner
    let combined = pcoll.combine_globally(Count, None);

    let result = combined.collect_par(None, Some(8))?;
    assert_eq!(result, vec![100]);
    Ok(())
}

/// Test runner with custom mode configuration
#[test]
fn custom_runner_mode() -> Result<()> {
    use rustflow::runner::{ExecMode, Runner};

    let p = TestPipeline::new();
    let data = from_vec(&p, vec![1u32, 2, 3, 4, 5]);
    let mapped = data.map(|x: &u32| x * 2);

    let runner = Runner {
        mode: ExecMode::Sequential,
        default_partitions: 4,
        #[cfg(feature = "checkpointing")]
        checkpoint_config: None,
    };

    let result = runner.run_collect::<u32>(&p, mapped.node_id())?;
    let expected: Vec<u32> = vec![2, 4, 6, 8, 10];
    assert_eq!(result, expected);
    Ok(())
}

/// Test parallel execution with very small partition count
#[test]
fn parallel_minimal_partitions() -> Result<()> {
    let p = TestPipeline::new();
    let data = from_vec(&p, vec![1u32, 2, 3, 4, 5]);
    let mapped = data.map(|x: &u32| x + 10);

    let result = mapped.collect_par(None, Some(1))?;
    assert_eq!(result.len(), 5);
    Ok(())
}

/// Test empty CoGroup in sequential mode
#[test]
fn empty_cogroup_sequential() -> Result<()> {
    let p = TestPipeline::new();
    let left: Vec<(String, u32)> = vec![];
    let right = from_vec(&p, vec![("a".to_string(), 10i32)]);

    let left_coll = from_vec(&p, left);
    let joined = left_coll.join_inner(&right);

    let result = joined.collect_seq()?;
    assert_eq!(result, Vec::<(String, (u32, i32))>::new());
    Ok(())
}

/// Test empty CoGroup in parallel mode
#[test]
fn empty_cogroup_parallel() -> Result<()> {
    let p = TestPipeline::new();
    let left = from_vec(&p, vec![("a".to_string(), 1u32)]);
    let right: Vec<(String, i32)> = vec![];

    let right_coll = from_vec(&p, right);
    let joined = left.join_inner(&right_coll);

    let result = joined.collect_par(None, Some(2))?;
    assert_eq!(result, Vec::<(String, (u32, i32))>::new());
    Ok(())
}

/// Test materialized terminal node
#[test]
fn materialized_terminal() -> Result<()> {
    let p = TestPipeline::new();
    let data = from_vec(&p, vec![1u32, 2, 3, 4, 5]);

    // Process and collect
    let result = data.collect_seq()?;
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
    Ok(())
}

/// Test large dataset with many partitions
#[test]
fn large_dataset_many_partitions() -> Result<()> {
    let p = TestPipeline::new();
    let data: Vec<u32> = (1..=10000).collect();
    let pcoll = from_vec(&p, data).filter(|x: &u32| *x % 2 == 0);

    let result = pcoll.collect_par(None, Some(32))?;
    assert_eq!(result.len(), 5000);
    Ok(())
}

/// Test chain with multiple barriers
#[test]
fn multiple_barriers_sequential() -> Result<()> {
    let p = TestPipeline::new();
    let data = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("b".to_string(), 2),
            ("a".to_string(), 3),
            ("b".to_string(), 4),
        ],
    );

    let result = data
        .group_by_key()
        .map_values(|v: &Vec<u32>| v.iter().sum::<u32>())
        .group_by_key() // Second barrier
        .collect_seq()?;

    assert_eq!(result.len(), 2);
    Ok(())
}

/// Test chain with multiple barriers in parallel
#[test]
fn multiple_barriers_parallel() -> Result<()> {
    let p = TestPipeline::new();
    let data = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("b".to_string(), 2),
            ("a".to_string(), 3),
            ("c".to_string(), 5),
        ],
    );

    let result = data
        .combine_values(Count)
        .group_by_key() // Second barrier
        .collect_par(None, Some(4))?;

    assert_eq!(result.len(), 3);
    Ok(())
}

/// Test very large fanout value
#[test]
fn very_large_fanout() -> Result<()> {
    let p = TestPipeline::new();
    let data: Vec<u32> = (1..=200).collect();
    let pcoll = from_vec(&p, data);
    let combined = pcoll.combine_globally(Count, Some(50));

    let result = combined.collect_par(None, Some(16))?;
    assert_eq!(result, vec![200]);
    Ok(())
}

/// Test fanout with non-power-of-two values
#[test]
fn fanout_non_power_of_two() -> Result<()> {
    let p = TestPipeline::new();
    let data: Vec<u32> = (1..=75).collect();
    let pcoll = from_vec(&p, data);
    let combined = pcoll.combine_globally(Count, Some(7));

    let result = combined.collect_par(None, Some(10))?;
    assert_eq!(result, vec![75]);
    Ok(())
}

/// Test parallel mode with zero-length partitions after filtering
#[test]
fn parallel_with_aggressive_filter() -> Result<()> {
    let p = TestPipeline::new();
    let data: Vec<u32> = (1..=100).collect();
    let pcoll = from_vec(&p, data).filter(|x: &u32| *x > 95);

    let result = pcoll.collect_par(None, Some(10))?;
    assert_eq!(result.len(), 5);
    assert!(result.iter().all(|&x| x > 95));
    Ok(())
}

// Checkpointing tests - only compiled when checkpointing feature is enabled
#[cfg(feature = "checkpointing")]
mod checkpointing_tests {
    use super::*;
    use rustflow::checkpoint::{CheckpointConfig, CheckpointPolicy};
    use rustflow::runner::{ExecMode, Runner};
    use tempfile::TempDir;

    /// Test sequential execution with checkpointing enabled
    #[test]
    fn sequential_with_checkpointing() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let checkpoint_dir = temp_dir.path().to_path_buf();

        let p = TestPipeline::new();
        let data = from_vec(&p, vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let mapped = data
            .map(|x: &u32| x * 2)
            .key_by(|x: &u32| format!("k{}", x % 3))
            .group_by_key();

        let config = CheckpointConfig {
            enabled: true,
            directory: checkpoint_dir,
            policy: CheckpointPolicy::EveryNNodes(2),
            auto_recover: false,
            max_checkpoints: Some(5),
        };

        let runner = Runner {
            mode: ExecMode::Sequential,
            default_partitions: 4,
            checkpoint_config: Some(config),
        };

        let result = runner.run_collect::<(String, Vec<u32>)>(&p, mapped.node_id())?;
        assert!(!result.is_empty());
        Ok(())
    }

    /// Test parallel execution with checkpointing enabled
    #[test]
    fn parallel_with_checkpointing() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let checkpoint_dir = temp_dir.path().to_path_buf();

        let p = TestPipeline::new();
        let data: Vec<u32> = (1..=50).collect();
        let pcoll = from_vec(&p, data)
            .filter(|x: &u32| *x % 2 == 0)
            .map(|x: &u32| x * 3);

        let config = CheckpointConfig {
            enabled: true,
            directory: checkpoint_dir,
            policy: CheckpointPolicy::EveryNNodes(3),
            auto_recover: false,
            max_checkpoints: Some(10),
        };

        let runner = Runner {
            mode: ExecMode::Parallel {
                threads: Some(2),
                partitions: Some(4),
            },
            default_partitions: 4,
            checkpoint_config: Some(config),
        };

        let result = runner.run_collect::<u32>(&p, pcoll.node_id())?;
        assert_eq!(result.len(), 25);
        Ok(())
    }

    /// Test checkpoint recovery in sequential mode
    #[test]
    fn sequential_checkpoint_recovery() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let checkpoint_dir = temp_dir.path().to_path_buf();

        let p = TestPipeline::new();
        let data: Vec<u32> = (1..=20).collect();
        let pcoll = from_vec(&p, data).map(|x: &u32| x + 1);

        // First run with checkpointing
        let config = CheckpointConfig {
            enabled: true,
            directory: checkpoint_dir.clone(),
            policy: CheckpointPolicy::EveryNNodes(1),
            auto_recover: true,
            max_checkpoints: Some(5),
        };

        let runner = Runner {
            mode: ExecMode::Sequential,
            default_partitions: 4,
            checkpoint_config: Some(config.clone()),
        };

        let _result = runner.run_collect::<u32>(&p, pcoll.node_id())?;

        // Second run with auto-recovery enabled (should detect checkpoints)
        let p2 = TestPipeline::new();
        let data2: Vec<u32> = (1..=20).collect();
        let pcoll2 = from_vec(&p2, data2).map(|x: &u32| x + 1);

        let runner2 = Runner {
            mode: ExecMode::Sequential,
            default_partitions: 4,
            checkpoint_config: Some(config),
        };

        let result2 = runner2.run_collect::<u32>(&p2, pcoll2.node_id())?;
        assert_eq!(result2.len(), 20);
        Ok(())
    }

    /// Test checkpointing with barriers
    #[test]
    fn checkpoint_with_barriers() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let checkpoint_dir = temp_dir.path().to_path_buf();

        let p = TestPipeline::new();
        let data = from_vec(
            &p,
            vec![
                ("a".to_string(), 1u32),
                ("b".to_string(), 2),
                ("a".to_string(), 3),
                ("c".to_string(), 4),
            ],
        );
        let combined = data.combine_values(Count);

        let config = CheckpointConfig {
            enabled: true,
            directory: checkpoint_dir,
            policy: CheckpointPolicy::AfterEveryBarrier,
            auto_recover: false,
            max_checkpoints: Some(5),
        };

        let runner = Runner {
            mode: ExecMode::Sequential,
            default_partitions: 4,
            checkpoint_config: Some(config),
        };

        let result = runner.run_collect::<(String, u64)>(&p, combined.node_id())?;
        assert_eq!(result.len(), 3);
        Ok(())
    }

    /// Test checkpointing with CombineGlobal
    #[test]
    fn checkpoint_with_combine_global() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let checkpoint_dir = temp_dir.path().to_path_buf();

        let p = TestPipeline::new();
        let data: Vec<u32> = (1..=100).collect();
        let pcoll = from_vec(&p, data);
        let combined = pcoll.combine_globally(Count, Some(4));

        let config = CheckpointConfig {
            enabled: true,
            directory: checkpoint_dir,
            policy: CheckpointPolicy::AfterEveryBarrier,
            auto_recover: false,
            max_checkpoints: Some(5),
        };

        let runner = Runner {
            mode: ExecMode::Sequential,
            default_partitions: 4,
            checkpoint_config: Some(config),
        };

        let result = runner.run_collect::<u64>(&p, combined.node_id())?;
        assert_eq!(result, vec![100]);
        Ok(())
    }

    /// Test parallel checkpointing with multiple partitions
    #[test]
    fn parallel_checkpoint_multiple_partitions() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let checkpoint_dir = temp_dir.path().to_path_buf();

        let p = TestPipeline::new();
        let data: Vec<u32> = (1..=200).collect();
        let pcoll = from_vec(&p, data)
            .key_by(|x: &u32| format!("k{}", x % 5))
            .combine_values(Count);

        let config = CheckpointConfig {
            enabled: true,
            directory: checkpoint_dir,
            policy: CheckpointPolicy::EveryNNodes(2),
            auto_recover: false,
            max_checkpoints: Some(10),
        };

        let runner = Runner {
            mode: ExecMode::Parallel {
                threads: Some(4),
                partitions: Some(8),
            },
            default_partitions: 8,
            checkpoint_config: Some(config),
        };

        let result = runner.run_collect::<(String, u64)>(&p, pcoll.node_id())?;
        assert_eq!(result.len(), 5);
        Ok(())
    }
}
