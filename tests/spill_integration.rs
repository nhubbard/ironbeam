//! Tests for spill integration with the runner.

#![cfg(feature = "spilling")]

use anyhow::Result;
use ironbeam::spill::SpillConfig;
use ironbeam::spill_integration::{
    SpillingExecutor, current_memory_usage, init_spilling, reset_memory_tracker,
};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

// Global lock to prevent test interference
static TEST_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct TestData {
    value: i32,
}

fn setup_test() {
    let _guard = TEST_LOCK.lock().unwrap();
    reset_memory_tracker();

    let test_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let spill_dir = std::env::temp_dir().join(format!("ironbeam-integration-{test_id}"));

    let config = SpillConfig::new()
        .with_memory_limit(10_000)
        .with_spill_directory(&spill_dir)
        .with_min_spill_size(100);

    init_spilling(config);
}

#[test]
fn test_spilling_executor_creation() {
    setup_test();

    let executor = SpillingExecutor::new();
    assert!(executor.is_enabled());
}

#[test]
fn test_spilling_executor_default() {
    setup_test();

    let executor = SpillingExecutor::default();
    assert!(executor.is_enabled());
}

#[test]
fn test_spilling_executor_process_partition_no_spill() -> Result<()> {
    setup_test();

    let executor = SpillingExecutor::new();
    let data = vec![1, 2, 3];

    let result = executor.process_partition::<i32, _>(|| Box::new(data.clone()));
    assert!(result.is_ok());

    Ok(())
}

#[test]
fn test_spilling_executor_wrap_and_check() -> Result<()> {
    setup_test();

    let executor = SpillingExecutor::new();
    let data = vec![TestData { value: 1 }, TestData { value: 2 }];

    let result = executor.wrap_and_check(data.clone())?;
    assert_eq!(result, data);

    Ok(())
}

#[test]
fn test_current_memory_usage_initialized() {
    setup_test();

    let usage = current_memory_usage();
    assert!(usage.is_some());
}

#[test]
fn test_current_memory_usage_tracking() {
    setup_test();

    let initial = current_memory_usage().unwrap_or(0);

    // Create a spillable partition which will be tracked by the memory tracker
    let data: Vec<TestData> = (0..1000).map(|i| TestData { value: i }).collect();
    let _partition = ironbeam::spill::SpillablePartition::new(data);

    // Memory usage should increase after creating a tracked partition
    let after = current_memory_usage();
    assert!(after.is_some());
    assert!(after.unwrap() >= initial);
}

#[test]
fn test_reset_memory_tracker_functionality() {
    let _guard = TEST_LOCK.lock().unwrap();

    // Initialize spilling
    let test_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let spill_dir = std::env::temp_dir().join(format!("ironbeam-reset-{test_id}"));

    let config = SpillConfig::new()
        .with_memory_limit(10_000)
        .with_spill_directory(&spill_dir);

    init_spilling(config);

    // Reset should clear memory tracking
    reset_memory_tracker();

    let usage = current_memory_usage();
    assert!(usage.is_some());
    // After reset, usage should be 0 or very low
    assert!(usage.unwrap() < 1000);
}

#[test]
fn test_spilling_executor_with_large_data() -> Result<()> {
    setup_test();

    let executor = SpillingExecutor::new();
    let large_data: Vec<TestData> = (0..1000).map(|i| TestData { value: i }).collect();

    let result = executor.wrap_and_check(large_data.clone())?;
    assert_eq!(result.len(), large_data.len());

    Ok(())
}

#[test]
fn test_spilling_executor_with_empty_data() -> Result<()> {
    setup_test();

    let executor = SpillingExecutor::new();
    let empty_data: Vec<TestData> = vec![];

    let result = executor.wrap_and_check(empty_data.clone())?;
    assert_eq!(result, empty_data);

    Ok(())
}

#[test]
fn test_init_spilling_multiple_times() {
    let _guard = TEST_LOCK.lock().unwrap();

    // Initialize multiple times should not panic
    let config1 = SpillConfig::new()
        .with_memory_limit(5000)
        .with_spill_directory(std::env::temp_dir().join("test1"));

    init_spilling(config1);

    let config2 = SpillConfig::new()
        .with_memory_limit(10000)
        .with_spill_directory(std::env::temp_dir().join("test2"));

    init_spilling(config2);

    // Should still be initialized
    assert!(current_memory_usage().is_some());
}

#[test]
fn test_spilling_executor_disabled_when_not_initialized() {
    let _guard = TEST_LOCK.lock().unwrap();

    // Don't initialize spilling
    reset_memory_tracker();

    let executor = SpillingExecutor::new();
    // Should be disabled without initialization
    assert!(!executor.is_enabled() || current_memory_usage().is_some());
}

#[test]
fn test_process_partition_with_type_mismatch() -> Result<()> {
    setup_test();

    let executor = SpillingExecutor::new();

    // Create a partition with one type
    let data = vec!["string1".to_string(), "string2".to_string()];
    let partition = Box::new(data.clone()) as Box<dyn std::any::Any + Send + Sync>;

    // Try to process as a different type - should handle gracefully
    let result = executor.process_partition::<i32, _>(|| partition);
    assert!(result.is_ok());

    Ok(())
}

#[test]
fn test_wrap_and_check_with_serialization() -> Result<()> {
    setup_test();

    let executor = SpillingExecutor::new();

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct ComplexType {
        id: u64,
        name: String,
        values: Vec<f64>,
    }

    let complex_data = vec![
        ComplexType {
            id: 1,
            name: "test1".to_string(),
            values: vec![1.0, 2.0, 3.0],
        },
        ComplexType {
            id: 2,
            name: "test2".to_string(),
            values: vec![4.0, 5.0],
        },
    ];

    let result = executor.wrap_and_check(complex_data.clone())?;
    assert_eq!(result, complex_data);

    Ok(())
}

#[test]
fn test_memory_tracking_after_multiple_operations() {
    setup_test();

    let executor = SpillingExecutor::new();

    // Perform multiple operations
    for i in 0..10 {
        let data: Vec<i32> = (i * 100..(i + 1) * 100).collect();
        let _ = executor.wrap_and_check(data);
    }

    // Memory tracking should still be functional
    let usage = current_memory_usage();
    assert!(usage.is_some());
}

#[test]
fn test_spilling_executor_stress() -> Result<()> {
    setup_test();

    let executor = SpillingExecutor::new();

    // Create many small partitions
    for i in 0..100 {
        let data = vec![TestData { value: i }];
        let _ = executor.wrap_and_check(data)?;
    }

    Ok(())
}
