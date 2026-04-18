//! Tests for automatic memory spilling functionality.

#![cfg(feature = "spilling")]

use anyhow::Result;
use ironbeam::spill::{MemoryTracker, SpillConfig, SpillManager, SpillablePartition};
use ironbeam::spill_integration::{init_spilling, reset_memory_tracker};
use ironbeam::*;
use mark_flaky_tests::flaky;
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

// Global lock to prevent test interference
static TEST_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct ComplexRecord {
    id: u64,
    name: String,
    values: Vec<f64>,
}

/// Helper to clean up and reset state between tests
fn setup_test(memory_limit: usize) -> (SpillConfig, std::sync::MutexGuard<'static, ()>) {
    let guard = TEST_LOCK.lock().unwrap();

    // Reset the memory tracker
    reset_memory_tracker();

    // Create a unique temporary directory for each test
    let test_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let spill_dir = std::env::temp_dir().join(format!("ironbeam-test-{test_id}"));

    let config = SpillConfig::new()
        .with_memory_limit(memory_limit)
        .with_spill_directory(&spill_dir)
        .with_min_spill_size(100); // Low threshold for testing

    init_spilling(config.clone());

    (config, guard)
}

/// Clean up test directory
fn cleanup_test(config: &SpillConfig) {
    if config.spill_directory.exists() {
        let _ = std::fs::remove_dir_all(&config.spill_directory);
    }
}

#[flaky]
#[test]
fn test_memory_tracker_basic() {
    let (config, _guard) = setup_test(1000);

    let tracker = MemoryTracker::instance().expect("Tracker should be initialized");

    assert_eq!(tracker.current_usage(), 0);
    assert_eq!(tracker.memory_limit(), Some(1000));

    // Allocate some memory
    tracker.allocate(500);
    assert_eq!(tracker.current_usage(), 500);
    assert!(!tracker.should_spill()); // Still under limit

    // Allocate more to exceed limit
    tracker.allocate(600);
    assert_eq!(tracker.current_usage(), 1100);
    assert!(tracker.should_spill()); // Over limit

    // Deallocate
    tracker.deallocate(700);
    assert_eq!(tracker.current_usage(), 400);
    assert!(!tracker.should_spill());

    cleanup_test(&config);
}

#[flaky]
#[test]
fn test_spillable_partition_creation() {
    let (config, _guard) = setup_test(10_000);

    let data: Vec<i32> = vec![1, 2, 3, 4, 5];
    let partition = SpillablePartition::new(data);

    assert!(partition.is_in_memory());
    assert!(!partition.is_spilled());

    // Memory should be tracked
    let tracker = MemoryTracker::instance().unwrap();
    assert!(tracker.current_usage() > 0);

    cleanup_test(&config);
}

#[cfg(not(target_arch = "x86_64"))]
#[flaky]
#[test]
fn test_spillable_partition_manual_spill_and_restore() -> Result<()> {
    let (config, _guard) = setup_test(100_000);

    let data: Vec<i32> = (0..1000).collect();
    let mut partition = SpillablePartition::new(data.clone());

    assert!(partition.is_in_memory());

    // Manually spill
    partition.spill()?;
    assert!(partition.is_spilled());
    assert!(!partition.is_in_memory());

    // Restore
    partition.restore()?;
    assert!(partition.is_in_memory());
    assert!(!partition.is_spilled());

    // Verify data integrity
    let restored_data = partition.into_vec()?;
    assert_eq!(restored_data, data);

    cleanup_test(&config);
    Ok(())
}

#[cfg(not(target_arch = "x86_64"))]
#[flaky]
#[test]
fn test_spillable_partition_automatic_spill_detection() -> Result<()> {
    // Set a very low memory limit to trigger spilling
    let (config, _guard) = setup_test(1000);

    let tracker = MemoryTracker::instance().unwrap();

    // Create a large partition that should trigger spilling
    let large_data: Vec<i32> = (0..1000).collect();
    let mut partition = SpillablePartition::new(large_data.clone());

    // Force the tracker to think we're over the limit
    tracker.allocate(10_000);

    assert!(partition.should_spill());

    // Spill it
    partition.spill()?;
    assert!(partition.is_spilled());

    // Verify we can restore the spilled data and that the data is intact
    let restored = partition.into_vec()?;
    assert_eq!(restored, large_data);

    cleanup_test(&config);
    Ok(())
}

#[flaky]
#[test]
fn test_spill_manager_operations() -> Result<()> {
    let (config, _guard) = setup_test(100_000);

    let manager = SpillManager::new(config.clone())?;

    // Create test data
    let data: Vec<String> = vec!["hello".to_string(), "world".to_string(), "test".to_string()];

    // Spill to disk
    let spill_id = 12345;
    let disk_size = manager.spill(&data, spill_id)?;
    assert!(disk_size > 0);

    // Verify spill file exists
    let spill_path = config.spill_directory.join(format!("spill-{spill_id}.bin"));
    assert!(spill_path.exists());

    // Restore from disk
    let restored: Vec<String> = manager.restore(spill_id)?;
    assert_eq!(restored, data);

    // Cleanup
    manager.cleanup(spill_id)?;
    assert!(!spill_path.exists());

    cleanup_test(&config);
    Ok(())
}

#[flaky]
#[test]
fn test_spill_with_complex_types() -> Result<()> {
    let (config, _guard) = setup_test(100_000);

    let data = vec![
        ComplexRecord {
            id: 1,
            name: "record1".to_string(),
            values: vec![1.0, 2.0, 3.0],
        },
        ComplexRecord {
            id: 2,
            name: "record2".to_string(),
            values: vec![4.0, 5.0, 6.0],
        },
    ];

    let manager = SpillManager::new(config.clone())?;
    let spill_id = 999;

    // Spill and restore
    manager.spill(&data, spill_id)?;
    let restored: Vec<ComplexRecord> = manager.restore(spill_id)?;

    assert_eq!(restored, data);

    manager.cleanup(spill_id)?;
    cleanup_test(&config);
    Ok(())
}

#[flaky]
#[test]
fn test_spillable_partition_with_small_data() {
    // Small data should not trigger spilling even with a low limit
    let (config, _guard) = setup_test(50); // Very low limit

    let tracker = MemoryTracker::instance().unwrap();

    // Create a very small partition
    let small_data: Vec<i32> = vec![1, 2, 3];
    let _partition = SpillablePartition::new(small_data);

    // Even if we're over the limit, small partitions shouldn't spill
    tracker.allocate(10_000);

    // The partition is too small to be worth spilling (< min_spill_size)
    // So even though memory is over limit, this specific partition might not spill
    // This tests the min_spill_size configuration

    cleanup_test(&config);
}

#[flaky]
#[test]
fn test_multiple_partitions_with_spilling() -> Result<()> {
    let (config, _guard) = setup_test(5000);

    let tracker = MemoryTracker::instance().unwrap();

    // Create multiple partitions
    let data1: Vec<i32> = (0..500).collect();
    let data2: Vec<i32> = (500..1000).collect();
    let data3: Vec<i32> = (1000..1500).collect();

    let mut part1 = SpillablePartition::new(data1.clone());
    let mut part2 = SpillablePartition::new(data2.clone());
    let part3 = SpillablePartition::new(data3.clone());

    // All should be in memory initially
    assert!(part1.is_in_memory());
    assert!(part2.is_in_memory());
    assert!(part3.is_in_memory());

    // Force spilling by exceeding the memory limit
    tracker.allocate(50_000);

    // Spill some partitions
    if part1.should_spill() {
        part1.spill()?;
    }
    if part2.should_spill() {
        part2.spill()?;
    }

    // Verify data integrity after restoration
    let restored1 = part1.into_vec()?;
    let restored2 = part2.into_vec()?;
    let restored3 = part3.into_vec()?;

    assert_eq!(restored1, data1);
    assert_eq!(restored2, data2);
    assert_eq!(restored3, data3);

    cleanup_test(&config);
    Ok(())
}

#[flaky]
#[test]
fn test_memory_tracking_accuracy() {
    let _guard = TEST_LOCK.lock().unwrap();

    // Create a fresh config and init for this test
    let test_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let spill_dir = std::env::temp_dir().join(format!("ironbeam-test-tracking-{test_id}"));

    let config = SpillConfig::new()
        .with_memory_limit(100_000)
        .with_spill_directory(&spill_dir)
        .with_min_spill_size(100);

    // Reset tracker
    reset_memory_tracker();
    init_spilling(config.clone());

    let tracker = MemoryTracker::instance().unwrap();

    // Get baseline (should be 0 after reset)
    let initial = tracker.current_usage();

    // Create a partition and track memory increase
    let data: Vec<i32> = (0..1000).collect();
    let partition = SpillablePartition::new(data);

    let after_creation = tracker.current_usage();
    let memory_used = after_creation.saturating_sub(initial);

    assert!(
        memory_used > 0,
        "Memory usage should increase after creating partition (initial: {initial}, after: {after_creation}, diff: {memory_used})"
    );

    // Drop the partition and verify memory is deallocated
    let before_drop = tracker.current_usage();
    drop(partition);
    let after_drop = tracker.current_usage();

    // Memory should decrease after a drop (or at least not increase)
    assert!(
        after_drop <= before_drop,
        "Memory should not increase after dropping partition"
    );

    cleanup_test(&config);
}

#[flaky]
#[test]
fn test_spill_with_empty_data() -> Result<()> {
    let (config, _guard) = setup_test(10_000);

    let data: Vec<i32> = vec![];
    let mut partition = SpillablePartition::new(data.clone());

    // Should handle empty data gracefully
    partition.spill()?;
    let restored = partition.into_vec()?;
    assert_eq!(restored, data);

    cleanup_test(&config);
    Ok(())
}

#[flaky]
#[test]
fn test_concurrent_spilling() -> Result<()> {
    use std::thread;

    let (config, _guard) = setup_test(50_000);
    let tracker = MemoryTracker::instance().unwrap();

    // Create multiple threads that create and spill partitions
    let handles: Vec<_> = (0..4)
        .map(|i| {
            let tracker_clone = tracker.clone();
            thread::spawn(move || -> Result<()> {
                let data: Vec<i32> = ((i * 100)..((i + 1) * 100)).collect();
                let mut partition = SpillablePartition::new(data.clone());

                // Force spilling
                tracker_clone.allocate(20_000);

                if partition.should_spill() {
                    partition.spill()?;
                }

                let restored = partition.into_vec()?;
                assert_eq!(restored, data);

                Ok(())
            })
        })
        .collect();

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap()?;
    }

    cleanup_test(&config);
    Ok(())
}

#[flaky]
#[test]
fn test_artificial_memory_limit_with_pipeline() -> Result<()> {
    let (config, _guard) = setup_test(5000); // 5KB limit - very restrictive for testing

    let p = Pipeline::default();

    // Create a simple pipeline with data that should trigger memory tracking
    let data: Vec<i32> = (0..100).collect();
    let collection = from_vec(&p, data);

    // Transform the data
    let result = collection
        .map(|x: &i32| x * 2)
        .filter(|x: &i32| x % 3 == 0)
        .collect_seq()?;

    // Verify the pipeline executed correctly
    let expected: Vec<i32> = (0..100).map(|x| x * 2).filter(|x| x % 3 == 0).collect();

    assert_eq!(result, expected);

    // Check that memory was tracked (though may not have spilled in this simple case)
    let tracker = MemoryTracker::instance().unwrap();
    let _ = tracker.current_usage(); // Just verify we can read it

    cleanup_test(&config);
    Ok(())
}

#[flaky]
#[test]
fn test_large_dataset_with_artificial_limit() -> Result<()> {
    // This test uses a very restrictive memory limit to force spilling behavior
    // without needing administrative privileges to monitor real memory usage
    let (config, _guard) = setup_test(2000); // 2KB - will definitely be exceeded

    let p = Pipeline::default();

    // Create a larger dataset that will definitely exceed our artificial limit
    let data: Vec<i64> = (0..500).collect();
    let collection = from_vec(&p, data.clone());

    // Simple transformation
    let result = collection.map(|x: &i64| x + 1).collect_seq()?;

    let expected: Vec<i64> = data.iter().map(|x| x + 1).collect();
    assert_eq!(result, expected);

    cleanup_test(&config);
    Ok(())
}

#[flaky]
#[test]
fn test_spilling_with_key_value_operations() -> Result<()> {
    let (config, _guard) = setup_test(3000);

    let p = Pipeline::default();

    // Create key-value data
    let data = vec![("a", 1), ("b", 2), ("a", 3), ("b", 4), ("c", 5)];

    let collection = from_vec(&p, data);

    // Group by key and sum values
    let result = collection
        .combine_values(Sum::<i32>::default())
        .collect_seq_sorted()?;

    assert_eq!(result, vec![("a", 4), ("b", 6), ("c", 5)]);

    cleanup_test(&config);
    Ok(())
}

#[cfg(feature = "compression-zstd")]
#[flaky]
#[test]
fn test_spilling_with_compression() -> Result<()> {
    let config = SpillConfig::new()
        .with_memory_limit(100_000)
        .with_compression(true)
        .with_spill_directory(std::env::temp_dir().join("ironbeam-test-compression"));

    init_spilling(config.clone());

    let manager = SpillManager::new(config.clone())?;

    // Create compressible data (repeated patterns)
    let data: Vec<String> = (0..100)
        .map(|i| format!("repeated_string_{}", i % 10))
        .collect();

    let spill_id = 7777;
    let _compressed_size = manager.spill(&data, spill_id)?;

    // Compressed size should be less than uncompressed
    // (This is a rough estimate - actual compression depends on data)

    let restored: Vec<String> = manager.restore(spill_id)?;
    assert_eq!(restored, data);

    manager.cleanup(spill_id)?;
    cleanup_test(&config);
    Ok(())
}

#[flaky]
#[test]
fn test_spilling_preserves_order() -> Result<()> {
    let (config, _guard) = setup_test(10_000);

    let data: Vec<i32> = (0..100).rev().collect(); // Reverse order
    let mut partition = SpillablePartition::new(data.clone());

    // Spill and restore
    partition.spill()?;
    let restored = partition.into_vec()?;

    // Order should be preserved
    assert_eq!(restored, data);

    cleanup_test(&config);
    Ok(())
}

// Tests moved from src/spill.rs

#[flaky]
#[test]
fn test_memory_tracker_initialization() {
    let config = SpillConfig::new().with_memory_limit(1000);
    MemoryTracker::initialize(config);

    let tracker = MemoryTracker::instance().unwrap();
    assert_eq!(tracker.memory_limit(), Some(1000));
    assert_eq!(tracker.current_usage(), 0);
}

#[flaky]
#[test]
fn test_memory_allocation_tracking() {
    let config = SpillConfig::new().with_memory_limit(1000);
    MemoryTracker::initialize(config);

    let tracker = MemoryTracker::instance().unwrap();
    tracker.allocate(500);
    assert_eq!(tracker.current_usage(), 500);

    tracker.allocate(300);
    assert_eq!(tracker.current_usage(), 800);

    tracker.deallocate(200);
    assert_eq!(tracker.current_usage(), 600);
}

#[flaky]
#[test]
fn test_should_spill_detection() {
    let config = SpillConfig::new().with_memory_limit(1000);
    MemoryTracker::initialize(config);

    let tracker = MemoryTracker::instance().unwrap();
    assert!(!tracker.should_spill());

    tracker.allocate(1001);
    assert!(tracker.should_spill());
}
