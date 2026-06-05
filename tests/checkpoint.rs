//! Tests for the checkpoint module.

#[cfg(feature = "checkpointing")]
mod checkpoint_tests {
    use ironbeam::checkpoint::{
        CheckpointConfig, CheckpointManager, CheckpointMetadata, CheckpointPolicy, CheckpointState,
        compute_checksum, current_timestamp_ms,
    };
    use std::fs::{self, File};
    use std::io::{Read, Write};
    use tempfile::TempDir;

    #[test]
    fn test_checkpoint_config_default() {
        let config = CheckpointConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.policy, CheckpointPolicy::AfterEveryBarrier);
        assert!(config.auto_recover);
    }

    #[test]
    fn test_checkpoint_manager_creation() {
        let tmp = TempDir::new().unwrap();
        let config = CheckpointConfig {
            enabled: true,
            directory: tmp.path().to_path_buf(),
            ..Default::default()
        };

        let manager = CheckpointManager::new(config).unwrap();
        assert!(tmp.path().exists());
        assert!(manager.last_checkpoint_time.is_none());
    }

    #[test]
    fn test_should_checkpoint_policies() {
        let tmp = TempDir::new().unwrap();

        // AfterEveryBarrier
        let config = CheckpointConfig {
            enabled: true,
            directory: tmp.path().to_path_buf(),
            policy: CheckpointPolicy::AfterEveryBarrier,
            ..Default::default()
        };
        let mut manager = CheckpointManager::new(config).unwrap();
        assert!(manager.should_checkpoint(0, true, 10));
        assert!(!manager.should_checkpoint(0, false, 10));

        // EveryNNodes
        let config = CheckpointConfig {
            enabled: true,
            directory: tmp.path().to_path_buf(),
            policy: CheckpointPolicy::EveryNNodes(3),
            ..Default::default()
        };
        let mut manager = CheckpointManager::new(config).unwrap();
        assert!(!manager.should_checkpoint(0, false, 10));
        assert!(!manager.should_checkpoint(1, false, 10));
        assert!(!manager.should_checkpoint(2, false, 10));
        assert!(manager.should_checkpoint(3, false, 10));
        assert!(manager.should_checkpoint(6, false, 10));
    }

    #[test]
    fn test_save_and_load_checkpoint() {
        let tmp = TempDir::new().unwrap();
        let config = CheckpointConfig {
            enabled: true,
            directory: tmp.path().to_path_buf(),
            ..Default::default()
        };
        let mut manager = CheckpointManager::new(config).unwrap();

        let timestamp = current_timestamp_ms();
        let metadata_str = format!("test_pipeline:5:{timestamp}:4");
        let checksum = compute_checksum(metadata_str.as_bytes());
        let state = CheckpointState {
            pipeline_id: "test_pipeline".to_string(),
            completed_node_index: 5,
            timestamp,
            partition_count: 4,
            checksum,
            exec_mode: "sequential".to_string(),
            metadata: CheckpointMetadata {
                total_nodes: 10,
                last_node_type: "GroupByKey".to_string(),
                progress_percent: 50,
            },
        };

        let path = manager.save_checkpoint(&state).unwrap();
        assert!(path.exists());

        let loaded = manager.load_checkpoint(&path).unwrap();
        assert_eq!(loaded.pipeline_id, "test_pipeline");
        assert_eq!(loaded.completed_node_index, 5);
        assert_eq!(loaded.partition_count, 4);
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn test_find_latest_checkpoint() {
        let tmp = TempDir::new().unwrap();
        let config = CheckpointConfig {
            enabled: true,
            directory: tmp.path().to_path_buf(),
            ..Default::default()
        };
        let mut manager = CheckpointManager::new(config).unwrap();

        // No checkpoints initially
        assert!(manager.find_latest_checkpoint("test").unwrap().is_none());

        // Create multiple checkpoints
        for i in 0..3 {
            let timestamp = current_timestamp_ms() + i * 1000;
            let metadata_str = format!("test:{i}:{timestamp}:1");
            let checksum = compute_checksum(metadata_str.as_bytes());
            let state = CheckpointState {
                pipeline_id: "test".to_string(),
                completed_node_index: i as usize,
                timestamp,
                partition_count: 1,
                checksum,
                exec_mode: "sequential".to_string(),
                metadata: CheckpointMetadata {
                    total_nodes: 10,
                    last_node_type: "Stateless".to_string(),
                    progress_percent: (i * 33) as u8,
                },
            };
            manager.save_checkpoint(&state).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Find latest
        let latest = manager.find_latest_checkpoint("test").unwrap().unwrap();
        let loaded = manager.load_checkpoint(&latest).unwrap();
        assert_eq!(loaded.completed_node_index, 2); // Last one
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn test_cleanup_old_checkpoints() {
        let tmp = TempDir::new().unwrap();
        let config = CheckpointConfig {
            enabled: true,
            directory: tmp.path().to_path_buf(),
            max_checkpoints: Some(2),
            ..Default::default()
        };
        let mut manager = CheckpointManager::new(config).unwrap();

        // Create 4 checkpoints
        for i in 0..4 {
            let timestamp = current_timestamp_ms() + i * 1000;
            let metadata_str = format!("test:{i}:{timestamp}:1");
            let checksum = compute_checksum(metadata_str.as_bytes());
            let state = CheckpointState {
                pipeline_id: "test".to_string(),
                completed_node_index: i as usize,
                timestamp,
                partition_count: 1,
                checksum,
                exec_mode: "sequential".to_string(),
                metadata: CheckpointMetadata {
                    total_nodes: 10,
                    last_node_type: "Stateless".to_string(),
                    progress_percent: (i * 25) as u8,
                },
            };
            manager.save_checkpoint(&state).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Should only have 2 checkpoints left
        let checkpoints: Vec<_> = fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert_eq!(checkpoints.len(), 2);
    }

    #[test]
    fn test_checkpoint_disabled_skips_all() {
        let tmp = TempDir::new().unwrap();
        let config = CheckpointConfig {
            enabled: false,
            directory: tmp.path().to_path_buf(),
            ..Default::default()
        };
        let mut manager = CheckpointManager::new(config).unwrap();
        // should_checkpoint must return false regardless of barrier status or policy
        assert!(!manager.should_checkpoint(0, true, 10));
        assert!(!manager.should_checkpoint(0, false, 10));
        assert!(!manager.should_checkpoint(100, true, 10));
        // find_latest_checkpoint must return None immediately
        assert!(manager.find_latest_checkpoint("any").unwrap().is_none());
    }

    #[test]
    fn test_new_disabled_does_not_create_directory() {
        let tmp = TempDir::new().unwrap();
        let checkpoint_dir = tmp.path().join("should_not_exist");
        let config = CheckpointConfig {
            enabled: false,
            directory: checkpoint_dir.clone(),
            ..Default::default()
        };
        let _manager = CheckpointManager::new(config).unwrap();
        assert!(!checkpoint_dir.exists());
    }

    #[test]
    fn test_should_checkpoint_time_interval() {
        let tmp = TempDir::new().unwrap();

        // Interval of 0 seconds: always fires (no prior time recorded)
        let config = CheckpointConfig {
            enabled: true,
            directory: tmp.path().to_path_buf(),
            policy: CheckpointPolicy::TimeInterval(0),
            ..Default::default()
        };
        let mut manager = CheckpointManager::new(config).unwrap();
        assert!(manager.should_checkpoint(0, false, 10));

        // Interval of 9999 seconds: only fires on the very first call when last_checkpoint_time is None
        let config = CheckpointConfig {
            enabled: true,
            directory: tmp.path().to_path_buf(),
            policy: CheckpointPolicy::TimeInterval(9999),
            ..Default::default()
        };
        let mut manager = CheckpointManager::new(config).unwrap();
        // first call: no prior time → should fire
        assert!(manager.should_checkpoint(0, false, 10));
        // simulate a checkpoint having just been recorded
        manager.last_checkpoint_time = Some(std::time::SystemTime::now());
        // second call: elapsed is ~0, far below 9999 s → should not fire
        assert!(!manager.should_checkpoint(1, false, 10));
    }

    #[test]
    fn test_should_checkpoint_hybrid() {
        let tmp = TempDir::new().unwrap();

        // Triggers on barrier when barriers=true, even with a huge time interval
        let config = CheckpointConfig {
            enabled: true,
            directory: tmp.path().to_path_buf(),
            policy: CheckpointPolicy::Hybrid { barriers: true, interval_secs: 9999 },
            ..Default::default()
        };
        let mut manager = CheckpointManager::new(config).unwrap();
        // first call with no prior time: time condition is true as well
        assert!(manager.should_checkpoint(0, false, 10));
        // set a very recent last checkpoint time so the time branch is suppressed
        manager.last_checkpoint_time = Some(std::time::SystemTime::now());
        // not a barrier: neither branch fires
        assert!(!manager.should_checkpoint(1, false, 10));
        // is a barrier: barrier branch fires
        assert!(manager.should_checkpoint(1, true, 10));

        // Triggers by time (interval_secs=0) even without a barrier
        let config = CheckpointConfig {
            enabled: true,
            directory: tmp.path().to_path_buf(),
            policy: CheckpointPolicy::Hybrid { barriers: false, interval_secs: 0 },
            ..Default::default()
        };
        let mut manager = CheckpointManager::new(config).unwrap();
        assert!(manager.should_checkpoint(0, false, 10));
        assert!(manager.should_checkpoint(0, true, 10));
    }

    #[test]
    fn test_find_latest_checkpoint_nonexistent_directory() {
        let tmp = TempDir::new().unwrap();
        let config = CheckpointConfig {
            enabled: true,
            directory: tmp.path().to_path_buf(),
            ..Default::default()
        };
        let manager = CheckpointManager::new(config).unwrap();
        // Remove the directory after creation to exercise the exists() guard
        fs::remove_dir_all(tmp.path()).unwrap();
        assert!(manager.find_latest_checkpoint("test").unwrap().is_none());
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn test_cleanup_unlimited_retention() {
        let tmp = TempDir::new().unwrap();
        let config = CheckpointConfig {
            enabled: true,
            directory: tmp.path().to_path_buf(),
            max_checkpoints: None, // unlimited
            ..Default::default()
        };
        let mut manager = CheckpointManager::new(config).unwrap();

        // Create 5 checkpoints — none should be deleted
        for i in 0..5u64 {
            let timestamp = current_timestamp_ms() + i * 1000;
            let metadata_str = format!("test:{i}:{timestamp}:1");
            let checksum = compute_checksum(metadata_str.as_bytes());
            let state = CheckpointState {
                pipeline_id: "test".to_string(),
                completed_node_index: i as usize,
                timestamp,
                partition_count: 1,
                checksum,
                exec_mode: "sequential".to_string(),
                metadata: CheckpointMetadata {
                    total_nodes: 10,
                    last_node_type: "Stateless".to_string(),
                    progress_percent: (i * 20) as u8,
                },
            };
            manager.save_checkpoint(&state).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        let checkpoints: Vec<_> = fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert_eq!(checkpoints.len(), 5);
    }

    #[test]
    fn test_checksum_verification() {
        let tmp = TempDir::new().unwrap();
        let config = CheckpointConfig {
            enabled: true,
            directory: tmp.path().to_path_buf(),
            ..Default::default()
        };
        let mut manager = CheckpointManager::new(config).unwrap();

        let timestamp = current_timestamp_ms();
        let metadata_str = format!("test:0:{timestamp}:1");
        let checksum = compute_checksum(metadata_str.as_bytes());
        let state = CheckpointState {
            pipeline_id: "test".to_string(),
            completed_node_index: 0,
            timestamp,
            partition_count: 1,
            checksum,
            exec_mode: "sequential".to_string(),
            metadata: CheckpointMetadata {
                total_nodes: 10,
                last_node_type: "Source".to_string(),
                progress_percent: 0,
            },
        };

        let path = manager.save_checkpoint(&state).unwrap();

        // Corrupt the checkpoint by modifying the completed_node_index
        let mut file = File::open(&path).unwrap();
        let mut data = Vec::new();
        file.read_to_end(&mut data).unwrap();
        drop(file);

        let mut corrupted_state: CheckpointState = postcard::from_bytes(&data).unwrap();
        corrupted_state.completed_node_index = 999; // Corrupt data
        let corrupted_data = postcard::to_allocvec(&corrupted_state).unwrap();

        let mut file = File::create(&path).unwrap();
        file.write_all(&corrupted_data).unwrap();
        drop(file);

        // Loading should fail due to checksum mismatch
        assert!(manager.load_checkpoint(&path).is_err());
    }
}

#[cfg(not(feature = "checkpointing"))]
#[test]
fn checkpoint_tests_skipped() {
    // This ensures the test file compiles even without the checkpointing feature
}
