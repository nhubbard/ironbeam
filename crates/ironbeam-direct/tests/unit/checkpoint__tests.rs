// Unit tests for src/checkpoint.rs.
use super::*;
use ironbeam_core::ErrorKind;
use std::process::Command;

#[test]
fn checkpoint_crash_child() {
    let Ok(directory) = std::env::var("IRONBEAM_CHECKPOINT_CHILD_DIRECTORY") else {
        return;
    };
    let config = CheckpointConfig {
        directory: directory.into(),
        max_checkpoints: 2,
        ..CheckpointConfig::default()
    };
    let identity = Identity::new::<i32>(
        "graph".into(),
        "source".into(),
        "input".into(),
        "output".into(),
        "crash-test".into(),
    );
    let metrics = Arc::new(MetricsState::default());
    let manager = CheckpointManager::new(&config, identity, Arc::clone(&metrics)).unwrap();
    let mut store = OutputStore::<i32>::new(1024, None, metrics).unwrap();
    store.push(1).unwrap();
    store.push(2).unwrap();
    manager.save(2, &store).unwrap();
    panic!("checkpoint child did not exit at its fault stage");
}

#[test]
fn bounded_checkpoint_survives_process_exit_at_publication_boundaries() {
    for (stage, expected_offset) in [
        ("bounded_data_synced", 1),
        ("bounded_history_synced", 1),
        ("bounded_latest_renamed", 2),
        ("bounded_latest_synced", 2),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let config = CheckpointConfig {
            directory: directory.path().to_path_buf(),
            max_checkpoints: 2,
            ..CheckpointConfig::default()
        };
        let identity = Identity::new::<i32>(
            "graph".into(),
            "source".into(),
            "input".into(),
            "output".into(),
            "crash-test".into(),
        );
        let metrics = Arc::new(MetricsState::default());
        let manager = CheckpointManager::new(&config, identity, Arc::clone(&metrics)).unwrap();
        let mut store = OutputStore::<i32>::new(1024, None, Arc::clone(&metrics)).unwrap();
        store.push(1).unwrap();
        manager.save(1, &store).unwrap();

        let result = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "checkpoint::tests::checkpoint_crash_child"])
            .env("IRONBEAM_CHECKPOINT_CHILD_DIRECTORY", directory.path())
            .env("IRONBEAM_CHECKPOINT_EXIT_STAGE", stage)
            .output()
            .unwrap();
        assert_eq!(result.status.code(), Some(86), "stage {stage}: {result:?}");
        let mut recovered = OutputStore::<i32>::new(1024, None, metrics).unwrap();
        assert_eq!(manager.recover(&mut recovered).unwrap(), expected_offset);
        assert_eq!(
            recovered.into_vec().unwrap(),
            vec![1, 2][..expected_offset].to_vec()
        );
        if stage == "bounded_latest_renamed" {
            fs::remove_file(directory.path().join("latest.json")).unwrap();
            let mut fallback =
                OutputStore::<i32>::new(1024, None, Arc::new(MetricsState::default())).unwrap();
            assert_eq!(manager.recover(&mut fallback).unwrap(), 1);
            assert_eq!(fallback.into_vec().unwrap(), vec![1]);
        }
    }
}

#[test]
fn progress_checksum_has_a_stable_cross_platform_wire_shape() {
    assert_eq!(
        progress_sha256(12, 24, "rows.bin", "abc123"),
        "05e810a630c9c8d081d0a6809282731b4cd183d88e7c8266ba38b4d433c4e582"
    );
}

#[test]
fn manager_rejects_zero_interval_and_missing_data_file() {
    let dir = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        directory: dir.path().to_path_buf(),
        interval_batches: 0,
        binary_build_id: "test".into(),
        ..CheckpointConfig::default()
    };
    let identity = Identity::new::<i32>(
        "graph".into(),
        "source".into(),
        "input".into(),
        "output".into(),
        "test".into(),
    );
    assert_eq!(
        CheckpointManager::new(&config, identity, Arc::new(MetricsState::default()))
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    assert_eq!(
        sha256_file(&dir.path().join("missing")).unwrap_err().kind,
        ErrorKind::Io
    );
    for max_checkpoints in [0, MAX_RETAINED_CHECKPOINTS + 1] {
        let config = CheckpointConfig {
            directory: dir.path().to_path_buf(),
            max_checkpoints,
            ..CheckpointConfig::default()
        };
        let identity = Identity::new::<i32>(
            "graph".into(),
            "source".into(),
            "input".into(),
            "output".into(),
            "test".into(),
        );
        assert_eq!(
            CheckpointManager::new(&config, identity, Arc::new(MetricsState::default()))
                .err()
                .unwrap()
                .kind,
            ErrorKind::Graph
        );
    }
}

#[test]
fn recovery_reports_directory_replaced_after_manager_setup() {
    let temporary = tempfile::tempdir().unwrap();
    let directory = temporary.path().join("checkpoint");
    let config = CheckpointConfig {
        directory: directory.clone(),
        binary_build_id: "directory-race-v1".into(),
        ..CheckpointConfig::default()
    };
    let identity = Identity::new::<i32>(
        "graph".into(),
        "source".into(),
        "input".into(),
        "output".into(),
        "directory-race-v1".into(),
    );
    let manager =
        CheckpointManager::new(&config, identity, Arc::new(MetricsState::default())).unwrap();
    fs::remove_dir(&directory).unwrap();
    fs::write(&directory, b"former checkpoint directory").unwrap();
    assert_eq!(
        manager.recover_fold_state(1024).err().unwrap().kind,
        ErrorKind::Io
    );
}

#[test]
fn recovery_rejects_manifest_count_that_disagrees_with_valid_data() {
    let directory = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        directory: directory.path().to_path_buf(),
        binary_build_id: "count-test".into(),
        ..CheckpointConfig::default()
    };
    let identity = Identity::new::<i32>(
        "graph".into(),
        "source".into(),
        "input".into(),
        "output".into(),
        "count-test".into(),
    );
    let metrics = Arc::new(MetricsState::default());
    let manager = CheckpointManager::new(&config, identity, Arc::clone(&metrics)).unwrap();
    let mut store = OutputStore::<i32>::new(1024, None, Arc::clone(&metrics)).unwrap();
    store.push(7).unwrap();
    manager.save(1, &store).unwrap();

    let path = directory.path().join("latest.json");
    let mut manifest: Manifest = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    manifest.output_count = 2;
    manifest.progress_sha256 = progress_sha256(
        manifest.source_offset,
        manifest.output_count,
        &manifest.data_file,
        &manifest.data_sha256,
    );
    fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let mut recovered = OutputStore::<i32>::new(1024, None, metrics).unwrap();
    let error = manager.recover(&mut recovered).unwrap_err();
    assert_eq!(error.kind, ErrorKind::Recovery);
    assert!(error.message.contains("output count mismatch"));
}

#[test]
fn recovery_reports_missing_checkpoint_data_as_corrupt_state() {
    let directory = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        directory: directory.path().to_path_buf(),
        binary_build_id: "missing-data-test".into(),
        ..CheckpointConfig::default()
    };
    let identity = Identity::new::<i32>(
        "graph".into(),
        "source".into(),
        "input".into(),
        "output".into(),
        "missing-data-test".into(),
    );
    let metrics = Arc::new(MetricsState::default());
    let manager = CheckpointManager::new(&config, identity, Arc::clone(&metrics)).unwrap();
    let mut store = OutputStore::<i32>::new(1024, None, Arc::clone(&metrics)).unwrap();
    store.push(7).unwrap();
    manager.save(1, &store).unwrap();
    let manifest: Manifest =
        serde_json::from_slice(&fs::read(directory.path().join("latest.json")).unwrap()).unwrap();
    fs::remove_file(directory.path().join(manifest.data_file)).unwrap();

    let mut recovered = OutputStore::<i32>::new(1024, None, metrics).unwrap();
    let error = manager.recover(&mut recovered).unwrap_err();
    assert_eq!(error.kind, ErrorKind::Recovery);
    assert!(error.message.contains("data file is missing"));
}

#[test]
fn bounded_history_retains_a_limit_and_recovers_the_previous_valid_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        directory: directory.path().to_path_buf(),
        max_checkpoints: 3,
        binary_build_id: "history-test".into(),
        ..CheckpointConfig::default()
    };
    let identity = Identity::new::<i32>(
        "graph".into(),
        "source".into(),
        "input".into(),
        "output".into(),
        "history-test".into(),
    );
    let metrics = Arc::new(MetricsState::default());
    let manager = CheckpointManager::new(&config, identity, Arc::clone(&metrics)).unwrap();
    let mut store = OutputStore::<i32>::new(1024, None, Arc::clone(&metrics)).unwrap();
    let mut data_files = Vec::new();
    for offset in 1..=4 {
        store.push(offset).unwrap();
        manager.save(offset as usize, &store).unwrap();
        let manifest: Manifest =
            serde_json::from_slice(&fs::read(directory.path().join("latest.json")).unwrap())
                .unwrap();
        data_files.push(manifest.data_file);
    }
    assert!(!directory.path().join(&data_files[0]).exists());
    assert!(directory.path().join(&data_files[1]).exists());
    assert!(directory.path().join(&data_files[2]).exists());
    assert!(directory.path().join(&data_files[3]).exists());
    let history_one: Manifest = serde_json::from_slice(
        &fs::read(directory.path().join("checkpoint-history-1.json")).unwrap(),
    )
    .unwrap();
    let history_two: Manifest = serde_json::from_slice(
        &fs::read(directory.path().join("checkpoint-history-2.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        (history_one.source_offset, history_two.source_offset),
        (3, 2)
    );

    fs::write(directory.path().join(&data_files[3]), b"corrupt latest").unwrap();
    let mut recovered = OutputStore::<i32>::new(1024, None, Arc::clone(&metrics)).unwrap();
    assert_eq!(manager.recover(&mut recovered).unwrap(), 3);
    assert_eq!(recovered.into_vec().unwrap(), vec![1, 2, 3]);

    let latest_path = directory.path().join("latest.json");
    let mut latest: Manifest = serde_json::from_slice(&fs::read(&latest_path).unwrap()).unwrap();
    latest.identity.graph_fingerprint.push_str("-other");
    fs::write(&latest_path, serde_json::to_vec(&latest).unwrap()).unwrap();
    let mut refused = OutputStore::<i32>::new(1024, None, metrics).unwrap();
    assert_eq!(
        manager.recover(&mut refused).unwrap_err().kind,
        ErrorKind::Incompatible
    );
}

#[test]
fn recovery_rejects_every_corrupt_retained_bounded_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        directory: directory.path().to_path_buf(),
        max_checkpoints: 2,
        ..CheckpointConfig::default()
    };
    let identity = Identity::new::<i32>(
        "graph".into(),
        "source".into(),
        "input".into(),
        "output".into(),
        "all-corrupt-test".into(),
    );
    let metrics = Arc::new(MetricsState::default());
    let manager = CheckpointManager::new(&config, identity, Arc::clone(&metrics)).unwrap();
    let mut store = OutputStore::<i32>::new(1024, None, Arc::clone(&metrics)).unwrap();
    store.push(1).unwrap();
    manager.save(1, &store).unwrap();
    store.push(2).unwrap();
    manager.save(2, &store).unwrap();
    fs::write(directory.path().join("latest.json"), b"{broken").unwrap();
    fs::write(
        directory.path().join("checkpoint-history-1.json"),
        b"{broken",
    )
    .unwrap();

    let mut recovered = OutputStore::<i32>::new(1024, None, metrics).unwrap();
    assert_eq!(
        manager.recover(&mut recovered).unwrap_err().kind,
        ErrorKind::Recovery
    );
}

#[test]
fn reducing_bounded_retention_prunes_previous_history_after_publication() {
    let directory = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        directory: directory.path().to_path_buf(),
        max_checkpoints: 3,
        ..CheckpointConfig::default()
    };
    let identity = Identity::new::<i32>(
        "graph".into(),
        "source".into(),
        "input".into(),
        "output".into(),
        "history-limit-test".into(),
    );
    let metrics = Arc::new(MetricsState::default());
    let manager = CheckpointManager::new(&config, identity.clone(), Arc::clone(&metrics)).unwrap();
    let mut store = OutputStore::<i32>::new(1024, None, Arc::clone(&metrics)).unwrap();
    store.push(1).unwrap();
    manager.save(1, &store).unwrap();
    store.push(2).unwrap();
    manager.save(2, &store).unwrap();
    store.push(3).unwrap();
    manager.save(3, &store).unwrap();
    let older: Manifest = serde_json::from_slice(
        &fs::read(directory.path().join("checkpoint-history-2.json")).unwrap(),
    )
    .unwrap();
    let prior: Manifest = serde_json::from_slice(
        &fs::read(directory.path().join("checkpoint-history-1.json")).unwrap(),
    )
    .unwrap();

    let latest_only = CheckpointManager::new(
        &CheckpointConfig {
            max_checkpoints: 1,
            ..config
        },
        identity,
        metrics,
    )
    .unwrap();
    store.push(4).unwrap();
    latest_only.save(4, &store).unwrap();
    for slot in 1..=2 {
        assert!(
            !directory
                .path()
                .join(format!("checkpoint-history-{slot}.json"))
                .exists()
        );
    }
    assert!(!directory.path().join(older.data_file).exists());
    assert!(!directory.path().join(prior.data_file).exists());
}

#[test]
fn fold_history_falls_back_to_the_previous_coded_state() {
    let directory = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        directory: directory.path().to_path_buf(),
        max_checkpoints: 2,
        ..CheckpointConfig::default()
    };
    let identity = Identity::new::<i32>(
        "graph".into(),
        "source".into(),
        "input".into(),
        "output".into(),
        "fold-history-test".into(),
    );
    let manager =
        CheckpointManager::new(&config, identity, Arc::new(MetricsState::default())).unwrap();
    manager.save_fold_state(1, b"first", 1024).unwrap();
    manager.save_fold_state(2, b"second", 1024).unwrap();
    let latest: Manifest =
        serde_json::from_slice(&fs::read(directory.path().join("latest.json")).unwrap()).unwrap();
    fs::write(directory.path().join(latest.data_file), b"bad").unwrap();
    assert_eq!(
        manager.recover_fold_state(1024).unwrap(),
        Some((1, b"first".to_vec()))
    );
}

#[test]
fn pair_recovery_rejects_an_invalid_side_tag_even_with_valid_checksums() {
    let directory = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        directory: directory.path().to_path_buf(),
        binary_build_id: "pair-tag-v1".into(),
        ..CheckpointConfig::default()
    };
    let identity = Identity::new_pair::<i32, String>(
        "graph".into(),
        "source".into(),
        "input".into(),
        "left".into(),
        "right".into(),
        "pair-tag-v1".into(),
    );
    let metrics = Arc::new(MetricsState::default());
    let manager = CheckpointManager::new(&config, identity, Arc::clone(&metrics)).unwrap();
    let mut left = OutputStore::<i32>::new(1024, None, Arc::clone(&metrics)).unwrap();
    left.push(7).unwrap();
    let mut right = OutputStore::<String>::new(1024, None, Arc::clone(&metrics)).unwrap();
    right.push("seven".into()).unwrap();
    manager.save_pair(1, &left, &right).unwrap();

    let manifest_path = directory.path().join("latest.json");
    let mut manifest: Manifest =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    let data_path = directory.path().join(&manifest.data_file);
    let mut bytes = fs::read(&data_path).unwrap();
    bytes[8] = 2;
    fs::write(&data_path, bytes).unwrap();
    manifest.data_sha256 = sha256_file(&data_path).unwrap();
    manifest.progress_sha256 = manifest.progress_hash();
    fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

    let mut recovered_left = OutputStore::<i32>::new(1024, None, Arc::clone(&metrics)).unwrap();
    let mut recovered_right = OutputStore::<String>::new(1024, None, metrics).unwrap();
    let error = manager
        .recover_pair(&mut recovered_left, &mut recovered_right)
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Recovery);
    assert!(error.message.contains("invalid side tag"));

    let mut bytes = fs::read(&data_path).unwrap();
    bytes[8] = 0;
    fs::write(&data_path, bytes).unwrap();
    manifest.data_sha256 = sha256_file(&data_path).unwrap();
    manifest.output_count = 3;
    manifest.progress_sha256 = manifest.progress_hash();
    fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let error = manager
        .recover_pair(&mut recovered_left, &mut recovered_right)
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Recovery);
    assert!(error.message.contains("output count mismatch"));

    manifest.kind = CheckpointKind::Output;
    manifest.progress_sha256 = manifest.progress_hash();
    fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let error = manager
        .recover_pair(&mut recovered_left, &mut recovered_right)
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Recovery);
    assert!(error.message.contains("expected pair output"));
}

#[test]
fn saving_does_not_delete_files_named_by_an_untrusted_old_manifest() {
    let parent = tempfile::tempdir().unwrap();
    let checkpoint_dir = parent.path().join("checkpoints");
    let config = CheckpointConfig {
        directory: checkpoint_dir.clone(),
        binary_build_id: "cleanup-test".into(),
        ..CheckpointConfig::default()
    };
    let identity = Identity::new::<i32>(
        "graph".into(),
        "source".into(),
        "input".into(),
        "output".into(),
        "cleanup-test".into(),
    );
    let metrics = Arc::new(MetricsState::default());
    let manager = CheckpointManager::new(&config, identity, Arc::clone(&metrics)).unwrap();
    let mut store = OutputStore::<i32>::new(1024, None, Arc::clone(&metrics)).unwrap();
    store.push(7).unwrap();
    manager.save(1, &store).unwrap();
    let manifest_path = checkpoint_dir.join("latest.json");
    let original: Manifest = serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    assert!(original.data_file.starts_with("checkpoint-data-"));

    let outside = parent.path().join("outside.txt");
    fs::write(&outside, b"keep outside").unwrap();
    let sibling = checkpoint_dir.join("notes.txt");
    fs::write(&sibling, b"keep sibling").unwrap();
    for data_file in ["../outside.txt", "notes.txt", "latest.json"] {
        let mut forged = Manifest {
            version: original.version,
            kind: original.kind,
            identity: original.identity.clone(),
            source_offset: original.source_offset,
            output_count: original.output_count,
            data_file: data_file.into(),
            data_sha256: original.data_sha256.clone(),
            progress_sha256: String::new(),
        };
        forged.progress_sha256 = progress_sha256(
            forged.source_offset,
            forged.output_count,
            &forged.data_file,
            &forged.data_sha256,
        );
        fs::write(&manifest_path, serde_json::to_vec(&forged).unwrap()).unwrap();
        manager.save(2, &store).unwrap();
        assert_eq!(fs::read(&outside).unwrap(), b"keep outside");
        assert_eq!(fs::read(&sibling).unwrap(), b"keep sibling");
        assert!(manifest_path.exists());
    }
    let original_data = checkpoint_dir.join(&original.data_file);
    for case in 0..4 {
        let mut forged = Manifest {
            version: original.version,
            kind: original.kind,
            identity: original.identity.clone(),
            source_offset: original.source_offset,
            output_count: original.output_count,
            data_file: original.data_file.clone(),
            data_sha256: original.data_sha256.clone(),
            progress_sha256: original.progress_sha256.clone(),
        };
        match case {
            0 => forged.version.major += 1,
            1 => forged.identity.graph_fingerprint.push_str("-other"),
            2 => forged.data_file = "checkpoint-data-/../outside.txt".into(),
            _ => forged.progress_sha256 = "invalid".into(),
        }
        fs::write(&manifest_path, serde_json::to_vec(&forged).unwrap()).unwrap();
        manager.save(2, &store).unwrap();
        assert!(original_data.exists());
    }
    let mut recovered = OutputStore::<i32>::new(1024, None, metrics).unwrap();
    assert_eq!(manager.recover(&mut recovered).unwrap(), 2);
    let previous: Manifest = serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    let previous_data = checkpoint_dir.join(previous.data_file);
    assert!(previous_data.exists());
    manager.save(3, &store).unwrap();
    assert!(!previous_data.exists());
}

#[test]
fn completed_recovery_rejects_a_partial_source_offset() {
    let dir = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        directory: dir.path().to_path_buf(),
        binary_build_id: "partial-test".into(),
        ..CheckpointConfig::default()
    };
    let identity = Identity::new::<i32>(
        "graph".into(),
        "source".into(),
        "input".into(),
        "output".into(),
        "partial-test".into(),
    );
    let metrics = Arc::new(MetricsState::default());
    let manager = CheckpointManager::new(&config, identity, Arc::clone(&metrics)).unwrap();
    let mut store = OutputStore::<i32>::new(1024, None, Arc::clone(&metrics)).unwrap();
    assert!(!manager.recover_completed(2, &mut store).unwrap());
    store.push(7).unwrap();
    manager.save(1, &store).unwrap();
    let disabled = CheckpointManager::new(
        &CheckpointConfig {
            recover: false,
            ..config
        },
        manager.identity.clone(),
        Arc::clone(&metrics),
    )
    .unwrap();
    let mut skipped = OutputStore::<i32>::new(1024, None, Arc::clone(&metrics)).unwrap();
    assert!(!disabled.recover_completed(2, &mut skipped).unwrap());
    let mut recovered = OutputStore::<i32>::new(1024, None, metrics).unwrap();
    assert_eq!(
        manager
            .recover_completed(2, &mut recovered)
            .unwrap_err()
            .kind,
        ErrorKind::Recovery
    );
}

#[test]
fn fold_state_checkpoint_is_distinct_and_integrity_checked() {
    let dir = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        directory: dir.path().to_path_buf(),
        binary_build_id: "fold-state-test".into(),
        ..CheckpointConfig::default()
    };
    let identity = Identity::new::<i32>(
        "graph".into(),
        "source".into(),
        "input".into(),
        "output".into(),
        "fold-state-test".into(),
    );
    let metrics = Arc::new(MetricsState::default());
    let manager = CheckpointManager::new(&config, identity, Arc::clone(&metrics)).unwrap();
    assert!(manager.recover_fold_state(1024).unwrap().is_none());
    manager.save_fold_state(2, b"group state", 1024).unwrap();
    assert_eq!(
        manager.recover_fold_state(1024).unwrap(),
        Some((2, b"group state".to_vec()))
    );
    let mut output = OutputStore::<i32>::new(1024, None, metrics).unwrap();
    assert!(!manager.recover_completed(3, &mut output).unwrap());
    assert_eq!(
        manager.recover(&mut output).unwrap_err().kind,
        ErrorKind::Recovery
    );

    let manifest_path = dir.path().join("latest.json");
    let mut manifest: Manifest =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    let data_path = dir.path().join(&manifest.data_file);
    fs::write(&data_path, b"tampered").unwrap();
    assert_eq!(
        manager.recover_fold_state(1024).unwrap_err().kind,
        ErrorKind::Recovery
    );
    fs::write(&data_path, b"group state").unwrap();
    manifest.kind = CheckpointKind::Output;
    fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    assert_eq!(
        manager.recover_fold_state(1024).unwrap_err().kind,
        ErrorKind::Recovery
    );
    manifest.kind = CheckpointKind::FoldState;
    manifest.output_count = 1;
    manifest.progress_sha256 = manifest.progress_hash();
    fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    assert_eq!(
        manager.recover_fold_state(1024).unwrap_err().kind,
        ErrorKind::Recovery
    );
}

#[test]
fn fold_state_respects_the_job_memory_budget_on_save_and_recovery() {
    let directory = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        directory: directory.path().to_path_buf(),
        binary_build_id: "fold-budget-test".into(),
        ..CheckpointConfig::default()
    };
    let identity = Identity::new::<i32>(
        "graph".into(),
        "source".into(),
        "input".into(),
        "output".into(),
        "fold-budget-test".into(),
    );
    let manager =
        CheckpointManager::new(&config, identity, Arc::new(MetricsState::default())).unwrap();
    assert_eq!(
        manager
            .save_fold_state(1, b"group state", 4)
            .unwrap_err()
            .kind,
        ErrorKind::Resource
    );
    assert!(!directory.path().join("latest.json").exists());
    manager.save_fold_state(1, b"group state", 1024).unwrap();
    assert_eq!(
        manager
            .save_fold_state(2, b"larger group state", 4)
            .unwrap_err()
            .kind,
        ErrorKind::Resource
    );
    assert_eq!(
        manager.recover_fold_state(4).unwrap_err().kind,
        ErrorKind::Resource
    );
    assert_eq!(
        manager.recover_fold_state(1024).unwrap(),
        Some((1, b"group state".to_vec()))
    );
}

#[cfg(unix)]
#[test]
fn checkpoint_recovery_rejects_symlinked_data_even_with_matching_bytes() {
    use std::os::unix::fs::symlink;

    let parent = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        directory: parent.path().join("checkpoints"),
        binary_build_id: "symlink-test".into(),
        ..CheckpointConfig::default()
    };
    let identity = Identity::new::<i32>(
        "graph".into(),
        "source".into(),
        "input".into(),
        "output".into(),
        "symlink-test".into(),
    );
    let manager =
        CheckpointManager::new(&config, identity, Arc::new(MetricsState::default())).unwrap();
    manager.save_fold_state(1, b"group state", 1024).unwrap();
    let manifest: Manifest =
        serde_json::from_slice(&fs::read(config.directory.join("latest.json")).unwrap()).unwrap();
    let outside = parent.path().join("outside.txt");
    fs::write(&outside, b"group state").unwrap();
    let data_path = config.directory.join(&manifest.data_file);
    fs::remove_file(&data_path).unwrap();
    symlink(&outside, &data_path).unwrap();
    assert_eq!(
        manager.recover_fold_state(1024).unwrap_err().kind,
        ErrorKind::Recovery
    );
    assert_eq!(fs::read(outside).unwrap(), b"group state");

    fs::remove_file(&data_path).unwrap();
    fs::write(&data_path, b"group state").unwrap();
    let manifest_path = config.directory.join("latest.json");
    let external_manifest = parent.path().join("external-manifest.json");
    fs::copy(&manifest_path, &external_manifest).unwrap();
    fs::remove_file(&manifest_path).unwrap();
    symlink(&external_manifest, &manifest_path).unwrap();
    assert_eq!(
        manager.recover_fold_state(1024).unwrap_err().kind,
        ErrorKind::Recovery
    );
}

#[test]
fn checkpoint_recovery_rejects_oversized_manifest_before_parsing() {
    let directory = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        directory: directory.path().to_path_buf(),
        binary_build_id: "manifest-limit-test".into(),
        ..CheckpointConfig::default()
    };
    let identity = Identity::new::<i32>(
        "graph".into(),
        "source".into(),
        "input".into(),
        "output".into(),
        "manifest-limit-test".into(),
    );
    let manager =
        CheckpointManager::new(&config, identity, Arc::new(MetricsState::default())).unwrap();
    fs::write(
        directory.path().join("latest.json"),
        vec![b' '; usize::try_from(MAX_MANIFEST_BYTES).unwrap() + 1],
    )
    .unwrap();
    assert_eq!(
        manager.recover_fold_state(1024).unwrap_err().kind,
        ErrorKind::Recovery
    );
}
