// Unit tests for src/stream_checkpoint.rs.
use super::*;
use std::process::Command;

#[test]
fn grouped_state_falls_back_to_verified_history_and_prunes_old_blobs() {
    let temporary = tempfile::tempdir().unwrap();
    let checkpoint = StreamCheckpoint::new::<i32>(
        &CheckpointConfig {
            directory: temporary.path().into(),
            max_checkpoints: 2,
            binary_build_id: "test-build".into(),
            ..CheckpointConfig::default()
        },
        "graph",
        "source",
        "sink",
        "events",
        "sink",
    )
    .unwrap();
    let mut first = signal();
    first.checkpoint_position = Some(vec![1]);
    let mut second = first.clone();
    second.checkpoint_position = Some(vec![2]);
    checkpoint.save_state(&first, b"first-fold", 1024).unwrap();
    checkpoint
        .save_state(&second, b"second-fold", 1024)
        .unwrap();
    assert_eq!(
        checkpoint.load_state(1024).unwrap(),
        Some((second.clone(), b"second-fold".to_vec()))
    );
    let latest = checkpoint.load_at(&checkpoint.path).unwrap().unwrap();
    let state = latest.state.unwrap();
    fs::write(checkpoint.state_path(&state).unwrap(), b"broken-fold").unwrap();
    assert_eq!(
        checkpoint.load_state(1024).unwrap(),
        Some((first, b"first-fold".to_vec()))
    );
    let mut third = second;
    third.checkpoint_position = Some(vec![3]);
    checkpoint.save_state(&third, b"third-fold", 1024).unwrap();
    assert_eq!(
        checkpoint.load_state(1024).unwrap(),
        Some((third, b"third-fold".to_vec()))
    );
    let states = fs::read_dir(temporary.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("stream-state-")
        })
        .count();
    assert_eq!(states, 2);
}

#[test]
fn grouped_recovery_requires_coded_state_and_enforces_the_current_budget() {
    let temporary = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        directory: temporary.path().into(),
        binary_build_id: "test-build".into(),
        ..CheckpointConfig::default()
    };
    let checkpoint =
        StreamCheckpoint::new::<i32>(&config, "graph", "source", "sink", "events", "sink").unwrap();
    checkpoint.save(&signal()).unwrap();
    assert_eq!(
        checkpoint.load_state(1024).unwrap_err().kind,
        ErrorKind::Incompatible
    );

    checkpoint
        .save_state(&signal(), b"fold-state", 1024)
        .unwrap();
    assert_eq!(
        checkpoint.load_state(9).unwrap_err().kind,
        ErrorKind::Resource
    );
    assert_eq!(
        checkpoint.load_state(10).unwrap(),
        Some((signal(), b"fold-state".to_vec()))
    );

    let no_recover = StreamCheckpoint::new::<i32>(
        &CheckpointConfig {
            recover: false,
            ..config
        },
        "graph",
        "source",
        "sink",
        "events",
        "sink",
    )
    .unwrap();
    assert!(no_recover.load_state(10).unwrap().is_none());
}

#[test]
fn grouped_state_rejects_invalid_digest_missing_blob_and_corrupt_retry() {
    for invalid_hash in [
        String::from("../outside"),
        "0".repeat(64),
        "A".repeat(64),
        format!("{}g", "0".repeat(63)),
    ] {
        let temporary = tempfile::tempdir().unwrap();
        let checkpoint = checkpoint(temporary.path(), "sink");
        checkpoint.save_state(&signal(), b"fold", 1024).unwrap();
        let mut manifest = checkpoint.load_at(&checkpoint.path).unwrap().unwrap();
        manifest.state.as_mut().unwrap().hash = invalid_hash;
        manifest.checksum = checksum(
            manifest.version,
            &manifest.identity,
            &manifest.progress,
            manifest.state.as_ref(),
        )
        .unwrap();
        fs::write(&checkpoint.path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        assert_eq!(
            checkpoint.load_state(1024).unwrap_err().kind,
            ErrorKind::Recovery
        );
    }

    let temporary = tempfile::tempdir().unwrap();
    let checkpoint = checkpoint(temporary.path(), "sink");
    assert_eq!(
        checkpoint
            .save_state(&signal(), b"fold", 3)
            .unwrap_err()
            .kind,
        ErrorKind::Resource
    );
    assert!(checkpoint.load_state(1024).unwrap().is_none());
    checkpoint.save_state(&signal(), b"fold", 1024).unwrap();
    checkpoint.save_state(&signal(), b"fold", 1024).unwrap();
    let state = checkpoint
        .load_at(&checkpoint.path)
        .unwrap()
        .unwrap()
        .state
        .unwrap();
    fs::write(checkpoint.state_path(&state).unwrap(), b"evil").unwrap();
    assert_eq!(
        checkpoint.load_state(1024).unwrap_err().kind,
        ErrorKind::Recovery
    );
    assert_eq!(
        checkpoint
            .save_state(&signal(), b"fold", 1024)
            .unwrap_err()
            .kind,
        ErrorKind::Recovery
    );
}

#[cfg(unix)]
#[test]
fn grouped_state_and_history_symlinks_fail_closed() {
    let temporary = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        directory: temporary.path().into(),
        max_checkpoints: 2,
        binary_build_id: "test-build".into(),
        ..CheckpointConfig::default()
    };
    let checkpoint =
        StreamCheckpoint::new::<i32>(&config, "graph", "source", "sink", "events", "sink").unwrap();
    checkpoint.save_state(&signal(), b"fold", 1024).unwrap();
    let state = checkpoint
        .load_at(&checkpoint.path)
        .unwrap()
        .unwrap()
        .state
        .unwrap();
    let state_path = checkpoint.state_path(&state).unwrap();
    let outside = temporary.path().join("outside");
    fs::write(&outside, b"fold").unwrap();
    fs::remove_file(&state_path).unwrap();
    std::os::unix::fs::symlink(&outside, &state_path).unwrap();
    assert_eq!(
        checkpoint.load_state(1024).unwrap_err().kind,
        ErrorKind::Recovery
    );
    assert_eq!(fs::read(&outside).unwrap(), b"fold");

    fs::remove_file(&state_path).unwrap();
    fs::write(&state_path, b"fold").unwrap();
    let history = checkpoint.slot_path(1);
    std::os::unix::fs::symlink(&outside, &history).unwrap();
    let mut next = signal();
    next.checkpoint_position = Some(vec![5]);
    assert_eq!(
        checkpoint.save(&next).unwrap_err().kind,
        ErrorKind::Recovery
    );
    assert_eq!(checkpoint.load().unwrap(), Some(signal()));
    assert_eq!(fs::read(&outside).unwrap(), b"fold");
}

#[test]
fn grouped_state_rejects_wrong_blob_size_before_decoding() {
    let temporary = tempfile::tempdir().unwrap();
    let checkpoint = checkpoint(temporary.path(), "sink");
    checkpoint.save_state(&signal(), b"fold", 1024).unwrap();
    let state = checkpoint
        .load_at(&checkpoint.path)
        .unwrap()
        .unwrap()
        .state
        .unwrap();
    let path = checkpoint.state_path(&state).unwrap();
    fs::write(&path, b"short").unwrap();
    assert_eq!(
        checkpoint.load_state(1024).unwrap_err().kind,
        ErrorKind::Recovery
    );
    assert_eq!(
        checkpoint.read_state(&state, 1024).unwrap_err().kind,
        ErrorKind::Recovery
    );
    fs::remove_file(&path).unwrap();
    fs::create_dir(&path).unwrap();
    assert_eq!(
        checkpoint.read_state(&state, 1024).unwrap_err().kind,
        ErrorKind::Recovery
    );
}

#[test]
fn grouped_state_pruning_preserves_files_outside_the_state_namespace() {
    let temporary = tempfile::tempdir().unwrap();
    let checkpoint = checkpoint(temporary.path(), "sink");
    checkpoint.save_state(&signal(), b"old-fold", 1024).unwrap();
    let old = checkpoint
        .load_at(&checkpoint.path)
        .unwrap()
        .unwrap()
        .state
        .unwrap();
    let protected = [
        "stream-state-short.bin".to_owned(),
        format!("stream-state-{}.bin", "G".repeat(64)),
        format!("stream-state-{}.bin", "A".repeat(64)),
        format!("stream-state-{}.tmp", "0".repeat(64)),
    ];
    for name in &protected {
        fs::write(temporary.path().join(name), b"unrelated").unwrap();
    }
    let mut next = signal();
    next.checkpoint_position = Some(vec![5]);
    checkpoint.save_state(&next, b"new-fold", 1024).unwrap();
    assert!(!checkpoint.state_path(&old).unwrap().exists());
    for name in &protected {
        assert_eq!(fs::read(temporary.path().join(name)).unwrap(), b"unrelated");
    }
    assert_eq!(
        checkpoint.load_state(1024).unwrap(),
        Some((next, b"new-fold".to_vec()))
    );
}

#[test]
fn stream_checkpoint_crash_child() {
    let Ok(directory) = std::env::var("IRONBEAM_STREAM_CHECKPOINT_CHILD_DIRECTORY") else {
        return;
    };
    let config = CheckpointConfig {
        directory: directory.into(),
        max_checkpoints: 2,
        binary_build_id: "test-build".into(),
        ..CheckpointConfig::default()
    };
    let checkpoint =
        StreamCheckpoint::new::<i32>(&config, "graph", "source", "sink", "events", "sink").unwrap();
    let mut progress = signal();
    progress.checkpoint_position = Some(vec![2]);
    checkpoint.save(&progress).unwrap();
    panic!("stream checkpoint child did not exit at its fault stage");
}

#[test]
fn grouped_state_crash_child() {
    let Ok(directory) = std::env::var("IRONBEAM_GROUPED_STATE_CHILD_DIRECTORY") else {
        return;
    };
    let config = CheckpointConfig {
        directory: directory.into(),
        max_checkpoints: 2,
        binary_build_id: "test-build".into(),
        ..CheckpointConfig::default()
    };
    let checkpoint =
        StreamCheckpoint::new::<i32>(&config, "graph", "source", "sink", "events", "sink").unwrap();
    let mut progress = signal();
    progress.checkpoint_position = Some(vec![2]);
    checkpoint
        .save_state(&progress, b"second-fold", 1024)
        .unwrap();
    panic!("grouped state child did not exit at its fault stage");
}

#[test]
fn grouped_state_survives_process_exit_at_publication_boundaries() {
    for (stage, expected_cursor, expected_state) in [
        ("stream_history_synced", 1, b"first-fold".as_slice()),
        ("stream_latest_renamed", 2, b"second-fold".as_slice()),
        ("stream_latest_synced", 2, b"second-fold".as_slice()),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let config = CheckpointConfig {
            directory: directory.path().into(),
            max_checkpoints: 2,
            binary_build_id: "test-build".into(),
            ..CheckpointConfig::default()
        };
        let checkpoint =
            StreamCheckpoint::new::<i32>(&config, "graph", "source", "sink", "events", "sink")
                .unwrap();
        let mut first = signal();
        first.checkpoint_position = Some(vec![1]);
        checkpoint.save_state(&first, b"first-fold", 1024).unwrap();

        let result = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "stream_checkpoint::tests::grouped_state_crash_child",
            ])
            .env("IRONBEAM_GROUPED_STATE_CHILD_DIRECTORY", directory.path())
            .env("IRONBEAM_CHECKPOINT_EXIT_STAGE", stage)
            .output()
            .unwrap();
        assert_eq!(result.status.code(), Some(86), "stage {stage}: {result:?}");
        let (progress, state) = checkpoint.load_state(1024).unwrap().unwrap();
        assert_eq!(progress.checkpoint_position, Some(vec![expected_cursor]));
        assert_eq!(state, expected_state);
        if stage == "stream_latest_renamed" {
            fs::remove_file(&checkpoint.path).unwrap();
            let (progress, state) = checkpoint.load_state(1024).unwrap().unwrap();
            assert_eq!(progress.checkpoint_position, Some(vec![1]));
            assert_eq!(state, b"first-fold");
        }
    }
}

#[test]
fn stream_checkpoint_survives_process_exit_at_publication_boundaries() {
    for (stage, expected_cursor) in [
        ("stream_history_synced", 1),
        ("stream_latest_renamed", 2),
        ("stream_latest_synced", 2),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let config = CheckpointConfig {
            directory: directory.path().into(),
            max_checkpoints: 2,
            binary_build_id: "test-build".into(),
            ..CheckpointConfig::default()
        };
        let checkpoint =
            StreamCheckpoint::new::<i32>(&config, "graph", "source", "sink", "events", "sink")
                .unwrap();
        let mut first = signal();
        first.checkpoint_position = Some(vec![1]);
        checkpoint.save(&first).unwrap();

        let result = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "stream_checkpoint::tests::stream_checkpoint_crash_child",
            ])
            .env(
                "IRONBEAM_STREAM_CHECKPOINT_CHILD_DIRECTORY",
                directory.path(),
            )
            .env("IRONBEAM_CHECKPOINT_EXIT_STAGE", stage)
            .output()
            .unwrap();
        assert_eq!(result.status.code(), Some(86), "stage {stage}: {result:?}");
        assert_eq!(
            checkpoint.load().unwrap().unwrap().checkpoint_position,
            Some(vec![expected_cursor])
        );
        if stage == "stream_latest_renamed" {
            fs::remove_file(&checkpoint.path).unwrap();
            assert_eq!(
                checkpoint.load().unwrap().unwrap().checkpoint_position,
                Some(vec![1])
            );
        }
    }
}

fn checkpoint(directory: &std::path::Path, sink_digest: &str) -> StreamCheckpoint {
    StreamCheckpoint::new::<i32>(
        &CheckpointConfig {
            directory: directory.into(),
            interval_batches: 1,
            max_checkpoints: 1,
            recover: true,
            binary_build_id: "test-build".into(),
        },
        "graph",
        "source",
        sink_digest,
        "events",
        "sink",
    )
    .unwrap()
}

fn signal() -> ProgressSignal {
    ProgressSignal {
        source_key: "events".into(),
        split_id: "split-a".into(),
        checkpoint_position: Some(vec![4]),
        event_time_ns: Some(10),
        completed: false,
    }
}

#[test]
fn roundtrip_and_reject_identity_or_corruption() {
    let temporary = tempfile::tempdir().unwrap();
    let checkpoint = checkpoint(temporary.path(), "sink-v1");
    assert!(checkpoint.load().unwrap().is_none());
    checkpoint.save(&signal()).unwrap();
    assert_eq!(checkpoint.load().unwrap(), Some(signal()));
    assert!(
        self::checkpoint(temporary.path(), "sink-v2")
            .load()
            .is_err()
    );
    fs::write(&checkpoint.path, b"{broken").unwrap();
    assert!(checkpoint.load().is_err());
}

#[test]
fn invalid_progress_never_replaces_manifest() {
    let temporary = tempfile::tempdir().unwrap();
    let checkpoint = checkpoint(temporary.path(), "sink");
    checkpoint.save(&signal()).unwrap();
    let mut invalid = signal();
    invalid.source_key = "other".into();
    assert!(checkpoint.save(&invalid).is_err());
    invalid = signal();
    invalid.split_id.clear();
    assert!(checkpoint.save(&invalid).is_err());
    assert_eq!(checkpoint.load().unwrap(), Some(signal()));
}

#[test]
fn stream_checkpoint_requires_a_stable_build_identity_and_batch_interval() {
    let temporary = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        directory: temporary.path().into(),
        interval_batches: 1,
        max_checkpoints: 1,
        recover: true,
        binary_build_id: String::new(),
    };
    assert!(
        StreamCheckpoint::new::<i32>(&config, "graph", "source", "sink", "events", "sink").is_err()
    );
    assert!(!temporary.path().join("stream-latest.json").exists());

    let config = CheckpointConfig {
        interval_batches: 1,
        max_checkpoints: 0,
        binary_build_id: "test-build".into(),
        ..config
    };
    assert!(
        StreamCheckpoint::new::<i32>(&config, "graph", "source", "sink", "events", "sink").is_err()
    );

    let config = CheckpointConfig {
        interval_batches: 2,
        max_checkpoints: 1,
        ..config
    };
    assert!(
        StreamCheckpoint::new::<i32>(&config, "graph", "source", "sink", "events", "sink").is_err()
    );
    assert!(!temporary.path().join("stream-latest.json").exists());
}

#[test]
fn stream_history_falls_back_and_never_crosses_execution_identity() {
    let temporary = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        directory: temporary.path().into(),
        max_checkpoints: 3,
        binary_build_id: "test-build".into(),
        ..CheckpointConfig::default()
    };
    let checkpoint =
        StreamCheckpoint::new::<i32>(&config, "graph", "source", "sink", "events", "sink").unwrap();
    for cursor in 1..=4 {
        let mut progress = signal();
        progress.checkpoint_position = Some(vec![cursor]);
        checkpoint.save(&progress).unwrap();
    }
    let history_one: Manifest =
        serde_json::from_slice(&fs::read(temporary.path().join("stream-history-1.json")).unwrap())
            .unwrap();
    let history_two: Manifest =
        serde_json::from_slice(&fs::read(temporary.path().join("stream-history-2.json")).unwrap())
            .unwrap();
    assert_eq!(history_one.progress.checkpoint_position, Some(vec![3]));
    assert_eq!(history_two.progress.checkpoint_position, Some(vec![2]));
    fs::write(&checkpoint.path, b"{broken").unwrap();
    assert_eq!(
        checkpoint.load().unwrap().unwrap().checkpoint_position,
        Some(vec![3])
    );

    let foreign =
        StreamCheckpoint::new::<i32>(&config, "other", "source", "sink", "events", "sink").unwrap();
    assert_eq!(foreign.load().unwrap_err().kind, ErrorKind::Incompatible);

    let latest_only = StreamCheckpoint::new::<i32>(
        &CheckpointConfig {
            max_checkpoints: 1,
            ..config
        },
        "graph",
        "source",
        "sink",
        "events",
        "sink",
    )
    .unwrap();
    let mut progress = signal();
    progress.checkpoint_position = Some(vec![5]);
    latest_only.save(&progress).unwrap();
    assert!(!temporary.path().join("stream-history-1.json").exists());
    assert!(!temporary.path().join("stream-history-2.json").exists());
    assert_eq!(latest_only.load().unwrap(), Some(progress));
}

#[test]
fn recovery_rejects_every_corrupt_retained_stream_snapshot() {
    let temporary = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        directory: temporary.path().into(),
        max_checkpoints: 2,
        binary_build_id: "test-build".into(),
        ..CheckpointConfig::default()
    };
    let checkpoint =
        StreamCheckpoint::new::<i32>(&config, "graph", "source", "sink", "events", "sink").unwrap();
    for cursor in 1..=2 {
        let mut progress = signal();
        progress.checkpoint_position = Some(vec![cursor]);
        checkpoint.save(&progress).unwrap();
    }
    fs::write(&checkpoint.path, b"{broken").unwrap();
    fs::write(temporary.path().join("stream-history-1.json"), b"{broken").unwrap();

    assert_eq!(checkpoint.load().unwrap_err().kind, ErrorKind::Recovery);
}

#[test]
fn pair_checkpoint_binds_both_sink_and_coder_contracts() {
    let temporary = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        directory: temporary.path().into(),
        interval_batches: 1,
        max_checkpoints: 1,
        recover: true,
        binary_build_id: "test-build".into(),
    };
    let pair = |right_digest: &str| {
        StreamCheckpoint::new_pair::<i32, i64>(
            &config,
            "graph",
            "source",
            "events",
            ("left", "left-v1"),
            ("right", right_digest),
        )
        .unwrap()
    };
    pair("right-v1").save(&signal()).unwrap();
    assert_eq!(pair("right-v1").load().unwrap(), Some(signal()));
    assert!(pair("right-v2").load().is_err());
    assert!(
        StreamCheckpoint::new_pair::<i32, i32>(
            &config,
            "graph",
            "source",
            "events",
            ("left", "left-v1"),
            ("right", "right-v1"),
        )
        .unwrap()
        .load()
        .is_err()
    );
    assert!(
        StreamCheckpoint::new::<i32>(&config, "graph", "source", "left-v1", "events", "left",)
            .unwrap()
            .load()
            .is_err()
    );
}

#[cfg(unix)]
#[test]
fn reject_symlink_manifest() {
    let temporary = tempfile::tempdir().unwrap();
    let checkpoint = checkpoint(temporary.path(), "sink");
    let target = temporary.path().join("target");
    fs::write(&target, b"outside").unwrap();
    std::os::unix::fs::symlink(&target, &checkpoint.path).unwrap();
    assert!(checkpoint.load().is_err());
    assert!(checkpoint.save(&signal()).is_err());
    assert_eq!(fs::read(target).unwrap(), b"outside");
}

#[test]
fn reject_version_checksum_and_oversized_manifest() {
    let temporary = tempfile::tempdir().unwrap();
    let checkpoint = checkpoint(temporary.path(), "sink");
    checkpoint.save(&signal()).unwrap();
    let original: serde_json::Value =
        serde_json::from_slice(&fs::read(&checkpoint.path).unwrap()).unwrap();
    let mut changed = original.clone();
    changed["version"] = serde_json::json!(1);
    fs::write(&checkpoint.path, serde_json::to_vec(&changed).unwrap()).unwrap();
    assert!(checkpoint.load().is_err());
    let mut changed = original;
    changed["progress"]["event_time_ns"] = serde_json::json!(20);
    fs::write(&checkpoint.path, serde_json::to_vec(&changed).unwrap()).unwrap();
    assert!(checkpoint.load().is_err());
    fs::write(
        &checkpoint.path,
        vec![b' '; usize::try_from(MAX_MANIFEST_BYTES).unwrap() + 1],
    )
    .unwrap();
    assert!(checkpoint.load().is_err());
}

#[test]
fn rejects_invalid_cursor_and_can_disable_recovery() {
    let temporary = tempfile::tempdir().unwrap();
    let checkpoint = checkpoint(temporary.path(), "sink");
    let mut invalid = signal();
    invalid.checkpoint_position = None;
    assert!(checkpoint.save(&invalid).is_err());
    invalid.checkpoint_position = Some(vec![0; usize::try_from(MAX_MANIFEST_BYTES).unwrap() + 1]);
    assert!(checkpoint.save(&invalid).is_err());
    checkpoint.save(&signal()).unwrap();
    let no_recover = StreamCheckpoint::new::<i32>(
        &CheckpointConfig {
            directory: temporary.path().into(),
            interval_batches: 1,
            max_checkpoints: 1,
            recover: false,
            binary_build_id: "test-build".into(),
        },
        "graph",
        "source",
        "sink",
        "events",
        "sink",
    )
    .unwrap();
    assert!(no_recover.load().unwrap().is_none());
}

#[test]
fn load_rejects_validly_checksummed_incomplete_progress_without_cursor() {
    let temporary = tempfile::tempdir().unwrap();
    let checkpoint = checkpoint(temporary.path(), "sink");
    let mut progress = signal();
    progress.checkpoint_position = None;
    let manifest = Manifest {
        version: STREAM_CHECKPOINT_VERSION,
        identity: checkpoint.identity.clone(),
        checksum: checksum(
            STREAM_CHECKPOINT_VERSION,
            &checkpoint.identity,
            &progress,
            None,
        )
        .unwrap(),
        progress,
        state: None,
    };
    fs::write(&checkpoint.path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let error = checkpoint.load().unwrap_err();
    assert!(error.message.contains("requires a cursor"));
}

#[test]
fn reject_invalid_directory_and_oversized_signal() {
    let temporary = tempfile::tempdir().unwrap();
    let file = temporary.path().join("file");
    fs::write(&file, b"not a directory").unwrap();
    assert!(
        StreamCheckpoint::new::<i32>(
            &CheckpointConfig {
                directory: file,
                interval_batches: 1,
                max_checkpoints: 1,
                recover: true,
                binary_build_id: "test-build".into()
            },
            "graph",
            "source",
            "sink",
            "events",
            "sink"
        )
        .is_err()
    );
    let checkpoint = checkpoint(temporary.path(), "sink");
    let mut oversized = signal();
    oversized.split_id = "x".repeat(usize::try_from(MAX_MANIFEST_BYTES).unwrap());
    assert!(checkpoint.save(&oversized).is_err());
    assert!(!checkpoint.path.exists());
}

#[test]
fn recovery_reports_directory_replaced_after_checkpoint_setup() {
    let temporary = tempfile::tempdir().unwrap();
    let directory = temporary.path().join("checkpoint");
    let checkpoint = checkpoint(&directory, "sink");
    fs::remove_dir(&directory).unwrap();
    fs::write(&directory, b"former checkpoint directory").unwrap();
    assert_eq!(
        checkpoint.load().err().unwrap().kind,
        ironbeam_core::ErrorKind::Io
    );
}

#[test]
fn valid_checksum_cannot_override_progress_identity() {
    let temporary = tempfile::tempdir().unwrap();
    let checkpoint = checkpoint(temporary.path(), "sink");
    let mut altered = signal();
    altered.source_key = "other".into();
    let manifest = Manifest {
        version: STREAM_CHECKPOINT_VERSION,
        identity: checkpoint.identity.clone(),
        checksum: checksum(
            STREAM_CHECKPOINT_VERSION,
            &checkpoint.identity,
            &altered,
            None,
        )
        .unwrap(),
        progress: altered,
        state: None,
    };
    fs::write(&checkpoint.path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    assert!(checkpoint.load().is_err());
}
