//! Durable progress for a single resumable source and idempotent sink.
use crate::CheckpointConfig;
#[cfg(test)]
use crate::checkpoint::crash_after;
use crate::checkpoint::{MAX_RETAINED_CHECKPOINTS, sync_directory};
use ironbeam_core::{Element, Error, ErrorKind, ProgressSignal, Version};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
const STREAM_CHECKPOINT_VERSION: u32 = 1;

pub(crate) fn hex_bytes(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        result.push(char::from(DIGITS[usize::from(byte >> 4)]));
        result.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    result
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct StreamIdentity {
    graph_fingerprint: String,
    source_digest: String,
    sink_digest: String,
    source_key: String,
    sink_key: String,
    coder_id: String,
    coder_version: Version,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    second_sink: Option<SecondSinkIdentity>,
    binary_build_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SecondSinkIdentity {
    key: String,
    digest: String,
    coder_id: String,
    coder_version: Version,
}

#[derive(Clone, Serialize, Deserialize)]
struct Manifest {
    version: u32,
    identity: StreamIdentity,
    progress: ProgressSignal,
    checksum: String,
}

pub(crate) struct StreamCheckpoint {
    path: PathBuf,
    identity: StreamIdentity,
    recover: bool,
    max_checkpoints: usize,
}

impl StreamCheckpoint {
    pub(crate) fn new<T: Element>(
        config: &CheckpointConfig,
        graph_fingerprint: &str,
        source_digest: &str,
        sink_digest: &str,
        source_key: &str,
        sink_key: &str,
    ) -> Result<Self, Error> {
        if config.interval_batches != 1 {
            return Err(Error::graph(
                "checkpoint",
                "streaming requires checkpoint interval of one batch",
            ));
        }
        if !(1..=MAX_RETAINED_CHECKPOINTS).contains(&config.max_checkpoints) {
            return Err(Error::graph(
                "checkpoint",
                "checkpoint retention must be between 1 and 128",
            ));
        }
        if config.binary_build_id.is_empty() {
            return Err(Error::graph(
                "checkpoint",
                "streaming requires a stable binary build ID",
            ));
        }
        fs::create_dir_all(&config.directory)
            .map_err(|error| Error::io("checkpoint", "direct", error.to_string()))?;
        Ok(Self {
            path: config.directory.join("stream-latest.json"),
            identity: StreamIdentity {
                graph_fingerprint: graph_fingerprint.to_owned(),
                source_digest: source_digest.to_owned(),
                sink_digest: sink_digest.to_owned(),
                source_key: source_key.to_owned(),
                sink_key: sink_key.to_owned(),
                coder_id: T::coder_identity(),
                coder_version: T::CODER_VERSION,
                second_sink: None,
                binary_build_id: config.binary_build_id.clone(),
            },
            recover: config.recover,
            max_checkpoints: config.max_checkpoints,
        })
    }

    pub(crate) fn new_pair<T: Element, U: Element>(
        config: &CheckpointConfig,
        graph_fingerprint: &str,
        source_digest: &str,
        source_key: &str,
        left: (&str, &str),
        right: (&str, &str),
    ) -> Result<Self, Error> {
        let mut checkpoint = Self::new::<T>(
            config,
            graph_fingerprint,
            source_digest,
            left.1,
            source_key,
            left.0,
        )?;
        checkpoint.identity.second_sink = Some(SecondSinkIdentity {
            key: right.0.to_owned(),
            digest: right.1.to_owned(),
            coder_id: U::coder_identity(),
            coder_version: U::CODER_VERSION,
        });
        Ok(checkpoint)
    }

    pub(crate) fn load(&self) -> Result<Option<ProgressSignal>, Error> {
        if !self.recover {
            return Ok(None);
        }
        let mut latest_error = None;
        for slot in 0..self.max_checkpoints {
            let path = self.slot_path(slot);
            match self.load_at(&path) {
                Ok(Some(manifest)) => return Ok(Some(manifest.progress)),
                Ok(None) => {}
                Err(error) if error.kind == ErrorKind::Recovery => {
                    if latest_error.is_none() {
                        latest_error = Some(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
        if let Some(error) = latest_error {
            return Err(error);
        }
        Ok(None)
    }

    fn slot_path(&self, slot: usize) -> PathBuf {
        if slot == 0 {
            self.path.clone()
        } else {
            self.path
                .parent()
                .expect("stream checkpoint path has parent")
                .join(format!("stream-history-{slot}.json"))
        }
    }

    fn load_at(&self, path: &Path) -> Result<Option<Manifest>, Error> {
        let metadata = match fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(Error::io("checkpoint", "direct", error.to_string())),
        };
        if !metadata.file_type().is_file() || metadata.len() > MAX_MANIFEST_BYTES {
            return Err(Error::recovery(
                "checkpoint",
                "stream manifest must be a regular file within the size limit",
            ));
        }
        let bytes =
            fs::read(path).map_err(|error| Error::io("checkpoint", "direct", error.to_string()))?;
        let manifest: Manifest = serde_json::from_slice(&bytes)
            .map_err(|error| Error::recovery("checkpoint", error.to_string()))?;
        if manifest.version != STREAM_CHECKPOINT_VERSION || manifest.identity != self.identity {
            return Err(Error::incompatible(
                "stream checkpoint version or execution identity mismatch",
            ));
        }
        if manifest.checksum != checksum(manifest.version, &manifest.identity, &manifest.progress)?
        {
            return Err(Error::recovery(
                "checkpoint",
                "stream progress checksum mismatch",
            ));
        }
        self.validate_progress(&manifest.progress)?;
        Ok(Some(manifest))
    }

    pub(crate) fn save(&self, progress: &ProgressSignal) -> Result<(), Error> {
        self.validate_progress(progress)?;
        let manifest = Manifest {
            version: STREAM_CHECKPOINT_VERSION,
            identity: self.identity.clone(),
            progress: progress.clone(),
            checksum: checksum(STREAM_CHECKPOINT_VERSION, &self.identity, progress)?,
        };
        let encoded = serde_json::to_vec(&manifest)
            .map_err(|error| Error::recovery("checkpoint", error.to_string()))?;
        if u64::try_from(encoded.len()).unwrap_or(u64::MAX) > MAX_MANIFEST_BYTES {
            return Err(Error::resource(
                "checkpoint",
                "stream manifest exceeds size limit",
            ));
        }
        if let Ok(metadata) = fs::symlink_metadata(&self.path)
            && !metadata.file_type().is_file()
        {
            return Err(Error::recovery(
                "checkpoint",
                "stream manifest is not a regular file",
            ));
        }
        let directory = self
            .path
            .parent()
            .expect("stream checkpoint path has parent");
        let previous = (0..self.max_checkpoints)
            .map(|slot| self.load_at(&self.slot_path(slot)).ok().flatten())
            .collect::<Vec<_>>();
        let mut stale_history = Vec::new();
        for entry in fs::read_dir(directory)
            .map_err(|error| Error::io("checkpoint", "direct", error.to_string()))?
        {
            let entry =
                entry.map_err(|error| Error::io("checkpoint", "direct", error.to_string()))?;
            let name = entry.file_name();
            let Some(slot) = name
                .to_str()
                .and_then(|name| name.strip_prefix("stream-history-"))
                .and_then(|name| name.strip_suffix(".json"))
                .and_then(|name| name.parse::<usize>().ok())
            else {
                continue;
            };
            if slot >= self.max_checkpoints
                && slot < MAX_RETAINED_CHECKPOINTS
                && self.load_at(&entry.path()).ok().flatten().is_some()
            {
                stale_history.push(entry.path());
            }
        }
        for slot in (1..self.max_checkpoints).rev() {
            let target = self.slot_path(slot);
            if let Some(old) = &previous[slot - 1] {
                self.write_manifest(&target, old)?;
            }
        }
        if self.max_checkpoints > 1 {
            sync_directory(directory)?;
            #[cfg(test)]
            crash_after("stream_history_synced");
        }
        self.write_manifest(&self.path, &manifest)?;
        #[cfg(test)]
        crash_after("stream_latest_renamed");
        sync_directory(directory)?;
        #[cfg(test)]
        crash_after("stream_latest_synced");
        for path in stale_history {
            let _ = fs::remove_file(path);
        }
        Ok(())
    }

    fn write_manifest(&self, path: &Path, manifest: &Manifest) -> Result<(), Error> {
        let bytes = serde_json::to_vec(manifest)
            .map_err(|error| Error::recovery("checkpoint", error.to_string()))?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_MANIFEST_BYTES {
            return Err(Error::resource(
                "checkpoint",
                "stream manifest exceeds size limit",
            ));
        }
        if let Ok(metadata) = fs::symlink_metadata(path)
            && !metadata.file_type().is_file()
        {
            return Err(Error::recovery(
                "checkpoint",
                "stream manifest is not a regular file",
            ));
        }
        let directory = path.parent().expect("stream manifest path has parent");
        let mut temporary = tempfile::NamedTempFile::new_in(directory)
            .map_err(|error| Error::io("checkpoint", "direct", error.to_string()))?;
        temporary
            .write_all(&bytes)
            .map_err(|error| Error::io("checkpoint", "direct", error.to_string()))?;
        temporary
            .as_file()
            .sync_all()
            .map_err(|error| Error::io("checkpoint", "direct", error.to_string()))?;
        temporary
            .persist(path)
            .map_err(|error| Error::io("checkpoint", "direct", error.error.to_string()))?;
        Ok(())
    }

    fn validate_progress(&self, progress: &ProgressSignal) -> Result<(), Error> {
        if progress.source_key != self.identity.source_key || progress.split_id.is_empty() {
            return Err(Error::recovery(
                "checkpoint",
                "stream source or split identity mismatch",
            ));
        }
        if progress.checkpoint_position.as_ref().is_some_and(|cursor| {
            u64::try_from(cursor.len()).unwrap_or(u64::MAX) > MAX_MANIFEST_BYTES
        }) {
            return Err(Error::resource(
                "checkpoint",
                "stream source cursor exceeds size limit",
            ));
        }
        if !progress.completed && progress.checkpoint_position.is_none() {
            return Err(Error::recovery(
                "checkpoint",
                "incomplete stream progress requires a cursor",
            ));
        }
        Ok(())
    }
}

fn checksum(
    version: u32,
    identity: &StreamIdentity,
    progress: &ProgressSignal,
) -> Result<String, Error> {
    let bytes = serde_json::to_vec(&(version, identity, progress))
        .map_err(|error| Error::recovery("checkpoint", error.to_string()))?;
    Ok(hex_bytes(&Sha256::digest(bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

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
            StreamCheckpoint::new::<i32>(&config, "graph", "source", "sink", "events", "sink")
                .unwrap();
        let mut progress = signal();
        progress.checkpoint_position = Some(vec![2]);
        checkpoint.save(&progress).unwrap();
        panic!("stream checkpoint child did not exit at its fault stage");
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
            StreamCheckpoint::new::<i32>(&config, "graph", "source", "sink", "events", "sink")
                .is_err()
        );
        assert!(!temporary.path().join("stream-latest.json").exists());

        let config = CheckpointConfig {
            interval_batches: 1,
            max_checkpoints: 0,
            binary_build_id: "test-build".into(),
            ..config
        };
        assert!(
            StreamCheckpoint::new::<i32>(&config, "graph", "source", "sink", "events", "sink")
                .is_err()
        );

        let config = CheckpointConfig {
            interval_batches: 2,
            max_checkpoints: 1,
            ..config
        };
        assert!(
            StreamCheckpoint::new::<i32>(&config, "graph", "source", "sink", "events", "sink")
                .is_err()
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
            StreamCheckpoint::new::<i32>(&config, "graph", "source", "sink", "events", "sink")
                .unwrap();
        for cursor in 1..=4 {
            let mut progress = signal();
            progress.checkpoint_position = Some(vec![cursor]);
            checkpoint.save(&progress).unwrap();
        }
        let history_one: Manifest = serde_json::from_slice(
            &fs::read(temporary.path().join("stream-history-1.json")).unwrap(),
        )
        .unwrap();
        let history_two: Manifest = serde_json::from_slice(
            &fs::read(temporary.path().join("stream-history-2.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(history_one.progress.checkpoint_position, Some(vec![3]));
        assert_eq!(history_two.progress.checkpoint_position, Some(vec![2]));
        fs::write(&checkpoint.path, b"{broken").unwrap();
        assert_eq!(
            checkpoint.load().unwrap().unwrap().checkpoint_position,
            Some(vec![3])
        );

        let foreign =
            StreamCheckpoint::new::<i32>(&config, "other", "source", "sink", "events", "sink")
                .unwrap();
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
        changed["version"] = serde_json::json!(2);
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
        invalid.checkpoint_position =
            Some(vec![0; usize::try_from(MAX_MANIFEST_BYTES).unwrap() + 1]);
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
            checksum: checksum(STREAM_CHECKPOINT_VERSION, &checkpoint.identity, &progress).unwrap(),
            progress,
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
            checksum: checksum(STREAM_CHECKPOINT_VERSION, &checkpoint.identity, &altered).unwrap(),
            progress: altered,
        };
        fs::write(&checkpoint.path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        assert!(checkpoint.load().is_err());
    }
}
