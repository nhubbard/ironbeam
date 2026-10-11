//! Durable progress for a single resumable source and idempotent sink.
use crate::CheckpointConfig;
use ironbeam_core::{Element, Error, ProgressSignal, Version};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Write;
use std::path::PathBuf;

const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
const STREAM_CHECKPOINT_VERSION: u32 = 1;

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

#[derive(Serialize, Deserialize)]
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
        let metadata = match fs::symlink_metadata(&self.path) {
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
        let bytes = fs::read(&self.path)
            .map_err(|error| Error::io("checkpoint", "direct", error.to_string()))?;
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
        Ok(Some(manifest.progress))
    }

    pub(crate) fn save(&self, progress: &ProgressSignal) -> Result<(), Error> {
        self.validate_progress(progress)?;
        let manifest = Manifest {
            version: STREAM_CHECKPOINT_VERSION,
            identity: self.identity.clone(),
            progress: progress.clone(),
            checksum: checksum(STREAM_CHECKPOINT_VERSION, &self.identity, progress)?,
        };
        let bytes = serde_json::to_vec(&manifest)
            .map_err(|error| Error::recovery("checkpoint", error.to_string()))?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_MANIFEST_BYTES {
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
            .persist(&self.path)
            .map_err(|error| Error::io("checkpoint", "direct", error.error.to_string()))?;
        fs::File::open(directory)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| Error::io("checkpoint", "direct", error.to_string()))?;
        Ok(())
    }

    fn validate_progress(&self, progress: &ProgressSignal) -> Result<(), Error> {
        if progress.source_key != self.identity.source_key || progress.split_id.is_empty() {
            return Err(Error::recovery(
                "checkpoint",
                "stream source or split identity mismatch",
            ));
        }
        if progress
            .checkpoint_position
            .as_ref()
            .is_some_and(|cursor| cursor.len() > MAX_MANIFEST_BYTES as usize)
        {
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
    Ok(Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn checkpoint(directory: &std::path::Path, sink_digest: &str) -> StreamCheckpoint {
        StreamCheckpoint::new::<i32>(
            &CheckpointConfig {
                directory: directory.into(),
                interval_batches: 1,
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
        assert_eq!(checkpoint.load().unwrap(), Some(signal()));
    }

    #[test]
    fn pair_checkpoint_binds_both_sink_and_coder_contracts() {
        let temporary = tempfile::tempdir().unwrap();
        let config = CheckpointConfig {
            directory: temporary.path().into(),
            interval_batches: 1,
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
        let mut changed = original.clone();
        changed["progress"]["event_time_ns"] = serde_json::json!(20);
        fs::write(&checkpoint.path, serde_json::to_vec(&changed).unwrap()).unwrap();
        assert!(checkpoint.load().is_err());
        fs::write(
            &checkpoint.path,
            vec![b' '; MAX_MANIFEST_BYTES as usize + 1],
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
        invalid.checkpoint_position = Some(vec![0; MAX_MANIFEST_BYTES as usize + 1]);
        assert!(checkpoint.save(&invalid).is_err());
        checkpoint.save(&signal()).unwrap();
        let no_recover = StreamCheckpoint::new::<i32>(
            &CheckpointConfig {
                directory: temporary.path().into(),
                interval_batches: 1,
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
    fn reject_invalid_directory_and_oversized_signal() {
        let temporary = tempfile::tempdir().unwrap();
        let file = temporary.path().join("file");
        fs::write(&file, b"not a directory").unwrap();
        assert!(
            StreamCheckpoint::new::<i32>(
                &CheckpointConfig {
                    directory: file,
                    interval_batches: 1,
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
        oversized.split_id = "x".repeat(MAX_MANIFEST_BYTES as usize);
        assert!(checkpoint.save(&oversized).is_err());
        assert!(!checkpoint.path.exists());
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
