//! Versioned local batch checkpoints bound to graph, source, and output coder identity.
use crate::metrics::MetricsState;
use crate::spill::{OutputStore, io_error, visit_frames, write_frame};
use ironbeam_core::{CHECKPOINT_VERSION, Element, Error, Version};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

#[derive(Clone, Debug)]
pub struct CheckpointConfig {
    pub directory: PathBuf,
    pub interval_batches: usize,
    pub recover: bool,
    /// Stable identity of the executable transform registry (for example a build hash).
    pub binary_build_id: String,
}
impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            directory: std::env::temp_dir().join("ironbeam-v5-checkpoints"),
            interval_batches: 1,
            recover: true,
            binary_build_id: String::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Identity {
    graph_fingerprint: String,
    source_digest: String,
    source_key: String,
    output_key: String,
    coder_id: String,
    coder_version: Version,
    binary_build_id: String,
}
impl Identity {
    pub(crate) fn new<T: Element>(
        graph_fingerprint: String,
        source_digest: String,
        source_key: String,
        output_key: String,
        binary_build_id: String,
    ) -> Self {
        Self {
            graph_fingerprint,
            source_digest,
            source_key,
            output_key,
            coder_id: T::coder_identity(),
            coder_version: T::CODER_VERSION,
            binary_build_id,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Manifest {
    version: Version,
    identity: Identity,
    source_offset: usize,
    output_count: usize,
    data_file: String,
    data_sha256: String,
    #[serde(default)]
    progress_sha256: String,
}

pub(crate) struct CheckpointManager {
    config: CheckpointConfig,
    identity: Identity,
    metrics: Arc<MetricsState>,
}
impl CheckpointManager {
    pub(crate) fn new(
        config: &CheckpointConfig,
        identity: Identity,
        metrics: Arc<MetricsState>,
    ) -> Result<Self, Error> {
        if config.interval_batches == 0 {
            return Err(Error::graph(
                "checkpoint",
                "checkpoint interval must be positive",
            ));
        }
        fs::create_dir_all(&config.directory).map_err(|e| io_error("checkpoint", e))?;
        Ok(Self {
            config: config.clone(),
            identity,
            metrics,
        })
    }
    fn manifest_path(&self) -> PathBuf {
        self.config.directory.join("latest.json")
    }
    pub(crate) fn recover<T: Element>(&self, store: &mut OutputStore<T>) -> Result<usize, Error> {
        if !self.config.recover {
            return Ok(0);
        }
        let path = self.manifest_path();
        if !path.exists() {
            return Ok(0);
        }
        let bytes = fs::read(&path).map_err(|e| io_error("checkpoint", e))?;
        let manifest: Manifest = serde_json::from_slice(&bytes)
            .map_err(|e| Error::recovery("checkpoint", e.to_string()))?;
        if manifest.version != CHECKPOINT_VERSION {
            return Err(Error::incompatible("checkpoint schema version mismatch"));
        }
        if manifest.identity != self.identity {
            return Err(Error::incompatible(
                "checkpoint graph/source/coder identity mismatch",
            ));
        }
        validate_data_file_name(&manifest.data_file)?;
        if manifest.progress_sha256
            != progress_sha256(
                manifest.source_offset,
                manifest.output_count,
                &manifest.data_file,
                &manifest.data_sha256,
            )
        {
            return Err(Error::recovery(
                "checkpoint",
                "checkpoint progress checksum mismatch",
            ));
        }
        let data_path = self.config.directory.join(&manifest.data_file);
        if sha256_file(&data_path)? != manifest.data_sha256 {
            return Err(Error::recovery(
                "checkpoint",
                "checkpoint checksum mismatch",
            ));
        }
        let count = visit_frames(&data_path, "checkpoint", |bytes| {
            store.push(T::decode(bytes)?)
        })?;
        if count != manifest.output_count {
            return Err(Error::recovery(
                "checkpoint",
                "checkpoint output count mismatch",
            ));
        }
        self.metrics
            .recovered_elements
            .store(u64::try_from(count).unwrap_or(u64::MAX), Ordering::Relaxed);
        self.metrics
            .output_elements
            .store(u64::try_from(count).unwrap_or(u64::MAX), Ordering::Relaxed);
        Ok(manifest.source_offset)
    }
    pub(crate) fn recover_completed<T: Element>(
        &self,
        source_len: usize,
        store: &mut OutputStore<T>,
    ) -> Result<bool, Error> {
        if !self.config.recover || !self.manifest_path().exists() {
            return Ok(false);
        }
        if self.recover(store)? != source_len {
            return Err(Error::recovery(
                "checkpoint",
                "completed aggregate checkpoint source length mismatch",
            ));
        }
        Ok(true)
    }
    pub(crate) const fn should_save(&self, batches: usize) -> bool {
        batches.is_multiple_of(self.config.interval_batches)
    }
    pub(crate) fn save<T: Element>(
        &self,
        source_offset: usize,
        store: &OutputStore<T>,
    ) -> Result<(), Error> {
        let mut data_tmp = tempfile::Builder::new()
            .prefix("checkpoint-data-")
            .tempfile_in(&self.config.directory)
            .map_err(|e| io_error("checkpoint", e))?;
        let output_count = store.for_each_encoded(|bytes| {
            write_frame(&mut data_tmp, bytes, "checkpoint")?;
            Ok(())
        })?;
        data_tmp
            .as_file()
            .sync_all()
            .map_err(|e| io_error("checkpoint", e))?;
        let data_hash = sha256_file(data_tmp.path())?;
        let (_, data_path) = data_tmp.keep().map_err(|e| io_error("checkpoint", e))?;
        let data_file = data_path
            .file_name()
            .ok_or_else(|| Error::recovery("checkpoint", "checkpoint data path lacks file name"))?
            .to_string_lossy()
            .into_owned();
        let progress_hash = progress_sha256(source_offset, output_count, &data_file, &data_hash);
        let manifest = Manifest {
            version: CHECKPOINT_VERSION,
            identity: self.identity.clone(),
            source_offset,
            output_count,
            data_file,
            progress_sha256: progress_hash,
            data_sha256: data_hash,
        };
        let mut manifest_tmp = tempfile::NamedTempFile::new_in(&self.config.directory)
            .map_err(|e| io_error("checkpoint", e))?;
        serde_json::to_writer(&mut manifest_tmp, &manifest)
            .map_err(|e| Error::recovery("checkpoint", e.to_string()))?;
        manifest_tmp
            .flush()
            .map_err(|e| io_error("checkpoint", e))?;
        manifest_tmp
            .as_file()
            .sync_all()
            .map_err(|e| io_error("checkpoint", e))?;
        let old = fs::read(self.manifest_path())
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Manifest>(&bytes).ok());
        manifest_tmp
            .persist(self.manifest_path())
            .map_err(|e| io_error("checkpoint", e))?;
        if let Some(old) = old
            && old.version == CHECKPOINT_VERSION
            && old.identity == self.identity
            && old.data_file.starts_with("checkpoint-data-")
            && old.data_file != manifest.data_file
            && validate_data_file_name(&old.data_file).is_ok()
            && old.progress_sha256
                == progress_sha256(
                    old.source_offset,
                    old.output_count,
                    &old.data_file,
                    &old.data_sha256,
                )
        {
            let _ = fs::remove_file(self.config.directory.join(old.data_file));
        }
        self.metrics.checkpoints.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn validate_data_file_name(data_file: &str) -> Result<(), Error> {
    let mut components = Path::new(data_file).components();
    if !matches!(components.next(), Some(std::path::Component::Normal(_)))
        || components.next().is_some()
    {
        return Err(Error::recovery("checkpoint", "invalid data file name"));
    }
    Ok(())
}

fn progress_sha256(
    source_offset: usize,
    output_count: usize,
    data_file: &str,
    data_sha256: &str,
) -> String {
    let mut hash = Sha256::new();
    hash.update(b"ironbeam/checkpoint-progress/v1\0");
    hash.update(source_offset.to_string().as_bytes());
    hash.update([0]);
    hash.update(output_count.to_string().as_bytes());
    hash.update([0]);
    hash.update(data_file.as_bytes());
    hash.update([0]);
    hash.update(data_sha256.as_bytes());
    let digest = hash.finalize();
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

fn sha256_file(path: &Path) -> Result<String, Error> {
    let mut file = File::open(path).map_err(|e| io_error("checkpoint", e))?;
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|e| io_error("checkpoint", e))?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    let digest = hash.finalize();
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(hex, "{byte:02x}").map_err(|e| Error::recovery("checkpoint", e.to_string()))?;
    }
    Ok(hex)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironbeam_core::ErrorKind;

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
        let original: Manifest =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        assert!(original.data_file.starts_with("checkpoint-data-"));

        let outside = parent.path().join("outside.txt");
        fs::write(&outside, b"keep outside").unwrap();
        let sibling = checkpoint_dir.join("notes.txt");
        fs::write(&sibling, b"keep sibling").unwrap();
        for data_file in ["../outside.txt", "notes.txt", "latest.json"] {
            let mut forged = Manifest {
                version: original.version,
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
        let previous: Manifest =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
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
                ..config.clone()
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
}
