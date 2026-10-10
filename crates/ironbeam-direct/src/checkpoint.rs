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
            coder_id: T::CODER_ID.into(),
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
        let mut components = Path::new(&manifest.data_file).components();
        if !matches!(components.next(), Some(std::path::Component::Normal(_)))
            || components.next().is_some()
        {
            return Err(Error::recovery("checkpoint", "invalid data file name"));
        }
        if manifest.output_count > manifest.source_offset {
            return Err(Error::recovery(
                "checkpoint",
                "checkpoint output count exceeds source position",
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
    pub(crate) const fn should_save(&self, batches: usize) -> bool {
        batches.is_multiple_of(self.config.interval_batches)
    }
    pub(crate) fn save<T: Element>(
        &self,
        source_offset: usize,
        store: &OutputStore<T>,
    ) -> Result<(), Error> {
        let mut data_tmp = tempfile::NamedTempFile::new_in(&self.config.directory)
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
        let manifest = Manifest {
            version: CHECKPOINT_VERSION,
            identity: self.identity.clone(),
            source_offset,
            output_count,
            data_file,
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
        if let Some(old) = old {
            let _ = fs::remove_file(self.config.directory.join(old.data_file));
        }
        self.metrics.checkpoints.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
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
}
