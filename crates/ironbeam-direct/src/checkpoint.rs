//! Versioned local batch checkpoints bound to graph, source, and output coder identity.
use crate::metrics::MetricsState;
use crate::spill::{OutputStore, io_error, visit_frames, write_frame};
use ironbeam_core::{CHECKPOINT_VERSION, Element, Error, ErrorKind, Version};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
const MAX_RETAINED_CHECKPOINTS: usize = 128;

fn sync_directory(path: &Path) -> Result<(), Error> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io_error("checkpoint", error))
}

#[derive(Clone, Debug)]
pub struct CheckpointConfig {
    pub directory: PathBuf,
    pub interval_batches: usize,
    /// Number of bounded snapshots retained, including the latest (1..=128).
    pub max_checkpoints: usize,
    pub recover: bool,
    /// Stable identity of the executable transform registry (for example a build hash).
    pub binary_build_id: String,
}
impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            directory: std::env::temp_dir().join("ironbeam-v5-checkpoints"),
            interval_batches: 1,
            max_checkpoints: 1,
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
    #[serde(default)]
    second_output: Option<(String, String, Version)>,
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
            second_output: None,
            binary_build_id,
        }
    }
    pub(crate) fn new_pair<T: Element, U: Element>(
        graph_fingerprint: String,
        source_digest: String,
        source_key: String,
        left_key: String,
        right_key: String,
        binary_build_id: String,
    ) -> Self {
        Self {
            graph_fingerprint,
            source_digest,
            source_key,
            output_key: left_key,
            coder_id: T::coder_identity(),
            coder_version: T::CODER_VERSION,
            second_output: Some((right_key, U::coder_identity(), U::CODER_VERSION)),
            binary_build_id,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Manifest {
    version: Version,
    #[serde(default)]
    kind: CheckpointKind,
    identity: Identity,
    source_offset: usize,
    output_count: usize,
    data_file: String,
    data_sha256: String,
    #[serde(default)]
    progress_sha256: String,
}

#[derive(Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CheckpointKind {
    #[default]
    Output,
    FoldState,
    PairOutput,
}

impl Manifest {
    fn progress_hash(&self) -> String {
        let hash = progress_sha256(
            self.source_offset,
            self.output_count,
            &self.data_file,
            &self.data_sha256,
        );
        match self.kind {
            CheckpointKind::Output => hash,
            CheckpointKind::FoldState => fold_progress_sha256(&hash),
            CheckpointKind::PairOutput => pair_progress_sha256(&hash),
        }
    }
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
        if !(1..=MAX_RETAINED_CHECKPOINTS).contains(&config.max_checkpoints) {
            return Err(Error::graph(
                "checkpoint",
                "checkpoint retention must be between 1 and 128",
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
    fn history_path(&self, slot: usize) -> PathBuf {
        self.config
            .directory
            .join(format!("checkpoint-history-{slot}.json"))
    }
    fn load_manifest(
        &self,
        max_fold_state_bytes: Option<usize>,
    ) -> Result<Option<(Manifest, PathBuf)>, Error> {
        if !self.config.recover {
            return Ok(None);
        }
        let mut latest_error = None;
        for slot in 0..self.config.max_checkpoints {
            let path = if slot == 0 {
                self.manifest_path()
            } else {
                self.history_path(slot)
            };
            match self.load_manifest_at(&path, max_fold_state_bytes) {
                Ok(Some(checkpoint)) => return Ok(Some(checkpoint)),
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
    fn load_manifest_at(
        &self,
        path: &Path,
        max_fold_state_bytes: Option<usize>,
    ) -> Result<Option<(Manifest, PathBuf)>, Error> {
        let metadata = match fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(io_error("checkpoint", error)),
        };
        if !metadata.file_type().is_file() {
            return Err(Error::recovery(
                "checkpoint",
                "checkpoint manifest is not a regular file",
            ));
        }
        if metadata.len() > MAX_MANIFEST_BYTES {
            return Err(Error::recovery(
                "checkpoint",
                "checkpoint manifest exceeds size limit",
            ));
        }
        let bytes = fs::read(path).map_err(|e| io_error("checkpoint", e))?;
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
        if manifest.progress_sha256 != manifest.progress_hash() {
            return Err(Error::recovery(
                "checkpoint",
                "checkpoint progress checksum mismatch",
            ));
        }
        let data_path = self.config.directory.join(&manifest.data_file);
        let metadata = fs::symlink_metadata(&data_path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Error::recovery("checkpoint", "checkpoint data file is missing")
            } else {
                io_error("checkpoint", error)
            }
        })?;
        if !metadata.file_type().is_file() {
            return Err(Error::recovery(
                "checkpoint",
                "checkpoint data is not a regular file",
            ));
        }
        if manifest.kind == CheckpointKind::FoldState
            && let Some(limit) = max_fold_state_bytes
            && metadata.len() > u64::try_from(limit).unwrap_or(u64::MAX)
        {
            return Err(Error::resource(
                "checkpoint",
                "fold checkpoint exceeds job memory budget",
            ));
        }
        if sha256_file(&data_path)? != manifest.data_sha256 {
            return Err(Error::recovery(
                "checkpoint",
                "checkpoint checksum mismatch",
            ));
        }
        Ok(Some((manifest, data_path)))
    }
    pub(crate) fn recover<T: Element>(&self, store: &mut OutputStore<T>) -> Result<usize, Error> {
        let Some((manifest, data_path)) = self.load_manifest(None)? else {
            return Ok(0);
        };
        if manifest.kind != CheckpointKind::Output {
            return Err(Error::recovery("checkpoint", "expected output checkpoint"));
        }
        self.recover_output(&manifest, &data_path, store)
    }
    pub(crate) fn recover_pair<T: Element, U: Element>(
        &self,
        left: &mut OutputStore<T>,
        right: &mut OutputStore<U>,
    ) -> Result<Option<usize>, Error> {
        let Some((manifest, data_path)) = self.load_manifest(None)? else {
            return Ok(None);
        };
        if manifest.kind != CheckpointKind::PairOutput {
            return Err(Error::recovery(
                "checkpoint",
                "expected pair output checkpoint",
            ));
        }
        let count = visit_frames(&data_path, "checkpoint", |frame| {
            let (&side, bytes) = frame
                .split_first()
                .ok_or_else(|| Error::recovery("checkpoint", "pair frame lacks side tag"))?;
            match side {
                0 => left.push(T::decode(bytes)?),
                1 => right.push(U::decode(bytes)?),
                _ => Err(Error::recovery(
                    "checkpoint",
                    "pair frame has invalid side tag",
                )),
            }
        })?;
        if count != manifest.output_count {
            return Err(Error::recovery(
                "checkpoint",
                "checkpoint output count mismatch",
            ));
        }
        let recovered = u64::try_from(count).unwrap_or(u64::MAX);
        self.metrics
            .recovered_elements
            .store(recovered, Ordering::Relaxed);
        self.metrics
            .output_elements
            .store(recovered, Ordering::Relaxed);
        Ok(Some(manifest.source_offset))
    }
    pub(crate) fn recover_completed<T: Element>(
        &self,
        source_len: usize,
        store: &mut OutputStore<T>,
    ) -> Result<bool, Error> {
        let Some((manifest, data_path)) = self.load_manifest(None)? else {
            return Ok(false);
        };
        if manifest.kind != CheckpointKind::Output {
            return Ok(false);
        }
        if manifest.source_offset != source_len {
            return Err(Error::recovery(
                "checkpoint",
                "completed aggregate checkpoint source length mismatch",
            ));
        }
        self.recover_output(&manifest, &data_path, store)?;
        Ok(true)
    }
    fn recover_output<T: Element>(
        &self,
        manifest: &Manifest,
        data_path: &Path,
        store: &mut OutputStore<T>,
    ) -> Result<usize, Error> {
        let count = visit_frames(data_path, "checkpoint", |bytes| {
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
    pub(crate) fn recover_fold_state(
        &self,
        max_state_bytes: usize,
    ) -> Result<Option<(usize, Vec<u8>)>, Error> {
        let Some((manifest, data_path)) = self.load_manifest(Some(max_state_bytes))? else {
            return Ok(None);
        };
        if manifest.kind != CheckpointKind::FoldState {
            return Ok(None);
        }
        if manifest.output_count != 0 {
            return Err(Error::recovery(
                "checkpoint",
                "fold checkpoint has an output count",
            ));
        }
        let state = fs::read(data_path).map_err(|error| io_error("checkpoint", error))?;
        Ok(Some((manifest.source_offset, state)))
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
        self.persist_data(
            CheckpointKind::Output,
            source_offset,
            output_count,
            &data_path,
            data_hash,
        )
    }
    pub(crate) fn save_pair<T: Element, U: Element>(
        &self,
        source_offset: usize,
        left: &OutputStore<T>,
        right: &OutputStore<U>,
    ) -> Result<(), Error> {
        let mut data_tmp = tempfile::Builder::new()
            .prefix("checkpoint-data-")
            .tempfile_in(&self.config.directory)
            .map_err(|error| io_error("checkpoint", error))?;
        let mut write_side = |side: u8, bytes: &[u8]| {
            let mut tagged = Vec::with_capacity(bytes.len().saturating_add(1));
            tagged.push(side);
            tagged.extend_from_slice(bytes);
            write_frame(&mut data_tmp, &tagged, "checkpoint")?;
            Ok(())
        };
        let left_count = left.for_each_encoded(|bytes| write_side(0, bytes))?;
        let right_count = right.for_each_encoded(|bytes| write_side(1, bytes))?;
        data_tmp
            .as_file()
            .sync_all()
            .map_err(|error| io_error("checkpoint", error))?;
        let data_hash = sha256_file(data_tmp.path())?;
        let (_, data_path) = data_tmp
            .keep()
            .map_err(|error| io_error("checkpoint", error))?;
        self.persist_data(
            CheckpointKind::PairOutput,
            source_offset,
            left_count.saturating_add(right_count),
            &data_path,
            data_hash,
        )
    }
    pub(crate) fn save_fold_state(
        &self,
        source_offset: usize,
        state: &[u8],
        max_state_bytes: usize,
    ) -> Result<(), Error> {
        if state.len() > max_state_bytes {
            return Err(Error::resource(
                "checkpoint",
                "fold checkpoint exceeds job memory budget",
            ));
        }
        let mut data_tmp = tempfile::Builder::new()
            .prefix("checkpoint-data-")
            .tempfile_in(&self.config.directory)
            .map_err(|error| io_error("checkpoint", error))?;
        data_tmp
            .write_all(state)
            .map_err(|error| io_error("checkpoint", error))?;
        data_tmp
            .as_file()
            .sync_all()
            .map_err(|error| io_error("checkpoint", error))?;
        let data_hash = sha256_file(data_tmp.path())?;
        let (_, data_path) = data_tmp
            .keep()
            .map_err(|error| io_error("checkpoint", error))?;
        self.persist_data(
            CheckpointKind::FoldState,
            source_offset,
            0,
            &data_path,
            data_hash,
        )
    }
    fn persist_data(
        &self,
        kind: CheckpointKind,
        source_offset: usize,
        output_count: usize,
        data_path: &Path,
        data_hash: String,
    ) -> Result<(), Error> {
        // Make the new data file's directory entry durable before publishing
        // a manifest that points to it.
        sync_directory(&self.config.directory)?;
        let data_file = data_path
            .file_name()
            .ok_or_else(|| Error::recovery("checkpoint", "checkpoint data path lacks file name"))?
            .to_string_lossy()
            .into_owned();
        let mut manifest = Manifest {
            version: CHECKPOINT_VERSION,
            kind,
            identity: self.identity.clone(),
            source_offset,
            output_count,
            data_file,
            progress_sha256: String::new(),
            data_sha256: data_hash,
        };
        manifest.progress_sha256 = manifest.progress_hash();
        let previous = (0..self.config.max_checkpoints)
            .map(|slot| {
                let path = if slot == 0 {
                    self.manifest_path()
                } else {
                    self.history_path(slot)
                };
                self.load_manifest_at(&path, None)
                    .ok()
                    .flatten()
                    .map(|(manifest, _)| manifest)
            })
            .collect::<Vec<_>>();
        let mut stale_history = Vec::new();
        for entry in
            fs::read_dir(&self.config.directory).map_err(|error| io_error("checkpoint", error))?
        {
            let entry = entry.map_err(|error| io_error("checkpoint", error))?;
            let name = entry.file_name();
            let Some(slot) = name
                .to_str()
                .and_then(|name| name.strip_prefix("checkpoint-history-"))
                .and_then(|name| name.strip_suffix(".json"))
                .and_then(|name| name.parse::<usize>().ok())
            else {
                continue;
            };
            if slot >= self.config.max_checkpoints
                && slot < MAX_RETAINED_CHECKPOINTS
                && let Ok(Some((manifest, _))) = self.load_manifest_at(&entry.path(), None)
            {
                stale_history.push((entry.path(), manifest));
            }
        }
        for slot in (1..self.config.max_checkpoints).rev() {
            let target = self.history_path(slot);
            let source_manifest = previous[slot - 1].as_ref().filter(|old| old.kind == kind);
            if let Some(source_manifest) = source_manifest {
                self.write_manifest(&target, source_manifest)?;
            } else if target.exists() {
                fs::remove_file(&target).map_err(|error| io_error("checkpoint", error))?;
            }
        }
        // The archived manifests must be durable before a new latest manifest
        // can replace the only reference to its predecessor.
        if self.config.max_checkpoints > 1 {
            sync_directory(&self.config.directory)?;
        }
        self.write_manifest(&self.manifest_path(), &manifest)?;
        // Do not remove the previous data file until the new manifest's
        // directory entry is durable. A crash must leave one recoverable pair.
        sync_directory(&self.config.directory)?;
        let retained = std::iter::once(manifest.data_file.clone())
            .chain(
                previous
                    .iter()
                    .take(self.config.max_checkpoints.saturating_sub(1))
                    .filter_map(Option::as_ref)
                    .filter(|old| old.kind == kind)
                    .map(|old| old.data_file.clone()),
            )
            .collect::<HashSet<_>>();
        let mut pruned_stale = Vec::new();
        for (path, old) in stale_history {
            if fs::remove_file(path).is_ok() {
                pruned_stale.push(old);
            }
        }
        for old in previous.into_iter().flatten().chain(pruned_stale) {
            if old.data_file.starts_with("checkpoint-data-") && !retained.contains(&old.data_file) {
                let _ = fs::remove_file(self.config.directory.join(old.data_file));
            }
        }
        self.metrics.checkpoints.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    fn write_manifest(&self, path: &Path, manifest: &Manifest) -> Result<(), Error> {
        let mut temporary = tempfile::NamedTempFile::new_in(&self.config.directory)
            .map_err(|error| io_error("checkpoint", error))?;
        serde_json::to_writer(&mut temporary, manifest)
            .map_err(|error| Error::recovery("checkpoint", error.to_string()))?;
        temporary
            .flush()
            .map_err(|error| io_error("checkpoint", error))?;
        temporary
            .as_file()
            .sync_all()
            .map_err(|error| io_error("checkpoint", error))?;
        temporary
            .persist(path)
            .map_err(|error| io_error("checkpoint", error))?;
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

fn fold_progress_sha256(output_progress_hash: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(b"ironbeam/checkpoint-fold-progress/v1\0");
    hash.update(output_progress_hash.as_bytes());
    let digest = hash.finalize();
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

fn pair_progress_sha256(output_progress_hash: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(b"ironbeam/checkpoint-pair-progress/v1\0");
    hash.update(output_progress_hash.as_bytes());
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
            serde_json::from_slice(&fs::read(directory.path().join("latest.json")).unwrap())
                .unwrap();
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
        let mut latest: Manifest =
            serde_json::from_slice(&fs::read(&latest_path).unwrap()).unwrap();
        latest.identity.graph_fingerprint.push_str("-other");
        fs::write(&latest_path, serde_json::to_vec(&latest).unwrap()).unwrap();
        let mut refused = OutputStore::<i32>::new(1024, None, metrics).unwrap();
        assert_eq!(
            manager.recover(&mut refused).unwrap_err().kind,
            ErrorKind::Incompatible
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
        let manager =
            CheckpointManager::new(&config, identity.clone(), Arc::clone(&metrics)).unwrap();
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
            serde_json::from_slice(&fs::read(directory.path().join("latest.json")).unwrap())
                .unwrap();
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
            serde_json::from_slice(&fs::read(config.directory.join("latest.json")).unwrap())
                .unwrap();
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
}
