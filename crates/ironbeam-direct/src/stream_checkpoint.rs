//! Durable progress for a single resumable source and idempotent sink.
use crate::CheckpointConfig;
#[cfg(test)]
use crate::checkpoint::crash_after;
use crate::checkpoint::{MAX_RETAINED_CHECKPOINTS, sync_directory};
use ironbeam_core::{Element, Error, ErrorKind, ProgressSignal, Version};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs;
use std::io::{Read, Write};
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    state: Option<StateRef>,
    checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct StateRef {
    hash: String,
    len: u64,
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

    /// Recover the cursor and its matching opaque fold-state snapshot together.
    pub(crate) fn load_state(
        &self,
        max_bytes: usize,
    ) -> Result<Option<(ProgressSignal, Vec<u8>)>, Error> {
        if !self.recover {
            return Ok(None);
        }
        let mut latest_error = None;
        for slot in 0..self.max_checkpoints {
            match self.load_at(&self.slot_path(slot)) {
                Ok(Some(manifest)) => {
                    let Some(state) = &manifest.state else {
                        return Err(Error::incompatible(
                            "grouped stream checkpoint has no fold state",
                        ));
                    };
                    match self.read_state(state, max_bytes) {
                        Ok(bytes) => return Ok(Some((manifest.progress, bytes))),
                        Err(error) if error.kind == ErrorKind::Recovery => {
                            if latest_error.is_none() {
                                latest_error = Some(error);
                            }
                        }
                        Err(error) => return Err(error),
                    }
                }
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
        if manifest.checksum
            != checksum(
                manifest.version,
                &manifest.identity,
                &manifest.progress,
                manifest.state.as_ref(),
            )?
        {
            return Err(Error::recovery(
                "checkpoint",
                "stream progress checksum mismatch",
            ));
        }
        if let Some(state) = &manifest.state {
            self.validate_state_file(state)?;
        }
        self.validate_progress(&manifest.progress)?;
        Ok(Some(manifest))
    }

    pub(crate) fn save(&self, progress: &ProgressSignal) -> Result<(), Error> {
        self.save_manifest(progress, None)
    }

    pub(crate) fn save_state(
        &self,
        progress: &ProgressSignal,
        bytes: &[u8],
        max_bytes: usize,
    ) -> Result<(), Error> {
        self.validate_progress(progress)?;
        if bytes.len() > max_bytes {
            return Err(Error::resource(
                "checkpoint",
                "grouped stream fold state exceeds memory budget",
            ));
        }
        let state = StateRef {
            hash: hex_bytes(&Sha256::digest(bytes)),
            len: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        };
        self.write_state(&state, bytes)?;
        self.save_manifest(progress, Some(state))
    }

    fn save_manifest(
        &self,
        progress: &ProgressSignal,
        state: Option<StateRef>,
    ) -> Result<(), Error> {
        self.validate_progress(progress)?;
        let manifest = Manifest {
            version: STREAM_CHECKPOINT_VERSION,
            identity: self.identity.clone(),
            progress: progress.clone(),
            checksum: checksum(
                STREAM_CHECKPOINT_VERSION,
                &self.identity,
                progress,
                state.as_ref(),
            )?,
            state,
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
        self.prune_state_files()?;
        Ok(())
    }

    fn state_path(&self, state: &StateRef) -> Result<PathBuf, Error> {
        if state.hash.len() != 64
            || !state
                .hash
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(Error::recovery("checkpoint", "invalid stream state digest"));
        }
        Ok(self
            .path
            .parent()
            .expect("stream checkpoint path has parent")
            .join(format!("stream-state-{}.bin", state.hash)))
    }

    fn read_state(&self, state: &StateRef, max_bytes: usize) -> Result<Vec<u8>, Error> {
        if state.len > u64::try_from(max_bytes).unwrap_or(u64::MAX) {
            return Err(Error::resource(
                "checkpoint",
                "grouped stream fold state exceeds memory budget",
            ));
        }
        let path = self.state_path(state)?;
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| Error::recovery("checkpoint", error.to_string()))?;
        if !metadata.file_type().is_file() || metadata.len() != state.len {
            return Err(Error::recovery(
                "checkpoint",
                "stream state file size or type mismatch",
            ));
        }
        let bytes =
            fs::read(&path).map_err(|error| Error::recovery("checkpoint", error.to_string()))?;
        if hex_bytes(&Sha256::digest(&bytes)) != state.hash {
            return Err(Error::recovery(
                "checkpoint",
                "stream fold state checksum mismatch",
            ));
        }
        Ok(bytes)
    }

    fn validate_state_file(&self, state: &StateRef) -> Result<(), Error> {
        let path = self.state_path(state)?;
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| Error::recovery("checkpoint", error.to_string()))?;
        if !metadata.file_type().is_file() || metadata.len() != state.len {
            return Err(Error::recovery(
                "checkpoint",
                "stream state file size or type mismatch",
            ));
        }
        let mut file = fs::File::open(&path)
            .map_err(|error| Error::recovery("checkpoint", error.to_string()))?;
        let mut hasher = Sha256::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let count = file
                .read(&mut buffer)
                .map_err(|error| Error::recovery("checkpoint", error.to_string()))?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
        }
        if hex_bytes(&hasher.finalize()) != state.hash {
            return Err(Error::recovery(
                "checkpoint",
                "stream fold state checksum mismatch",
            ));
        }
        Ok(())
    }

    fn write_state(&self, state: &StateRef, bytes: &[u8]) -> Result<(), Error> {
        let path = self.state_path(state)?;
        if path.exists() {
            self.read_state(state, bytes.len())?;
            return Ok(());
        }
        let directory = path.parent().expect("stream state path has parent");
        let mut temporary = tempfile::NamedTempFile::new_in(directory)
            .map_err(|error| Error::io("checkpoint", "direct", error.to_string()))?;
        temporary
            .write_all(bytes)
            .and_then(|()| temporary.as_file().sync_all())
            .map_err(|error| Error::io("checkpoint", "direct", error.to_string()))?;
        match temporary.persist_noclobber(&path) {
            Ok(_) => sync_directory(directory)?,
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                self.read_state(state, bytes.len())?;
            }
            Err(error) => return Err(Error::io("checkpoint", "direct", error.error.to_string())),
        }
        Ok(())
    }

    fn prune_state_files(&self) -> Result<(), Error> {
        let directory = self
            .path
            .parent()
            .expect("stream checkpoint path has parent");
        let referenced = (0..self.max_checkpoints)
            .filter_map(|slot| self.load_at(&self.slot_path(slot)).ok().flatten())
            .filter_map(|manifest| manifest.state.map(|state| state.hash))
            .collect::<HashSet<_>>();
        let mut removed = false;
        for entry in fs::read_dir(directory)
            .map_err(|error| Error::io("checkpoint", "direct", error.to_string()))?
        {
            let entry =
                entry.map_err(|error| Error::io("checkpoint", "direct", error.to_string()))?;
            let name = entry.file_name();
            let Some(hash) = name
                .to_str()
                .and_then(|name| name.strip_prefix("stream-state-"))
                .and_then(|name| name.strip_suffix(".bin"))
            else {
                continue;
            };
            if hash.len() == 64
                && hash
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
                && !referenced.contains(hash)
            {
                fs::remove_file(entry.path())
                    .map_err(|error| Error::io("checkpoint", "direct", error.to_string()))?;
                removed = true;
            }
        }
        if removed {
            sync_directory(directory)?;
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
    state: Option<&StateRef>,
) -> Result<String, Error> {
    let bytes = if let Some(state) = state {
        serde_json::to_vec(&(version, identity, progress, state))
    } else {
        serde_json::to_vec(&(version, identity, progress))
    }
    .map_err(|error| Error::recovery("checkpoint", error.to_string()))?;
    Ok(hex_bytes(&Sha256::digest(bytes)))
}

#[cfg(test)]
#[path = "../tests/unit/stream_checkpoint__tests.rs"]
mod tests;
