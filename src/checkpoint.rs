//! Automatic checkpointing for fault tolerance in long-running batch jobs.
//!
//! The checkpoint module provides automatic state persistence and recovery
//! for Ironbeam pipelines, allowing jobs to resume from the last successful
//! checkpoint after failures or interruptions.
//!
//! # Features
//!
//! - **Automatic checkpoint creation** - Save state at configurable intervals
//! - **Transparent recovery** - Automatically resume from the last checkpoint
//! - **Configurable policies** - Control when and where checkpoints are created
//! - **State verification** - Checksums ensure checkpoint integrity
//!
//! # Usage
//!
//! ```no_run
//! use ironbeam::*;
//! use ironbeam::checkpoint::{CheckpointConfig, CheckpointPolicy};
//! use anyhow::Result;
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//! let data = from_vec(&p, (0..10000).collect::<Vec<i32>>());
//!
//! // Configure checkpointing
//! let checkpoint_config = CheckpointConfig {
//!     enabled: true,
//!     directory: "./checkpoints".into(),
//!     policy: CheckpointPolicy::AfterEveryBarrier,
//!     auto_recover: true,
//!     max_checkpoints: Some(5),
//! };
//!
//! let runner = Runner {
//!     mode: ExecMode::Parallel { threads: None, partitions: None },
//!     checkpoint_config: Some(checkpoint_config),
//!     ..Default::default()
//! };
//!
//! // Pipeline will automatically checkpoint and can recover from failures
//! let result = runner.run_collect::<i32>(&p, data.node_id())?;
//! # Ok(())
//! # }
//! ```

#[cfg(feature = "checkpointing")]
use anyhow::{Context, Result, anyhow};
#[cfg(feature = "checkpointing")]
use bincode::serde::{decode_from_slice, encode_to_vec};
#[cfg(feature = "checkpointing")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "checkpointing")]
use sha2::{Digest, Sha256};
#[cfg(feature = "checkpointing")]
use std::fs::{DirEntry, File, create_dir_all, read_dir, remove_file};
#[cfg(feature = "checkpointing")]
use std::io::{Read, Write};
#[cfg(feature = "checkpointing")]
use std::path::{Path, PathBuf};
#[cfg(feature = "checkpointing")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Configuration for checkpoint behavior.
///
/// Controls when, where, and how checkpoints are created during pipeline execution.
#[derive(Clone, Debug)]
#[cfg(feature = "checkpointing")]
pub struct CheckpointConfig {
    /// Enable or disable checkpointing.
    pub enabled: bool,
    /// Directory where checkpoint files are stored.
    pub directory: PathBuf,
    /// Policy determining when checkpoints are created.
    pub policy: CheckpointPolicy,
    /// Automatically recover from the latest checkpoint on startup.
    pub auto_recover: bool,
    /// Maximum number of checkpoints to retain (oldest are deleted first).
    /// None means keep all checkpoints.
    pub max_checkpoints: Option<usize>,
}

#[cfg(feature = "checkpointing")]
impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            directory: PathBuf::from("./Ironbeam_checkpoints"),
            policy: CheckpointPolicy::AfterEveryBarrier,
            auto_recover: true,
            max_checkpoints: Some(10),
        }
    }
}

/// Policy for determining when checkpoints are created during execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg(feature = "checkpointing")]
pub enum CheckpointPolicy {
    /// Create a checkpoint after every barrier node (`GroupByKey`, `CombineValues`, `CoGroup`, `CombineGlobal`).
    AfterEveryBarrier,
    /// Create a checkpoint after every N nodes in the execution chain.
    EveryNNodes(usize),
    /// Create a checkpoint after approximately every N seconds of execution.
    TimeInterval(u64),
    /// Create checkpoints after both barriers and time intervals (most frequent).
    Hybrid { barriers: bool, interval_secs: u64 },
}

/// Checkpoint state containing execution progress and intermediate results.
///
/// This is the serializable snapshot of a running pipeline at a specific point.
/// Note: Due to type-erasure in Partition buffers, full state recovery requires
/// the pipeline to support serialization. For now, we primarily checkpoint
/// progress markers and allow restart from the last completed barrier.
#[derive(Serialize, Deserialize)]
#[cfg(feature = "checkpointing")]
pub struct CheckpointState {
    /// Pipeline execution identifier (derived from pipeline content hash).
    pub pipeline_id: String,
    /// Index of the last successfully completed node in the chain.
    pub completed_node_index: usize,
    /// Timestamp when the checkpoint was created (milliseconds since epoch).
    pub timestamp: u64,
    /// Number of partitions being processed.
    pub partition_count: usize,
    /// SHA-256 checksum of metadata for integrity verification.
    pub checksum: String,
    /// Execution mode (sequential or parallel).
    pub exec_mode: String,
    /// Additional metadata for diagnostics.
    pub metadata: CheckpointMetadata,
}

/// Additional metadata stored with each checkpoint.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[cfg(feature = "checkpointing")]
pub struct CheckpointMetadata {
    /// Total number of nodes in the execution chain.
    pub total_nodes: usize,
    /// Type of the last completed node (for diagnostics).
    pub last_node_type: String,
    /// Progress percentage (0-100).
    pub progress_percent: u8,
}

/// Manages checkpoint creation, persistence, and recovery.
#[cfg(feature = "checkpointing")]
pub struct CheckpointManager {
    pub(crate) config: CheckpointConfig,
    pub last_checkpoint_time: Option<SystemTime>,
}

#[cfg(feature = "checkpointing")]
impl CheckpointManager {
    /// Create a new checkpoint manager with the given configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint directory cannot be created.
    pub fn new(config: CheckpointConfig) -> Result<Self> {
        if config.enabled {
            // Ensure the checkpoint directory exists
            create_dir_all(&config.directory).context("Failed to create checkpoint directory")?;
        }
        Ok(Self {
            config,
            last_checkpoint_time: None,
        })
    }

    /// Check if a checkpoint should be created based on the policy.
    pub fn should_checkpoint(
        &mut self,
        node_index: usize,
        is_barrier: bool,
        _total_nodes: usize,
    ) -> bool {
        if !self.config.enabled {
            return false;
        }

        match self.config.policy {
            CheckpointPolicy::AfterEveryBarrier => is_barrier,
            CheckpointPolicy::EveryNNodes(n) => node_index > 0 && node_index.is_multiple_of(n),
            CheckpointPolicy::TimeInterval(secs) => {
                let now = SystemTime::now();
                self.last_checkpoint_time.is_none_or(|last| {
                    now.duration_since(last)
                        .is_ok_and(|elapsed| elapsed >= Duration::from_secs(secs))
                })
            }
            CheckpointPolicy::Hybrid {
                barriers,
                interval_secs,
            } => {
                let should_by_barrier = barriers && is_barrier;
                let should_by_time = {
                    let now = SystemTime::now();
                    self.last_checkpoint_time.is_none_or(|last| {
                        now.duration_since(last)
                            .is_ok_and(|elapsed| elapsed >= Duration::from_secs(interval_secs))
                    })
                };
                should_by_barrier || should_by_time
            }
        }
    }

    /// Save a checkpoint to disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint file cannot be created or written to.
    pub fn save_checkpoint(&mut self, state: &CheckpointState) -> Result<PathBuf> {
        let filename = format!("checkpoint_{}_{}.bin", state.pipeline_id, state.timestamp);
        let path = self.config.directory.join(&filename);

        let encoded = encode_to_vec(state, bincode::config::standard())
            .context("Failed to serialize checkpoint")?;

        let mut file = File::create(&path).context("Failed to create checkpoint file")?;
        file.write_all(&encoded)
            .context("Failed to write checkpoint")?;
        file.sync_all()
            .context("Failed to sync checkpoint to disk")?;

        self.last_checkpoint_time = Some(SystemTime::now());

        // Clean up old checkpoints if needed
        self.cleanup_old_checkpoints(&state.pipeline_id)?;

        Ok(path)
    }

    /// Find the most recent checkpoint for a given pipeline.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint directory cannot be read or if no valid checkpoint is found.
    pub fn find_latest_checkpoint(&self, pipeline_id: &str) -> Result<Option<PathBuf>> {
        if !self.config.enabled {
            return Ok(None);
        }

        if !self.config.directory.exists() {
            return Ok(None);
        }

        let prefix = format!("checkpoint_{pipeline_id}_");
        let mut checkpoints: Vec<_> = read_dir(&self.config.directory)
            .context("Failed to read checkpoint directory")?
            .filter_map(Result::ok)
            .filter(|entry| {
                entry.file_name().to_str().is_some_and(|name| {
                    name.starts_with(&prefix)
                        && Path::new(name)
                            .extension()
                            .is_some_and(|ext| ext.eq_ignore_ascii_case("bin"))
                })
            })
            .collect();

        if checkpoints.is_empty() {
            return Ok(None);
        }

        // Sort by timestamp (encoded in filename)
        checkpoints.sort_by_key(|entry| {
            entry
                .file_name()
                .to_str()
                .and_then(|name| {
                    name.strip_prefix(&prefix)
                        .and_then(|s| s.strip_suffix(".bin"))
                        .and_then(|s| s.parse::<u64>().ok())
                })
                .unwrap_or(0)
        });

        Ok(checkpoints.last().map(DirEntry::path))
    }

    /// Load and verify a checkpoint from disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint file cannot be read or if the checksum verification fails.
    pub fn load_checkpoint(&self, path: &Path) -> Result<CheckpointState> {
        let mut file = File::open(path).context("Failed to open checkpoint file")?;
        let mut encoded = Vec::new();
        file.read_to_end(&mut encoded)
            .context("Failed to read checkpoint")?;

        let (state, _len): (CheckpointState, usize) =
            decode_from_slice(&encoded, bincode::config::standard())
                .context("Failed to deserialize checkpoint")?;

        // Verify checksum
        let metadata_str = format!(
            "{}:{}:{}:{}",
            state.pipeline_id, state.completed_node_index, state.timestamp, state.partition_count
        );
        let computed_checksum = compute_checksum(metadata_str.as_bytes());
        if computed_checksum != state.checksum {
            return Err(anyhow!(
                "Checkpoint integrity check failed: checksum mismatch"
            ));
        }

        Ok(state)
    }

    /// Delete old checkpoints beyond the retention limit.
    fn cleanup_old_checkpoints(&self, pipeline_id: &str) -> Result<()> {
        let Some(max_checkpoints) = self.config.max_checkpoints else {
            return Ok(());
        };

        let prefix = format!("checkpoint_{pipeline_id}_");
        let mut checkpoints: Vec<_> = read_dir(&self.config.directory)
            .context("Failed to read checkpoint directory")?
            .filter_map(Result::ok)
            .filter(|entry| {
                entry.file_name().to_str().is_some_and(|name| {
                    name.starts_with(&prefix)
                        && Path::new(name)
                            .extension()
                            .is_some_and(|ext| ext.eq_ignore_ascii_case("bin"))
                })
            })
            .collect();

        if checkpoints.len() <= max_checkpoints {
            return Ok(());
        }

        // Sort by timestamp
        checkpoints.sort_by_key(|entry| {
            entry
                .file_name()
                .to_str()
                .and_then(|name| {
                    name.strip_prefix(&prefix)
                        .and_then(|s| s.strip_suffix(".bin"))
                        .and_then(|s| s.parse::<u64>().ok())
                })
                .unwrap_or(0)
        });

        // Delete oldest checkpoints
        let to_delete = checkpoints.len() - max_checkpoints;
        for entry in checkpoints.iter().take(to_delete) {
            remove_file(entry.path()).ok(); // Ignore errors
        }

        Ok(())
    }

    /// Delete all checkpoints for a given pipeline.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint directory cannot be read or if any checkpoint file cannot be deleted.
    pub fn clear_checkpoints(&self, pipeline_id: &str) -> Result<()> {
        let prefix = format!("checkpoint_{pipeline_id}_");
        let checkpoints: Vec<_> = read_dir(&self.config.directory)
            .context("Failed to read checkpoint directory")?
            .filter_map(Result::ok)
            .filter(|entry| {
                entry.file_name().to_str().is_some_and(|name| {
                    name.starts_with(&prefix)
                        && Path::new(name)
                            .extension()
                            .is_some_and(|ext| ext.eq_ignore_ascii_case("bin"))
                })
            })
            .collect();

        for entry in checkpoints {
            remove_file(entry.path()).ok();
        }

        Ok(())
    }
}

/// Compute SHA-256 checksum of data.
#[cfg(feature = "checkpointing")]
#[must_use]
pub fn compute_checksum(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

/// Generate a stable pipeline ID from the pipeline structure.
#[cfg(feature = "checkpointing")]
pub(crate) fn generate_pipeline_id(pipeline_hash: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(pipeline_hash.as_bytes());
    format!("{:x}", hasher.finalize())[..16].to_string()
}

/// Get current timestamp in milliseconds since epoch.
#[cfg(feature = "checkpointing")]
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
