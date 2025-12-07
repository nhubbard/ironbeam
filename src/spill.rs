//! Automatic resource spilling for memory-constrained pipelines.
//!
//! This module provides transparent disk-based spilling when data exceeds memory thresholds.
//! When a partition grows too large to fit in memory, it is automatically serialized to disk
//! and can be transparently restored when needed.
//!
//! # Architecture
//!
//! - [`SpillConfig`] - Configuration for spilling behavior (thresholds, directories)
//! - [`SpillablePartition`] - Wrapper around `Partition` that tracks memory and spills to disk
//! - [`MemoryTracker`] - Global memory usage tracking across all spillable partitions
//! - [`SpillManager`] - Manages disk storage and cleanup of spilled data
//!
//! # Memory Tracking
//!
//! The memory tracker supports both real memory monitoring and artificial limits for testing:
//! - Real mode: Uses actual allocated memory size estimation
//! - Artificial mode: Uses a configurable threshold without requiring admin privileges
//!
//! # Example
//!
//! ```no_run
//! use ironbeam::spill::{SpillConfig, MemoryTracker};
//!
//! // Configure spilling with a 100MB memory limit
//! let config = SpillConfig::new()
//!     .with_memory_limit(100 * 1024 * 1024)
//!     .with_spill_directory("/tmp/spill");
//!
//! // Initialize the global memory tracker
//! MemoryTracker::initialize(config);
//!
//! // Memory tracking and spilling happens automatically during pipeline execution
//! ```

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::fs::{File, create_dir_all, remove_file};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Configuration for automatic memory spilling.
///
/// This configuration determines when and where data should be spilled to disk
/// to avoid out-of-memory conditions.
///
/// # Example
///
/// ```
/// use ironbeam::spill::SpillConfig;
///
/// let config = SpillConfig::new()
///     .with_memory_limit(100 * 1024 * 1024) // 100 MB
///     .with_spill_directory("/tmp/ironbeam-spill")
///     .with_compression(true);
/// ```
#[derive(Clone, Debug)]
pub struct SpillConfig {
    /// Maximum memory usage in bytes before spilling occurs.
    /// If None, spilling is disabled.
    pub memory_limit: Option<usize>,

    /// Directory where spilled data is stored.
    pub spill_directory: PathBuf,

    /// Whether to compress spilled data (reduces disk usage, increases CPU).
    pub compress: bool,

    /// Minimum size (in bytes) for a partition to be considered for spilling.
    /// Small partitions are kept in memory to avoid excessive I/O overhead.
    pub min_spill_size: usize,
}

impl Default for SpillConfig {
    fn default() -> Self {
        Self {
            memory_limit: None,
            spill_directory: std::env::temp_dir().join("ironbeam-spill"),
            compress: false,
            min_spill_size: 1024 * 1024, // 1 MB
        }
    }
}

impl SpillConfig {
    /// Create a new `SpillConfig` with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the memory limit in bytes.
    ///
    /// When total tracked memory exceeds this limit, partitions will be spilled to disk.
    #[must_use]
    pub const fn with_memory_limit(mut self, limit: usize) -> Self {
        self.memory_limit = Some(limit);
        self
    }

    /// Set the directory where spilled data will be stored.
    ///
    /// The directory will be created if it doesn't exist.
    #[must_use]
    pub fn with_spill_directory<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.spill_directory = path.into();
        self
    }

    /// Enable or disable compression for spilled data.
    #[must_use]
    pub const fn with_compression(mut self, compress: bool) -> Self {
        self.compress = compress;
        self
    }

    /// Set the minimum partition size for spilling.
    ///
    /// Partitions smaller than this will not be spilled to avoid excessive I/O.
    #[must_use]
    pub const fn with_min_spill_size(mut self, size: usize) -> Self {
        self.min_spill_size = size;
        self
    }
}

/// Global memory tracker for monitoring and enforcing memory limits.
///
/// This is a singleton that tracks total memory usage across all spillable partitions
/// in the pipeline. It supports both real memory tracking and artificial limits for testing.
pub struct MemoryTracker {
    inner: Arc<MemoryTrackerInner>,
}

struct MemoryTrackerInner {
    /// Current tracked memory usage in bytes
    current_usage: AtomicUsize,

    /// Configuration
    config: Mutex<SpillConfig>,

    /// Next spill file ID
    next_spill_id: AtomicU64,
}

// Global singleton instance
static MEMORY_TRACKER: Mutex<Option<MemoryTracker>> = Mutex::new(None);

impl MemoryTracker {
    /// Initialize the global memory tracker with the given configuration.
    ///
    /// This should be called once at the start of your program before creating any pipelines.
    /// Subsequent calls will update the configuration.
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned.
    pub fn initialize(config: SpillConfig) {
        let mut tracker = MEMORY_TRACKER.lock().unwrap();
        *tracker = Some(Self {
            inner: Arc::new(MemoryTrackerInner {
                current_usage: AtomicUsize::new(0),
                config: Mutex::new(config),
                next_spill_id: AtomicU64::new(0),
            }),
        });
    }

    /// Get the global memory tracker instance.
    ///
    /// Returns None if the tracker hasn't been initialized.
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned.
    #[must_use]
    pub fn instance() -> Option<Self> {
        MEMORY_TRACKER.lock().unwrap().clone()
    }

    /// Get the current memory usage in bytes.
    #[must_use]
    pub fn current_usage(&self) -> usize {
        self.inner.current_usage.load(Ordering::Relaxed)
    }

    /// Get the configured memory limit.
    ///
    /// Returns None if no limit is set (spilling disabled).
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned.
    #[must_use]
    pub fn memory_limit(&self) -> Option<usize> {
        self.inner.config.lock().unwrap().memory_limit
    }

    /// Check if memory usage exceeds the configured limit.
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned.
    #[must_use]
    pub fn should_spill(&self) -> bool {
        let config = self.inner.config.lock().unwrap();
        config
            .memory_limit
            .is_some_and(|limit| self.current_usage() > limit)
    }

    /// Register memory allocation.
    ///
    /// Should be called when creating or loading a partition into memory.
    pub fn allocate(&self, bytes: usize) {
        self.inner.current_usage.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Register memory deallocation.
    ///
    /// Should be called when dropping or spilling a partition.
    pub fn deallocate(&self, bytes: usize) {
        self.inner.current_usage.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Get a clone of the current configuration.
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned.
    #[must_use]
    pub fn config(&self) -> SpillConfig {
        self.inner.config.lock().unwrap().clone()
    }

    /// Allocate a new unique spill file ID.
    #[must_use]
    pub fn next_spill_id(&self) -> u64 {
        self.inner.next_spill_id.fetch_add(1, Ordering::Relaxed)
    }
}

impl Clone for MemoryTracker {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Manager for disk-based spill storage.
///
/// Handles serialization, compression, and cleanup of spilled partitions.
pub struct SpillManager {
    config: SpillConfig,
}

impl SpillManager {
    /// Create a new spill manager with the given configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the spill directory cannot be created.
    pub fn new(config: SpillConfig) -> Result<Self> {
        // Ensure spill directory exists
        create_dir_all(&config.spill_directory).context("Failed to create spill directory")?;

        Ok(Self { config })
    }

    /// Generate a path for a new spill file.
    fn spill_path(&self, spill_id: u64) -> PathBuf {
        self.config
            .spill_directory
            .join(format!("spill-{spill_id}.bin"))
    }

    /// Spill a partition to disk.
    ///
    /// Returns the spill ID that can be used to restore the data later.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or disk I/O fails.
    pub fn spill<T: 'static + Send + Sync + Clone + Serialize>(
        &self,
        data: &[T],
        spill_id: u64,
    ) -> Result<usize> {
        let path = self.spill_path(spill_id);
        let file = File::create(&path).context("Failed to create spill file")?;

        let mut writer = BufWriter::new(file);

        // Serialize the data using bincode 2.0 API
        let serialized = if self.config.compress {
            #[cfg(feature = "compression-zstd")]
            {
                let bytes = bincode::serde::encode_to_vec(data, bincode::config::standard())?;
                zstd::encode_all(&bytes[..], 3)?
            }
            #[cfg(not(feature = "compression-zstd"))]
            {
                bincode::serde::encode_to_vec(data, bincode::config::standard())?
            }
        } else {
            bincode::serde::encode_to_vec(data, bincode::config::standard())?
        };

        writer
            .write_all(&serialized)
            .context("Failed to write spill data")?;
        writer.flush().context("Failed to flush spill data")?;

        Ok(serialized.len())
    }

    /// Restore a spilled partition from disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the spill file cannot be read or deserialized.
    pub fn restore<T: 'static + Send + Sync + Clone + for<'de> Deserialize<'de>>(
        &self,
        spill_id: u64,
    ) -> Result<Vec<T>> {
        let path = self.spill_path(spill_id);
        let file = File::open(&path).context("Failed to open spill file")?;

        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new();
        reader
            .read_to_end(&mut buffer)
            .context("Failed to read spill file")?;

        // Deserialize the data using bincode 2.0 API
        let data: Vec<T> = if self.config.compress {
            #[cfg(feature = "compression-zstd")]
            {
                let decompressed = zstd::decode_all(&buffer[..])?;
                let (decoded, _) =
                    bincode::serde::decode_from_slice(&decompressed, bincode::config::standard())?;
                decoded
            }
            #[cfg(not(feature = "compression-zstd"))]
            {
                let (decoded, _) =
                    bincode::serde::decode_from_slice(&buffer, bincode::config::standard())?;
                decoded
            }
        } else {
            let (decoded, _) =
                bincode::serde::decode_from_slice(&buffer, bincode::config::standard())?;
            decoded
        };

        Ok(data)
    }

    /// Delete a spill file from disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be deleted.
    pub fn cleanup(&self, spill_id: u64) -> Result<()> {
        let path = self.spill_path(spill_id);
        if path.exists() {
            remove_file(&path).context("Failed to delete spill file")?;
        }
        Ok(())
    }
}

/// State of a spillable partition.
#[derive(Debug, Clone)]
enum PartitionState {
    /// Data is in memory
    InMemory,

    /// Data has been spilled to disk
    Spilled {
        spill_id: u64,
        #[allow(dead_code)]
        disk_size: usize,
    },
}

/// A partition wrapper that supports automatic spilling to disk.
///
/// This wrapper tracks the memory usage of a partition and automatically spills
/// it to disk when memory pressure is detected. The spilling is transparent to
/// the rest of the pipeline.
pub struct SpillablePartition<
    T: 'static + Send + Sync + Clone + Serialize + for<'de> Deserialize<'de>,
> {
    data: Option<Vec<T>>,
    state: PartitionState,
    memory_size: usize,
    tracker: Option<MemoryTracker>,
}

impl<T: 'static + Send + Sync + Clone + Serialize + for<'de> Deserialize<'de>>
    SpillablePartition<T>
{
    /// Create a new spillable partition from data.
    ///
    /// The memory size is estimated based on the vector capacity and element size.
    #[must_use]
    pub fn new(data: Vec<T>) -> Self {
        let memory_size = Self::estimate_size(&data);

        // Register with the global tracker if available
        if let Some(tracker) = MemoryTracker::instance() {
            tracker.allocate(memory_size);

            Self {
                data: Some(data),
                state: PartitionState::InMemory,
                memory_size,
                tracker: Some(tracker),
            }
        } else {
            Self {
                data: Some(data),
                state: PartitionState::InMemory,
                memory_size,
                tracker: None,
            }
        }
    }

    /// Estimate the memory size of a vector in bytes.
    const fn estimate_size(data: &[T]) -> usize {
        // Conservative estimate: length * element size + overhead
        let overhead = size_of::<Vec<T>>();
        size_of_val(data) + overhead
    }

    /// Check if this partition should be spilled to disk.
    #[must_use]
    pub fn should_spill(&self) -> bool {
        self.tracker.as_ref().is_some_and(|tracker| {
            let config = tracker.config();
            // Only spill if:
            // 1. We're in memory
            // 2. Global memory limit is exceeded
            // 3. This partition is large enough to be worth spilling
            matches!(self.state, PartitionState::InMemory)
                && tracker.should_spill()
                && self.memory_size >= config.min_spill_size
        })
    }

    /// Spill this partition to disk.
    ///
    /// # Errors
    ///
    /// Returns an error if disk I/O or serialization fails.
    pub fn spill(&mut self) -> Result<()> {
        if !matches!(self.state, PartitionState::InMemory) {
            return Ok(()); // Already spilled
        }

        let Some(ref tracker) = self.tracker else {
            return Ok(()); // No tracker, can't spill
        };

        let config = tracker.config();
        let manager = SpillManager::new(config)?;

        let data = self
            .data
            .as_ref()
            .ok_or_else(|| anyhow!("No data to spill"))?;

        let spill_id = tracker.next_spill_id();
        let disk_size = manager.spill(data, spill_id)?;

        // Free the in-memory data
        self.data = None;
        tracker.deallocate(self.memory_size);

        self.state = PartitionState::Spilled {
            spill_id,
            disk_size,
        };

        Ok(())
    }

    /// Restore this partition from disk into memory.
    ///
    /// # Errors
    ///
    /// Returns an error if disk I/O or deserialization fails.
    pub fn restore(&mut self) -> Result<()> {
        let PartitionState::Spilled { spill_id, .. } = self.state else {
            return Ok(()); // Already in memory
        };

        let Some(ref tracker) = self.tracker else {
            return Err(anyhow!("Cannot restore without tracker"));
        };

        let config = tracker.config();
        let manager = SpillManager::new(config)?;

        let data = manager.restore(spill_id)?;
        self.memory_size = Self::estimate_size(&data);

        // Register the restored memory
        tracker.allocate(self.memory_size);

        self.data = Some(data);
        self.state = PartitionState::InMemory;

        // Clean up the spill file
        let _ = manager.cleanup(spill_id);

        Ok(())
    }

    /// Get a reference to the data, restoring from disk if necessary.
    ///
    /// # Errors
    ///
    /// Returns an error if restoration from disk fails.
    pub fn data(&mut self) -> Result<&[T]> {
        if self.data.is_none() {
            self.restore()?;
        }

        self.data
            .as_deref()
            .ok_or_else(|| anyhow!("Failed to access partition data"))
    }

    /// Take ownership of the data, restoring from disk if necessary.
    ///
    /// # Errors
    ///
    /// Returns an error if restoration from disk fails.
    pub fn into_vec(mut self) -> Result<Vec<T>> {
        if self.data.is_none() {
            self.restore()?;
        }

        self.data
            .take()
            .ok_or_else(|| anyhow!("Failed to take partition data"))
    }

    /// Check if the data is currently in memory.
    #[must_use]
    pub const fn is_in_memory(&self) -> bool {
        matches!(self.state, PartitionState::InMemory)
    }

    /// Check if the data has been spilled to disk.
    #[must_use]
    pub const fn is_spilled(&self) -> bool {
        matches!(self.state, PartitionState::Spilled { .. })
    }

    /// Get the current memory size of this partition.
    #[must_use]
    pub const fn memory_size(&self) -> usize {
        if self.is_in_memory() {
            self.memory_size
        } else {
            0
        }
    }
}

impl<T: 'static + Send + Sync + Clone + Serialize + for<'de> Deserialize<'de>> Drop
    for SpillablePartition<T>
{
    fn drop(&mut self) {
        // Clean up spilled data if it exists
        if let PartitionState::Spilled { spill_id, .. } = self.state
            && let Some(ref tracker) = self.tracker
        {
            let config = tracker.config();
            if let Ok(manager) = SpillManager::new(config) {
                let _ = manager.cleanup(spill_id);
            }
        }

        // Deallocate tracked memory
        if self.is_in_memory()
            && let Some(ref tracker) = self.tracker
        {
            tracker.deallocate(self.memory_size);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_tracker_initialization() {
        let config = SpillConfig::new().with_memory_limit(1000);
        MemoryTracker::initialize(config);

        let tracker = MemoryTracker::instance().unwrap();
        assert_eq!(tracker.memory_limit(), Some(1000));
        assert_eq!(tracker.current_usage(), 0);
    }

    #[test]
    fn test_memory_allocation_tracking() {
        let config = SpillConfig::new().with_memory_limit(1000);
        MemoryTracker::initialize(config);

        let tracker = MemoryTracker::instance().unwrap();
        tracker.allocate(500);
        assert_eq!(tracker.current_usage(), 500);

        tracker.allocate(300);
        assert_eq!(tracker.current_usage(), 800);

        tracker.deallocate(200);
        assert_eq!(tracker.current_usage(), 600);
    }

    #[test]
    fn test_should_spill_detection() {
        let config = SpillConfig::new().with_memory_limit(1000);
        MemoryTracker::initialize(config);

        let tracker = MemoryTracker::instance().unwrap();
        assert!(!tracker.should_spill());

        tracker.allocate(1001);
        assert!(tracker.should_spill());
    }
}
