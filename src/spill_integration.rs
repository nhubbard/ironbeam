//! Integration of spilling functionality with the runner.
//!
//! This module provides helpers to integrate automatic spilling with the pipeline execution.
//! It wraps partition operations to automatically check memory pressure and spill when needed.

#[cfg(feature = "spilling")]
use crate::spill::{MemoryTracker, SpillConfig, SpillablePartition};
use crate::type_token::Partition;
use anyhow::Result;
use serde::{Deserialize, Serialize};

/// A helper to automatically manage memory pressure during pipeline execution.
///
/// This wraps partition operations and automatically spills data to persistent storage when
/// memory limits are exceeded.
#[cfg(feature = "spilling")]
pub struct SpillingExecutor {
    enabled: bool,
}

#[cfg(feature = "spilling")]
impl SpillingExecutor {
    /// Create a new spilling executor.
    ///
    /// If a `MemoryTracker` is initialized, spilling will be enabled automatically.
    #[must_use]
    pub fn new() -> Self {
        Self {
            enabled: MemoryTracker::instance().is_some(),
        }
    }

    /// Check if spilling is enabled.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Process a partition with automatic spilling support.
    ///
    /// This checks memory pressure after the partition is created and spills if needed.
    ///
    /// # Errors
    ///
    /// Returns an error if spilling to disk fails.
    pub fn process_partition<T, F>(&self, f: F) -> Result<Partition>
    where
        T: 'static + Send + Sync + Clone + Serialize + for<'de> Deserialize<'de>,
        F: FnOnce() -> Partition,
    {
        let partition = f();

        if !self.enabled {
            return Ok(partition);
        }

        // Try to check if we should spill
        if let Some(tracker) = MemoryTracker::instance()
            && tracker.should_spill()
        {
            // Attempt to spill the partition we just created
            // downcast consumes the partition, so we handle both success and failure
            return match partition.downcast::<Vec<T>>() {
                Ok(vec) => {
                    let mut spillable = SpillablePartition::new(*vec);
                    if spillable.should_spill() {
                        spillable.spill()?;
                    }
                    // For now, we restore immediately as the runner expects in-memory data
                    // In a full implementation, we'd keep track of spilled partitions
                    Ok(Box::new(spillable.into_vec()?) as Partition)
                }
                Err(original) => {
                    // Type mismatch, return the original partition
                    Ok(original)
                }
            }
        }

        Ok(partition)
    }

    /// Wrap a vector in a spillable partition and check if it should be spilled.
    ///
    /// # Errors
    ///
    /// Returns an error if spilling fails.
    pub fn wrap_and_check<T>(&self, data: Vec<T>) -> Result<Vec<T>>
    where
        T: 'static + Send + Sync + Clone + Serialize + for<'de> Deserialize<'de>,
    {
        if !self.enabled {
            return Ok(data);
        }

        let mut spillable = SpillablePartition::new(data);

        // Check if we should spill this partition
        if spillable.should_spill() {
            spillable.spill()?;
            // Restore it immediately for this implementation
            // In a more advanced implementation, we could keep it spilled
            // and only restore when needed
        }

        spillable.into_vec()
    }
}

#[cfg(feature = "spilling")]
impl Default for SpillingExecutor {
    fn default() -> Self {
        Self::new()
    }
}

/// Initialize the spilling system with the given configuration.
///
/// This should be called before running any pipelines if you want to enable
/// automatic memory spilling.
///
/// # Example
///
/// ```no_run
/// use ironbeam::spill_integration::init_spilling;
/// use ironbeam::spill::SpillConfig;
///
/// // Initialize with a 100MB memory limit
/// init_spilling(SpillConfig::new()
///     .with_memory_limit(100 * 1024 * 1024)
///     .with_spill_directory("/tmp/ironbeam-spill"));
/// ```
#[cfg(feature = "spilling")]
pub fn init_spilling(config: SpillConfig) {
    MemoryTracker::initialize(config);
}

/// Get the current memory usage from the global tracker.
///
/// Returns `None` if spilling is not initialized.
#[cfg(feature = "spilling")]
#[must_use]
pub fn current_memory_usage() -> Option<usize> {
    MemoryTracker::instance().map(|t| t.current_usage())
}

/// Reset the memory tracker (mainly for testing).
///
/// This resets the tracked memory usage to zero.
#[cfg(feature = "spilling")]
pub fn reset_memory_tracker() {
    if let Some(tracker) = MemoryTracker::instance() {
        let current = tracker.current_usage();
        if current > 0 {
            tracker.deallocate(current);
        }
    }
}

#[cfg(not(feature = "spilling"))]
pub struct SpillingExecutor;

#[cfg(not(feature = "spilling"))]
impl SpillingExecutor {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    #[must_use]
    pub fn is_enabled(&self) -> bool {
        false
    }
}

#[cfg(not(feature = "spilling"))]
impl Default for SpillingExecutor {
    fn default() -> Self {
        Self::new()
    }
}
