//! Testing utilities for Ironbeam pipelines.
//!
//! This module provides a comprehensive testing facility for end-users to write
//! idiomatic Rust tests for their data pipelines. It includes:
//!
//! - **Assertions**: Compare pipeline outputs with expected results
//! - **Test data builders**: Generate test data easily
//! - **Debug utilities**: Inspect pipelines during execution
//! - **Fixtures**: Pre-built test datasets for common scenarios
//! - **Mock I/O**: Test I/O operations without actual files
//!
//! # Quick Start
//!
//! ```no_run
//! use ironbeam::*;
//! use ironbeam::testing::*;
//!
//! #[test]
//! fn test_simple_pipeline() -> anyhow::Result<()> {
//!     let p = TestPipeline::new();
//!
//!     let result = from_vec(&p, vec![1, 2, 3])
//!         .map(|x: &i32| x * 2)
//!         .collect_seq()?;
//!
//!     assert_collections_equal(result, vec![2, 4, 6]);
//!     Ok(())
//! }
//! ```
//!
//! # Assertion Functions
//!
//! The testing module provides several assertion functions for comparing collections:
//!
//! - [`assert_collections_equal`]: Exact order-dependent comparison
//! - [`assert_collections_unordered_equal`]: Order-independent comparison
//! - [`assert_kv_collections_equal`]: Compare key-value pairs (sorted by key)
//! - [`assert_all`]: Verify all elements match a predicate
//! - [`assert_any`]: Verify at least one element matches a predicate
//! - [`assert_none`]: Verify no elements match a predicate
//!
//! # Test Data Builders
//!
//! Use [`TestDataBuilder`] to create test data fluently:
//!
//! ```
//! use ironbeam::testing::*;
//!
//! let data = TestDataBuilder::<i32>::new()
//!     .add_range(1..=10)
//!     .add_range(20..=25)
//!     .build();
//! ```
//!
//! # Debug Utilities
//!
//! Debug your pipelines during test execution:
//!
//! ```no_run
//! use ironbeam::*;
//! use ironbeam::testing::*;
//!
//! # fn main() -> anyhow::Result<()> {
//! let p = TestPipeline::new();
//! let result = from_vec(&p, vec![1, 2, 3])
//!     .debug_inspect("after source")
//!     .map(|x: &i32| x * 2)
//!     .debug_inspect("after map")
//!     .collect_seq()?;
//! # Ok(())
//! # }
//! ```

pub mod assertions;
pub mod builders;
pub mod debug;
pub mod fixtures;

#[cfg(any(feature = "io-csv", feature = "io-jsonl", feature = "io-parquet"))]
pub mod mock_io;

// Re-export commonly used items
pub use assertions::*;
pub use builders::*;
pub use debug::*;
pub use fixtures::*;

#[cfg(any(feature = "io-csv", feature = "io-jsonl", feature = "io-parquet"))]
pub use mock_io::*;

use crate::Pipeline;

/// A test-focused wrapper around [`Pipeline`] with additional debugging utilities.
///
/// This is a thin wrapper that provides the same functionality as [`Pipeline::default()`]
/// but can be extended with test-specific features in the future.
///
/// # Example
///
/// ```
/// use ironbeam::testing::TestPipeline;
/// use ironbeam::from_vec;
///
/// let p = TestPipeline::new();
/// let data = from_vec(&p, vec![1, 2, 3]);
/// ```
#[derive(Clone)]
pub struct TestPipeline {
    pipeline: Pipeline,
}

impl TestPipeline {
    /// Create a new test pipeline.
    #[must_use]
    pub fn new() -> Self {
        Self {
            pipeline: Pipeline::default(),
        }
    }

    /// Get the number of nodes in the pipeline graph.
    ///
    /// Useful for verifying that transformations are being added correctly.
    #[must_use]
    pub fn node_count(&self) -> usize {
        let (nodes, _) = self.pipeline.snapshot();
        nodes.len()
    }

    /// Get the number of edges in the pipeline graph.
    ///
    /// Useful for verifying that transformations are connected correctly.
    #[must_use]
    pub fn edge_count(&self) -> usize {
        let (_, edges) = self.pipeline.snapshot();
        edges.len()
    }

    /// Print a debug representation of the pipeline graph.
    ///
    /// Shows nodes and their connections for debugging purposes.
    pub fn debug_print_graph(&self) {
        let (nodes, edges) = self.pipeline.snapshot();
        println!("Pipeline Graph:");
        println!("  Nodes: {}", nodes.len());
        for id in nodes.keys() {
            println!("    NodeId({id:?})");
        }
        println!("  Edges: {}", edges.len());
        for (from, to) in &edges {
            println!("    {from:?} -> {to:?}");
        }
    }
}

impl Default for TestPipeline {
    fn default() -> Self {
        Self::new()
    }
}

// Allow TestPipeline to be used wherever Pipeline is expected
impl std::ops::Deref for TestPipeline {
    type Target = Pipeline;

    fn deref(&self) -> &Self::Target {
        &self.pipeline
    }
}

impl AsRef<Pipeline> for TestPipeline {
    fn as_ref(&self) -> &Pipeline {
        &self.pipeline
    }
}
