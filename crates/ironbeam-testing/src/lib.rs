//! Testing fixtures and assertions for Ironbeam 5 pipelines.
pub mod assertions;
pub mod builders;
pub mod debug;
pub mod fixtures;
pub mod mock_io;

pub use assertions::*;
pub use builders::*;
pub use debug::*;
pub use fixtures::*;
pub use mock_io::*;

use ironbeam_core::{Error, Pipeline};
use std::ops::Deref;

#[derive(Clone, Default)]
pub struct TestPipeline {
    pipeline: Pipeline,
}
impl TestPipeline {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn node_count(&self) -> Result<usize, Error> {
        Ok(self.pipeline.snapshot()?.0.nodes.len())
    }
    pub fn edge_count(&self) -> Result<usize, Error> {
        Ok(self
            .pipeline
            .snapshot()?
            .0
            .nodes
            .iter()
            .map(|node| usize::from(node.input.is_some()) + node.additional_inputs.len())
            .sum())
    }
    pub fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }
}
impl Deref for TestPipeline {
    type Target = Pipeline;
    fn deref(&self) -> &Self::Target {
        &self.pipeline
    }
}
