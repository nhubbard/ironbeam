//! Bounded, in-process Ironbeam 4 and 5 migration helpers.
//!
//! Bridges run the source graph locally and insert its materialized values into
//! a new graph. They do not transfer graph nodes, execution state, or metadata.
use ironbeam_core::{Element as V5Element, PCollection as V5Collection, Pipeline as V5Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_v4::{Element as V4Element, PCollection as V4Collection, Pipeline as V4Pipeline};
use std::fmt;

/// Imports needed for the v5 convenience and local terminal methods.
pub mod prelude {
    pub use ironbeam_direct::DirectCollectExt;
    pub use ironbeam_transforms::{ElementwiseExt, KeyedExt};
}

/// Migration spelling of the v4 source helper, with the stable v5 node key.
///
/// # Errors
/// Returns a typed v5 construction error.
pub fn from_vec<T: V5Element + Clone + Sync>(
    pipeline: &V5Pipeline,
    key: &str,
    values: Vec<T>,
) -> Result<V5Collection<T>, ironbeam_core::Error> {
    pipeline.from_vec(key, values)
}

/// Identifies which local execution or graph construction step failed.
#[derive(Debug)]
pub enum BridgeError {
    V4(anyhow::Error),
    V5(ironbeam_core::Error),
}
impl fmt::Display for BridgeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::V4(error) => write!(f, "Ironbeam 4 collection failed: {error}"),
            Self::V5(error) => write!(f, "Ironbeam 5 graph or Direct execution failed: {error}"),
        }
    }
}
impl std::error::Error for BridgeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::V4(error) => Some(error.as_ref()),
            Self::V5(error) => Some(error),
        }
    }
}

/// Materialize a bounded v4 collection with the local sequential runner and
/// insert its values into a new v5 graph.
///
/// # Errors
/// Returns the v4 execution or v5 graph error with its side identified.
pub fn v4_to_v5<T>(
    source: V4Collection<T>,
    target: &V5Pipeline,
    key: &str,
) -> Result<V5Collection<T>, BridgeError>
where
    T: V4Element + V5Element + Clone + Sync,
{
    let values = source.collect_seq().map_err(BridgeError::V4)?;
    target.from_vec(key, values).map_err(BridgeError::V5)
}

/// Materialize a bounded v5 collection with Direct and insert its values into
/// a new v4 graph. `key` names the v5 output used for materialization.
///
/// # Errors
/// Returns the v5 graph or Direct execution error with its side identified.
pub fn v5_to_v4<T>(
    source: &V5Collection<T>,
    target: &V4Pipeline,
    key: &str,
    config: &JobConfig,
) -> Result<V4Collection<T>, BridgeError>
where
    T: V4Element + V5Element + Clone + Sync,
{
    let output = source.output(key).map_err(BridgeError::V5)?;
    let (values, _) = DirectRunner
        .run_collect(&output, config)
        .map_err(BridgeError::V5)?;
    Ok(ironbeam_v4::from_vec(target, values))
}
