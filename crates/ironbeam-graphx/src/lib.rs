//! Translates v5 Ironbeam logical graphs into Apache Beam portable-pipeline
//! protobufs (`beam_proto::pipeline_v1::Pipeline`).
//!
//! See the workspace `README.md` and `V5_ALIGNMENT.md` for status and architecture.

pub mod environment;
pub mod ids;
pub mod sdf;
pub mod urns;
pub mod v5;
pub mod windowing;

pub use environment::{BEAM_SDK_VERSION, DEFAULT_SDK_CONTAINER_IMAGE};
