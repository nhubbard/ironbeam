//! Beam Docker environment for the portable Rust worker.
//!
//! The stock Go SDK container starts the staged Rust worker binary. Artifact
//! dependencies are added by the submission layer after staging.

use prost::Message;

use beam_proto::pipeline_v1::{DockerPayload, Environment};

use crate::urns;

macro_rules! define_beam_sdk_constants {
    ($version:literal) => {
        /// Beam SDK version advertised to Dataflow Runner v2.
        /// It matches the bundled protocol definitions and is at least 2.21.0.
        pub const BEAM_SDK_VERSION: &str = $version;

        /// GCR repository for the stock Go SDK worker container (no tag).
        pub const STOCK_GO_SDK_IMAGE_REPO: &str = "gcr.io/cloud-dataflow/v1beta3/beam_go_sdk";

        /// Stock Go SDK worker container tagged at [`BEAM_SDK_VERSION`].
        /// Its bootloader starts the staged Rust binary with Beam Fn API flags.
        pub const DEFAULT_SDK_CONTAINER_IMAGE: &str =
            concat!("gcr.io/cloud-dataflow/v1beta3/beam_go_sdk:", $version);

        /// Version URN sent to Dataflow Runner v2 with the Go SDK container.
        /// Update it with [`BEAM_SDK_VERSION`] and the bundled protocol definitions.
        pub const SDK_BASE_VERSION: &str =
            concat!("beam:version:sdk_base:apache/beam_go_sdk:", $version);
    };
}

define_beam_sdk_constants!("2.74.0");

/// Beam URNs advertised by the portable worker environment.
/// The list includes only operations and coders the harness accepts.
pub const MVP_CAPABILITIES: &[&str] = &[
    SDK_BASE_VERSION,
    urns::TRANSFORM_IMPULSE,
    urns::TRANSFORM_PARDO,
    urns::TRANSFORM_GROUP_BY_KEY,
    urns::TRANSFORM_FLATTEN,
    urns::CODER_BYTES,
    urns::CODER_KV,
    urns::CODER_ITERABLE,
    urns::CODER_LENGTH_PREFIX,
    urns::CODER_GLOBAL_WINDOW,
    urns::CODER_WINDOWED_VALUE,
    urns::CODER_IRONBEAM_V5,
];

/// Build a Beam Docker environment for the worker container image.
pub fn docker_environment(container_image: &str) -> Environment {
    let payload = DockerPayload {
        container_image: container_image.to_string(),
    }
    .encode_to_vec();

    Environment {
        urn: urns::ENV_DOCKER.to_string(),
        payload,
        display_data: Vec::new(),
        capabilities: MVP_CAPABILITIES.iter().map(|s| s.to_string()).collect(),
        dependencies: Vec::new(),
        resource_hints: std::collections::HashMap::new(),
    }
}

#[cfg(test)]
#[path = "../tests/unit/environment__tests.rs"]
mod tests;
