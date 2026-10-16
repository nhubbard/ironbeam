//! Single-environment model: one Docker environment pointing at the stock
//! SDK container (Go bootloader for MVP). Artifact dependencies are left
//! empty here — the submission layer rewrites them after staging the
//! worker binary to GCS.

use prost::Message;

use beam_proto::pipeline_v1::{DockerPayload, Environment};

use crate::urns;

macro_rules! define_beam_sdk_constants {
    ($version:literal) => {
        /// Beam SDK version we pose as. Must be ≥ 2.21.0 for Dataflow Runner v2.
        /// Matches our vendored proto vintage.
        pub const BEAM_SDK_VERSION: &str = $version;

        /// GCR repository for the stock Go SDK worker container (no tag).
        pub const STOCK_GO_SDK_IMAGE_REPO: &str = "gcr.io/cloud-dataflow/v1beta3/beam_go_sdk";

        /// Default worker container for MVP — stock Go SDK bootloader tagged at
        /// [`BEAM_SDK_VERSION`]. Language-agnostic: downloads the artifact with
        /// role `beam:artifact:role:go_worker_binary:v1` and execs it with Fn
        /// API endpoint flags.
        pub const DEFAULT_SDK_CONTAINER_IMAGE: &str =
            concat!("gcr.io/cloud-dataflow/v1beta3/beam_go_sdk:", $version);

        /// SDK version advertisement. Dataflow Runner v2 parses this URN to verify the
        /// submitter's SDK is ≥ 2.21.0; without it, `jobs.create` returns 400:
        ///   "using Runner v2 with Apache Beam SDK requires the SDK version 2.21.0 or later".
        ///
        /// We pose as the Go SDK because our worker rides on its container and uses
        /// its `beam:artifact:role:go_worker_binary:v1` artifact role. Bump
        /// [`BEAM_SDK_VERSION`] and re-vendor protos in lock-step if we need a newer
        /// proto surface.
        pub const SDK_BASE_VERSION: &str =
            concat!("beam:version:sdk_base:apache/beam_go_sdk:", $version);
    };
}

define_beam_sdk_constants!("2.74.0");

/// List of URNs our harness tells Dataflow it understands. Runner v2 checks
/// these to decide what it can dispatch to us. Keep the list narrow for MVP —
/// advertising a capability we don't actually implement makes Dataflow send us
/// bundles we'd crash on.
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
