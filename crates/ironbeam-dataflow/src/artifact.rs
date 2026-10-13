//! Rewrite `Environment.dependencies` so each SDK environment points at a
//! GCS-staged worker binary. Mirrors Go SDK
//! `sdks/go/pkg/beam/core/runtime/graphx/translate.go::UpdateDefaultEnvWorkerType`.
//!
//! We use role `beam:artifact:role:go_worker_binary:v1` so the stock Go SDK
//! container-boot treats our Rust binary as the worker artifact — replacing
//! this with a dedicated `rust_worker_binary` role is a v1.1 item.

use beam_proto::pipeline_v1::{ArtifactInformation, ArtifactUrlPayload, Pipeline};
use ironbeam_graphx::urns;
use prost::Message;

/// A GCS-staged object pointer. `url` is always `gs://bucket/path` form so the
/// Go container-boot can fetch it directly; `sha256` is the hex-encoded digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedArtifact {
    pub url: String,
    pub sha256: String,
}

/// Replace every `Environment.dependencies` entry with a single entry that
/// references the staged worker binary. Mutates in place.
///
/// An environment with no `payload` URN matching `beam:env:docker:v1` is still
/// annotated — the runner only reads this list for environments it launches.
pub fn attach_worker_binary(pipeline: &mut Pipeline, worker: &StagedArtifact) {
    let Some(comps) = pipeline.components.as_mut() else {
        return;
    };
    for env in comps.environments.values_mut() {
        env.dependencies = vec![worker_artifact_info(worker)];
    }
}

fn worker_artifact_info(worker: &StagedArtifact) -> ArtifactInformation {
    let url_payload = ArtifactUrlPayload {
        url: worker.url.clone(),
        sha256: worker.sha256.clone(),
    }
    .encode_to_vec();
    ArtifactInformation {
        type_urn: urns::ARTIFACT_TYPE_URL.to_string(),
        type_payload: url_payload,
        role_urn: urns::ARTIFACT_ROLE_GO_WORKER_BINARY.to_string(),
        role_payload: Vec::new(),
    }
}

#[cfg(test)]
#[path = "../tests/unit/artifact__tests.rs"]
mod tests;
