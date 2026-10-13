//! Shared bundle runner contract and Beam monitoring payloads.

use anyhow::{bail, Result};
use async_trait::async_trait;
use beam_proto::fn_execution_v1::{
    ProcessBundleResponse, ProcessBundleSplitRequest, ProcessBundleSplitResponse,
};
use beam_proto::pipeline_v1::MonitoringInfo;

use crate::bundle::BundlePlan;

/// Control-loop-facing contract for a planned Fn API bundle.
#[async_trait]
pub trait BundleRunner: Send + Sync {
    async fn run(&self, instruction_id: &str, plan: &BundlePlan) -> Result<ProcessBundleResponse>;

    /// Respond while the named bundle is still running. An empty response
    /// means no work was split; an SDF runner must override this to return
    /// primary and residual roots from its active source restriction.
    async fn split_bundle(
        &self,
        _request: &ProcessBundleSplitRequest,
    ) -> Result<ProcessBundleSplitResponse> {
        Ok(ProcessBundleSplitResponse::default())
    }

    /// Called only after the runner has committed the named bundle's output.
    /// Implementations must not acknowledge a finalization they did not register.
    async fn finalize_bundle(&self, instruction_id: &str) -> Result<()> {
        bail!("bundle finalization is unsupported for {instruction_id}")
    }
}

/// Build a `beam:metric:element_count:v1` monitoring payload for a collection.
pub(crate) fn element_count_info(pcollection_id: &str, count: u64) -> Result<MonitoringInfo> {
    let mut payload = Vec::new();
    crate::coders::encode_varint(count, &mut payload)?;
    Ok(MonitoringInfo {
        urn: "beam:metric:element_count:v1".to_string(),
        r#type: "beam:metrics:sum_int64:v1".to_string(),
        payload,
        labels: std::iter::once(("PCOLLECTION".to_string(), pcollection_id.to_string())).collect(),
        ..Default::default()
    })
}

#[cfg(test)]
#[path = "../tests/unit/executor__tests.rs"]
mod tests;
