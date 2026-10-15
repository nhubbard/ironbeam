//! `BeamFnProvision.GetProvisionInfo` — one-shot RPC the SDK fires on startup
//! to obtain pipeline options, the artifact retrieval token, and any runner-
//! supplied capability list.
//!
//! Dataflow populates the `pipeline_options` field with the JSON the driver
//! posted; this is where we recover our driver-side fingerprint.

use anyhow::{Context, Result};
use beam_proto::fn_execution_v1::provision_service_client::ProvisionServiceClient;
use beam_proto::fn_execution_v1::{GetProvisionInfoRequest, ProvisionInfo};
use tonic::transport::Channel;

use crate::endpoints::{normalize_endpoint, worker_request};

pub async fn get_provision_info(endpoint: &str) -> Result<ProvisionInfo> {
    get_provision_info_inner(endpoint, None).await
}

pub async fn get_provision_info_for_worker(
    endpoint: &str,
    worker_id: &str,
) -> Result<ProvisionInfo> {
    get_provision_info_inner(endpoint, Some(worker_id)).await
}

async fn get_provision_info_inner(
    endpoint: &str,
    worker_id: Option<&str>,
) -> Result<ProvisionInfo> {
    let url = normalize_endpoint(endpoint).context("provision endpoint")?;
    let channel = Channel::from_shared(url)
        .context("provision channel")?
        .connect()
        .await?;
    let mut client = ProvisionServiceClient::new(channel);
    let resp = client
        .get_provision_info(worker_request(GetProvisionInfoRequest {}, worker_id)?)
        .await?
        .into_inner();
    resp.info.context("GetProvisionInfoResponse.info was empty")
}

/// Pull a top-level key from `ProvisionInfo.pipeline_options` (a
/// `google.protobuf.Struct`) as a string, if present and of string type.
pub fn pipeline_option_str<'a>(info: &'a ProvisionInfo, key: &str) -> Option<&'a str> {
    let struct_ = info.pipeline_options.as_ref()?;
    match &struct_.fields.get(key)?.kind {
        Some(prost_types::value::Kind::StringValue(s)) => Some(s.as_str()),
        _ => None,
    }
}

/// Pull a top-level key as u64. JSON numbers must be safe integers; larger
/// unsigned values may be supplied as decimal strings without losing bits.
pub fn pipeline_option_u64(info: &ProvisionInfo, key: &str) -> Option<u64> {
    let struct_ = info.pipeline_options.as_ref()?;
    match &struct_.fields.get(key)?.kind {
        Some(prost_types::value::Kind::NumberValue(n))
            if n.is_finite() && *n >= 0.0 && n.fract() == 0.0 && *n <= 9_007_199_254_740_991.0 =>
        {
            Some(*n as u64)
        }
        Some(prost_types::value::Kind::StringValue(s)) => s.parse().ok(),
        _ => None,
    }
}
