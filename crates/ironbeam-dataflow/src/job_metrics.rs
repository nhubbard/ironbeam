//! Regional Dataflow job metrics retrieval.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;

use crate::{auth::TokenSource, options::DataflowOptions};

/// The latest metric values returned for one Dataflow job.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataflowJobMetrics {
    pub metric_time: Option<String>,
    pub metrics: Vec<DataflowMetric>,
}

/// One metric update. Dataflow also reports non-scalar values; `other` retains
/// those fields without treating them as a committed element count.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataflowMetric {
    pub name: Option<DataflowMetricName>,
    pub kind: Option<String>,
    pub cumulative: Option<bool>,
    pub scalar: Option<Value>,
    pub update_time: Option<String>,
    #[serde(flatten)]
    pub other: BTreeMap<String, Value>,
}

/// A Dataflow metric's namespace, name, and step or collection labels.
#[derive(Clone, Debug, Deserialize)]
pub struct DataflowMetricName {
    pub origin: Option<String>,
    pub name: String,
    #[serde(default)]
    pub context: BTreeMap<String, String>,
}

impl DataflowMetric {
    /// Decode a nonnegative integer scalar, accepting JSON numbers and the
    /// decimal strings used by some 64-bit JSON APIs. Other scalar kinds fail.
    pub fn scalar_u64(&self) -> anyhow::Result<Option<u64>> {
        let Some(value) = &self.scalar else {
            return Ok(None);
        };
        if let Some(number) = value.as_u64() {
            return Ok(Some(number));
        }
        if let Some(text) = value.as_str() {
            return Ok(Some(text.parse()?));
        }
        anyhow::bail!("Dataflow metric scalar is not a nonnegative integer")
    }
}

/// Fetch all current metrics from the regional Dataflow API. This raw service
/// view does not identify Ironbeam's committed sink counts on its own.
pub async fn get_job_metrics<T: TokenSource + ?Sized>(
    base_url: &str,
    token_source: &T,
    opts: &DataflowOptions,
    job_id: &str,
) -> anyhow::Result<DataflowJobMetrics> {
    let url = format!(
        "{}/v1b3/projects/{}/locations/{}/jobs/{}/metrics",
        base_url, opts.project, opts.region, job_id
    );
    let token = token_source.token().await?;
    let response = reqwest::Client::new()
        .get(&url)
        .bearer_auth(token)
        .send()
        .await?;
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("Dataflow jobs.getMetrics {url} failed: {status} — {text}");
    }
    Ok(serde_json::from_str(&text)?)
}
