//! Strict reading of committed Beam metrics used by local result coordination.

use anyhow::{bail, Context, Result};
use beam_proto::job_management_v1::MetricResults;
use beam_proto::pipeline_v1::MonitoringInfo;

fn one_committed_sum(
    metrics: &MetricResults,
    description: &str,
    matches: impl Fn(&MonitoringInfo) -> bool,
) -> Result<u64> {
    let mut matching = metrics.committed.iter().filter(|info| matches(info));
    let info = matching
        .next()
        .with_context(|| format!("no committed {description}"))?;
    if matching.next().is_some() {
        bail!("multiple committed {description} metrics");
    }
    if info.r#type != "beam:metrics:sum_int64:v1" {
        bail!("unexpected committed {description} type");
    }
    let mut payload = info.payload.as_slice();
    let count = crate::coders::decode_varint(&mut payload)
        .with_context(|| format!("invalid committed {description}"))?;
    if !payload.is_empty() {
        bail!("trailing bytes in committed {description}");
    }
    Ok(count)
}

/// Return one runner-committed element count for a PCollection.
/// A missing or ambiguous count cannot establish output completeness.
pub fn committed_element_count(metrics: &MetricResults, pcollection_id: &str) -> Result<u64> {
    one_committed_sum(
        metrics,
        &format!("element count for PCollection {pcollection_id}"),
        |info| {
            info.urn == "beam:metric:element_count:v1"
                && info
                    .labels
                    .get("PCOLLECTION")
                    .is_some_and(|label| label == pcollection_id)
        },
    )
}

/// Count rows successfully written by one v5 user sink across committed bundles.
pub fn committed_sink_input_count(metrics: &MetricResults, transform_id: &str) -> Result<u64> {
    one_committed_sum(
        metrics,
        &format!("sink input count for transform {transform_id}"),
        |info| {
            info.urn == "beam:metric:user:sum_int64:v1"
                && info
                    .labels
                    .get("PTRANSFORM")
                    .is_some_and(|label| label == transform_id)
                && info
                    .labels
                    .get("NAMESPACE")
                    .is_some_and(|label| label == "ironbeam.sink")
                && info
                    .labels
                    .get("NAME")
                    .is_some_and(|label| label == "input_elements")
        },
    )
}

#[cfg(test)]
#[path = "../tests/unit/portable_metrics__tests.rs"]
mod tests;
