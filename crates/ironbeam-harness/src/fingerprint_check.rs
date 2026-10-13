//! Reads the driver-side structural fingerprint out of the pipeline proto
//! display-data (or out of `PipelineOptions` once we add a JSON option) and
//! compares it to the one we compute locally at harness startup.
//!
//! A mismatch means the user's `build(&pipeline)` function produced
//! different graph shapes in the driver vs. the harness — a violation of
//! the contract. For MVP we log LOUDLY and keep going; future versions may
//! hard-fail.

use tracing::{info, warn};

pub const FINGERPRINT_URN: &str = "ironbeam:fingerprint:v1";

/// Extract the driver-side fingerprint from a `Pipeline`'s display_data list.
/// Returns `None` if the urn isn't present or the payload has wrong length.
pub fn fingerprint_from_pipeline_display_data(
    dd: &[beam_proto::pipeline_v1::DisplayData],
) -> Option<u64> {
    let entry = dd.iter().find(|d| d.urn == FINGERPRINT_URN)?;
    (entry.payload.len() == 8).then(|| {
        let mut b = [0u8; 8];
        b.copy_from_slice(&entry.payload);
        u64::from_le_bytes(b)
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FingerprintStatus {
    /// Driver fingerprint wasn't embedded in the pipeline — can't compare.
    Missing,
    /// Both sides agree.
    Match,
    /// Driver and harness saw different graphs.
    Mismatch { driver: u64, local: u64 },
}

pub fn compare(driver: Option<u64>, local: u64) -> FingerprintStatus {
    match driver {
        None => FingerprintStatus::Missing,
        Some(d) if d == local => FingerprintStatus::Match,
        Some(d) => FingerprintStatus::Mismatch { driver: d, local },
    }
}

pub fn log(status: FingerprintStatus) {
    match status {
        FingerprintStatus::Match => info!("pipeline fingerprint verified"),
        FingerprintStatus::Missing => warn!(
            "driver pipeline fingerprint missing — skipping cross-check \
             (the driver may be an older version)"
        ),
        FingerprintStatus::Mismatch { driver, local } => warn!(
            driver = format!("{driver:#018x}"),
            local = format!("{local:#018x}"),
            "pipeline fingerprint mismatch — driver and harness built different \
             graphs. Results may be incorrect; investigate build() determinism."
        ),
    }
}

#[cfg(test)]
#[path = "../tests/unit/fingerprint_check__tests.rs"]
mod tests;
