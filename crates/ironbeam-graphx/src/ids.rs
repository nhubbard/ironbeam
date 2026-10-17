//! Stable string-id minting for the Pipeline proto. Beam expects every
//! PTransform, PCollection, Coder, WindowingStrategy, and Environment to have
//! a unique string key in `Components.*`. We mint them deterministically from
//! the ironbeam graph so two translations of the same pipeline produce the
//! same proto bytes — which matters for the driver/harness fingerprint check.

pub const ENV_ID: &str = "ironbeam_env";
pub const WINDOWING_ID: &str = "global_windowing";

// Shared coder ids used by the v5 graph.
pub const CODER_BYTES: &str = "c_bytes";
pub const CODER_DOUBLE: &str = "c_double";
pub const CODER_GLOBAL_WINDOW: &str = "c_gw";
pub const CODER_INTERVAL_WINDOW: &str = "c_iw";
