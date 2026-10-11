//! Compatibility reexports of provider-neutral operation utilities.
pub use ironbeam_cloud_ops::utils::*;

use crate::fake::{FakeConfig, FakeCredentials};
use crate::traits::{CloudConfig, CloudCredentials};

/// Create credentials for tests using the original cloud package path.
#[must_use]
pub fn test_credentials() -> impl CloudCredentials {
    FakeCredentials {
        identifier: "test-user".to_string(),
        credential_type: "test".to_string(),
    }
}

/// Create a default test configuration using the original cloud package path.
#[must_use]
pub fn test_config() -> impl CloudConfig {
    FakeConfig::default()
}
