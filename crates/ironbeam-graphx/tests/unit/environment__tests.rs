// Unit tests for src/environment.rs.
use super::*;

#[test]
fn sdk_version_derived_constants_stay_in_sync() {
    assert!(
        SDK_BASE_VERSION.ends_with(BEAM_SDK_VERSION),
        "SDK_BASE_VERSION ({SDK_BASE_VERSION}) must end with BEAM_SDK_VERSION ({BEAM_SDK_VERSION})"
    );
    assert_eq!(
        DEFAULT_SDK_CONTAINER_IMAGE,
        format!("{STOCK_GO_SDK_IMAGE_REPO}:{BEAM_SDK_VERSION}")
    );
}
