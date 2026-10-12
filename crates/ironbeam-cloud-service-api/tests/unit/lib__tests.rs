// Unit tests for src/lib.rs.
use super::{CloudConfig, CloudCredentials, CloudResult, ResourceId};

struct MinimalCredentials;

impl CloudCredentials for MinimalCredentials {
    fn identifier(&self) -> &'static str {
        "account"
    }

    fn credential_type(&self) -> &'static str {
        "test"
    }

    fn validate(&self) -> CloudResult<()> {
        Ok(())
    }
}

struct MinimalConfig;

impl CloudConfig for MinimalConfig {}

#[test]
fn optional_service_settings_have_provider_neutral_defaults() {
    assert_eq!(MinimalCredentials.identifier(), "account");
    assert_eq!(MinimalCredentials.credential_type(), "test");
    assert!(MinimalCredentials.validate().is_ok());
    assert!(MinimalCredentials.metadata().is_empty());
    assert_eq!(MinimalConfig.region(), None);
    assert_eq!(MinimalConfig.endpoint(), None);
    assert_eq!(MinimalConfig.timeout_secs(), 30);
    assert_eq!(MinimalConfig.max_retries(), 3);
    assert!(MinimalConfig.extra().is_empty());
}

#[test]
fn resource_id_keeps_provider_and_namespace_identity() {
    let resource = ResourceId::new("provider", "queue", "jobs");
    assert_eq!(resource.to_string(), "provider:queue:jobs");
    assert_eq!(
        resource.with_namespace("project").to_string(),
        "provider:queue:project/jobs"
    );
}
