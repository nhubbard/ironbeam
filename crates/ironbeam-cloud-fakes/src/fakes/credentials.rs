use super::*;

// ============================================================================
// Fake Credentials and Config
// ============================================================================

#[derive(Debug, Clone)]
/// Fixed credential identity for testing provider authentication.
pub struct FakeCredentials {
    pub identifier: String,
    pub credential_type: String,
}

impl CloudCredentials for FakeCredentials {
    fn identifier(&self) -> &str {
        &self.identifier
    }

    fn credential_type(&self) -> &str {
        &self.credential_type
    }

    fn validate(&self) -> CloudResult<()> {
        if self.identifier.is_empty() {
            return Err(CloudIOError::new(
                ErrorKind::Authentication,
                "Empty identifier",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
/// Configurable region, endpoint, timeout, and retry values for tests.
pub struct FakeConfig {
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub timeout_secs: u64,
    pub max_retries: u32,
}

impl Default for FakeConfig {
    fn default() -> Self {
        Self {
            region: None,
            endpoint: None,
            timeout_secs: 30,
            max_retries: 3,
        }
    }
}

impl CloudConfig for FakeConfig {
    fn region(&self) -> Option<&str> {
        self.region.as_deref()
    }

    fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }

    fn timeout_secs(&self) -> u64 {
        self.timeout_secs
    }

    fn max_retries(&self) -> u32 {
        self.max_retries
    }
}
