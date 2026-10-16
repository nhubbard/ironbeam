//! GCP bearer-token acquisition for the Dataflow REST API.
//!
//! Modeled as a `TokenSource` trait so tests can inject a static token and the
//! production path can swap in any ADC implementation. Three impls ship:
//!
//! - [`StaticTokenSource`] — hard-coded token for tests / wiremock.
//! - [`GcloudCliTokenSource`] — shells out to `gcloud auth print-access-token`.
//!   Handy on a developer laptop where `gcloud auth application-default login`
//!   has already been run. Does **not** work inside a Dataflow worker
//!   container — there's no `gcloud` binary there.
//! - [`GcpAuthTokenSource`] — real ADC via the `gcp_auth` crate. Auto-detects
//!   credentials in this order: well-known ADC file, service-account JSON key
//!   (via `GOOGLE_APPLICATION_CREDENTIALS`), GCE/GKE metadata server. Works
//!   both on laptops and inside a Dataflow worker container. Use this by
//!   default; the `GcloudCliTokenSource` is retained as a zero-dep fallback.
//!
//! Tokens returned here always carry the `cloud-platform` OAuth scope.

use std::sync::Arc;

#[async_trait::async_trait]
pub trait TokenSource: Send + Sync + std::fmt::Debug {
    /// Return a bearer token for an authenticated Dataflow request.
    async fn token(&self) -> anyhow::Result<String>;
}

#[async_trait::async_trait]
impl<T: TokenSource + ?Sized> TokenSource for Arc<T> {
    async fn token(&self) -> anyhow::Result<String> {
        (**self).token().await
    }
}

#[async_trait::async_trait]
impl<T: TokenSource + ?Sized> TokenSource for &T {
    async fn token(&self) -> anyhow::Result<String> {
        (**self).token().await
    }
}

#[derive(Debug, Clone)]
pub struct StaticTokenSource {
    token: String,
}

impl StaticTokenSource {
    /// Create a fixed bearer token source for tests or explicit credentials.
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }
}

#[async_trait::async_trait]
impl TokenSource for StaticTokenSource {
    async fn token(&self) -> anyhow::Result<String> {
        Ok(self.token.clone())
    }
}

#[derive(Debug, Default, Clone)]
pub struct GcloudCliTokenSource;

#[async_trait::async_trait]
impl TokenSource for GcloudCliTokenSource {
    async fn token(&self) -> anyhow::Result<String> {
        let out = tokio::process::Command::new("gcloud")
            .args(["auth", "print-access-token"])
            .output()
            .await
            .map_err(|e| anyhow::anyhow!("failed to exec `gcloud`: {e}"))?;
        if !out.status.success() {
            return Err(anyhow::anyhow!(
                "`gcloud auth print-access-token` exited {}: {}",
                out.status,
                String::from_utf8_lossy(&out.stderr)
            ));
        }
        let token = String::from_utf8(out.stdout)
            .map_err(|e| anyhow::anyhow!("gcloud produced non-utf8 token: {e}"))?;
        Ok(token.trim().to_string())
    }
}

/// Scope all Dataflow / GCS calls to cloud-platform. Broader than strictly
/// needed (`devstorage.read_write` + `dataflow` would do) but matches what
/// `gcloud auth print-access-token` yields, so switching between
/// `GcloudCliTokenSource` and `GcpAuthTokenSource` is a no-op for the server.
pub const CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

/// Real Application Default Credentials via the `gcp_auth` crate. Works on:
///
/// - developer laptops where `gcloud auth application-default login` has been
///   run (reads `~/.config/gcloud/application_default_credentials.json`),
/// - any process where `GOOGLE_APPLICATION_CREDENTIALS` points at a
///   service-account JSON key,
/// - GCE / GKE / Dataflow workers (fetches tokens from the metadata server
///   at `http://metadata.google.internal/computeMetadata/v1/...`).
///
/// Safe to share across tasks — the inner provider is `Arc`-backed. Construct
/// once per process.
#[derive(Clone)]
pub struct GcpAuthTokenSource {
    provider: Arc<dyn gcp_auth::TokenProvider>,
    scopes: Arc<Vec<String>>,
}

impl GcpAuthTokenSource {
    /// Create a token source using the default cloud-platform scope. Blocks
    /// on provider discovery (a handful of short fs stats / metadata pings).
    pub async fn new() -> anyhow::Result<Self> {
        Self::with_scopes(&[CLOUD_PLATFORM_SCOPE]).await
    }

    /// Discover application-default credentials with the requested scopes.
    pub async fn with_scopes(scopes: &[&str]) -> anyhow::Result<Self> {
        let provider = gcp_auth::provider()
            .await
            .map_err(|e| anyhow::anyhow!("gcp_auth ADC discovery: {e}"))?;
        Ok(Self {
            provider,
            scopes: Arc::new(scopes.iter().map(|s| s.to_string()).collect()),
        })
    }
}

impl std::fmt::Debug for GcpAuthTokenSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcpAuthTokenSource")
            .field("scopes", &self.scopes)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl TokenSource for GcpAuthTokenSource {
    async fn token(&self) -> anyhow::Result<String> {
        let scopes: Vec<&str> = self.scopes.iter().map(String::as_str).collect();
        let token = self
            .provider
            .token(&scopes)
            .await
            .map_err(|e| anyhow::anyhow!("gcp_auth token: {e}"))?;
        Ok(token.as_str().to_string())
    }
}

#[cfg(test)]
#[path = "../tests/unit/auth__tests.rs"]
mod tests;
