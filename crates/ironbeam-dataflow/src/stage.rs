//! Upload worker binary + `pipeline.pb` to GCS.
//!
//! Abstracts over the storage backend behind a [`Stager`] trait so tests can
//! use a purely in-memory implementation and production code can hit the real
//! GCS JSON API via [`GcsStager`].

use std::path::Path;
use std::sync::{Arc, Mutex};

use sha2::{Digest, Sha256};

use crate::artifact::StagedArtifact;
use crate::auth::TokenSource;

#[async_trait::async_trait]
/// Uploads worker and pipeline artifacts for a Dataflow job.
pub trait Stager: Send + Sync {
    /// Upload bytes under `key` and return their staged artifact location.
    async fn put(&self, key: &str, bytes: Vec<u8>) -> anyhow::Result<StagedArtifact>;

    /// Upload a local file under `key` and return its staged artifact location.
    async fn put_file(&self, key: &str, path: &Path) -> anyhow::Result<StagedArtifact> {
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|e| anyhow::anyhow!("reading {} for upload: {e}", path.display()))?;
        self.put(key, bytes).await
    }
}

#[derive(Debug)]
/// Uploads Dataflow artifacts to a Cloud Storage bucket.
pub struct GcsStager<T: TokenSource> {
    base_url: String,
    bucket: String,
    prefix: String,
    http: reqwest::Client,
    token_source: T,
}

impl<T: TokenSource> GcsStager<T> {
    /// Build a stager from a `gs://bucket[/prefix]` URL.
    pub fn new(staging_location: &str, token_source: T) -> anyhow::Result<Self> {
        let (bucket, prefix) = parse_gs_url(staging_location)?;
        Ok(Self {
            base_url: "https://storage.googleapis.com".to_string(),
            bucket,
            prefix,
            http: reqwest::Client::new(),
            token_source,
        })
    }

    /// Override the HTTP base URL — used by tests hitting a wiremock server.
    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }

    fn object_name(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.prefix, key)
        }
    }
}

/// Split a `gs://bucket[/prefix]` staging URL into bucket and object prefix.
pub fn parse_gs_url(url: &str) -> anyhow::Result<(String, String)> {
    let rest = url
        .strip_prefix("gs://")
        .ok_or_else(|| anyhow::anyhow!("staging location must start with gs://; got {url}"))?;
    let mut parts = rest.splitn(2, '/');
    let bucket = parts
        .next()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow::anyhow!("missing bucket name in {url}"))?
        .to_string();
    let prefix = parts.next().unwrap_or("").trim_end_matches('/').to_string();
    Ok((bucket, prefix))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

#[async_trait::async_trait]
impl<T: TokenSource> Stager for GcsStager<T> {
    async fn put(&self, key: &str, bytes: Vec<u8>) -> anyhow::Result<StagedArtifact> {
        let token = self.token_source.token().await?;
        let object = self.object_name(key);
        // GCS JSON API simple upload:
        // POST /upload/storage/v1/b/{bucket}/o?uploadType=media&name={urlenc(name)}
        let mut url = reqwest::Url::parse(&format!(
            "{}/upload/storage/v1/b/{}/o",
            self.base_url, self.bucket
        ))?;
        url.query_pairs_mut()
            .append_pair("uploadType", "media")
            .append_pair("name", &object);
        let sha = sha256_hex(&bytes);
        let resp = self
            .http
            .post(url)
            .bearer_auth(token)
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(bytes)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "GCS upload to {} failed: {} — {}",
                object,
                status,
                body
            ));
        }
        Ok(StagedArtifact {
            url: format!("gs://{}/{}", self.bucket, object),
            sha256: sha,
        })
    }
}

/// In-memory stager. Records every upload in a shared map; returns fake
/// `gs://` URLs so downstream code paths can round-trip through the artifact
/// rewrite without any network.
type StoredUpload = (String, Vec<u8>);

#[derive(Debug, Default, Clone)]
/// Keeps staged artifacts in memory for tests.
pub struct MemoryStager {
    bucket: String,
    inner: Arc<Mutex<Vec<StoredUpload>>>,
}

impl MemoryStager {
    /// Create an in-memory stager for local tests.
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            inner: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Return a snapshot of staged object names and bytes.
    pub fn stored(&self) -> Vec<(String, Vec<u8>)> {
        self.inner.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl Stager for MemoryStager {
    async fn put(&self, key: &str, bytes: Vec<u8>) -> anyhow::Result<StagedArtifact> {
        let sha = sha256_hex(&bytes);
        self.inner.lock().unwrap().push((key.to_string(), bytes));
        Ok(StagedArtifact {
            url: format!("gs://{}/{}", self.bucket, key),
            sha256: sha,
        })
    }
}

#[cfg(test)]
#[path = "../tests/unit/stage__tests.rs"]
mod tests;
