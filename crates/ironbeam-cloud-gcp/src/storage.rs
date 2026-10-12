use crate::{GcpError, GcpProject};
use google_cloud_storage::client::Storage;

/// Google Cloud Storage operations using the official Rust client.
///
/// `read_bytes` holds the result in memory. Use [`client`](Self::client) for
/// large objects and streaming reads or writes.
#[derive(Clone)]
pub struct GcpStorage {
    client: Storage,
}

impl GcpProject {
    /// Connect to Cloud Storage with Application Default Credentials.
    pub async fn storage(&self) -> Result<GcpStorage, GcpError> {
        GcpStorage::connect().await
    }
}

impl GcpStorage {
    /// Create a Storage client using Application Default Credentials.
    pub async fn connect() -> Result<Self, GcpError> {
        Ok(Self {
            client: Storage::builder().build().await.map_err(GcpError::client)?,
        })
    }

    /// Access the official client for streaming, preconditions, and metadata.
    #[must_use]
    pub fn client(&self) -> &Storage {
        &self.client
    }

    /// Read one object into memory.
    pub async fn read_bytes(&self, bucket: &str, object: &str) -> Result<Vec<u8>, GcpError> {
        let bucket = GcpProject::bucket(bucket)?;
        if object.is_empty() {
            return Err(GcpError::InvalidResource("empty object name".into()));
        }
        let mut response = self
            .client
            .read_object(bucket, object)
            .send()
            .await
            .map_err(GcpError::client)?;
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .next()
            .await
            .transpose()
            .map_err(GcpError::client)?
        {
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }

    /// Create an object only if no current generation exists at its name.
    pub async fn write_new(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
    ) -> Result<(), GcpError> {
        let bucket = GcpProject::bucket(bucket)?;
        if object.is_empty() {
            return Err(GcpError::InvalidResource("empty object name".into()));
        }
        self.client
            .write_object(bucket, object, bytes::Bytes::from(data))
            .set_if_generation_match(0)
            .send_buffered()
            .await
            .map_err(GcpError::client)?;
        Ok(())
    }
}
