//! Opt-in hosted object storage implementations for Ironbeam's `ObjectIO`.
//!
//! The adapter uses the same provider-neutral API for S3, GCS, and Azure Blob.
//! Cloud clients are built lazily per bucket/container from their environment
//! credentials. The underlying async client runs on a dedicated runtime so
//! synchronous Ironbeam sources can call it from a runner thread.

use futures_util::TryStreamExt;
use ironbeam_cloud::{
    CloudIOError, CloudResult, ErrorKind, ObjectIO, ObjectMetadata, ObjectStores,
};
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, path::Path};
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, mpsc};

type Factory = dyn Fn(&str) -> CloudResult<Arc<dyn ObjectStore>> + Send + Sync;

/// Build one URI router for every enabled hosted provider.
///
/// Enable `aws`, `gcp`, or `azure` on this crate. The router recognizes
/// `s3://`, `gs://`, and `az://` respectively. Clients are created lazily for
/// each bucket or container, so construction does not require credentials.
/// A caller can also register a local or custom provider on the returned
/// [`ObjectStores`] before using it.
///
/// # Errors
/// Returns an error if a provider's dedicated request runtime cannot start.
pub fn object_stores_from_env() -> CloudResult<ObjectStores> {
    let stores = ObjectStores::new();
    #[cfg(any(feature = "aws", feature = "gcp", feature = "azure"))]
    let mut stores = stores;
    #[cfg(feature = "aws")]
    stores.register("s3", RemoteObjectIO::s3_from_env()?)?;
    #[cfg(feature = "gcp")]
    stores.register("gs", RemoteObjectIO::gcs_from_env()?)?;
    #[cfg(feature = "azure")]
    stores.register("az", RemoteObjectIO::azure_from_env()?)?;
    Ok(stores)
}

/// One provider family, with clients cached separately for each bucket.
pub struct RemoteObjectIO {
    factory: Arc<Factory>,
    buckets: Mutex<HashMap<String, Arc<dyn ObjectStore>>>,
    runtime: tokio::runtime::Runtime,
}

impl RemoteObjectIO {
    /// Adapt any `object_store` provider family to Ironbeam's `ObjectIO`.
    /// The factory receives a bucket or container name and is called once per
    /// name on first use. This also supports custom S3-compatible stores.
    ///
    /// # Errors
    /// Returns an error if the dedicated client runtime cannot start.
    pub fn new(
        factory: impl Fn(&str) -> CloudResult<Arc<dyn ObjectStore>> + Send + Sync + 'static,
    ) -> CloudResult<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|error| CloudIOError::new(ErrorKind::InternalError, error.to_string()))?;
        Ok(Self {
            factory: Arc::new(factory),
            buckets: Mutex::new(HashMap::new()),
            runtime,
        })
    }

    /// Build S3 clients for each bucket using the normal AWS environment and
    /// credential chain. Requires the `aws` feature.
    #[cfg(feature = "aws")]
    /// # Errors
    /// Returns an error if the runtime cannot start; bucket configuration
    /// errors surface on the first operation for that bucket.
    pub fn s3_from_env() -> CloudResult<Self> {
        Self::new(|bucket| {
            object_store::aws::AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()
                .map(|store| Arc::new(store) as Arc<dyn ObjectStore>)
                .map_err(store_error)
        })
    }

    /// Build GCS clients for each bucket using Application Default Credentials.
    /// Requires the `gcp` feature.
    #[cfg(feature = "gcp")]
    /// # Errors
    /// Returns an error if the runtime cannot start; bucket configuration
    /// errors surface on the first operation for that bucket.
    pub fn gcs_from_env() -> CloudResult<Self> {
        Self::new(|bucket| {
            object_store::gcp::GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(bucket)
                .build()
                .map(|store| Arc::new(store) as Arc<dyn ObjectStore>)
                .map_err(store_error)
        })
    }

    /// Build Azure Blob clients for each container using Azure environment
    /// credentials. Requires the `azure` feature.
    #[cfg(feature = "azure")]
    /// # Errors
    /// Returns an error if the runtime cannot start; container configuration
    /// errors surface on the first operation for that container.
    pub fn azure_from_env() -> CloudResult<Self> {
        Self::new(|container| {
            object_store::azure::MicrosoftAzureBuilder::from_env()
                .with_container_name(container)
                .build()
                .map(|store| Arc::new(store) as Arc<dyn ObjectStore>)
                .map_err(store_error)
        })
    }

    fn store(&self, bucket: &str) -> CloudResult<Arc<dyn ObjectStore>> {
        if bucket.is_empty() || bucket.contains('/') {
            return Err(CloudIOError::new(
                ErrorKind::InvalidInput,
                "invalid bucket name",
            ));
        }
        let mut buckets = self.buckets.lock().map_err(|_| {
            CloudIOError::new(
                ErrorKind::InternalError,
                "object client cache lock poisoned",
            )
        })?;
        if let Some(store) = buckets.get(bucket) {
            return Ok(Arc::clone(store));
        }
        let store = (self.factory)(bucket)?;
        buckets.insert(bucket.to_owned(), Arc::clone(&store));
        drop(buckets);
        Ok(store)
    }

    fn run<T: Send + 'static>(
        &self,
        operation: impl Future<Output = object_store::Result<T>> + Send + 'static,
    ) -> CloudResult<T> {
        let (send, receive) = mpsc::sync_channel(1);
        self.runtime.spawn(async move {
            let _ = send.send(operation.await);
        });
        receive
            .recv()
            .map_err(|_| {
                CloudIOError::new(
                    ErrorKind::InternalError,
                    "object request task ended unexpectedly",
                )
            })?
            .map_err(store_error)
    }
}

#[allow(clippy::needless_pass_by_value)] // map_err passes the owned provider error.
fn store_error(error: object_store::Error) -> CloudIOError {
    let kind = match error {
        object_store::Error::NotFound { .. } => ErrorKind::NotFound,
        object_store::Error::AlreadyExists { .. } => ErrorKind::AlreadyExists,
        object_store::Error::PermissionDenied { .. } => ErrorKind::Authorization,
        object_store::Error::Unauthenticated { .. } => ErrorKind::Authentication,
        object_store::Error::InvalidPath { .. } => ErrorKind::InvalidInput,
        _ => ErrorKind::Other,
    };
    CloudIOError::new(kind, error.to_string())
}

fn metadata(meta: ObjectMeta) -> ObjectMetadata {
    ObjectMetadata {
        key: meta.location.to_string(),
        size: meta.size,
        content_type: None,
        last_modified: Some(meta.last_modified.timestamp()),
        etag: meta.e_tag,
        custom_metadata: HashMap::new(),
    }
}

impl ObjectIO for RemoteObjectIO {
    fn put_object(&self, bucket: &str, key: &str, data: &[u8]) -> CloudResult<()> {
        let store = self.store(bucket)?;
        let path = Path::parse(key)
            .map_err(|error| CloudIOError::new(ErrorKind::InvalidInput, error.to_string()))?;
        let bytes = data.to_vec();
        self.run(async move { store.put(&path, bytes.into()).await.map(|_| ()) })
    }

    fn get_object(&self, bucket: &str, key: &str) -> CloudResult<Vec<u8>> {
        let store = self.store(bucket)?;
        let path = Path::parse(key)
            .map_err(|error| CloudIOError::new(ErrorKind::InvalidInput, error.to_string()))?;
        self.run(async move { Ok(store.get(&path).await?.bytes().await?.to_vec()) })
    }

    fn delete_object(&self, bucket: &str, key: &str) -> CloudResult<()> {
        let store = self.store(bucket)?;
        let path = Path::parse(key)
            .map_err(|error| CloudIOError::new(ErrorKind::InvalidInput, error.to_string()))?;
        self.run(async move { store.delete(&path).await })
    }

    fn list_objects(&self, bucket: &str, prefix: Option<&str>) -> CloudResult<Vec<ObjectMetadata>> {
        let store = self.store(bucket)?;
        let prefix = prefix.unwrap_or("").to_owned();
        // object_store's prefix is segment-based; query the parent segment,
        // then apply ObjectIO's byte-prefix contract locally.
        let parent = prefix.rsplit_once('/').map(|(path, _)| path.to_owned());
        let parent = parent
            .filter(|path| !path.is_empty())
            .map(|path| {
                Path::parse(path)
                    .map_err(|error| CloudIOError::new(ErrorKind::InvalidInput, error.to_string()))
            })
            .transpose()?;
        self.run(async move {
            let mut objects: Vec<_> = store.list(parent.as_ref()).try_collect().await?;
            objects.retain(|meta: &ObjectMeta| meta.location.as_ref().starts_with(&prefix));
            objects.sort_by(|a, b| a.location.cmp(&b.location));
            Ok(objects.into_iter().map(metadata).collect())
        })
    }

    fn object_exists(&self, bucket: &str, key: &str) -> CloudResult<bool> {
        match self.get_metadata(bucket, key) {
            Ok(_) => Ok(true),
            Err(error) if error.kind == ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn get_metadata(&self, bucket: &str, key: &str) -> CloudResult<ObjectMetadata> {
        let store = self.store(bucket)?;
        let path = Path::parse(key)
            .map_err(|error| CloudIOError::new(ErrorKind::InvalidInput, error.to_string()))?;
        self.run(async move { store.head(&path).await.map(metadata) })
    }

    fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
    ) -> CloudResult<()> {
        let source = self.store(src_bucket)?;
        let destination = self.store(dst_bucket)?;
        let src_path = Path::parse(src_key)
            .map_err(|error| CloudIOError::new(ErrorKind::InvalidInput, error.to_string()))?;
        let dst_path = Path::parse(dst_key)
            .map_err(|error| CloudIOError::new(ErrorKind::InvalidInput, error.to_string()))?;
        if src_bucket == dst_bucket {
            self.run(async move { source.copy(&src_path, &dst_path).await })
        } else {
            self.run(async move {
                let bytes = source.get(&src_path).await?.bytes().await?;
                destination.put(&dst_path, bytes.into()).await.map(|_| ())
            })
        }
    }
}
