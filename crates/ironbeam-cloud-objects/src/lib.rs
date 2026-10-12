//! Provider-neutral object storage contract and URI routing.
use std::collections::HashMap;
use std::error::Error;
use std::fmt;

// ============================================================================
// Core Error Type
// ============================================================================

/// Generic error type for cloud IO operations
#[derive(Debug, Clone)]
pub struct CloudIOError {
    pub message: String,
    pub kind: ErrorKind,
    pub source: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorKind {
    Authentication,
    Authorization,
    NotFound,
    AlreadyExists,
    InvalidInput,
    Network,
    Timeout,
    ServiceUnavailable,
    RateLimited,
    InternalError,
    Other,
}

impl fmt::Display for CloudIOError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl Error for CloudIOError {}

impl CloudIOError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind,
            source: None,
        }
    }

    #[must_use]
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }
}

pub type CloudResult<T> = Result<T, CloudIOError>;

// ============================================================================
// ObjectIO - Object Storage
// ============================================================================

/// Metadata for an object in storage
#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    pub key: String,
    pub size: u64,
    pub content_type: Option<String>,
    pub last_modified: Option<i64>, // Unix timestamp
    pub etag: Option<String>,
    pub custom_metadata: HashMap<String, String>,
}

/// Trait for object storage operations
pub trait ObjectIO: Send + Sync {
    /// Upload data to object storage
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket doesn't exist, permissions are not enough, or the upload fails
    fn put_object(&self, bucket: &str, key: &str, data: &[u8]) -> CloudResult<()>;

    /// Atomically create an immutable object, failing with `AlreadyExists`
    /// if the key is present. Providers must not expose partial bytes, and
    /// successful writes must be durable before returning. The default fails
    /// closed for providers that have not implemented this contract.
    ///
    /// # Errors
    ///
    /// Returns an error when the object already exists, the provider cannot
    /// guarantee atomic creation, or the write fails.
    fn put_object_if_absent(&self, _bucket: &str, _key: &str, _data: &[u8]) -> CloudResult<()> {
        Err(CloudIOError::new(
            ErrorKind::InvalidInput,
            "provider does not support atomic conditional object creation",
        ))
    }

    /// Atomically create an immutable object from a reader without holding the
    /// entire object in memory. Providers must fail closed when unsupported.
    fn put_object_if_absent_from(
        &self,
        _bucket: &str,
        _key: &str,
        _reader: &mut dyn std::io::Read,
    ) -> CloudResult<()> {
        Err(CloudIOError::new(
            ErrorKind::InvalidInput,
            "provider does not support streaming conditional object creation",
        ))
    }

    /// Open an object for a bounded-memory sequential read. Providers must
    /// fail closed when unsupported.
    fn open_object(&self, _bucket: &str, _key: &str) -> CloudResult<Box<dyn std::io::Read + Send>> {
        Err(CloudIOError::new(
            ErrorKind::InvalidInput,
            "provider does not support streaming object reads",
        ))
    }

    /// Download data from object storage
    ///
    /// # Errors
    ///
    /// Returns an error if the object doesn't exist, permissions are not enough, or the download fails
    fn get_object(&self, bucket: &str, key: &str) -> CloudResult<Vec<u8>>;

    /// Delete an object
    ///
    /// # Errors
    ///
    /// Returns an error if the object doesn't exist, permissions are not enough, or the deletion fails
    fn delete_object(&self, bucket: &str, key: &str) -> CloudResult<()>;

    /// List objects with a prefix. Final-result publication relies on this
    /// listing converging with completed conditional writes and deletes; a
    /// stale listing can delay publication, while fragment checksums and the
    /// completed attempt manifest protect the published bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket doesn't exist, permissions are not enough, or the listing fails
    fn list_objects(&self, bucket: &str, prefix: Option<&str>) -> CloudResult<Vec<ObjectMetadata>>;

    /// Check if an object exists
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket doesn't exist, permissions are not enough, or the check fails
    fn object_exists(&self, bucket: &str, key: &str) -> CloudResult<bool>;

    /// Get object metadata without downloading content
    ///
    /// # Errors
    ///
    /// Returns an error if the object doesn't exist, permissions are not enough, or the operation fails
    fn get_metadata(&self, bucket: &str, key: &str) -> CloudResult<ObjectMetadata>;

    /// Copy an object within or between buckets
    ///
    /// # Errors
    ///
    /// Returns an error if the source doesn't exist, permissions are not enough, or the copy fails
    fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
    ) -> CloudResult<()>;
}

pub mod objects;
pub use objects::{LocalObjectIO, ObjectLocation, ObjectStores};
