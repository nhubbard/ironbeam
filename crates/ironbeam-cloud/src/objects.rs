//! URI-based object access with pluggable providers and a usable local backend.
//!
//! Register each provider once, then use the same operations for `gs://`,
//! `s3://`, or local `file://` resources. The scheme selects an [`ObjectIO`]
//! implementation; this module does not embed provider credentials.

use crate::{CloudIOError, CloudResult, ErrorKind, ObjectIO, ObjectMetadata};
use std::collections::HashMap;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

/// A bucket and object name with its provider scheme.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectLocation {
    pub scheme: String,
    pub bucket: String,
    pub key: String,
}

impl ObjectLocation {
    /// Parse `scheme://bucket/key`. An empty key is valid for listing a bucket.
    ///
    /// # Errors
    /// Returns an error for an invalid scheme or missing bucket.
    pub fn parse(uri: &str) -> CloudResult<Self> {
        let (scheme, rest) = uri
            .split_once("://")
            .ok_or_else(|| invalid("missing ://"))?;
        if scheme.is_empty()
            || !scheme.bytes().enumerate().all(|(i, byte)| {
                byte.is_ascii_lowercase()
                    || (i > 0 && (byte.is_ascii_digit() || matches!(byte, b'+' | b'-' | b'.')))
            })
        {
            return Err(invalid("invalid object scheme"));
        }
        let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
        if bucket.is_empty() || bucket == "." || bucket == ".." || bucket.contains(['\\', '?', '#'])
        {
            return Err(invalid("invalid object bucket"));
        }
        Ok(Self {
            scheme: scheme.to_owned(),
            bucket: bucket.to_owned(),
            key: key.to_owned(),
        })
    }

    /// Return a URI for this location.
    #[must_use]
    pub fn uri(&self) -> String {
        format!("{}://{}/{}", self.scheme, self.bucket, self.key)
    }
}

fn invalid(message: &str) -> CloudIOError {
    CloudIOError::new(ErrorKind::InvalidInput, message)
}

/// Route object operations by URI scheme. One registry can span providers.
#[derive(Default)]
pub struct ObjectStores {
    providers: HashMap<String, Arc<dyn ObjectIO>>,
}

impl ObjectStores {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a provider implementation under a URI scheme.
    ///
    /// # Errors
    /// Returns an error for an invalid scheme or duplicate registration.
    pub fn register(&mut self, scheme: &str, provider: impl ObjectIO + 'static) -> CloudResult<()> {
        ObjectLocation::parse(&format!("{scheme}://validation"))?;
        if self.providers.contains_key(scheme) {
            return Err(CloudIOError::new(
                ErrorKind::AlreadyExists,
                format!("provider {scheme} is already registered"),
            ));
        }
        self.providers.insert(scheme.to_owned(), Arc::new(provider));
        Ok(())
    }

    /// Get a registered provider for format readers or graph adapters.
    ///
    /// # Errors
    /// Returns an error if the scheme is not registered.
    pub fn provider(&self, scheme: &str) -> CloudResult<Arc<dyn ObjectIO>> {
        self.providers.get(scheme).cloned().ok_or_else(|| {
            CloudIOError::new(
                ErrorKind::InvalidInput,
                format!("no object provider registered for {scheme}"),
            )
        })
    }

    fn resolve(&self, uri: &str) -> CloudResult<(ObjectLocation, &dyn ObjectIO)> {
        let location = ObjectLocation::parse(uri)?;
        let provider = self.providers.get(&location.scheme).ok_or_else(|| {
            CloudIOError::new(
                ErrorKind::InvalidInput,
                format!("no object provider registered for {}", location.scheme),
            )
        })?;
        Ok((location, provider.as_ref()))
    }

    /// Read one object.
    /// # Errors
    /// Returns a parse, provider, or object read error.
    pub fn read(&self, uri: &str) -> CloudResult<Vec<u8>> {
        let (loc, store) = self.resolve(uri)?;
        store.get_object(&loc.bucket, &loc.key)
    }

    /// Write one object.
    /// # Errors
    /// Returns a parse, provider, or object write error.
    pub fn write(&self, uri: &str, bytes: &[u8]) -> CloudResult<()> {
        let (loc, store) = self.resolve(uri)?;
        store.put_object(&loc.bucket, &loc.key, bytes)
    }

    /// Delete one object.
    /// # Errors
    /// Returns a parse, provider, or object deletion error.
    pub fn delete(&self, uri: &str) -> CloudResult<()> {
        let (loc, store) = self.resolve(uri)?;
        store.delete_object(&loc.bucket, &loc.key)
    }

    /// Check whether an object exists.
    /// # Errors
    /// Returns a parse or provider error.
    pub fn exists(&self, uri: &str) -> CloudResult<bool> {
        let (loc, store) = self.resolve(uri)?;
        store.object_exists(&loc.bucket, &loc.key)
    }

    /// Read object metadata.
    /// # Errors
    /// Returns a parse, provider, or object metadata error.
    pub fn metadata(&self, uri: &str) -> CloudResult<ObjectMetadata> {
        let (loc, store) = self.resolve(uri)?;
        store.get_metadata(&loc.bucket, &loc.key)
    }

    /// List objects under a URI prefix, returning complete locations.
    /// # Errors
    /// Returns a parse, provider, or listing error.
    pub fn list(&self, uri_prefix: &str) -> CloudResult<Vec<(ObjectLocation, ObjectMetadata)>> {
        let (loc, store) = self.resolve(uri_prefix)?;
        let objects = store.list_objects(&loc.bucket, Some(&loc.key))?;
        Ok(objects
            .into_iter()
            .map(|meta| {
                let object_loc = ObjectLocation {
                    key: meta.key.clone(),
                    ..loc.clone()
                };
                (object_loc, meta)
            })
            .collect())
    }

    /// Copy an object within one provider or between registered providers.
    /// # Errors
    /// Returns a parse, provider, read, or write error.
    pub fn copy(&self, source: &str, destination: &str) -> CloudResult<()> {
        let (src, src_store) = self.resolve(source)?;
        let (dst, dst_store) = self.resolve(destination)?;
        if src.scheme == dst.scheme {
            src_store.copy_object(&src.bucket, &src.key, &dst.bucket, &dst.key)
        } else {
            let bytes = src_store.get_object(&src.bucket, &src.key)?;
            dst_store.put_object(&dst.bucket, &dst.key, &bytes)
        }
    }
}

impl<T: ObjectIO + ?Sized> ObjectIO for Arc<T> {
    fn put_object(&self, bucket: &str, key: &str, data: &[u8]) -> CloudResult<()> {
        (**self).put_object(bucket, key, data)
    }
    fn get_object(&self, bucket: &str, key: &str) -> CloudResult<Vec<u8>> {
        (**self).get_object(bucket, key)
    }
    fn delete_object(&self, bucket: &str, key: &str) -> CloudResult<()> {
        (**self).delete_object(bucket, key)
    }
    fn list_objects(&self, bucket: &str, prefix: Option<&str>) -> CloudResult<Vec<ObjectMetadata>> {
        (**self).list_objects(bucket, prefix)
    }
    fn object_exists(&self, bucket: &str, key: &str) -> CloudResult<bool> {
        (**self).object_exists(bucket, key)
    }
    fn get_metadata(&self, bucket: &str, key: &str) -> CloudResult<ObjectMetadata> {
        (**self).get_metadata(bucket, key)
    }
    fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
    ) -> CloudResult<()> {
        (**self).copy_object(src_bucket, src_key, dst_bucket, dst_key)
    }
}

/// Filesystem-backed object storage for local jobs and tests.
///
/// Buckets are directories below `root`. Reads and writes reject paths that
/// escape that root, including existing symlinks.
#[derive(Clone, Debug)]
pub struct LocalObjectIO {
    root: PathBuf,
}

impl LocalObjectIO {
    /// Create a local store rooted at `root`.
    /// # Errors
    /// Returns a filesystem error if the root cannot be created or resolved.
    pub fn new(root: impl AsRef<Path>) -> CloudResult<Self> {
        fs::create_dir_all(root.as_ref()).map_err(fs_error)?;
        Ok(Self {
            root: root.as_ref().canonicalize().map_err(fs_error)?,
        })
    }

    fn path(&self, bucket: &str, key: &str) -> CloudResult<PathBuf> {
        if bucket.is_empty() || bucket == "." || bucket == ".." || bucket.contains(['/', '\\']) {
            return Err(invalid("invalid bucket name"));
        }
        let key_path = Path::new(key);
        if key_path
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
            || key.contains('\\')
        {
            return Err(invalid(
                "object key must be a relative path without traversal",
            ));
        }
        Ok(self.root.join(bucket).join(key_path))
    }

    fn checked_existing(&self, path: &Path) -> CloudResult<PathBuf> {
        let canonical = path.canonicalize().map_err(fs_error)?;
        if !canonical.starts_with(&self.root) {
            return Err(invalid("object path escapes storage root"));
        }
        Ok(canonical)
    }

    fn checked_parent(&self, path: &Path) -> CloudResult<()> {
        let parent = path
            .parent()
            .ok_or_else(|| invalid("object has no parent"))?;
        fs::create_dir_all(parent).map_err(fs_error)?;
        self.checked_existing(parent)?;
        if path.exists() {
            self.checked_existing(path)?;
        }
        Ok(())
    }

    fn metadata_for(&self, bucket: &str, key: &str) -> CloudResult<ObjectMetadata> {
        let path = self.path(bucket, key)?;
        let path = self.checked_existing(&path)?;
        let metadata = fs::metadata(path).map_err(fs_error)?;
        if !metadata.is_file() {
            return Err(invalid("object path is not a file"));
        }
        Ok(ObjectMetadata {
            key: key.to_owned(),
            size: metadata.len(),
            content_type: None,
            last_modified: metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                .and_then(|duration| i64::try_from(duration.as_secs()).ok()),
            etag: None,
            custom_metadata: HashMap::new(),
        })
    }
}

#[allow(clippy::needless_pass_by_value)] // map_err passes the owned I/O error.
fn fs_error(error: std::io::Error) -> CloudIOError {
    let kind = match error.kind() {
        std::io::ErrorKind::NotFound => ErrorKind::NotFound,
        std::io::ErrorKind::PermissionDenied => ErrorKind::Authorization,
        _ => ErrorKind::Other,
    };
    CloudIOError::new(kind, error.to_string())
}

impl ObjectIO for LocalObjectIO {
    fn put_object(&self, bucket: &str, key: &str, data: &[u8]) -> CloudResult<()> {
        let path = self.path(bucket, key)?;
        self.checked_parent(&path)?;
        fs::write(path, data).map_err(fs_error)
    }

    fn get_object(&self, bucket: &str, key: &str) -> CloudResult<Vec<u8>> {
        let path = self.path(bucket, key)?;
        fs::read(self.checked_existing(&path)?).map_err(fs_error)
    }

    fn delete_object(&self, bucket: &str, key: &str) -> CloudResult<()> {
        let path = self.path(bucket, key)?;
        fs::remove_file(self.checked_existing(&path)?).map_err(fs_error)
    }

    fn list_objects(&self, bucket: &str, prefix: Option<&str>) -> CloudResult<Vec<ObjectMetadata>> {
        let bucket_dir = self.path(bucket, "")?;
        if !bucket_dir.exists() {
            return Err(CloudIOError::new(ErrorKind::NotFound, "bucket not found"));
        }
        let mut pending = vec![self.checked_existing(&bucket_dir)?];
        let mut objects = Vec::new();
        while let Some(dir) = pending.pop() {
            for entry in fs::read_dir(dir).map_err(fs_error)? {
                let entry = entry.map_err(fs_error)?;
                let ty = entry.file_type().map_err(fs_error)?;
                if ty.is_symlink() {
                    continue;
                }
                if ty.is_dir() {
                    pending.push(entry.path());
                } else if ty.is_file() {
                    let relative = entry
                        .path()
                        .strip_prefix(&bucket_dir)
                        .map_err(|_| invalid("invalid object path"))?
                        .to_string_lossy()
                        .replace('\\', "/");
                    if prefix.is_none_or(|p| relative.starts_with(p)) {
                        objects.push(self.metadata_for(bucket, &relative)?);
                    }
                }
            }
        }
        objects.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(objects)
    }

    fn object_exists(&self, bucket: &str, key: &str) -> CloudResult<bool> {
        match self.get_metadata(bucket, key) {
            Ok(_) => Ok(true),
            Err(error) if error.kind == ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn get_metadata(&self, bucket: &str, key: &str) -> CloudResult<ObjectMetadata> {
        self.metadata_for(bucket, key)
    }

    fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
    ) -> CloudResult<()> {
        let bytes = self.get_object(src_bucket, src_key)?;
        self.put_object(dst_bucket, dst_key, &bytes)
    }
}
