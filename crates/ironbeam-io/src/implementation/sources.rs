use ironbeam_core::{
    Element, Error, PCollection, Pipeline, Replay, Sink, SinkMode, SinkSpec, SourceProgress,
    TypedSink, TypedSourceIterator,
};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

#[allow(clippy::redundant_pub_crate)] // Used by the parent crate but not part of the public facade.
pub(super) fn sync_directory(path: &Path, key: &str) -> Result<(), Error> {
    fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io(key, error))
}

#[allow(clippy::redundant_pub_crate)] // Used by the parent crate but not part of the public facade.
pub(super) fn create_dir_all_durable(path: &Path, key: &str) -> Result<(), Error> {
    let path = if path.as_os_str().is_empty() {
        Path::new(".")
    } else {
        path
    };
    let mut missing = Vec::new();
    let mut current = path;
    while !current.exists() {
        missing.push(current.to_path_buf());
        current = current
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
    }
    fs::create_dir_all(path).map_err(|error| io(key, error))?;
    for directory in missing.into_iter().rev() {
        let parent = directory
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        sync_directory(parent, key)?;
    }
    Ok(())
}

#[doc(hidden)]
#[allow(clippy::missing_const_for_fn, clippy::unnecessary_wraps)]
// Without compression this is an identity transform; keep the same fallible API for all builds.
pub fn decode_source_bytes(bytes: Vec<u8>, path: &Path, key: &str) -> Result<Vec<u8>, Error> {
    #[cfg(any(
        feature = "compression-gzip",
        feature = "compression-zstd",
        feature = "compression-bzip2",
        feature = "compression-xz"
    ))]
    {
        use std::io::Read;
        let mut reader =
            ironbeam_compression::auto_detect_reader(std::io::Cursor::new(bytes), path)
                .map_err(|e| io(key, e))?;
        let mut decoded = Vec::new();
        reader.read_to_end(&mut decoded).map_err(|e| io(key, e))?;
        Ok(decoded)
    }
    #[cfg(not(any(
        feature = "compression-gzip",
        feature = "compression-zstd",
        feature = "compression-bzip2",
        feature = "compression-xz"
    )))]
    {
        let _ = (path, key);
        Ok(bytes)
    }
}

#[cfg(any(
    feature = "compression-gzip",
    feature = "compression-zstd",
    feature = "compression-bzip2",
    feature = "compression-xz"
))]
fn compressed_extension(path: &Path) -> bool {
    path.extension()
        .and_then(|value| value.to_str())
        .is_some_and(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "gz" | "gzip" | "zst" | "zstd" | "bz2" | "xz"
            )
        })
}

#[doc(hidden)]
pub fn open_encoded_output(path: &Path, key: &str) -> Result<Box<dyn std::io::Write>, Error> {
    let file = fs::File::create(path).map_err(|e| io(key, e))?;
    #[cfg(any(
        feature = "compression-gzip",
        feature = "compression-zstd",
        feature = "compression-bzip2",
        feature = "compression-xz"
    ))]
    {
        ironbeam_compression::auto_detect_writer(file, path).map_err(|e| io(key, e))
    }
    #[cfg(not(any(
        feature = "compression-gzip",
        feature = "compression-zstd",
        feature = "compression-bzip2",
        feature = "compression-xz"
    )))]
    {
        Ok(Box::new(file))
    }
}

#[doc(hidden)]
pub fn open_lazy_input(path: &Path, key: &str) -> Result<Box<dyn std::io::Read + Send>, Error> {
    let file = fs::File::open(path).map_err(|e| io(key, e))?;
    #[cfg(any(
        feature = "compression-gzip",
        feature = "compression-zstd",
        feature = "compression-bzip2",
        feature = "compression-xz"
    ))]
    if compressed_extension(path) {
        return ironbeam_compression::auto_detect_reader(file, path).map_err(|e| io(key, e));
    }
    Ok(Box::new(file))
}

type FileWriter<T> = dyn Fn(&Path, &[T]) -> Result<(), Error> + Send + Sync;

#[cfg(feature = "cloud")]
pub use ironbeam_cloud_objects as cloud;

#[doc(hidden)]
pub fn io(key: &str, error: impl std::fmt::Display) -> Error {
    Error::io(key, "file", error.to_string())
}
#[doc(hidden)]
pub fn source<T: Element>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    format: &str,
    read: impl Fn(&[u8]) -> Result<Vec<T>, Error> + Send + Sync + 'static,
) -> Result<PCollection<T>, Error> {
    let path = path.as_ref().to_path_buf();
    let bytes = fs::read(&path).map_err(|e| io(key, e))?;
    let expected = Sha256::digest(&bytes);
    let hex = expected.iter().fold(String::new(), |mut output, byte| {
        let _ = write!(output, "{byte:02x}");
        output
    });
    let digest = format!("{}:{}:{}", format, path.display(), hex);
    let name = key.to_owned();
    pipeline.bounded_source_with_semantic(
        key,
        &format!("ironbeam/io/{format}-source"),
        SourceProgress {
            stable_split_id: true,
            replay: Replay::AtLeastOnce,
            reports_event_time: false,
        },
        digest,
        None,
        move || {
            let current = fs::read(&path).map_err(|e| io(&name, e))?;
            if Sha256::digest(&current) != expected {
                return Err(Error::recovery(
                    &name,
                    "source contents changed after graph construction",
                ));
            }
            let decoded = decode_source_bytes(current, &path, &name)?;
            let values = read(&decoded)?;
            Ok(Box::new(values.into_iter().map(Ok)) as TypedSourceIterator<T>)
        },
    )
}

#[cfg(feature = "file")]
#[doc(hidden)]
pub fn file_hash(path: &Path, key: &str) -> Result<Vec<u8>, Error> {
    use std::io::Read;
    let mut file = fs::File::open(path).map_err(|e| io(key, e))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|e| io(key, e))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_vec())
}

#[cfg(feature = "file")]
#[doc(hidden)]
pub fn lazy_source<T: Element>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    format: &str,
    read: impl Fn(PathBuf) -> Result<TypedSourceIterator<T>, Error> + Send + Sync + 'static,
) -> Result<PCollection<T>, Error> {
    let path = path.as_ref().to_path_buf();
    let expected = file_hash(&path, key)?;
    let hex = expected.iter().fold(String::new(), |mut output, byte| {
        let _ = write!(output, "{byte:02x}");
        output
    });
    let digest = format!("{format}:{}:{hex}", path.display());
    let name = key.to_owned();
    pipeline.bounded_source_with_semantic(
        key,
        &format!("ironbeam/io/{format}-source"),
        SourceProgress {
            stable_split_id: true,
            replay: Replay::AtLeastOnce,
            reports_event_time: false,
        },
        digest,
        None,
        move || {
            if file_hash(&path, &name)? != expected {
                return Err(Error::recovery(
                    &name,
                    "source contents changed after graph construction",
                ));
            }
            read(path.clone())
        },
    )
}
