//! Job-owned output spilling for bounded collection requests.
use crate::metrics::MetricsState;
use ironbeam_core::{Element, Error};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tempfile::{Builder, TempDir};

const MAX_FRAME_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct SpillConfig {
    pub directory: PathBuf,
    pub threshold_bytes: usize,
}
impl Default for SpillConfig {
    fn default() -> Self {
        Self {
            directory: std::env::temp_dir().join("ironbeam-v5-spill"),
            threshold_bytes: 64 * 1024 * 1024,
        }
    }
}

pub(crate) fn io_error(node: &str, error: impl std::fmt::Display) -> Error {
    Error::io(node, "direct", error.to_string())
}

pub(crate) fn write_frame(writer: &mut impl Write, bytes: &[u8], node: &str) -> Result<u64, Error> {
    let len = u64::try_from(bytes.len())
        .map_err(|_| Error::resource(node, "frame exceeds u64 length"))?;
    writer
        .write_all(&len.to_le_bytes())
        .map_err(|e| io_error(node, e))?;
    writer.write_all(bytes).map_err(|e| io_error(node, e))?;
    Ok(len + 8)
}

pub(crate) fn visit_frames(
    path: &Path,
    node: &str,
    mut visit: impl FnMut(&[u8]) -> Result<(), Error>,
) -> Result<usize, Error> {
    let mut reader = File::open(path).map_err(|e| io_error(node, e))?;
    let mut count = 0;
    loop {
        let mut length = [0_u8; 8];
        if reader
            .read(&mut length[..1])
            .map_err(|e| io_error(node, e))?
            == 0
        {
            break;
        }
        reader
            .read_exact(&mut length[1..])
            .map_err(|e| Error::recovery(node, e.to_string()))?;
        let len = u64::from_le_bytes(length);
        if len > MAX_FRAME_BYTES {
            return Err(Error::recovery(node, "encoded frame exceeds safety limit"));
        }
        let mut bytes = vec![
            0_u8;
            usize::try_from(len)
                .map_err(|_| Error::recovery(node, "frame length exceeds usize"))?
        ];
        reader
            .read_exact(&mut bytes)
            .map_err(|e| Error::recovery(node, e.to_string()))?;
        visit(&bytes)?;
        count += 1;
    }
    Ok(count)
}

pub(crate) struct OutputStore<T: Element> {
    memory: Vec<T>,
    memory_bytes: usize,
    memory_budget: usize,
    spill: Option<SpillConfig>,
    temp: Option<TempDir>,
    paths: Vec<PathBuf>,
    metrics: Arc<MetricsState>,
}
impl<T: Element> OutputStore<T> {
    pub(crate) fn new(
        memory_budget: usize,
        spill: Option<&SpillConfig>,
        metrics: Arc<MetricsState>,
    ) -> Result<Self, Error> {
        if let Some(config) = spill {
            if !T::supports_coder() {
                return Err(Error::unsupported(
                    T::TYPE_ID,
                    "direct",
                    "element-coder-for-spill",
                ));
            }
            if config.threshold_bytes == 0 {
                return Err(Error::graph("spill", "spill threshold must be positive"));
            }
            fs::create_dir_all(&config.directory).map_err(|e| io_error("spill", e))?;
        }
        Ok(Self {
            memory: Vec::new(),
            memory_bytes: 0,
            memory_budget,
            spill: spill.cloned(),
            temp: None,
            paths: Vec::new(),
            metrics,
        })
    }
    pub(crate) fn push(&mut self, value: T) -> Result<(), Error> {
        let size = if T::supports_coder() {
            value.encode()?.len().max(std::mem::size_of::<T>()).max(1)
        } else {
            std::mem::size_of::<T>().max(1)
        };
        let limit = self.spill.as_ref().map_or(self.memory_budget, |s| {
            self.memory_budget.min(s.threshold_bytes)
        });
        if self.spill.is_none() && self.memory_bytes.saturating_add(size) > self.memory_budget {
            return Err(Error::resource(
                T::TYPE_ID,
                "output exceeds job memory budget; enable spilling",
            ));
        }
        if self.spill.is_some()
            && self.memory_bytes.saturating_add(size) > limit
            && !self.memory.is_empty()
        {
            self.flush()?;
        }
        self.memory_bytes = self.memory_bytes.saturating_add(size);
        self.memory.push(value);
        if self.spill.is_some() && self.memory_bytes > limit {
            self.flush()?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<(), Error> {
        if self.memory.is_empty() {
            return Ok(());
        }
        let config = self
            .spill
            .as_ref()
            .ok_or_else(|| Error::resource(T::TYPE_ID, "spill is disabled"))?;
        if self.temp.is_none() {
            self.temp = Some(
                Builder::new()
                    .prefix("job-")
                    .tempdir_in(&config.directory)
                    .map_err(|e| io_error("spill", e))?,
            );
        }
        let path = self
            .temp
            .as_ref()
            .expect("temporary directory created")
            .path()
            .join(format!("segment-{:08}.bin", self.paths.len()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| io_error("spill", e))?;
        let mut written = 0_u64;
        for value in &self.memory {
            written += write_frame(&mut file, &value.encode()?, "spill")?;
        }
        file.sync_all().map_err(|e| io_error("spill", e))?;
        self.paths.push(path);
        self.metrics.spill_files.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .spilled_bytes
            .fetch_add(written, Ordering::Relaxed);
        self.memory.clear();
        self.memory_bytes = 0;
        Ok(())
    }
    pub(crate) fn for_each_encoded(
        &self,
        mut visit: impl FnMut(&[u8]) -> Result<(), Error>,
    ) -> Result<usize, Error> {
        let mut count = 0;
        for path in &self.paths {
            count += visit_frames(path, "spill", &mut visit)?;
        }
        for value in &self.memory {
            let bytes = value.encode()?;
            visit(&bytes)?;
            count += 1;
        }
        Ok(count)
    }
    pub(crate) fn into_vec(self) -> Result<Vec<T>, Error> {
        let mut output = Vec::new();
        for path in &self.paths {
            visit_frames(path, "spill", |bytes| {
                output.push(T::decode(bytes)?);
                Ok(())
            })?;
        }
        output.extend(self.memory);
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn frame_reader_rejects_truncation_oversize_and_visitor_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("frames");
        fs::write(&path, []).unwrap();
        assert_eq!(visit_frames(&path, "test", |_| Ok(())).unwrap(), 0);
        fs::write(&path, [1_u8]).unwrap();
        assert_eq!(
            visit_frames(&path, "test", |_| Ok(())).unwrap_err().kind,
            ironbeam_core::ErrorKind::Recovery
        );
        fs::write(&path, (MAX_FRAME_BYTES + 1).to_le_bytes()).unwrap();
        assert_eq!(
            visit_frames(&path, "test", |_| Ok(())).unwrap_err().kind,
            ironbeam_core::ErrorKind::Recovery
        );
        fs::write(&path, [2_u64.to_le_bytes().as_slice(), &[1_u8]].concat()).unwrap();
        assert_eq!(
            visit_frames(&path, "test", |_| Ok(())).unwrap_err().kind,
            ironbeam_core::ErrorKind::Recovery
        );
        let mut valid = Vec::new();
        assert_eq!(write_frame(&mut valid, &[4, 5], "test").unwrap(), 10);
        fs::write(&path, valid).unwrap();
        assert_eq!(
            visit_frames(&path, "test", |_| Err(Error::resource("test", "visitor")))
                .unwrap_err()
                .kind,
            ironbeam_core::ErrorKind::Resource
        );
        assert_eq!(
            visit_frames(&path, "test", |bytes| {
                assert_eq!(bytes, [4, 5]);
                Ok(())
            })
            .unwrap(),
            1
        );
        assert_eq!(
            visit_frames(&dir.path().join("missing"), "test", |_| Ok(()))
                .unwrap_err()
                .kind,
            ironbeam_core::ErrorKind::Io
        );
    }

    #[test]
    fn frame_writer_propagates_io_failure() {
        struct Fail;
        impl Write for Fail {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::Error::other("disk full"))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        assert_eq!(
            write_frame(&mut Fail, &[1], "test").unwrap_err().kind,
            ironbeam_core::ErrorKind::Io
        );
    }

    #[test]
    fn store_rejects_invalid_coders_and_corrupt_segments() {
        struct Opaque;
        impl Element for Opaque {
            const TYPE_ID: &'static str = "test/opaque";
            const CODER_ID: &'static str = "test/opaque";
        }
        let dir = tempfile::tempdir().unwrap();
        let metrics = Arc::new(MetricsState::default());
        let config = SpillConfig {
            directory: dir.path().to_path_buf(),
            threshold_bytes: 1,
        };
        assert!(matches!(
            OutputStore::<Opaque>::new(10, Some(&config), metrics.clone())
                .err()
                .unwrap()
                .kind,
            ironbeam_core::ErrorKind::UnsupportedCapability { .. }
        ));
        let invalid = SpillConfig {
            threshold_bytes: 0,
            ..config
        };
        assert_eq!(
            OutputStore::<bool>::new(10, Some(&invalid), metrics.clone())
                .err()
                .unwrap()
                .kind,
            ironbeam_core::ErrorKind::Graph
        );
        let mut oversize = OutputStore::<i32>::new(
            1,
            Some(&SpillConfig {
                directory: dir.path().to_path_buf(),
                threshold_bytes: 1,
            }),
            metrics.clone(),
        )
        .unwrap();
        oversize.push(7).unwrap();
        assert_eq!(metrics.spill_files.load(Ordering::Relaxed), 1);
        let mut plain = OutputStore::<Opaque>::new(10, None, metrics.clone()).unwrap();
        plain.flush().unwrap();
        plain.push(Opaque).unwrap();
        assert_eq!(
            plain.flush().unwrap_err().kind,
            ironbeam_core::ErrorKind::Resource
        );
        let mut broken = OutputStore::<bool>::new(10, None, metrics).unwrap();
        broken.paths.push(dir.path().join("missing"));
        assert_eq!(
            broken.for_each_encoded(|_| Ok(())).unwrap_err().kind,
            ironbeam_core::ErrorKind::Io
        );
        broken.paths.clear();
        let path = dir.path().join("invalid-bool");
        let mut encoded = Vec::new();
        write_frame(&mut encoded, &[2], "test").unwrap();
        fs::write(&path, encoded).unwrap();
        broken.paths.push(path);
        assert_eq!(
            broken.into_vec().unwrap_err().kind,
            ironbeam_core::ErrorKind::Incompatible
        );
    }
}
