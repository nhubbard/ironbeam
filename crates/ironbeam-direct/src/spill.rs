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
    pub(crate) const fn memory_usage(&self) -> usize {
        self.memory_bytes
    }
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
            value.encoded_len()?.max(std::mem::size_of::<T>()).max(1)
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
    pub(crate) fn extend_batch(&mut self, values: Vec<T>) -> Result<(), Error> {
        if self.spill.is_some() {
            for value in values {
                self.push(value)?;
            }
            return Ok(());
        }
        let minimum = std::mem::size_of::<T>().max(1);
        let additional = if T::supports_coder() {
            if let Some(fixed) = T::fixed_encoded_len() {
                values.len().saturating_mul(fixed.max(minimum))
            } else {
                values.iter().try_fold(0_usize, |total, value| {
                    Ok::<_, Error>(total.saturating_add(value.encoded_len()?.max(minimum)))
                })?
            }
        } else {
            values.len().saturating_mul(minimum)
        };
        if self.memory_bytes.saturating_add(additional) > self.memory_budget {
            return Err(Error::resource(
                T::TYPE_ID,
                "output exceeds job memory budget; enable spilling",
            ));
        }
        self.memory_bytes = self.memory_bytes.saturating_add(additional);
        self.memory.extend(values);
        Ok(())
    }
    pub(crate) fn flush(&mut self) -> Result<(), Error> {
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
#[path = "../tests/unit/spill__tests.rs"]
mod tests;
