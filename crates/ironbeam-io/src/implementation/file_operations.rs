/// Expand a local file glob in sorted order and flatten its bounded sources.
/// Each matched file receives a stable child key under `key`.
pub fn file_glob_source<T: Element>(
    pipeline: &Pipeline,
    key: &str,
    pattern: &str,
    make_source: impl Fn(&Pipeline, &str, &Path) -> Result<PCollection<T>, Error>,
) -> Result<PCollection<T>, Error> {
    let mut paths = glob::glob(pattern)
        .map_err(|error| Error::graph(key, error.to_string()))?
        .map(|path| path.map_err(|error| io(key, error)))
        .collect::<Result<Vec<_>, _>>()?;
    paths.sort();
    if paths.is_empty() {
        return Err(io(key, format!("no files matched {pattern}")));
    }
    let mut sources = Vec::with_capacity(paths.len());
    for path in &paths {
        let digest = Sha256::digest(path.to_string_lossy().as_bytes());
        let path_hash = digest.iter().fold(String::new(), |mut output, byte| {
            let _ = write!(output, "{byte:02x}");
            output
        });
        let path_key = format!("{key}/file-{path_hash}");
        sources.push(make_source(pipeline, &path_key, path)?);
    }
    let first = sources.remove(0);
    if sources.is_empty() {
        first.map_named(key, |value| value)
    } else {
        first.flatten_named(key, &sources)
    }
}

#[cfg(feature = "file")]
#[doc(hidden)]
pub fn file_sharded_source<T: Element>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    records_per_shard: usize,
    format: &str,
    count: fn(&Path) -> Result<usize, Error>,
    read_range: fn(PathBuf, usize, usize) -> Result<TypedSourceIterator<T>, Error>,
) -> Result<PCollection<T>, Error> {
    if records_per_shard == 0 {
        return Err(Error::graph(key, "records per shard must be positive"));
    }
    let path = path.as_ref().to_path_buf();
    let expected = file_hash(&path, key)?;
    let total = count(&path)?;
    let shards = total.max(1).div_ceil(records_per_shard);
    let mut sources = Vec::with_capacity(shards);
    for index in 0..shards {
        let start = index * records_per_shard;
        let end = start.saturating_add(records_per_shard).min(total);
        let child_key = format!("{key}/shard-{index}");
        let digest = format!("{format}:{}:{expected:02x?}:{start}:{end}", path.display());
        let source_path = path.clone();
        let source_expected = expected.clone();
        let name = child_key.clone();
        sources.push(pipeline.bounded_source_with_semantic(
            &child_key,
            &format!("ironbeam/io/{format}-shard"),
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            digest,
            (format != "jsonl").then_some(end - start),
            move || {
                if file_hash(&source_path, &name)? != source_expected {
                    return Err(Error::recovery(
                        &name,
                        "source contents changed after graph construction",
                    ));
                }
                read_range(source_path.clone(), start, end)
            },
        )?);
    }
    let first = sources.remove(0);
    if sources.is_empty() {
        first.map_named(key, |value| value)
    } else {
        first.flatten_named(key, &sources)
    }
}

#[cfg(feature = "file")]
#[doc(hidden)]
pub fn count_lazy_records<T: Element>(
    path: &Path,
    read: fn(PathBuf) -> Result<TypedSourceIterator<T>, Error>,
) -> Result<usize, Error> {
    let mut count = 0_usize;
    for value in read(path.to_path_buf())? {
        value?;
        count = count
            .checked_add(1)
            .ok_or_else(|| io("shard", "record count overflow"))?;
    }
    Ok(count)
}

#[cfg(feature = "file")]
#[doc(hidden)]
pub fn read_lazy_range<T: Element>(
    path: PathBuf,
    start: usize,
    end: usize,
    read: fn(PathBuf) -> Result<TypedSourceIterator<T>, Error>,
) -> Result<TypedSourceIterator<T>, Error> {
    Ok(Box::new(read(path)?.skip(start).take(end - start)))
}

struct FileSink<T> {
    path: PathBuf,
    key: String,
    values: Vec<T>,
    write: Arc<FileWriter<T>>,
}
impl<T: Element> TypedSink<T> for FileSink<T> {
    fn write(&mut self, value: T) -> Result<(), Error> {
        self.values.push(value);
        Ok(())
    }
    fn commit(&mut self) -> Result<(), Error> {
        let parent = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        create_dir_all_durable(parent, &self.key)?;
        let extension = self
            .path
            .extension()
            .and_then(|part| part.to_str())
            .unwrap_or("");
        let suffix = if extension.is_empty() {
            String::new()
        } else {
            format!(".{extension}")
        };
        let temp = tempfile::Builder::new()
            .suffix(&suffix)
            .tempfile_in(parent)
            .map_err(|e| io(&self.key, e))?;
        (self.write)(temp.path(), &self.values)?;
        temp.as_file().sync_all().map_err(|e| io(&self.key, e))?;
        temp.persist(&self.path).map_err(|e| io(&self.key, e))?;
        sync_directory(parent, &self.key)?;
        Ok(())
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.values.clear();
        Ok(())
    }
}
#[doc(hidden)]
pub fn sink<T: Element>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
    write: impl Fn(&Path, &[T]) -> Result<(), Error> + Send + Sync + 'static,
) -> Result<Sink<T>, Error> {
    let path = path.as_ref().to_path_buf();
    let name = key.to_owned();
    let write: Arc<FileWriter<T>> = Arc::new(write);
    input.sink_named(
        key,
        SinkSpec {
            mode: SinkMode::FinalResult,
            replay: Replay::Idempotent,
            commit: true,
        },
        move || {
            Ok(Box::new(FileSink::<T> {
                path: path.clone(),
                key: name.clone(),
                values: Vec::new(),
                write: Arc::clone(&write),
            }))
        },
    )
}

#[cfg(all(feature = "parallel-io", feature = "file"))]
#[doc(hidden)]
pub fn parallel_file_writer<T: Sync>(
    path: &Path,
    values: &[T],
    shards: usize,
    write_fragment: impl Fn(&Path, &[T], bool) -> Result<(), Error> + Sync,
) -> Result<(), Error> {
    use std::io::Write;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let fragments = tempfile::tempdir_in(parent).map_err(|e| io("parallel-io", e))?;
    let requested = shards.min(values.len().max(1));
    let chunk_size = values.len().max(1).div_ceil(requested);
    let count = values.len().max(1).div_ceil(chunk_size);
    let parts = (0..count)
        .map(|index| fragments.path().join(format!("part-{index}")))
        .collect::<Vec<_>>();
    std::thread::scope(|scope| {
        let write_fragment = &write_fragment;
        let tasks = parts
            .iter()
            .enumerate()
            .map(|(index, part)| {
                let start = index * chunk_size;
                let end = (start + chunk_size).min(values.len());
                scope.spawn(move || write_fragment(part, &values[start..end], index == 0))
            })
            .collect::<Vec<_>>();
        for task in tasks {
            task.join().map_err(|_| {
                Error::execution("parallel-io", "direct", "writer shard panicked")
            })??;
        }
        Ok::<(), Error>(())
    })?;
    let file = fs::File::create(path).map_err(|e| io("parallel-io", e))?;
    #[cfg(any(
        feature = "compression-gzip",
        feature = "compression-zstd",
        feature = "compression-bzip2",
        feature = "compression-xz"
    ))]
    let mut output: Box<dyn Write> =
        ironbeam_compression::auto_detect_writer(file, path).map_err(|e| io("parallel-io", e))?;
    #[cfg(not(any(
        feature = "compression-gzip",
        feature = "compression-zstd",
        feature = "compression-bzip2",
        feature = "compression-xz"
    )))]
    let mut output: Box<dyn Write> = Box::new(file);
    for part in parts {
        let mut input = fs::File::open(part).map_err(|e| io("parallel-io", e))?;
        std::io::copy(&mut input, &mut output).map_err(|e| io("parallel-io", e))?;
    }
    output.flush().map_err(|e| io("parallel-io", e))
}
