#[cfg(all(feature = "parallel-io", feature = "file"))]
#[doc(hidden)]
pub fn framed_parallel_sink<T: Element + Sync>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
    shards: usize,
    write: fn(&Path, &[T]) -> Result<(), Error>,
) -> Result<Sink<T>, Error> {
    if shards == 0 {
        return Err(Error::graph(
            key,
            "parallel writer needs at least one shard",
        ));
    }
    sink(input, key, path, move |path, values| {
        parallel_file_writer(path, values, shards, |part, chunk, _| write(part, chunk))
    })
}

#[cfg(all(feature = "parallel-io", feature = "file"))]
#[doc(hidden)]
pub fn parallel_container_writer<T: Sync>(
    path: &Path,
    values: &[T],
    shards: usize,
    extension: &str,
    write: impl Fn(&Path, &[T]) -> Result<(), Error> + Sync,
    read: fn(&[u8]) -> Result<Vec<T>, Error>,
) -> Result<(), Error> {
    if values.is_empty() {
        return write(path, values);
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let fragments = tempfile::tempdir_in(parent).map_err(|e| io("parallel-io", e))?;
    let requested = shards.min(values.len());
    let chunk_size = values.len().div_ceil(requested);
    let count = values.len().div_ceil(chunk_size);
    let parts = (0..count)
        .map(|index| fragments.path().join(format!("part-{index}.{extension}")))
        .collect::<Vec<_>>();
    std::thread::scope(|scope| {
        let write = &write;
        let tasks = parts
            .iter()
            .enumerate()
            .map(|(index, part)| {
                let start = index * chunk_size;
                let end = (start + chunk_size).min(values.len());
                scope.spawn(move || write(part, &values[start..end]))
            })
            .collect::<Vec<_>>();
        for task in tasks {
            task.join().map_err(|_| {
                Error::execution("parallel-io", "direct", "writer shard panicked")
            })??;
        }
        Ok::<(), Error>(())
    })?;
    let mut merged = Vec::with_capacity(values.len());
    for part in parts {
        let bytes = fs::read(part).map_err(|e| io("parallel-io", e))?;
        merged.extend(read(&bytes)?);
    }
    write(path, &merged)
}

#[cfg(all(test, feature = "file", feature = "compression-gzip"))]
#[path = "../../tests/unit/lib__lazy_input_tests.rs"]
mod lazy_input_tests;
