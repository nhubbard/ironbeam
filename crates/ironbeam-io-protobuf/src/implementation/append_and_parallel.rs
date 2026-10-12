/// Append length-delimited Protobuf records at each bundle commit.
///
/// Replays may duplicate records. Use `protobuf_sink` for whole-job replacement.
/// This local filesystem sink requires a path shared by all workers.
pub fn protobuf_append_sink<T: Element + prost::Message>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<T>, Error> {
    let path = path.as_ref().to_path_buf();
    if path
        .extension()
        .and_then(|part| part.to_str())
        .is_some_and(|part| {
            matches!(
                part.to_ascii_lowercase().as_str(),
                "gz" | "gzip" | "zst" | "zstd" | "bz2" | "xz"
            )
        })
    {
        return Err(Error::graph(
            key,
            "append sink does not support compressed output",
        ));
    }
    let name = key.to_owned();
    input.sink_named_with_config(
        key,
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("protobuf-append:{}", path.display()),
        move || {
            Ok(Box::new(ProtobufAppendSink::<T> {
                path: path.clone(),
                key: name.clone(),
                values: Vec::new(),
            }))
        },
    )
}

#[cfg(feature = "parallel-io")]
pub fn protobuf_parallel_sink<T: Element + prost::Message + Sync>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
    shards: usize,
) -> Result<Sink<T>, Error> {
    framed_parallel_sink(input, key, path, shards, write_protobuf::<T>)
}

fn count_protobuf_records<T: Element + prost::Message + Default>(
    path: &Path,
) -> Result<usize, Error> {
    count_lazy_records(path, read_protobuf_lazy::<T>)
}
fn read_protobuf_range<T: Element + prost::Message + Default>(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<T>, Error> {
    read_lazy_range(path, start, end, read_protobuf_lazy::<T>)
}
pub fn protobuf_sharded_source<T: Element + prost::Message + Default>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    records_per_shard: usize,
) -> Result<PCollection<T>, Error> {
    file_sharded_source(
        pipeline,
        key,
        path,
        records_per_shard,
        "protobuf",
        count_protobuf_records::<T>,
        read_protobuf_range::<T>,
    )
}

#[cfg(test)]
#[path = "../../tests/unit/lib__fragment_tests.rs"]
mod fragment_tests;
