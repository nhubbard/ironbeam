#[cfg(feature = "parallel-io")]
/// Write `TFRecord` data through parallel local output shards.
pub fn tfrecord_parallel_sink(
    input: &PCollection<Vec<u8>>,
    key: &str,
    path: impl AsRef<Path>,
    shards: usize,
) -> Result<Sink<Vec<u8>>, Error> {
    framed_parallel_sink(input, key, path, shards, write_tfrecord)
}

fn read_tfrecord_examples(bytes: &[u8]) -> Result<Vec<Example>, Error> {
    read_tfrecord(bytes)?
        .into_iter()
        .enumerate()
        .map(|(index, bytes)| {
            <Example as prost::Message>::decode(bytes.as_slice())
                .map_err(|e| io("tfrecord-examples", format!("record {}: {e}", index + 1)))
        })
        .collect()
}
fn read_tfrecord_examples_lazy(path: PathBuf) -> Result<TypedSourceIterator<Example>, Error> {
    Ok(Box::new(read_tfrecord_lazy(path)?.enumerate().map(
        |(index, result)| {
            let bytes = result?;
            <Example as prost::Message>::decode(bytes.as_slice())
                .map_err(|e| io("tfrecord-examples", format!("record {}: {e}", index + 1)))
        },
    )))
}
/// Read TensorFlow `tf.Example` messages from a bounded `TFRecord` file.
pub fn tfrecord_examples_source(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<Example>, Error> {
    source(
        pipeline,
        key,
        path,
        "tfrecord-examples",
        read_tfrecord_examples,
    )
}
/// Lazily decode TensorFlow `tf.Example` messages from a bounded `TFRecord` file.
pub fn tfrecord_examples_streaming_source(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<Example>, Error> {
    lazy_source(
        pipeline,
        key,
        path,
        "tfrecord-examples-streaming",
        read_tfrecord_examples_lazy,
    )
}
fn count_tfrecord_examples(path: &Path) -> Result<usize, Error> {
    count_lazy_records(path, read_tfrecord_examples_lazy)
}
fn read_tfrecord_examples_range(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<Example>, Error> {
    read_lazy_range(path, start, end, read_tfrecord_examples_lazy)
}
/// Split `TFRecord` Example data into stable record ranges.
pub fn tfrecord_examples_sharded_source(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    records_per_shard: usize,
) -> Result<PCollection<Example>, Error> {
    file_sharded_source(
        pipeline,
        key,
        path,
        records_per_shard,
        "tfrecord-examples",
        count_tfrecord_examples,
        read_tfrecord_examples_range,
    )
}

fn count_tfrecord_records(path: &Path) -> Result<usize, Error> {
    count_lazy_records(path, read_tfrecord_lazy)
}
fn read_tfrecord_range(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<Vec<u8>>, Error> {
    read_lazy_range(path, start, end, read_tfrecord_lazy)
}
/// Split a bounded `TFRecord` file into stable record ranges.
pub fn tfrecord_sharded_source(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    records_per_shard: usize,
) -> Result<PCollection<Vec<u8>>, Error> {
    file_sharded_source(
        pipeline,
        key,
        path,
        records_per_shard,
        "tfrecord",
        count_tfrecord_records,
        read_tfrecord_range,
    )
}

#[cfg(test)]
#[path = "../../tests/unit/lib__fragment_tests.rs"]
mod fragment_tests;
