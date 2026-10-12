pub fn arrow_ipc_batches_source(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<ArrowBatch>, Error> {
    source(
        pipeline,
        key,
        path,
        "arrow-ipc-batches",
        read_arrow_ipc_batches,
    )
}
fn read_arrow_ipc_batches_lazy(path: PathBuf) -> Result<TypedSourceIterator<ArrowBatch>, Error> {
    use arrow::ipc::reader::FileReader;
    let file = fs::File::open(path).map_err(|e| io("arrow-ipc", e))?;
    let reader = FileReader::try_new(file, None).map_err(|e| io("arrow-ipc", e))?;
    Ok(Box::new(reader.map(|batch| {
        batch.map(ArrowBatch).map_err(|e| io("arrow-ipc", e))
    })))
}
pub fn arrow_ipc_batches_streaming_source(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<ArrowBatch>, Error> {
    lazy_source(
        pipeline,
        key,
        path,
        "arrow-ipc-batches-streaming",
        read_arrow_ipc_batches_lazy,
    )
}
pub fn arrow_ipc_batches_sink(
    input: &PCollection<ArrowBatch>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<ArrowBatch>, Error> {
    sink(input, key, path, write_arrow_ipc_batches)
}

fn batch_fragment_format() -> String {
    let type_id = ArrowBatch::type_identity();
    let coder = ArrowBatch::coder_identity();
    format!(
        "arrow-ipc-coded-batches-v1:{}:{type_id}:{}:{coder}:{}:{}",
        type_id.len(),
        coder.len(),
        ArrowBatch::CODER_VERSION.major,
        ArrowBatch::CODER_VERSION.minor
    )
}

/// Reopen staging for native Arrow IPC record-batch final output.
pub struct ArrowIpcBatchFragmentStore {
    local: LocalFinalResult,
}

impl ArrowIpcBatchFragmentStore {
    pub fn require_bundle_attempts<I, S>(mut self, bundle_ids: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.local = self.local.require_bundle_attempts(bundle_ids)?;
        Ok(self)
    }

    pub fn accepted_fragments(&self) -> Result<Vec<AcceptedFragment>, Error> {
        self.local.accepted_fragments()
    }
}

pub fn arrow_ipc_batch_fragment_store(
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
    key: &str,
) -> Result<ArrowIpcBatchFragmentStore, Error> {
    Ok(ArrowIpcBatchFragmentStore {
        local: sequence_fragment_store(staging, job, graph, key, &batch_fragment_format())?,
    })
}

/// Stage complete coded record batches per bundle for publication after `DONE`.
pub fn arrow_ipc_batch_fragment_sink(
    input: &PCollection<ArrowBatch>,
    key: &str,
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
) -> Result<Sink<ArrowBatch>, Error> {
    sequence_fragment_sink(
        input,
        key,
        staging,
        job,
        graph,
        &batch_fragment_format(),
        encode_fragment_row::<ArrowBatch>,
    )
}

/// Validate coded batches and atomically publish one native Arrow IPC file.
/// `expected_batches` is the committed count of `ArrowBatch` elements.
pub fn publish_arrow_ipc_batch_fragments(
    store: &ArrowIpcBatchFragmentStore,
    expected_batches: u64,
    target: impl AsRef<Path>,
) -> Result<PublishedResult, Error> {
    use arrow::datatypes::Schema;
    use arrow::ipc::writer::FileWriter;

    store
        .local
        .publish_merged(expected_batches, target, |parts, _, output| {
            let mut writer = None;
            let mut schema: Option<arrow::datatypes::SchemaRef> = None;
            let mut batches = 0_u64;
            for part in parts {
                let mut payload = part.open_payload()?;
                let mut remaining = part.payload_len;
                let mut part_batches = 0_u64;
                while remaining > 0 {
                    if remaining < 8 {
                        return Err(Error::incompatible(
                            "truncated Arrow IPC batch fragment frame",
                        ));
                    }
                    let mut prefix = [0_u8; 8];
                    payload
                        .read_exact(&mut prefix)
                        .map_err(|error| io("arrow-ipc-fragment", error))?;
                    remaining -= 8;
                    let length = u64::from_le_bytes(prefix);
                    if length > remaining {
                        return Err(Error::incompatible(
                            "Arrow IPC batch fragment exceeds payload",
                        ));
                    }
                    let length = usize::try_from(length).map_err(|_| {
                        Error::resource("arrow-ipc-fragment", "batch exceeds usize")
                    })?;
                    let mut bytes = Vec::new();
                    bytes.try_reserve_exact(length).map_err(|error| {
                        Error::resource("arrow-ipc-fragment", error.to_string())
                    })?;
                    bytes.resize(length, 0);
                    payload
                        .read_exact(&mut bytes)
                        .map_err(|error| io("arrow-ipc-fragment", error))?;
                    remaining -= length as u64;
                    let batch = ArrowBatch::decode(&bytes)?.0;
                    if let Some(expected) = &schema {
                        if batch.schema().as_ref() != expected.as_ref() {
                            return Err(Error::incompatible(
                                "Arrow IPC fragment batch schema differs",
                            ));
                        }
                    } else {
                        schema = Some(batch.schema());
                        writer = Some(
                            FileWriter::try_new(&mut *output, &batch.schema())
                                .map_err(|error| io("arrow-ipc-fragment", error))?,
                        );
                    }
                    writer
                        .as_mut()
                        .ok_or_else(|| Error::incompatible("Arrow IPC fragment writer missing"))?
                        .write(&batch)
                        .map_err(|error| io("arrow-ipc-fragment", error))?;
                    part_batches = part_batches.checked_add(1).ok_or_else(|| {
                        Error::incompatible("Arrow IPC fragment batch count overflow")
                    })?;
                }
                if part_batches != part.rows {
                    return Err(Error::incompatible(
                        "Arrow IPC fragment decoded batch count disagrees",
                    ));
                }
                batches = batches
                    .checked_add(part_batches)
                    .ok_or_else(|| Error::incompatible("Arrow IPC result batch count overflow"))?;
            }
            if let Some(mut writer) = writer {
                writer
                    .finish()
                    .map_err(|error| io("arrow-ipc-fragment", error))?;
            } else {
                let mut writer = FileWriter::try_new(output, &std::sync::Arc::new(Schema::empty()))
                    .map_err(|error| io("arrow-ipc-fragment", error))?;
                writer
                    .finish()
                    .map_err(|error| io("arrow-ipc-fragment", error))?;
            }
            Ok(batches)
        })
}

#[cfg(feature = "parallel-io")]
pub fn arrow_ipc_batches_parallel_sink(
    input: &PCollection<ArrowBatch>,
    key: &str,
    path: impl AsRef<Path>,
    shards: usize,
) -> Result<Sink<ArrowBatch>, Error> {
    if shards == 0 {
        return Err(Error::graph(
            key,
            "parallel writer needs at least one shard",
        ));
    }
    sink(input, key, path, move |path, values| {
        parallel_container_writer(
            path,
            values,
            shards,
            "arrow",
            write_arrow_ipc_batches,
            read_arrow_ipc_batches,
        )
    })
}

pub fn arrow_ipc_glob_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    pattern: &str,
) -> Result<PCollection<T>, Error> {
    file_glob_source(pipeline, key, pattern, |pipeline, key, path| {
        arrow_ipc_source(pipeline, key, path)
    })
}
pub fn arrow_ipc_batches_glob_source(
    pipeline: &Pipeline,
    key: &str,
    pattern: &str,
) -> Result<PCollection<ArrowBatch>, Error> {
    file_glob_source(pipeline, key, pattern, |pipeline, key, path| {
        arrow_ipc_batches_source(pipeline, key, path)
    })
}
fn count_arrow_ipc_records<T: Element + DeserializeOwned>(path: &Path) -> Result<usize, Error> {
    count_lazy_records(path, read_arrow_ipc_lazy::<T>)
}
fn read_arrow_ipc_range<T: Element + DeserializeOwned>(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<T>, Error> {
    read_lazy_range(path, start, end, read_arrow_ipc_lazy::<T>)
}
pub fn arrow_ipc_sharded_source<T: Element + DeserializeOwned>(
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
        "arrow-ipc",
        count_arrow_ipc_records::<T>,
        read_arrow_ipc_range::<T>,
    )
}
fn count_arrow_batches(path: &Path) -> Result<usize, Error> {
    count_lazy_records(path, read_arrow_ipc_batches_lazy)
}
fn read_arrow_batches_range(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<ArrowBatch>, Error> {
    read_lazy_range(path, start, end, read_arrow_ipc_batches_lazy)
}
pub fn arrow_ipc_batches_sharded_source(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    batches_per_shard: usize,
) -> Result<PCollection<ArrowBatch>, Error> {
    file_sharded_source(
        pipeline,
        key,
        path,
        batches_per_shard,
        "arrow-ipc-batches",
        count_arrow_batches,
        read_arrow_batches_range,
    )
}

#[cfg(test)]
#[path = "../../tests/unit/lib__row_fragment_tests.rs"]
mod row_fragment_tests;

#[cfg(test)]
#[path = "../../tests/unit/lib__batch_fragment_tests.rs"]
mod batch_fragment_tests;
