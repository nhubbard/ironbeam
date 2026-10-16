/// Open a local Arrow IPC row fragment store for one job.
pub fn arrow_ipc_row_fragment_store<T: Element>(
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
    key: &str,
) -> Result<ArrowIpcRowFragmentStore<T>, Error> {
    if !T::supports_coder() {
        return Err(Error::unsupported(
            key,
            "arrow-ipc-fragment",
            "element-coder",
        ));
    }
    Ok(ArrowIpcRowFragmentStore {
        local: sequence_fragment_store(staging, job, graph, key, &row_fragment_format::<T>())?,
        marker: std::marker::PhantomData,
    })
}

fn encode_fragment_row<T: Element>(value: &T, output: &mut dyn Write) -> Result<(), Error> {
    let bytes = value.encode()?;
    let length = u64::try_from(bytes.len())
        .map_err(|_| Error::resource("arrow-ipc-fragment", "encoded row length overflow"))?;
    output
        .write_all(&length.to_le_bytes())
        .and_then(|()| output.write_all(&bytes))
        .map_err(|error| io("arrow-ipc-fragment", error))
}

/// Stage coded rows per bundle for native Arrow IPC publication after `DONE`.
pub fn arrow_ipc_row_fragment_sink<T: Element + Serialize + DeserializeOwned>(
    input: &PCollection<T>,
    key: &str,
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
) -> Result<Sink<T>, Error> {
    if !T::supports_coder() {
        return Err(Error::unsupported(
            key,
            "arrow-ipc-fragment",
            "element-coder",
        ));
    }
    sequence_fragment_sink(
        input,
        key,
        staging,
        job,
        graph,
        &row_fragment_format::<T>(),
        encode_fragment_row::<T>,
    )
}

/// Validate coded rows and atomically publish one native Arrow IPC file.
pub fn publish_arrow_ipc_row_fragments<T: Element + Serialize + DeserializeOwned>(
    store: &ArrowIpcRowFragmentStore<T>,
    expected_rows: u64,
    target: impl AsRef<Path>,
) -> Result<PublishedResult, Error> {
    use arrow::datatypes::{FieldRef, Schema};
    use arrow::ipc::writer::FileWriter;
    use serde_arrow::schema::{SchemaLike, TracingOptions};

    let fields = Vec::<FieldRef>::from_type::<T>(TracingOptions::default())
        .map_err(|error| io("arrow-ipc-fragment", error))?;
    let schema = std::sync::Arc::new(Schema::new(fields.clone()));
    store
        .local
        .publish_merged(expected_rows, target, |parts, _, output| {
            let mut writer = FileWriter::try_new(output, &schema)
                .map_err(|error| io("arrow-ipc-fragment", error))?;
            let mut batch_rows = Vec::<T>::with_capacity(1_024);
            let mut rows = 0_u64;
            for part in parts {
                let mut payload = part.open_payload()?;
                let mut remaining = part.payload_len;
                let mut part_rows = 0_u64;
                while remaining > 0 {
                    if remaining < 8 {
                        return Err(Error::incompatible(
                            "truncated Arrow IPC row fragment frame",
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
                            "Arrow IPC row fragment exceeds payload",
                        ));
                    }
                    let length = usize::try_from(length)
                        .map_err(|_| Error::resource("arrow-ipc-fragment", "row exceeds usize"))?;
                    let mut bytes = Vec::new();
                    bytes.try_reserve_exact(length).map_err(|error| {
                        Error::resource("arrow-ipc-fragment", error.to_string())
                    })?;
                    bytes.resize(length, 0);
                    payload
                        .read_exact(&mut bytes)
                        .map_err(|error| io("arrow-ipc-fragment", error))?;
                    remaining -= length as u64;
                    batch_rows.push(T::decode(&bytes)?);
                    part_rows = part_rows.checked_add(1).ok_or_else(|| {
                        Error::incompatible("Arrow IPC fragment row count overflow")
                    })?;
                    if batch_rows.len() == 1_024 {
                        let batch = serde_arrow::to_record_batch(&fields, &batch_rows)
                            .map_err(|error| io("arrow-ipc-fragment", error))?;
                        writer
                            .write(&batch)
                            .map_err(|error| io("arrow-ipc-fragment", error))?;
                        batch_rows.clear();
                    }
                }
                if part_rows != part.rows {
                    return Err(Error::incompatible(
                        "Arrow IPC fragment decoded row count disagrees",
                    ));
                }
                rows = rows
                    .checked_add(part_rows)
                    .ok_or_else(|| Error::incompatible("Arrow IPC result row count overflow"))?;
            }
            if !batch_rows.is_empty() {
                let batch = serde_arrow::to_record_batch(&fields, &batch_rows)
                    .map_err(|error| io("arrow-ipc-fragment", error))?;
                writer
                    .write(&batch)
                    .map_err(|error| io("arrow-ipc-fragment", error))?;
            }
            writer
                .finish()
                .map_err(|error| io("arrow-ipc-fragment", error))?;
            Ok(rows)
        })
}

#[cfg(feature = "parallel-io")]
/// Write typed rows through parallel Arrow IPC output shards.
pub fn arrow_ipc_parallel_sink<T: Element + Serialize + DeserializeOwned + Sync>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
    shards: usize,
) -> Result<Sink<T>, Error> {
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
            write_arrow_ipc::<T>,
            read_arrow_ipc::<T>,
        )
    })
}

#[derive(Clone, Debug)]
pub struct ArrowBatch(pub arrow::record_batch::RecordBatch);

impl Serialize for ArrowBatch {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        encode_arrow_batch(&self.0)
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}
impl<'de> serde::Deserialize<'de> for ArrowBatch {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = <Vec<u8> as serde::Deserialize>::deserialize(deserializer)?;
        Self::decode(&bytes).map_err(serde::de::Error::custom)
    }
}

fn encode_arrow_batch(batch: &arrow::record_batch::RecordBatch) -> Result<Vec<u8>, Error> {
    use arrow::ipc::writer::FileWriter;
    let mut bytes = Vec::new();
    let mut writer =
        FileWriter::try_new(&mut bytes, &batch.schema()).map_err(|e| io("arrow-ipc", e))?;
    writer.write(batch).map_err(|e| io("arrow-ipc", e))?;
    writer.finish().map_err(|e| io("arrow-ipc", e))?;
    Ok(bytes)
}

impl Element for ArrowBatch {
    const TYPE_ID: &'static str = "arrow/record-batch";
    const CODER_ID: &'static str = "ironbeam/arrow-ipc-file-v1";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        encode_arrow_batch(&self.0)
    }
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        use arrow::ipc::reader::FileReader;
        let mut reader = FileReader::try_new(std::io::Cursor::new(bytes), None)
            .map_err(|e| io("arrow-ipc", e))?;
        let batch = reader
            .next()
            .ok_or_else(|| Error::incompatible("empty Arrow batch coder payload"))?
            .map_err(|e| io("arrow-ipc", e))?;
        if reader.next().is_some() {
            return Err(Error::incompatible(
                "multiple batches in Arrow batch coder payload",
            ));
        }
        Ok(Self(batch))
    }
}

fn read_arrow_ipc_batches(bytes: &[u8]) -> Result<Vec<ArrowBatch>, Error> {
    use arrow::ipc::reader::FileReader;
    FileReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| io("arrow-ipc", e))?
        .map(|batch| batch.map(ArrowBatch).map_err(|e| io("arrow-ipc", e)))
        .collect()
}
fn write_arrow_ipc_batches(path: &Path, values: &[ArrowBatch]) -> Result<(), Error> {
    use arrow::{
        datatypes::{FieldRef, Schema},
        ipc::writer::FileWriter,
    };
    let schema = values.first().map_or_else(
        || std::sync::Arc::new(Schema::new(Vec::<FieldRef>::new())),
        |batch| batch.0.schema(),
    );
    let file = fs::File::create(path).map_err(|e| io("arrow-ipc", e))?;
    let mut writer = FileWriter::try_new(file, &schema).map_err(|e| io("arrow-ipc", e))?;
    for batch in values {
        writer.write(&batch.0).map_err(|e| io("arrow-ipc", e))?;
    }
    writer.finish().map_err(|e| io("arrow-ipc", e))?;
    Ok(())
}
