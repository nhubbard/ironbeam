/// Stage coder-framed rows per bundle for a native Parquet result after `DONE`.
pub fn parquet_fragment_sink<T: Element + Serialize + DeserializeOwned>(
    input: &PCollection<T>,
    key: &str,
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
) -> Result<Sink<T>, Error> {
    if !T::supports_coder() {
        return Err(Error::unsupported(key, "parquet-fragment", "element-coder"));
    }
    sequence_fragment_sink(
        input,
        key,
        staging,
        job,
        graph,
        &fragment_format::<T>(),
        encode_fragment_row::<T>,
    )
}

/// Validate coder-framed rows and atomically publish a native Parquet file.
pub fn publish_parquet_fragments<T: Element + Serialize + DeserializeOwned>(
    store: &ParquetFragmentStore<T>,
    expected_rows: u64,
    target: impl AsRef<Path>,
) -> Result<PublishedResult, Error> {
    use arrow::datatypes::{FieldRef, Schema};
    use parquet::arrow::arrow_writer::ArrowWriter;
    use serde_arrow::schema::{SchemaLike, TracingOptions};

    let fields = Vec::<FieldRef>::from_type::<T>(TracingOptions::default())
        .map_err(|error| io("parquet-fragment", error))?;
    let schema = std::sync::Arc::new(Schema::new(fields.clone()));
    store
        .local
        .publish_merged(expected_rows, target, |parts, _, output| {
            let mut writer = ArrowWriter::try_new(output, schema, None)
                .map_err(|error| io("parquet-fragment", error))?;
            let mut batch_rows = Vec::<T>::with_capacity(1_024);
            let mut rows = 0_u64;
            for part in parts {
                let mut payload = part.open_payload()?;
                let mut remaining = part.payload_len;
                let mut part_rows = 0_u64;
                while remaining > 0 {
                    if remaining < 8 {
                        return Err(Error::incompatible("truncated Parquet fragment frame"));
                    }
                    let mut prefix = [0_u8; 8];
                    payload
                        .read_exact(&mut prefix)
                        .map_err(|error| io("parquet-fragment", error))?;
                    remaining -= 8;
                    let length = u64::from_le_bytes(prefix);
                    if length > remaining {
                        return Err(Error::incompatible("Parquet fragment row exceeds payload"));
                    }
                    let length = usize::try_from(length)
                        .map_err(|_| Error::resource("parquet-fragment", "row exceeds usize"))?;
                    let mut bytes = Vec::new();
                    bytes
                        .try_reserve_exact(length)
                        .map_err(|error| Error::resource("parquet-fragment", error.to_string()))?;
                    bytes.resize(length, 0);
                    payload
                        .read_exact(&mut bytes)
                        .map_err(|error| io("parquet-fragment", error))?;
                    remaining -= length as u64;
                    batch_rows.push(T::decode(&bytes)?);
                    part_rows = part_rows.checked_add(1).ok_or_else(|| {
                        Error::incompatible("Parquet fragment row count overflow")
                    })?;
                    if batch_rows.len() == 1_024 {
                        let batch = serde_arrow::to_record_batch(&fields, &batch_rows)
                            .map_err(|error| io("parquet-fragment", error))?;
                        writer
                            .write(&batch)
                            .map_err(|error| io("parquet-fragment", error))?;
                        batch_rows.clear();
                    }
                }
                if part_rows != part.rows {
                    return Err(Error::incompatible(
                        "Parquet fragment decoded row count disagrees",
                    ));
                }
                rows = rows
                    .checked_add(part_rows)
                    .ok_or_else(|| Error::incompatible("Parquet result row count overflow"))?;
            }
            if !batch_rows.is_empty() {
                let batch = serde_arrow::to_record_batch(&fields, &batch_rows)
                    .map_err(|error| io("parquet-fragment", error))?;
                writer
                    .write(&batch)
                    .map_err(|error| io("parquet-fragment", error))?;
            }
            writer
                .close()
                .map_err(|error| io("parquet-fragment", error))?;
            Ok(rows)
        })
}

#[cfg(feature = "parallel-io")]
pub fn parquet_parallel_sink<T: Element + Serialize + DeserializeOwned + Sync>(
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
            "parquet",
            write_parquet::<T>,
            read_parquet::<T>,
        )
    })
}

/// Expand a sorted local glob into bounded Parquet sources.
pub fn parquet_glob_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    pattern: &str,
) -> Result<PCollection<T>, Error> {
    file_glob_source(pipeline, key, pattern, |pipeline, key, path| {
        parquet_source(pipeline, key, path)
    })
}

fn count_parquet_records<T: Element + DeserializeOwned>(path: &Path) -> Result<usize, Error> {
    count_lazy_records(path, read_parquet_lazy::<T>)
}
fn read_parquet_range<T: Element + DeserializeOwned>(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<T>, Error> {
    read_lazy_range(path, start, end, read_parquet_lazy::<T>)
}
/// Split a bounded Parquet file by finite record ranges.
pub fn parquet_sharded_source<T: Element + DeserializeOwned>(
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
        "parquet",
        count_parquet_records::<T>,
        read_parquet_range::<T>,
    )
}

#[cfg(test)]
#[path = "../../tests/unit/lib__fragment_tests.rs"]
mod fragment_tests;
