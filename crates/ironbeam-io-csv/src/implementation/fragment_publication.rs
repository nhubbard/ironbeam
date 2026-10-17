/// Stage one header-bearing CSV fragment per bundle for coordinator publication.
pub fn csv_fragment_sink<T: Element + Serialize>(
    input: &PCollection<T>,
    key: &str,
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
) -> Result<Sink<T>, Error> {
    let staging = staging.as_ref();
    let store = csv_fragment_store(staging, job, graph, key)?;
    let config = serde_json::to_string(&("csv-fragments-v1", staging, job, graph, key))
        .map_err(|error| Error::incompatible(error.to_string()))?;
    input.sink_named_with_config_and_finalization(
        key,
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        config,
        move || {
            Ok(Box::new(CsvFragmentSink::<T> {
                store: store.clone(),
                bundle_id: None,
                writer: None,
                rows: 0,
                staged: false,
                marker: std::marker::PhantomData,
            }))
        },
    )
}

/// Merge accepted bundles into one CSV file after the runner reports success.
pub fn publish_csv_fragments<T: DeserializeOwned>(
    store: &CsvFragmentStore,
    expected_rows: u64,
    target: impl AsRef<Path>,
) -> Result<PublishedResult, Error> {
    let target = target.as_ref();
    if target
        .extension()
        .and_then(|value| value.to_str())
        .is_some_and(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "gz" | "gzip" | "zst" | "zstd" | "bz2" | "xz"
            )
        })
    {
        return Err(Error::graph(
            "csv-fragment",
            "compressed final result needs a format merger",
        ));
    }
    store
        .local
        .publish_merged(expected_rows, target, |parts, _, output| {
            let mut writer = csv::WriterBuilder::new()
                .has_headers(false)
                .from_writer(output);
            let mut header: Option<csv::StringRecord> = None;
            let mut rows = 0_u64;
            for part in parts {
                if part.rows == 0 {
                    if part.payload_len != 0 {
                        return Err(Error::incompatible("empty CSV fragment has a payload"));
                    }
                    continue;
                }
                let mut reader = csv::Reader::from_reader(part.open_payload()?);
                let part_header = reader
                    .headers()
                    .map_err(|error| io("csv-fragment", error))?
                    .clone();
                if part_header.is_empty() {
                    return Err(Error::incompatible("CSV fragment is missing a header"));
                }
                match &header {
                    Some(previous) if previous != &part_header => {
                        return Err(Error::incompatible("CSV fragment headers disagree"));
                    }
                    None => {
                        writer
                            .write_record(&part_header)
                            .map_err(|error| io("csv-fragment", error))?;
                        header = Some(part_header);
                    }
                    _ => {}
                }
                let mut part_rows = 0_u64;
                for record in reader.records() {
                    let record = record.map_err(|error| io("csv-fragment", error))?;
                    record
                        .deserialize::<T>(header.as_ref())
                        .map_err(|error| io("csv-fragment", error))?;
                    writer
                        .write_record(&record)
                        .map_err(|error| io("csv-fragment", error))?;
                    part_rows = part_rows
                        .checked_add(1)
                        .ok_or_else(|| Error::incompatible("CSV row count overflow"))?;
                }
                if part_rows != part.rows {
                    return Err(Error::incompatible(
                        "CSV fragment decoded row count disagrees",
                    ));
                }
                rows = rows
                    .checked_add(part_rows)
                    .ok_or_else(|| Error::incompatible("CSV row count overflow"))?;
            }
            writer.flush().map_err(|error| io("csv-fragment", error))?;
            Ok(rows)
        })
}

#[cfg(feature = "parallel-io")]
fn write_csv_fragment<T: Serialize>(path: &Path, values: &[T], first: bool) -> Result<(), Error> {
    if first {
        require_named_header(values)?;
    }
    let file = std::fs::File::create(path).map_err(|e| io("csv", e))?;
    let mut writer = csv::WriterBuilder::new()
        .has_headers(first)
        .from_writer(file);
    for value in values {
        writer.serialize(value).map_err(|e| io("csv", e))?;
    }
    writer.flush().map_err(|e| io("csv", e))
}

#[cfg(feature = "parallel-io")]
/// Serialize named records concurrently with one CSV header and ordered rows.
pub fn csv_parallel_sink<T: Element + Serialize + Sync>(
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
        parallel_file_writer(path, values, shards, write_csv_fragment::<T>)
    })
}

#[cfg(test)]
#[path = "../../tests/unit/lib__fragment_tests.rs"]
mod fragment_tests;
