use ironbeam_core::{Element, Error, PCollection, Pipeline, Sink, TypedSourceIterator};
#[cfg(feature = "parallel-io")]
use ironbeam_io::parallel_container_writer;
use ironbeam_io::{
    count_lazy_records, file_glob_source, file_sharded_source, io, lazy_source,
    open_encoded_output, open_lazy_input, read_lazy_range, sink, source,
};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn reader_schema_format(raw_schema: &str) -> String {
    let schema_digest = Sha256::digest(raw_schema.as_bytes());
    let mut format = String::from("avro-reader-");
    for byte in schema_digest {
        use std::fmt::Write as _;
        write!(format, "{byte:02x}").expect("writing to String cannot fail");
    }
    format
}

fn read_avro<T: DeserializeOwned>(bytes: &[u8]) -> Result<Vec<T>, Error> {
    let reader = apache_avro::Reader::new(bytes).map_err(|e| io("avro", e))?;
    reader
        .map(|row| {
            let value = row.map_err(|e| io("avro", e))?;
            apache_avro::from_value(&value).map_err(|e| io("avro", e))
        })
        .collect()
}

fn write_avro<T: Serialize>(
    path: &Path,
    values: &[T],
    schema: &apache_avro::Schema,
) -> Result<(), Error> {
    let file = open_encoded_output(path, "avro")?;
    let mut writer = apache_avro::Writer::new(schema, file).map_err(|e| io("avro", e))?;
    for value in values {
        let value = apache_avro::to_value(value).map_err(|e| io("avro", e))?;
        writer.append_value(value).map_err(|e| io("avro", e))?;
    }
    writer.flush().map_err(|e| io("avro", e))?;
    Ok(())
}

#[derive(Serialize, serde::Deserialize)]
struct AvroJson {
    value: String,
}

fn read_avro_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<Vec<T>, Error> {
    let rows: Vec<AvroJson> = read_avro(bytes)?;
    rows.into_iter()
        .map(|row| serde_json::from_str(&row.value).map_err(|e| io("avro", e)))
        .collect()
}

/// Read native Avro records from a bounded file.
pub fn avro_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    source(pipeline, key, path, "avro", read_avro::<T>)
}

/// Read a native Avro container through a declared reader schema.
///
/// Avro uses the file's embedded writer schema to resolve changes such as
/// added fields with defaults or a narrower projection, before deserializing
/// each record to `T`.
///
/// The reader schema is parsed when the graph is built and contributes to
/// source identity, so a resumed job cannot silently change its projection.
pub fn avro_source_with_reader_schema<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    reader_schema: impl AsRef<str>,
) -> Result<PCollection<T>, Error> {
    let raw_schema = reader_schema.as_ref();
    let schema = apache_avro::Schema::parse_str(raw_schema).map_err(|error| io(key, error))?;
    let format = reader_schema_format(raw_schema);
    source(pipeline, key, path, &format, move |bytes| {
        let reader = apache_avro::Reader::builder(bytes)
            .reader_schema(&schema)
            .build()
            .map_err(|error| io("avro", error))?;
        reader
            .map(|row| {
                let value = row.map_err(|error| io("avro", error))?;
                apache_avro::from_value(&value).map_err(|error| io("avro", error))
            })
            .collect()
    })
}

/// Lazily read a bounded Avro file through a declared reader schema.
///
/// Each record is resolved against the file's embedded writer schema and the
/// supplied reader schema before it is deserialized to `T`. This keeps the
/// source incremental rather than loading the container into memory.
pub fn avro_streaming_source_with_reader_schema<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    reader_schema: impl AsRef<str>,
) -> Result<PCollection<T>, Error> {
    let raw_schema = reader_schema.as_ref();
    let schema = Arc::new(apache_avro::Schema::parse_str(raw_schema).map_err(|e| io(key, e))?);
    let format = format!("{}-streaming", reader_schema_format(raw_schema));
    lazy_source(pipeline, key, path, &format, move |path| {
        read_avro_lazy_with_schema(path, Arc::clone(&schema))
    })
}

#[allow(clippy::needless_pass_by_value)] // The shared lazy-source factory takes PathBuf.
fn read_avro_lazy<T: Element + DeserializeOwned>(
    path: PathBuf,
) -> Result<TypedSourceIterator<T>, Error> {
    let file = open_lazy_input(&path, "avro")?;
    let reader = apache_avro::Reader::new(file).map_err(|e| io("avro", e))?;
    Ok(Box::new(reader.map(|row| {
        let value = row.map_err(|e| io("avro", e))?;
        apache_avro::from_value(&value).map_err(|e| io("avro", e))
    })))
}

#[allow(clippy::needless_pass_by_value)] // The shared lazy-source factory takes PathBuf.
fn read_avro_lazy_with_schema<T: Element + DeserializeOwned>(
    path: PathBuf,
    schema: Arc<apache_avro::Schema>,
) -> Result<TypedSourceIterator<T>, Error> {
    let file = open_lazy_input(&path, "avro")?;
    let reader = apache_avro::Reader::new(file).map_err(|error| io("avro", error))?;
    Ok(Box::new(reader.map(move |row| {
        let value = row.map_err(|error| io("avro", error))?;
        let value = value.resolve(&schema).map_err(|error| io("avro", error))?;
        apache_avro::from_value(&value).map_err(|error| io("avro", error))
    })))
}

/// Lazily read a bounded Avro file.
pub fn avro_streaming_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    lazy_source(pipeline, key, path, "avro-streaming", read_avro_lazy::<T>)
}

/// Expand a sorted local glob into bounded native Avro sources.
pub fn avro_glob_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    pattern: &str,
) -> Result<PCollection<T>, Error> {
    file_glob_source(pipeline, key, pattern, |pipeline, key, path| {
        avro_source(pipeline, key, path)
    })
}

/// Expand a sorted local glob into bounded Avro sources resolved with a reader schema.
pub fn avro_glob_source_with_reader_schema<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    pattern: &str,
    reader_schema: impl AsRef<str>,
) -> Result<PCollection<T>, Error> {
    let reader_schema = reader_schema.as_ref();
    file_glob_source(pipeline, key, pattern, |pipeline, key, path| {
        avro_source_with_reader_schema(pipeline, key, path, reader_schema)
    })
}

fn count_avro_records<T: Element + DeserializeOwned>(path: &Path) -> Result<usize, Error> {
    count_lazy_records(path, read_avro_lazy::<T>)
}

fn read_avro_range<T: Element + DeserializeOwned>(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<T>, Error> {
    read_lazy_range(path, start, end, read_avro_lazy::<T>)
}

/// Split a bounded Avro file by finite record ranges.
pub fn avro_sharded_source<T: Element + DeserializeOwned>(
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
        "avro",
        count_avro_records::<T>,
        read_avro_range::<T>,
    )
}

/// Split an Avro file into finite record ranges resolved with a reader schema.
///
/// The file's writer schema is checked against the requested reader schema
/// while counting rows and again as each shard is read.
pub fn avro_sharded_source_with_reader_schema<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    records_per_shard: usize,
    reader_schema: impl AsRef<str>,
) -> Result<PCollection<T>, Error> {
    let raw_schema = reader_schema.as_ref();
    let schema = Arc::new(apache_avro::Schema::parse_str(raw_schema).map_err(|e| io(key, e))?);
    let format = format!("{}-sharded", reader_schema_format(raw_schema));
    let count_schema = Arc::clone(&schema);
    file_sharded_source(
        pipeline,
        key,
        path,
        records_per_shard,
        &format,
        move |path| {
            count_lazy_records(path, |path| {
                read_avro_lazy_with_schema::<T>(path, Arc::clone(&count_schema))
            })
        },
        move |path, start, end| {
            read_lazy_range(path, start, end, |path| {
                read_avro_lazy_with_schema::<T>(path, Arc::clone(&schema))
            })
        },
    )
}

/// Write native Avro records using a caller supplied writer schema.
/// The schema is validated during graph construction.
pub fn avro_sink<T: Element + Serialize>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
    schema: impl AsRef<str>,
) -> Result<Sink<T>, Error> {
    let schema = apache_avro::Schema::parse_str(schema.as_ref()).map_err(|e| io(key, e))?;
    sink(input, key, path, move |path, values| {
        write_avro(path, values, &schema)
    })
}

/// Write ordered Avro shards, then publish one complete container.
#[cfg(feature = "parallel-io")]
pub fn avro_parallel_sink<T: Element + Serialize + DeserializeOwned + Sync>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
    schema: impl AsRef<str>,
    shards: usize,
) -> Result<Sink<T>, Error> {
    if shards == 0 {
        return Err(Error::graph(
            key,
            "parallel writer needs at least one shard",
        ));
    }
    let schema = apache_avro::Schema::parse_str(schema.as_ref()).map_err(|e| io(key, e))?;
    sink(input, key, path, move |path, values| {
        parallel_container_writer(
            path,
            values,
            shards,
            "avro",
            |part, rows| write_avro(part, rows, &schema),
            read_avro::<T>,
        )
    })
}

/// Read the earlier preview Avro container with JSON strings in `value`.
pub fn avro_json_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    source(pipeline, key, path, "avro-json", read_avro_json::<T>)
}

/// Write the earlier preview Avro container with JSON strings in `value`.
pub fn avro_json_sink<T: Element + Serialize>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<T>, Error> {
    let schema = apache_avro::Schema::parse_str(
        r#"{"type":"record","name":"IronbeamRecord","fields":[{"name":"value","type":"string"}]}"#,
    )
    .map_err(|e| io(key, e))?;
    sink(input, key, path, move |path, values| {
        let rows: Vec<AvroJson> = values
            .iter()
            .map(|value| {
                serde_json::to_string(value)
                    .map(|value| AvroJson { value })
                    .map_err(|e| io("avro", e))
            })
            .collect::<Result<_, _>>()?;
        write_avro(path, &rows, &schema)
    })
}
