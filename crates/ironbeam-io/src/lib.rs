#![cfg(any(
    feature = "jsonl",
    feature = "csv",
    feature = "xml",
    feature = "avro",
    feature = "parquet"
))]
#![allow(clippy::missing_errors_doc)]
//! Bounded file adapters for the v5 logical graph.
//! Sources bind their input bytes at graph construction. Final-result sinks write
//! to a sibling temporary file and publish only after the whole job succeeds.
use ironbeam_core::{
    Element, Error, PCollection, Pipeline, Replay, Sink, SinkMode, SinkSpec, SourceProgress,
    TypedSink, TypedSourceIterator,
};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

type FileWriter<T> = dyn Fn(&Path, &[T]) -> Result<(), Error> + Send + Sync;

fn io(key: &str, error: impl std::fmt::Display) -> Error {
    Error::io(key, "file", error.to_string())
}
fn source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    format: &str,
    read: fn(&[u8]) -> Result<Vec<T>, Error>,
) -> Result<PCollection<T>, Error> {
    let path = path.as_ref().to_path_buf();
    let bytes = fs::read(&path).map_err(|e| io(key, e))?;
    let expected = Sha256::digest(&bytes);
    let hex = expected.iter().fold(String::new(), |mut output, byte| {
        let _ = write!(output, "{byte:02x}");
        output
    });
    let digest = format!("{}:{}:{}", format, path.display(), hex);
    let name = key.to_owned();
    pipeline.bounded_source_with_semantic(
        key,
        &format!("ironbeam/io/{format}-source"),
        SourceProgress {
            stable_split_id: true,
            replay: Replay::AtLeastOnce,
            reports_event_time: false,
        },
        digest,
        None,
        move || {
            let current = fs::read(&path).map_err(|e| io(&name, e))?;
            if Sha256::digest(&current) != expected {
                return Err(Error::recovery(
                    &name,
                    "source contents changed after graph construction",
                ));
            }
            let values = read(&current)?;
            Ok(Box::new(values.into_iter().map(Ok)) as TypedSourceIterator<T>)
        },
    )
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
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(|e| io(&self.key, e))?;
        let temp = tempfile::NamedTempFile::new_in(parent).map_err(|e| io(&self.key, e))?;
        (self.write)(temp.path(), &self.values)?;
        temp.as_file().sync_all().map_err(|e| io(&self.key, e))?;
        temp.persist(&self.path).map_err(|e| io(&self.key, e))?;
        Ok(())
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.values.clear();
        Ok(())
    }
}
fn sink<T: Element + Serialize>(
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

#[cfg(feature = "jsonl")]
fn read_jsonl<T: DeserializeOwned>(bytes: &[u8]) -> Result<Vec<T>, Error> {
    use std::io::BufRead;
    std::io::BufReader::new(bytes)
        .lines()
        .enumerate()
        .filter_map(|(line, value)| match value {
            Ok(value) if value.trim().is_empty() => None,
            other => Some((line, other)),
        })
        .map(|(line, value)| {
            let value = value.map_err(|e| io("jsonl", e))?;
            serde_json::from_str(&value).map_err(|e| io("jsonl", format!("line {}: {e}", line + 1)))
        })
        .collect()
}
#[cfg(feature = "jsonl")]
fn write_jsonl<T: Serialize>(path: &Path, values: &[T]) -> Result<(), Error> {
    use std::io::Write;
    let mut file = std::io::BufWriter::new(fs::File::create(path).map_err(|e| io("jsonl", e))?);
    for value in values {
        serde_json::to_writer(&mut file, value).map_err(|e| io("jsonl", e))?;
        file.write_all(b"\n").map_err(|e| io("jsonl", e))?;
    }
    file.flush().map_err(|e| io("jsonl", e))
}
#[cfg(feature = "jsonl")]
pub fn jsonl_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    source(pipeline, key, path, "jsonl", read_jsonl::<T>)
}
#[cfg(feature = "jsonl")]
pub fn jsonl_sink<T: Element + Serialize>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<T>, Error> {
    sink(input, key, path, write_jsonl::<T>)
}

#[cfg(feature = "csv")]
fn read_csv<T: DeserializeOwned>(bytes: &[u8]) -> Result<Vec<T>, Error> {
    let mut reader = csv::Reader::from_reader(bytes);
    reader
        .deserialize()
        .map(|value| value.map_err(|e| io("csv", e)))
        .collect()
}
#[cfg(feature = "csv")]
fn write_csv<T: Serialize>(path: &Path, values: &[T]) -> Result<(), Error> {
    let mut writer = csv::Writer::from_path(path).map_err(|e| io("csv", e))?;
    for value in values {
        writer.serialize(value).map_err(|e| io("csv", e))?;
    }
    writer.flush().map_err(|e| io("csv", e))
}
#[cfg(feature = "csv")]
pub fn csv_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    source(pipeline, key, path, "csv", read_csv::<T>)
}
#[cfg(feature = "csv")]
pub fn csv_sink<T: Element + Serialize>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<T>, Error> {
    sink(input, key, path, write_csv::<T>)
}

#[cfg(feature = "xml")]
#[derive(serde::Deserialize)]
#[serde(rename = "records")]
#[serde(bound(deserialize = "T: serde::Deserialize<'de>"))]
struct XmlRead<T> {
    #[serde(rename = "item", default = "empty_items")]
    item: Vec<T>,
}
#[cfg(feature = "xml")]
const fn empty_items<T>() -> Vec<T> {
    Vec::new()
}
#[cfg(feature = "xml")]
#[derive(Serialize)]
#[serde(rename = "records")]
struct XmlWrite<'a, T> {
    #[serde(rename = "item")]
    item: &'a [T],
}
#[cfg(feature = "xml")]
fn read_xml<T: DeserializeOwned>(bytes: &[u8]) -> Result<Vec<T>, Error> {
    let contents = std::str::from_utf8(bytes).map_err(|e| io("xml", e))?;
    quick_xml::de::from_str::<XmlRead<T>>(contents)
        .map(|v| v.item)
        .map_err(|e| io("xml", e))
}
#[cfg(feature = "xml")]
fn write_xml<T: Serialize>(path: &Path, values: &[T]) -> Result<(), Error> {
    let contents =
        quick_xml::se::to_string(&XmlWrite { item: values }).map_err(|e| io("xml", e))?;
    fs::write(path, contents).map_err(|e| io("xml", e))
}
#[cfg(feature = "xml")]
pub fn xml_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    source(pipeline, key, path, "xml", read_xml::<T>)
}
#[cfg(feature = "xml")]
pub fn xml_sink<T: Element + Serialize>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<T>, Error> {
    sink(input, key, path, write_xml::<T>)
}

#[cfg(feature = "avro")]
fn read_avro<T: DeserializeOwned>(bytes: &[u8]) -> Result<Vec<T>, Error> {
    let reader = apache_avro::Reader::new(bytes).map_err(|e| io("avro", e))?;
    reader
        .map(|row| {
            let value = row.map_err(|e| io("avro", e))?;
            apache_avro::from_value(&value).map_err(|e| io("avro", e))
        })
        .collect()
}
#[cfg(feature = "avro")]
fn write_avro<T: Serialize>(
    path: &Path,
    values: &[T],
    schema: &apache_avro::Schema,
) -> Result<(), Error> {
    let file = fs::File::create(path).map_err(|e| io("avro", e))?;
    let mut writer = apache_avro::Writer::new(schema, file).map_err(|e| io("avro", e))?;
    for value in values {
        let value = apache_avro::to_value(value).map_err(|e| io("avro", e))?;
        writer.append_value(value).map_err(|e| io("avro", e))?;
    }
    writer.flush().map_err(|e| io("avro", e))?;
    Ok(())
}
#[cfg(feature = "avro")]
#[derive(Serialize, serde::Deserialize)]
struct AvroJson {
    value: String,
}
#[cfg(feature = "avro")]
fn read_avro_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<Vec<T>, Error> {
    let rows: Vec<AvroJson> = read_avro(bytes)?;
    rows.into_iter()
        .map(|row| serde_json::from_str(&row.value).map_err(|e| io("avro", e)))
        .collect()
}
#[cfg(feature = "avro")]
pub fn avro_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    source(pipeline, key, path, "avro", read_avro::<T>)
}
#[cfg(feature = "avro")]
/// Write native Avro records using a caller supplied writer schema.
/// The schema is validated when the graph is built; record compatibility is
/// checked by the Avro writer before the final file is published.
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

#[cfg(feature = "avro")]
/// Read the Phase 4 preview Avro container with JSON strings in `value`.
pub fn avro_json_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    source(pipeline, key, path, "avro-json", read_avro_json::<T>)
}

#[cfg(feature = "avro")]
/// Write the Phase 4 preview Avro container with JSON strings in `value`.
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

#[cfg(feature = "parquet")]
fn read_parquet<T: DeserializeOwned>(bytes: &[u8]) -> Result<Vec<T>, Error> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let data = bytes::Bytes::copy_from_slice(bytes);
    let reader = ParquetRecordBatchReaderBuilder::try_new(data)
        .map_err(|e| io("parquet", e))?
        .build()
        .map_err(|e| io("parquet", e))?;
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| io("parquet", e))?;
        rows.extend(
            serde_arrow::from_record_batch::<Vec<T>>(&batch).map_err(|e| io("parquet", e))?,
        );
    }
    Ok(rows)
}
#[cfg(feature = "parquet")]
fn write_parquet<T: Serialize + DeserializeOwned>(path: &Path, values: &[T]) -> Result<(), Error> {
    use arrow::datatypes::FieldRef;
    use parquet::arrow::arrow_writer::ArrowWriter;
    use serde_arrow::schema::{SchemaLike, TracingOptions};
    let fields =
        Vec::<FieldRef>::from_type::<T>(TracingOptions::default()).map_err(|e| io("parquet", e))?;
    let batch = serde_arrow::to_record_batch(&fields, &values).map_err(|e| io("parquet", e))?;
    let file = fs::File::create(path).map_err(|e| io("parquet", e))?;
    let mut writer =
        ArrowWriter::try_new(file, batch.schema(), None).map_err(|e| io("parquet", e))?;
    writer.write(&batch).map_err(|e| io("parquet", e))?;
    writer.close().map_err(|e| io("parquet", e))?;
    Ok(())
}
#[cfg(feature = "parquet")]
pub fn parquet_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    source(pipeline, key, path, "parquet", read_parquet::<T>)
}
#[cfg(feature = "parquet")]
pub fn parquet_sink<T: Element + Serialize + DeserializeOwned>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<T>, Error> {
    sink(input, key, path, write_parquet::<T>)
}
