//! XML sources and deferred final-result sinks for Ironbeam 5.
#![allow(clippy::missing_errors_doc)]

use ironbeam_core::{Element, Error, PCollection, Pipeline, Sink};
use serde::{Serialize, de::DeserializeOwned};
use std::io::Write;
use std::path::Path;

#[derive(serde::Deserialize)]
#[serde(rename = "records")]
#[serde(bound(deserialize = "T: serde::Deserialize<'de>"))]
struct XmlRead<T> {
    #[serde(rename = "item", default = "empty_items")]
    item: Vec<T>,
}
const fn empty_items<T>() -> Vec<T> {
    Vec::new()
}
#[derive(Serialize)]
#[serde(rename = "records")]
struct XmlWrite<'a, T> {
    #[serde(rename = "item")]
    item: &'a [T],
}
fn read_xml<T: DeserializeOwned>(bytes: &[u8]) -> Result<Vec<T>, Error> {
    let contents = std::str::from_utf8(bytes).map_err(|e| ironbeam_io::io("xml", e))?;
    quick_xml::de::from_str::<XmlRead<T>>(contents)
        .map(|v| v.item)
        .map_err(|e| ironbeam_io::io("xml", e))
}
fn write_xml<T: Serialize>(path: &Path, values: &[T]) -> Result<(), Error> {
    let contents = quick_xml::se::to_string(&XmlWrite { item: values })
        .map_err(|e| ironbeam_io::io("xml", e))?;
    let mut writer = ironbeam_io::open_encoded_output(path, "xml")?;
    writer
        .write_all(contents.as_bytes())
        .map_err(|e| ironbeam_io::io("xml", e))
}
pub fn xml_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    ironbeam_io::source(pipeline, key, path, "xml", read_xml::<T>)
}
/// V4-compatible bounded XML source. XML uses one whole-file shard; the
/// shard-size hint is accepted for migration compatibility.
pub fn xml_streaming_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    records_per_shard: usize,
) -> Result<PCollection<T>, Error> {
    let _ = records_per_shard;
    xml_source(pipeline, key, path)
}
pub fn xml_sink<T: Element + Serialize>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<T>, Error> {
    ironbeam_io::sink(input, key, path, write_xml::<T>)
}

#[cfg(feature = "parallel-io")]
pub fn xml_parallel_sink<T: Element + Serialize + DeserializeOwned + Sync>(
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
    ironbeam_io::sink(input, key, path, move |path, values| {
        ironbeam_io::parallel_container_writer(
            path,
            values,
            shards,
            "xml",
            write_xml::<T>,
            read_xml::<T>,
        )
    })
}

/// Expand an XML file glob into stable bounded source nodes.
pub fn xml_glob_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    pattern: &str,
) -> Result<PCollection<T>, Error> {
    ironbeam_io::file_glob_source(pipeline, key, pattern, |pipeline, key, path| {
        xml_source(pipeline, key, path)
    })
}
