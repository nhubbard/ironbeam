use ironbeam_core::{Element, Error, PCollection, Pipeline, Sink};
use ironbeam_io::final_result::{AcceptedFragment, LocalFinalResult, PublishedResult};
use ironbeam_io::sequence_final::{sequence_fragment_sink, sequence_fragment_store};
use serde::{Serialize, de::DeserializeOwned};
use std::io::{Read, Write};
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
/// Read a bounded XML file into typed values.
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
/// Write typed values to a local XML file.
pub fn xml_sink<T: Element + Serialize>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<T>, Error> {
    ironbeam_io::sink(input, key, path, write_xml::<T>)
}

/// A local, graph-bound staging scope for finalized XML item bundles.
#[derive(Clone)]
pub struct XmlFragmentStore {
    local: LocalFinalResult,
}

impl XmlFragmentStore {
    /// Require the publisher to match the runner's completed bundle attempts.
    ///
    /// # Errors
    ///
    /// Rejects empty or duplicate attempt IDs.
    pub fn require_bundle_attempts<I, S>(mut self, bundle_ids: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.local = self.local.require_bundle_attempts(bundle_ids)?;
        Ok(self)
    }

    /// Return accepted XML fragments after runner finalization.
    pub fn accepted_fragments(&self) -> Result<Vec<AcceptedFragment>, Error> {
        self.local.accepted_fragments()
    }
}

/// Open a local XML fragment store for one job and graph.
pub fn xml_fragment_store(
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
    key: &str,
) -> Result<XmlFragmentStore, Error> {
    Ok(XmlFragmentStore {
        local: sequence_fragment_store(staging, job, graph, key, "xml-items")?,
    })
}

fn encode_xml_item<T: Serialize>(value: &T, output: &mut dyn Write) -> Result<(), Error> {
    let document = quick_xml::se::to_string(&XmlWrite {
        item: std::slice::from_ref(value),
    })
    .map_err(|error| ironbeam_io::io("xml-fragment", error))?;
    let body = document
        .strip_prefix("<records>")
        .and_then(|body| body.strip_suffix("</records>"))
        .ok_or_else(|| Error::incompatible("XML item serializer returned an unexpected root"))?;
    output
        .write_all(body.as_bytes())
        .map_err(|error| ironbeam_io::io("xml-fragment", error))
}

/// Stage complete XML item elements per bundle for coordinator publication.
pub fn xml_fragment_sink<T: Element + Serialize>(
    input: &PCollection<T>,
    key: &str,
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
) -> Result<Sink<T>, Error> {
    sequence_fragment_sink(
        input,
        key,
        staging,
        job,
        graph,
        "xml-items",
        encode_xml_item::<T>,
    )
}

/// Validate each accepted fragment and publish one XML document after job success.
pub fn publish_xml_fragments<T: DeserializeOwned>(
    store: &XmlFragmentStore,
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
            "xml-fragment",
            "compressed final result needs a format merger",
        ));
    }
    store
        .local
        .publish_merged(expected_rows, target, |parts, _, output| {
            output
                .write_all(b"<records>")
                .map_err(|error| ironbeam_io::io("xml-fragment", error))?;
            let mut rows = 0_u64;
            for part in parts {
                let mut wrapped = b"<records>".to_vec();
                part.open_payload()?
                    .read_to_end(&mut wrapped)
                    .map_err(|error| ironbeam_io::io("xml-fragment", error))?;
                wrapped.extend_from_slice(b"</records>");
                let decoded = read_xml::<T>(&wrapped)?;
                if u64::try_from(decoded.len())
                    .map_err(|_| Error::incompatible("XML row count overflow"))?
                    != part.rows
                {
                    return Err(Error::incompatible(
                        "XML fragment decoded row count disagrees",
                    ));
                }
                part.copy_payload(output)?;
                rows = rows
                    .checked_add(part.rows)
                    .ok_or_else(|| Error::incompatible("XML row count overflow"))?;
            }
            output
                .write_all(b"</records>")
                .map_err(|error| ironbeam_io::io("xml-fragment", error))?;
            Ok(rows)
        })
}

#[cfg(feature = "parallel-io")]
/// Write XML values through parallel local output shards.
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

#[cfg(test)]
#[path = "../../tests/unit/lib__fragment_tests.rs"]
mod fragment_tests;
