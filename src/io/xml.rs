//! XML I/O utilities and `VecOps` integration.
//!
//! This module provides:
//! - **Typed vector I/O** with Serde: [`read_xml_vec`] and [`write_xml_vec`]
//! - **Deterministic parallel writer**: [`write_xml_par`] (feature `parallel-io`)
//! - **Streaming ingestion** (single-shard): [`XmlShards`], [`build_xml_shards`], [`read_xml_range`]
//! - **Execution runner integration**: [`XmlVecOps<T>`] implements [`VecOps`] over `XmlShards`
//!
//! # Wire Format
//!
//! All XML files written by this module use the following structure:
//!
//! ```xml
//! <records><item>…</item><item>…</item></records>
//! ```
//!
//! The `<records>` root and `<item>` child elements are internal conventions;
//! users only interact with `T` via Serde.
//!
//! # Notes
//! - XML is not byte-splittable. Sharding always produces exactly **one shard**
//!   covering all records. The `records_per_shard` parameter of
//!   [`build_xml_shards`] is accepted for API consistency but is not used.
//! - Compression is auto-detected from the file extension or magic bytes
//!   when the respective feature flags are enabled.
//! - The parallel writer parallelizes the CPU-bound serde step; the final file
//!   is written sequentially.

use crate::Partition;
use crate::io::compression::{auto_detect_reader, auto_detect_writer};
use crate::type_token::VecOps;
use anyhow::{Context, Result};
use quick_xml::events::Event;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::any::Any;
use std::fs::{File, create_dir_all};
use std::io::{BufReader, Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;

// ── Internal Serde wrappers ────────────────────────────────────────────────────

const fn default_items<T>() -> Vec<T> {
    Vec::new()
}

/// Wire-format read wrapper: `<records><item>…</item>…</records>`.
#[derive(Deserialize)]
#[serde(rename = "records")]
struct XmlFileRead<T> {
    #[serde(rename = "item", default = "default_items")]
    item: Vec<T>,
}

/// Wire-format write wrapper that borrows the slice to avoid cloning.
#[derive(Serialize)]
#[serde(rename = "records")]
struct XmlFileWrite<'a, T>
where
    T: Serialize,
{
    #[serde(rename = "item")]
    item: &'a [T],
}

// ── Private helpers ────────────────────────────────────────────────────────────

/// Open `path` with compression auto-detection and return a buffered reader.
fn open_xml_reader(path: &Path) -> Result<BufReader<Box<dyn Read>>> {
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let inner = auto_detect_reader(f, path)
        .with_context(|| format!("setup decompression for {}", path.display()))?;
    Ok(BufReader::new(inner))
}

/// Count direct child elements of the root element using the event-based reader.
///
/// Works purely on XML structure — no type parameter required.
fn xml_count_items(path: &Path) -> Result<u64> {
    let rdr = open_xml_reader(path)?;
    let mut reader = quick_xml::Reader::from_reader(rdr);
    reader.config_mut().trim_text(true);
    let mut count = 0u64;
    let mut depth = 0i32;
    let mut buf = Vec::new();
    loop {
        match reader
            .read_event_into(&mut buf)
            .with_context(|| format!("count items in {}", path.display()))?
        {
            // depth 1 = root element; depth 2 = direct child = one record.
            Event::Start(_) => {
                depth += 1;
                if depth == 2 {
                    count += 1;
                }
            }
            // A self-closing tag at depth 1 is also a direct child.
            Event::Empty(_) if depth == 1 => {
                count += 1;
            }
            Event::End(_) => depth -= 1,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }
    Ok(count)
}

/// Build [`XmlShards`] from a counted total.
///
/// XML is not splittable, so this always produces at most one shard.
fn make_xml_shards(path: PathBuf, total: u64) -> XmlShards {
    if total == 0 {
        return XmlShards {
            path,
            ranges: vec![],
            total_records: 0,
        };
    }
    XmlShards {
        path,
        ranges: vec![(0, total)],
        total_records: total,
    }
}

// ── Public vector I/O ──────────────────────────────────────────────────────────

/// Read an XML file into a typed `Vec<T>`.
///
/// The file must use the `<records><item>…</item>…</records>` format produced
/// by [`write_xml_vec`]. Compression is auto-detected.
///
/// # Errors
/// Returns an error if the file cannot be opened, read, or any item fails to
/// deserialize into `T`.
pub fn read_xml_vec<T: DeserializeOwned>(path: impl AsRef<Path>) -> Result<Vec<T>> {
    let path = path.as_ref();
    let rdr = open_xml_reader(path)?;
    let wrapper: XmlFileRead<T> = quick_xml::de::from_reader(rdr)
        .with_context(|| format!("deserialize XML from {}", path.display()))?;
    Ok(wrapper.item)
}

/// Write a typed slice to an XML file.
///
/// Parent directories are created as needed. Compression is auto-detected from
/// the file extension (e.g., `.gz`, `.zst`).
///
/// # Returns
/// The number of items written (`data.len()`).
///
/// # Errors
/// Returns an error if the file/dirs cannot be created or serialization fails.
pub fn write_xml_vec<T: Serialize>(path: impl AsRef<Path>, data: &[T]) -> Result<usize> {
    let path = path.as_ref();
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        create_dir_all(parent).with_context(|| format!("mkdir -p {}", parent.display()))?;
    }
    let f = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut w = auto_detect_writer(f, path)
        .with_context(|| format!("setup compression for {}", path.display()))?;
    // quick-xml's Serializer uses fmt::Write; serialize to a String, then copy bytes.
    let wrapper = XmlFileWrite { item: data };
    let xml = quick_xml::se::to_string(&wrapper)
        .with_context(|| format!("serialise XML to {}", path.display()))?;
    w.write_all(xml.as_bytes())
        .with_context(|| format!("write XML to {}", path.display()))?;
    w.flush()
        .with_context(|| format!("flush {}", path.display()))?;
    Ok(data.len())
}

/// Write XML in parallel while keeping **deterministic final order**.
///
/// Each `T` is serialised to an XML string fragment in parallel via rayon; the
/// fragments are then written sequentially inside a single `<records>` root.
///
/// `Serializer::with_root` sets each element's tag to `"item"` regardless of the
/// concrete type name.
///
/// `shards`: accepted for API consistency but ignored — rayon auto-scales.
///
/// # Returns
/// The number of items written (`data.len()`).
///
/// # Errors
/// Returns an error if the file cannot be created/written or any item fails to
/// serialise.
///
/// # Feature
/// Requires the `parallel-io` feature.
#[cfg(feature = "parallel-io")]
pub fn write_xml_par<T: Serialize + Sync>(
    path: impl AsRef<Path>,
    data: &[T],
    shards: Option<usize>,
) -> Result<usize> {
    use rayon::prelude::*;

    let path = path.as_ref();
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        create_dir_all(parent).with_context(|| format!("mkdir -p {}", parent.display()))?;
    }

    let _ = shards; // rayon auto-scales

    // Parallel serde: each T → "<item>…</item>" fragment String.
    let fragments: Vec<String> = data
        .par_iter()
        .map(|item| -> Result<String> {
            let mut buf = String::new();
            let ser = quick_xml::se::Serializer::with_root(&mut buf, Some("item"))
                .map_err(anyhow::Error::from)?;
            item.serialize(ser).map_err(anyhow::Error::from)?;
            Ok(buf)
        })
        .collect::<Result<Vec<_>>>()
        .with_context(|| format!("serialise records to XML for {}", path.display()))?;

    // Sequential write: root element wrapping all fragments.
    let f = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut w = auto_detect_writer(f, path)
        .with_context(|| format!("setup compression for {}", path.display()))?;
    w.write_all(b"<records>")
        .with_context(|| format!("write XML header to {}", path.display()))?;
    for frag in &fragments {
        w.write_all(frag.as_bytes())
            .with_context(|| format!("write XML fragment to {}", path.display()))?;
    }
    w.write_all(b"</records>")
        .with_context(|| format!("write XML footer to {}", path.display()))?;
    w.flush()
        .with_context(|| format!("flush {}", path.display()))?;
    Ok(data.len())
}

// ── Streaming shards ───────────────────────────────────────────────────────────

/// Streaming XML sharding metadata.
///
/// Produced by [`build_xml_shards`] and consumed by [`read_xml_range`] and the
/// execution engine via [`XmlVecOps`].
///
/// Because XML is not byte-splittable, `ranges` always contains at most one
/// entry covering the entire file.
#[derive(Clone)]
pub struct XmlShards {
    /// Source file path.
    pub path: PathBuf,
    /// Record ranges `(start, end)` (0-based, end-exclusive). At most one entry.
    pub ranges: Vec<(u64, u64)>,
    /// Total number of records in the file.
    pub total_records: u64,
}

/// Build [`XmlShards`] by counting items in `path`, producing at most one shard.
///
/// For an empty file, returns an empty shard set. Compression is auto-detected.
///
/// # Arguments
/// - `path`: XML file path.
/// - `records_per_shard`: accepted for API consistency; XML always uses one shard.
///
/// # Errors
/// Returns an error if the file cannot be opened or read.
pub fn build_xml_shards(path: impl AsRef<Path>, records_per_shard: usize) -> Result<XmlShards> {
    let _ = records_per_shard;
    let path = path.as_ref().to_path_buf();
    let total = xml_count_items(&path)?;
    Ok(make_xml_shards(path, total))
}

/// Read a `[start, end)` record range from an XML file into `Vec<T>`.
///
/// Because XML must be loaded in full, this reads the entire file then slices
/// `items[start..end]`.
///
/// # Errors
/// Returns an error if the file cannot be opened or items fail to deserialize.
pub fn read_xml_range<T: DeserializeOwned + Clone>(
    src: &XmlShards,
    start: u64,
    end: u64,
) -> Result<Vec<T>> {
    let all = read_xml_vec::<T>(&src.path)?;
    let s = usize::try_from(start).unwrap_or(0).min(all.len());
    let e = usize::try_from(end).unwrap_or(all.len()).min(all.len());
    Ok(all[s..e].to_vec())
}

// ── VecOps adapter ─────────────────────────────────────────────────────────────

/// `VecOps` adapter for streaming XML via [`XmlShards`].
///
/// Enables the execution engine to determine total length (`len`), split into
/// partitions (`split`) — always one partition — and read the entire dataset
/// (`clone_any`) for sequential paths.
pub struct XmlVecOps<T>(PhantomData<T>);

impl<T> XmlVecOps<T> {
    /// Construct an `Arc` to the adapter.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self(PhantomData))
    }
}

impl<T> VecOps for XmlVecOps<T>
where
    T: DeserializeOwned + Send + Sync + Clone + 'static,
{
    fn len(&self, data: &dyn Any) -> Option<usize> {
        let s = data.downcast_ref::<XmlShards>()?;
        usize::try_from(s.total_records).ok()
    }

    fn split(&self, data: &dyn Any, _n: usize) -> Option<Vec<Partition>> {
        let s = data.downcast_ref::<XmlShards>()?;
        let mut parts = Vec::<Partition>::with_capacity(s.ranges.len());
        for &(start, end) in &s.ranges {
            let v: Vec<T> = read_xml_range::<T>(s, start, end).ok()?;
            parts.push(Box::new(v) as Partition);
        }
        Some(parts)
    }

    fn clone_any(&self, data: &dyn Any) -> Option<Partition> {
        let s = data.downcast_ref::<XmlShards>()?;
        let v: Vec<T> = read_xml_range::<T>(s, 0, s.total_records).ok()?;
        Some(Box::new(v) as Partition)
    }
}
