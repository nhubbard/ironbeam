//! XML sources and sinks for [`PCollection`].
//!
//! This module provides typed, serde-backed XML I/O that integrates with the
//! Ironbeam pipeline. You can either:
//!
//! - **Vector I/O** — read the whole file into memory or write an in-memory collection:
//!   - [`read_xml`] -> `PCollection<T>`
//!   - [`PCollection::write_xml`]
//!
//! - **Streaming I/O** — build a source that shards an XML file (one shard, since
//!   XML is not splittable) and parses it lazily in the runner:
//!   - [`read_xml_streaming`] -> `PCollection<T>`
//!
//! All functions are serde-driven: your record type `T` should
//! `#[derive(serde::Deserialize)]` for reads and additionally
//! `#[derive(serde::Serialize)]` for writes.
//!
//! ## Wire format
//!
//! Files are written and read as `<records><item>…</item>…</records>`. This
//! structure is internal; callers never need to reference it.
//!
//! ## Feature flags
//! - `io-xml`: enables XML helpers.
//! - `parallel-io`: enables the parallel writer (`write_xml_par`).
//!
//! ## Examples
//!
//! Read an XML file and write it back out (vector I/O):
//! ```no_run
//! use ironbeam::*;
//! use serde::{Deserialize, Serialize};
//! use anyhow::Result;
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! struct Row { k: String, v: u64 }
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//! let rows = read_xml::<Row>(&p, "data/input.xml")?;
//! let doubled = rows.map(|r: &Row| Row { k: r.k.clone(), v: r.v * 2 });
//! doubled.write_xml("data/out.xml")?;
//! # Ok(())
//! # }
//! ```
//!
//! Streaming read shard-by-shard (always one shard for XML):
//! ```no_run
//! use ironbeam::*;
//! use serde::{Deserialize, Serialize};
//! use anyhow::Result;
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! struct Row { k: String, v: u64 }
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//! let stream = read_xml_streaming::<Row>(&p, "data/input.xml", 100_000)?;
//! let out = stream.collect_seq()?;
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use crate::io::glob::expand_glob;
use crate::io::xml::{XmlShards, XmlVecOps, build_xml_shards, read_xml_vec, write_xml_vec};
use crate::node::Node;
use crate::type_token::TypeTag;
use crate::{PCollection, Pipeline, RFBound, from_vec};
use anyhow::{Context, Result, anyhow, bail};
use regex::Regex;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::path::Path;

#[cfg(feature = "io-xml")]
impl<T: RFBound + Serialize> PCollection<T> {
    /// Execute the pipeline, collect results, and write them to a single XML file.
    ///
    /// The collection is first gathered into memory sequentially to preserve
    /// deterministic ordering, then written as one XML file.
    ///
    /// Returns the number of rows written.
    ///
    /// # Example
    /// ```no_run
    /// use ironbeam::*;
    /// use serde::{Serialize, Deserialize};
    /// use anyhow::Result;
    ///
    /// #[derive(Clone, Serialize, Deserialize)]
    /// struct Row { k: String, v: u64 }
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let rows = from_vec(&p, vec![Row { k: "a".into(), v: 1 }]);
    /// let n = rows.write_xml("data/out.xml")?;
    /// assert_eq!(n, 1);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Returns an error if the pipeline cannot be executed or the file cannot
    /// be written.
    pub fn write_xml(self, path: impl AsRef<Path>) -> Result<usize> {
        let rows: Vec<T> = self.collect_seq()?;
        write_xml_vec(path, &rows)
    }
}

#[cfg_attr(docsrs, doc(cfg(all(feature = "io-xml", feature = "parallel-io"))))]
#[cfg(all(feature = "io-xml", feature = "parallel-io"))]
impl<T: RFBound + Serialize> PCollection<T> {
    /// Execute the pipeline in parallel and write the result as XML.
    ///
    /// Collects results in parallel, serializes each record to an XML fragment
    /// in parallel, then writes a single XML file in deterministic order.
    ///
    /// `shards = Some(n)` sets the number of collection shards; `None` lets the
    /// runner decide.
    ///
    /// Returns the number of rows written.
    ///
    /// # Example
    /// ```no_run
    /// use ironbeam::*;
    /// use serde::{Serialize, Deserialize};
    /// use anyhow::Result;
    ///
    /// #[derive(Clone, Serialize, Deserialize)]
    /// struct Row { k: String, v: u64 }
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let rows = from_vec(&p, vec![Row { k: "a".into(), v: 1 }]);
    /// rows.write_xml_par("data/out.xml", Some(4))?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Returns an error if the pipeline cannot be executed or the file cannot
    /// be written.
    pub fn write_xml_par(self, path: impl AsRef<Path>, shards: Option<usize>) -> Result<usize> {
        use crate::io::xml::write_xml_par;
        let data = self.collect_par(shards, None)?;
        write_xml_par(path, &data, None)
    }
}

/// Read an XML file(s) into a typed `PCollection<T>` (vector mode).
///
/// Eagerly parses the entire file(s) into memory and returns a source
/// collection. For large files, prefer [`read_xml_streaming`].
///
/// ### Glob Pattern Support
///
/// `path` can be either:
/// - A single file path: `"data/input.xml"`
/// - A glob pattern: `"data/*.xml"` or `"data/year=2024/month=*/*.xml"`
///
/// When a glob pattern is provided, all matching files are read and
/// concatenated in sorted (lexicographic) order for deterministic results.
///
/// *Enabled when the `io-xml` feature is on.*
///
/// # Errors
/// Returns an error if no files match, any file cannot be opened, or any
/// item fails to deserialize.
///
/// # Panics
/// If the `path` parameter is invalid UTF-8 or the regex engine fails.
///
/// # Examples
///
/// Single file:
/// ```no_run
/// use ironbeam::*;
/// use serde::{Deserialize, Serialize};
/// use anyhow::Result;
///
/// #[derive(Clone, Serialize, Deserialize)]
/// struct Row { k: String, v: u64 }
///
/// # fn main() -> Result<()> {
/// let p = Pipeline::default();
/// let rows = read_xml::<Row>(&p, "data/input.xml")?;
/// let out = rows.collect_seq()?;
/// # Ok(())
/// # }
/// ```
///
/// Glob pattern:
/// ```no_run
/// use ironbeam::*;
/// use serde::{Deserialize, Serialize};
/// use anyhow::Result;
///
/// #[derive(Clone, Serialize, Deserialize)]
/// struct Row { k: String, v: u64 }
///
/// # fn main() -> Result<()> {
/// let p = Pipeline::default();
/// let rows = read_xml::<Row>(&p, "data/*.xml")?;
/// let out = rows.collect_seq()?;
/// # Ok(())
/// # }
/// ```
#[cfg_attr(docsrs, doc(cfg(feature = "io-xml")))]
#[cfg(feature = "io-xml")]
pub fn read_xml<T>(p: &Pipeline, path: impl AsRef<Path>) -> Result<PCollection<T>>
where
    T: RFBound + DeserializeOwned,
{
    let path_str = path
        .as_ref()
        .to_str()
        .ok_or_else(|| anyhow!("path contains invalid UTF-8"))?;

    let glob_regex = Regex::new(r"[*?\[]").expect("valid glob regex");
    if glob_regex.is_match(path_str) {
        let files =
            expand_glob(path_str).with_context(|| format!("expanding glob pattern: {path_str}"))?;

        if files.is_empty() {
            bail!("no files found matching pattern: {path_str}");
        }

        let mut all_data = Vec::new();
        for file in files {
            let data: Vec<T> =
                read_xml_vec(&file).with_context(|| format!("reading {}", file.display()))?;
            all_data.extend(data);
        }
        Ok(from_vec(p, all_data))
    } else {
        let v = read_xml_vec::<T>(path)?;
        Ok(from_vec(p, v))
    }
}

/// Create a **streaming** XML source, sharded by record count.
///
/// Builds an [`XmlShards`] descriptor (counting records up front) and inserts
/// a `Source` node that reads and deserializes only its shard when executed by
/// the runner. Because XML is not splittable, there is always at most one shard.
///
/// *Enabled when the `io-xml` feature is on.*
///
/// # Arguments
/// - `p`: Pipeline to attach the source to.
/// - `path`: XML file path.
/// - `records_per_shard`: accepted for API consistency; XML always uses one shard.
///
/// # Errors
/// Returns an error if the file cannot be opened or parsed.
///
/// # Example
/// ```no_run
/// use ironbeam::*;
/// use serde::{Deserialize, Serialize};
/// use anyhow::Result;
///
/// #[derive(Clone, Serialize, Deserialize)]
/// struct Row { k: String, v: u64 }
///
/// # fn main() -> Result<()> {
/// let p = Pipeline::default();
/// let stream = read_xml_streaming::<Row>(&p, "data/input.xml", 100_000)?;
/// let out = stream.collect_seq()?;
/// # Ok(())
/// # }
/// ```
#[cfg_attr(docsrs, doc(cfg(feature = "io-xml")))]
#[cfg(feature = "io-xml")]
pub fn read_xml_streaming<T>(
    p: &Pipeline,
    path: impl AsRef<Path>,
    records_per_shard: usize,
) -> Result<PCollection<T>>
where
    T: RFBound + DeserializeOwned,
{
    let shards: XmlShards = build_xml_shards(path, records_per_shard)?;
    let id = p.insert_node(Node::Source {
        payload: Arc::new(shards),
        vec_ops: XmlVecOps::<T>::new(),
        elem_tag: TypeTag::of::<T>(),
    });
    Ok(PCollection {
        pipeline: p.clone(),
        id,
        _t: PhantomData,
    })
}
