use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use serde::de::DeserializeOwned;
use serde::Serialize;
use crate::{read_jsonl_vec, write_jsonl_par, PCollection, Pipeline, RFBound};
use crate::io::jsonl::{build_jsonl_shards, write_jsonl_vec, JsonlShards, JsonlVecOps};
use crate::node::Node;
use crate::type_token::TypeTag;

/// Read a JSONL file into a typed PCollection<T>.
#[cfg(feature = "io-jsonl")]
pub fn read_jsonl<T>(p: &Pipeline, path: impl AsRef<Path>) -> anyhow::Result<PCollection<T>>
where
    T: RFBound + DeserializeOwned,
{
    let data: Vec<T> = read_jsonl_vec(path)?;
    Ok(crate::from_vec(p, data))
}

#[cfg(feature = "io-jsonl")]
impl<T: RFBound + Serialize> PCollection<T> {
    /// Execute the pipeline and write the result to a JSONL file.
    /// Returns number of records written.
    pub fn write_jsonl(self, path: impl AsRef<Path>) -> anyhow::Result<usize> {
        let data = self.collect_seq()?;
        write_jsonl_vec(path, &data)
    }
}

// --------- Sources: JSONL streaming ----------
#[cfg(feature = "io-jsonl")]
pub fn read_jsonl_streaming<T>(
    p: &Pipeline,
    path: impl AsRef<Path>,
    lines_per_shard: usize,
) -> anyhow::Result<PCollection<T>>
where
    T: RFBound + DeserializeOwned,
{
    let shards: JsonlShards = build_jsonl_shards(path, lines_per_shard)?;
    let id = p.insert_node(Node::Source {
        payload: Arc::new(shards),
        vec_ops: JsonlVecOps::<T>::new(),
        elem_tag: TypeTag::of::<T>(),
    });
    Ok(PCollection {
        pipeline: p.clone(),
        id,
        _t: PhantomData,
    })
}

// --------- Sink: parallel JSONL writer (stable order) ----------
#[cfg_attr(docsrs, doc(cfg(all(feature = "io-jsonl", feature = "parallel-io"))))]
#[cfg(all(feature = "io-jsonl", feature = "parallel-io"))]
impl<T: RFBound + Serialize> PCollection<T> {
    /// Execute sequentially and write JSONL in parallel (stable file order).
    pub fn write_jsonl_par(self, path: impl AsRef<Path>, shards: Option<usize>) -> anyhow::Result<usize> {
        let data = self.collect_seq()?; // deterministic order of elements
        write_jsonl_par(path, &data, shards)
    }
}