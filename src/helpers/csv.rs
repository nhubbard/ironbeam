use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use serde::Serialize;
use serde::de::DeserializeOwned;
use crate::{PCollection, Pipeline, RFBound};
use crate::io::csv::{build_csv_shards, CsvShards, CsvVecOps};
use crate::node::Node;
use crate::type_token::TypeTag;

// --------- Sources: CSV (vector + streaming) ----------
#[cfg(feature = "io-csv")]
pub fn read_csv<T>(
    p: &Pipeline,
    path: impl AsRef<Path>,
    has_headers: bool,
) -> anyhow::Result<PCollection<T>>
where
    T: RFBound + DeserializeOwned,
{
    let v = crate::io::csv::read_csv_vec::<T>(path, has_headers)?;
    Ok(crate::from_vec(p, v))
}

#[cfg(feature = "io-csv")]
impl<T: RFBound + Serialize> PCollection<T> {
    pub fn write_csv(self, path: impl AsRef<Path>, has_headers: bool) -> anyhow::Result<usize> {
        let v = self.collect_seq()?;
        crate::io::csv::write_csv_vec(path, has_headers, &v)
    }
}

#[cfg_attr(docsrs, doc(cfg(all(feature = "io-csv", feature = "parallel-io"))))]
#[cfg(all(feature = "io-csv", feature = "parallel-io"))]
impl<T: RFBound + Serialize> PCollection<T> {
    pub fn write_csv_par(
        self,
        path: impl AsRef<Path>,
        shards: Option<usize>,
        has_headers: bool,
    ) -> anyhow::Result<usize> {
        let data = self.collect_par(shards, None)?;
        crate::io::csv::write_csv_vec(path, has_headers, &data)
    }
}

#[cfg(feature = "io-csv")]
pub fn read_csv_streaming<T>(
    p: &Pipeline,
    path: impl AsRef<Path>,
    has_headers: bool,
    rows_per_shard: usize,
) -> anyhow::Result<PCollection<T>>
where
    T: RFBound + DeserializeOwned,
{
    let shards: CsvShards = build_csv_shards(path, has_headers, rows_per_shard)?;
    let id = p.insert_node(Node::Source {
        payload: Arc::new(shards),
        vec_ops: CsvVecOps::<T>::new(),
        elem_tag: TypeTag::of::<T>(),
    });
    Ok(PCollection {
        pipeline: p.clone(),
        id,
        _t: PhantomData,
    })
}