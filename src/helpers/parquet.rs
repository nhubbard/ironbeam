use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use serde::de::DeserializeOwned;
use serde::Serialize;
use crate::{PCollection, Pipeline, RFBound};
use crate::io::parquet::{build_parquet_shards, ParquetShards, ParquetVecOps};
use crate::node::Node;
use crate::type_token::TypeTag;

#[cfg(feature = "io-parquet")]
impl<T: RFBound + DeserializeOwned + Serialize> PCollection<T> {
    pub fn write_parquet(self, path: impl AsRef<Path>) -> anyhow::Result<usize> {
        let rows: Vec<T> = self.collect_seq()?;
        crate::io::parquet::write_parquet_vec(path, &rows)
    }
}

#[cfg(feature = "io-parquet")]
pub fn read_parquet_streaming<T>(
    p: &Pipeline,
    path: impl AsRef<Path>,
    groups_per_shard: usize,
) -> anyhow::Result<PCollection<T>>
where
    T: RFBound + DeserializeOwned,
{
    let shards: ParquetShards = build_parquet_shards(path, groups_per_shard)?;
    let id = p.insert_node(Node::Source {
        payload: Arc::new(shards),
        vec_ops: ParquetVecOps::<T>::new(),
        elem_tag: TypeTag::of::<T>(),
    });
    Ok(PCollection {
        pipeline: p.clone(),
        id,
        _t: PhantomData,
    })
}