//! Per-PCollection element coders, attached automatically at build time when
//! the `coders` feature is on.
//!
//! ironbeam erases every element type behind `Partition = Box<dyn Any>` (a
//! boxed `Vec<T>`), and ops are `Arc<dyn DynOp>` closures that expose only
//! `apply`. A backend that ships elements across a wire (the Dataflow harness)
//! therefore has no static knowledge of `T`. Rust has no runtime reflection and
//! a wire codec is monomorphic, so the concrete `T` must be captured where it
//! is still statically known — at each combinator call site.
//!
//! Under `coders`, [`RFBound`](crate::RFBound) is tightened to also
//! require `serde::{Serialize, DeserializeOwned}`, and every node-creating
//! combinator stashes an [`ElementCoder`] for its output type on the pipeline
//! graph keyed by [`NodeId`]. The pre-`GroupByKey` node is upgraded to a
//! KV-aware coder so the value can be emitted as two independently
//! length-prefixed halves, mirroring Beam's `kv<lp, lp>` coder concept.

use std::marker::PhantomData;

use anyhow::{Context, Result, anyhow};
use serde::{Serialize, de::DeserializeOwned};

use crate::type_token::Partition;

/// Turns a single element's postcard bytes into a one-element `Box<Vec<T>>`
/// partition and back. Implementations are type-monomorphic: the concrete `T`
/// is baked in at construction.
pub trait ElementCoder: Send + Sync {
    /// Wrap one postcard-encoded element as a `Partition` of `Vec<T>` holding a
    /// single element.
    fn decode_one(&self, bytes: &[u8]) -> Result<Partition>;

    /// Downcast `partition` to `Vec<T>` and postcard-encode every element,
    /// returning one byte-vec per element.
    fn encode_all(&self, partition: Partition) -> Result<Vec<Vec<u8>>>;

    /// Debug-friendly name of the `T` this coder handles. Used in error
    /// messages only.
    fn type_name(&self) -> &'static str;

    /// `true` for KV-aware coders (see [`PostcardKvCoder`]). Lets a backend
    /// assert that the pre-`GroupByKey` edge carries a coder that can split
    /// each `(K, V)` into separately length-prefixed halves.
    fn is_kv(&self) -> bool {
        false
    }

    /// For sinks whose PCollection coder is `kv<lp<bytes>, lp<bytes>>`, split
    /// each element into separately-encoded `(key, value)` halves. The default
    /// errors; only KV-aware coders implement it.
    fn encode_kv_pairs(&self, _partition: Partition) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Err(anyhow!(
            "coder for {} is not KV-aware; this node must carry a KV coder \
             (it should be the predecessor of a GroupByKey)",
            self.type_name()
        ))
    }
}

/// Default coder: pairs `T` with postcard. Matches ironbeam's
/// `Partition = Box<Vec<T>>` shape. Encoding is deterministic for every element
/// type **except** those containing a `HashMap`/`HashSet` (their iteration
/// order is unstable), which matters when the type is used as a grouping key.
pub struct PostcardCoder<T>(PhantomData<fn() -> T>);

impl<T> PostcardCoder<T> {
    #[must_use]
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> Default for PostcardCoder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ElementCoder for PostcardCoder<T>
where
    T: Serialize + DeserializeOwned + 'static + Send + Sync + Clone,
{
    fn decode_one(&self, bytes: &[u8]) -> Result<Partition> {
        let v: T = postcard::from_bytes(bytes)
            .with_context(|| format!("postcard decode into {}", std::any::type_name::<T>()))?;
        Ok(Box::new(vec![v]))
    }

    fn encode_all(&self, partition: Partition) -> Result<Vec<Vec<u8>>> {
        let vec = partition.downcast::<Vec<T>>().map_err(|_| {
            anyhow!(
                "partition downcast failed: expected Vec<{}>",
                std::any::type_name::<T>()
            )
        })?;
        let mut out = Vec::with_capacity(vec.len());
        for v in vec.into_iter() {
            out.push(
                postcard::to_allocvec(&v)
                    .with_context(|| format!("postcard encode {}", std::any::type_name::<T>()))?,
            );
        }
        Ok(out)
    }

    fn type_name(&self) -> &'static str {
        std::any::type_name::<T>()
    }
}

/// KV-aware coder. Wire-identical to `PostcardCoder<(K, V)>` for the in-bundle
/// paths (`decode_one`/`encode_all` round-trip a postcard-encoded tuple), but
/// also implements [`ElementCoder::encode_kv_pairs`], returning the `K` and `V`
/// halves encoded separately. The pre-`GroupByKey` node carries this so the
/// runner can read each half as an independently length-prefixed,
/// postcard-encoded value — mirroring Beam's `kv<lp, lp>` coder concept.
pub struct PostcardKvCoder<K, V>(PhantomData<fn() -> (K, V)>);

impl<K, V> PostcardKvCoder<K, V> {
    #[must_use]
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<K, V> Default for PostcardKvCoder<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> ElementCoder for PostcardKvCoder<K, V>
where
    K: Serialize + DeserializeOwned + 'static + Send + Sync + Clone,
    V: Serialize + DeserializeOwned + 'static + Send + Sync + Clone,
{
    fn decode_one(&self, bytes: &[u8]) -> Result<Partition> {
        let pair: (K, V) = postcard::from_bytes(bytes)
            .with_context(|| format!("postcard decode into {}", std::any::type_name::<(K, V)>()))?;
        Ok(Box::new(vec![pair]))
    }

    fn encode_all(&self, partition: Partition) -> Result<Vec<Vec<u8>>> {
        let vec = partition.downcast::<Vec<(K, V)>>().map_err(|_| {
            anyhow!(
                "partition downcast failed: expected Vec<{}>",
                std::any::type_name::<(K, V)>()
            )
        })?;
        let mut out = Vec::with_capacity(vec.len());
        for pair in vec.into_iter() {
            out.push(
                postcard::to_allocvec(&pair).with_context(|| {
                    format!("postcard encode {}", std::any::type_name::<(K, V)>())
                })?,
            );
        }
        Ok(out)
    }

    fn type_name(&self) -> &'static str {
        std::any::type_name::<(K, V)>()
    }

    fn is_kv(&self) -> bool {
        true
    }

    fn encode_kv_pairs(&self, partition: Partition) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let vec = partition.downcast::<Vec<(K, V)>>().map_err(|_| {
            anyhow!(
                "partition downcast failed: expected Vec<{}>",
                std::any::type_name::<(K, V)>()
            )
        })?;
        let mut out = Vec::with_capacity(vec.len());
        for (k, v) in vec.into_iter() {
            let kb = postcard::to_allocvec(&k)
                .with_context(|| format!("postcard encode key {}", std::any::type_name::<K>()))?;
            let vb = postcard::to_allocvec(&v)
                .with_context(|| format!("postcard encode value {}", std::any::type_name::<V>()))?;
            out.push((kb, vb));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postcard_coder_roundtrip_u64() {
        let c = PostcardCoder::<u64>::new();
        let bytes = postcard::to_allocvec(&42u64).unwrap();
        let partition = c.decode_one(&bytes).unwrap();
        let encoded = c.encode_all(partition).unwrap();
        assert_eq!(encoded, vec![bytes]);
    }

    #[test]
    fn encode_multi_element_partition() {
        let c = PostcardCoder::<u64>::new();
        let p: Partition = Box::new(vec![1u64, 2, 3]);
        let out = c.encode_all(p).unwrap();
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn downcast_mismatch_errors() {
        let c = PostcardCoder::<u64>::new();
        let wrong: Partition = Box::new(vec!["oops".to_string()]);
        assert!(c.encode_all(wrong).is_err());
    }

    #[test]
    fn kv_coder_encodes_halves_separately() {
        let c = PostcardKvCoder::<String, u64>::new();
        let partition: Partition =
            Box::new(vec![("alice".to_string(), 1u64), ("bob".to_string(), 2u64)]);
        assert!(c.is_kv());
        let pairs = c.encode_kv_pairs(partition).unwrap();
        assert_eq!(pairs.len(), 2);
        assert_eq!(
            postcard::from_bytes::<String>(&pairs[0].0).unwrap(),
            "alice"
        );
        assert_eq!(postcard::from_bytes::<u64>(&pairs[0].1).unwrap(), 1);
    }

    #[test]
    fn kv_coder_intermediate_tuple_roundtrip_matches_postcard() {
        let c = PostcardKvCoder::<String, u64>::new();
        let tuple = ("x".to_string(), 42u64);
        let bytes = postcard::to_allocvec(&tuple).unwrap();
        let partition = c.decode_one(&bytes).unwrap();
        assert_eq!(c.encode_all(partition).unwrap(), vec![bytes]);
    }

    #[test]
    fn plain_coder_rejects_kv_encode() {
        let c = PostcardCoder::<(String, u64)>::new();
        let p: Partition = Box::new(vec![("k".to_string(), 1u64)]);
        let err = c.encode_kv_pairs(p).unwrap_err();
        assert!(format!("{err}").contains("not KV-aware"), "got: {err}");
    }
}
