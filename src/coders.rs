//! Per-PCollection element coders, attached automatically at build time when
//! the `coders` feature is on.
//!
//! Ironbeam erases every element type behind `Partition = Box<dyn Any>` (a
//! boxed `Vec<T>`), and ops are `Arc<dyn DynOp>` closures that expose only
//! `apply`. A backend that ships elements across a wire (the Dataflow harness)
//! therefore has no static knowledge of `T`. Rust has no runtime reflection and
//! a wire codec is monomorphic, so the concrete `T` must be captured where it
//! is still statically known — at each combinator call site.
//!
//! Under `coders`, [`Element`](crate::Element) is tightened to also
//! require `serde::{Serialize, DeserializeOwned}`, and every node-creating
//! combinator stashes an [`ElementCoder`] for its output type on the pipeline
//! graph keyed by [`NodeId`](crate::node_id::NodeId). The pre-`GroupByKey` node is upgraded to a
//! KV-aware coder so the value can be emitted as two independently
//! length-prefixed halves, mirroring Beam's `kv<lp, lp>` coder concept.

use std::any::type_name;
use std::marker::PhantomData;

use anyhow::{Context, Result, anyhow};
use postcard::{from_bytes, to_allocvec};
use serde::{Serialize, de::DeserializeOwned};

use crate::type_token::Partition;

/// Turns a single element's postcard bytes into a one-element `Box<Vec<T>>`
/// partition and back. Implementations are type-monomorphic: the concrete `T`
/// is baked in at construction.
pub trait ElementCoder: Send + Sync {
    /// Wrap one postcard-encoded element as a `Partition` of `Vec<T>` holding a
    /// single element.
    ///
    /// # Errors
    /// Returns an error if `bytes` is not a valid postcard encoding of the
    /// coder's element type.
    fn decode_one(&self, bytes: &[u8]) -> Result<Partition>;

    /// Downcast `partition` to `Vec<T>` and postcard-encode every element,
    /// returning one byte-vec per element.
    ///
    /// # Errors
    /// Returns an error if `partition` is not the expected `Vec<T>`, or if an
    /// element fails to postcard-encode.
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

    /// For sinks whose `PCollection` coder is `kv<lp<bytes>, lp<bytes>>`, split
    /// each element into separately-encoded `(key, value)` halves. The default
    /// errors; only KV-aware coders implement it.
    ///
    /// # Errors
    /// The default implementation always errors (the coder is not KV-aware).
    /// KV-aware implementations error if `partition` is not the expected
    /// `Vec<(K, V)>`, or if a half fails to postcard-encode.
    fn encode_kv_pairs(&self, _partition: Partition) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Err(anyhow!(
            "coder for {} is not KV-aware; this node must carry a KV coder \
             (it should be the predecessor of a GroupByKey)",
            self.type_name()
        ))
    }
}

/// Default coder: pairs `T` with postcard.
///
/// Matches ironbeam's `Partition = Box<Vec<T>>` shape. Encoding is
/// deterministic for every element type **except** those containing a
/// `HashMap`/`HashSet` (their iteration order is unstable), which matters when
/// the type is used as a grouping key.
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
        let v: T = from_bytes(bytes)
            .with_context(|| format!("postcard decode into {}", type_name::<T>()))?;
        Ok(Box::new(vec![v]))
    }

    fn encode_all(&self, partition: Partition) -> Result<Vec<Vec<u8>>> {
        let vec = partition.downcast::<Vec<T>>().map_err(|_| {
            anyhow!(
                "partition downcast failed: expected Vec<{}>",
                type_name::<T>()
            )
        })?;
        let mut out = Vec::with_capacity(vec.len());
        for v in vec.into_iter() {
            out.push(
                to_allocvec(&v).with_context(|| format!("postcard encode {}", type_name::<T>()))?,
            );
        }
        Ok(out)
    }

    fn type_name(&self) -> &'static str {
        type_name::<T>()
    }
}

/// KV-aware coder.
///
/// Wire-identical to `PostcardCoder<(K, V)>` for the in-bundle
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
        let pair: (K, V) = from_bytes(bytes)
            .with_context(|| format!("postcard decode into {}", type_name::<(K, V)>()))?;
        Ok(Box::new(vec![pair]))
    }

    fn encode_all(&self, partition: Partition) -> Result<Vec<Vec<u8>>> {
        let vec = partition.downcast::<Vec<(K, V)>>().map_err(|_| {
            anyhow!(
                "partition downcast failed: expected Vec<{}>",
                type_name::<(K, V)>()
            )
        })?;
        let mut out = Vec::with_capacity(vec.len());
        for pair in vec.into_iter() {
            out.push(
                to_allocvec(&pair)
                    .with_context(|| format!("postcard encode {}", type_name::<(K, V)>()))?,
            );
        }
        Ok(out)
    }

    fn type_name(&self) -> &'static str {
        type_name::<(K, V)>()
    }

    fn is_kv(&self) -> bool {
        true
    }

    fn encode_kv_pairs(&self, partition: Partition) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let vec = partition.downcast::<Vec<(K, V)>>().map_err(|_| {
            anyhow!(
                "partition downcast failed: expected Vec<{}>",
                type_name::<(K, V)>()
            )
        })?;
        let mut out = Vec::with_capacity(vec.len());
        for (k, v) in vec.into_iter() {
            let kb = to_allocvec(&k)
                .with_context(|| format!("postcard encode key {}", type_name::<K>()))?;
            let vb = to_allocvec(&v)
                .with_context(|| format!("postcard encode value {}", type_name::<V>()))?;
            out.push((kb, vb));
        }
        Ok(out)
    }
}
