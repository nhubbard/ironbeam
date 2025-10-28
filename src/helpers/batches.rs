use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use crate::{PCollection, RFBound};
use crate::collection::BatchMapOp;
use crate::node::{DynOp, Node};

impl<T: RFBound> PCollection<T> {
    /// Batch map: applies `f` to consecutive slices of size `batch_size`, concatenates the results.
    /// Great for expensive transforms where per-element closures are too chatty.
    pub fn map_batches<O, F>(self, batch_size: usize, f: F) -> PCollection<O>
    where
        O: RFBound,
        F: 'static + Send + Sync + Fn(&[T]) -> Vec<O>,
    {
        let op: Arc<dyn DynOp> = Arc::new(BatchMapOp::<T, O, F>(
            batch_size,
            f,
            PhantomData,
        ));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }
}

impl<K: RFBound + Eq + Hash, V: RFBound> PCollection<(K, V)> {
    /// Batch map only the value: runs `f` on consecutive value slices of size `batch_size`,
    /// preserves keys and order within each partition.
    ///
    /// **Contract:** `f(chunk).len() == chunk.len()`. If not, this will `assert!` and panic.
    pub fn map_values_batches<O, F>(self, batch_size: usize, f: F) -> PCollection<(K, O)>
    where
        O: RFBound,
        F: 'static + Send + Sync + Fn(&[V]) -> Vec<O>,
    {
        let op: Arc<dyn DynOp> = Arc::new(crate::collection::BatchMapValuesOp::<K, V, O, F>(
            batch_size,
            f,
            PhantomData,
        ));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }
}