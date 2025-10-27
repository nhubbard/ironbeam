use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use crate::{PCollection, RFBound};
use crate::collection::{FilterValuesOp, MapValuesOp};
use crate::node::{DynOp, Node};

impl<K: RFBound + Eq + Hash, V: RFBound> PCollection<(K, V)> {
    /// Map only the value: (K, V) -> (K, O)
    pub fn map_values<O, F>(self, f: F) -> PCollection<(K, O)>
    where
        O: RFBound,
        F: 'static + Send + Sync + Fn(&V) -> O,
    {
        let op: Arc<dyn DynOp> = Arc::new(MapValuesOp::<K, V, O, F>(f, PhantomData));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }

    /// Filter by value: keep entries where `pred(&V)` is true.
    pub fn filter_values<F>(self, pred: F) -> PCollection<(K, V)>
    where
        F: 'static + Send + Sync + Fn(&V) -> bool,
    {
        let op: Arc<dyn DynOp> = Arc::new(FilterValuesOp::<K, V, F>(pred, PhantomData));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }
}