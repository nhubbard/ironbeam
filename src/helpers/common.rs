use std::marker::PhantomData;
use std::sync::Arc;
use crate::{ExecMode, PCollection, RFBound, Runner};
use crate::node::{DynOp, Node};

impl<T: RFBound> PCollection<T> {
    pub fn map<O, F>(self, f: F) -> PCollection<O>
    where
        O: RFBound,
        F: 'static + Send + Sync + Fn(&T) -> O,
    {
        let op: Arc<dyn DynOp> = Arc::new(crate::collection::MapOp::<T, O, F>(f, PhantomData));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }

    pub fn filter<F>(self, pred: F) -> PCollection<T>
    where
        F: 'static + Send + Sync + Fn(&T) -> bool,
    {
        let op: Arc<dyn DynOp> = Arc::new(crate::collection::FilterOp::<T, F>(pred, PhantomData));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }

    pub fn flat_map<O, F>(self, f: F) -> PCollection<O>
    where
        O: RFBound,
        F: 'static + Send + Sync + Fn(&T) -> Vec<O>,
    {
        let op: Arc<dyn DynOp> = Arc::new(crate::collection::FlatMapOp::<T, O, F>(f, PhantomData));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }
}

impl<T: RFBound> PCollection<T> {
    pub fn collect(self) -> anyhow::Result<Vec<T>> {
        self.collect_seq()
    }
    pub fn collect_seq(self) -> anyhow::Result<Vec<T>> {
        Runner {
            mode: ExecMode::Sequential,
            ..Default::default()
        }
        .run_collect::<T>(&self.pipeline, self.id)
    }
    pub fn collect_par(self, threads: Option<usize>, partitions: Option<usize>) -> anyhow::Result<Vec<T>> {
        Runner {
            mode: ExecMode::Parallel {
                threads,
                partitions,
            },
            ..Default::default()
        }
        .run_collect::<T>(&self.pipeline, self.id)
    }
}