use std::any::Any;
use std::hash::Hash;
use crate::pipeline::Pipeline;
use crate::node_id::NodeId;
use crate::node::{Node, DynOp};
use crate::runner::{Runner, ExecMode, Partition};
use anyhow::Result;
use serde::{Serialize, de::DeserializeOwned};
use std::marker::PhantomData;
use std::sync::Arc;

pub trait RFBound: 'static + Send + Sync + Clone + Serialize + DeserializeOwned {}
impl<T> RFBound for T where T: 'static + Send + Sync + Clone + Serialize + DeserializeOwned {}

#[derive(Clone)]
pub struct PCollection<T> {
    pub(crate) pipeline: Pipeline,
    pub(crate) id: NodeId,
    _t: PhantomData<T>,
}

pub fn from_vec<T>(p: &Pipeline, data: Vec<T>) -> PCollection<T>
where T: RFBound
{
    let id = p.add_source(data);
    PCollection { pipeline: p.clone(), id, _t: PhantomData }
}

/// ---- Stateless DynOps ----
struct MapOp<I, O, F>(F, PhantomData<(I,O)>);
impl<I, O, F> DynOp for MapOp<I, O, F>
where
    I: RFBound,
    O: RFBound,
    F: Send + Sync + Fn(&I) -> O + 'static,
{
    fn apply(&self, input: Partition) -> Partition {
        let v = *input.downcast::<Vec<I>>().expect("MapOp input");
        let out: Vec<O> = v.iter().map(|i| self.0(i)).collect();
        Box::new(out) as Partition
    }
}
struct FilterOp<T, P>(P, PhantomData<T>);
impl<T,P> DynOp for FilterOp<T,P>
where T: RFBound, P: Send + Sync + Fn(&T)->bool + 'static
{
    fn apply(&self, input: Box<dyn Any + Send + Sync>) -> Box<dyn Any + Send + Sync> {
        let v = *input.downcast::<Vec<T>>().expect("FilterOp input");
        Box::new(v.into_iter().filter(|t| self.0(t)).collect::<Vec<T>>())
    }
}
struct FlatMapOp<I,O,F>(F, PhantomData<(I,O)>);
impl<I,O,F> DynOp for FlatMapOp<I,O,F>
where I: RFBound, O: RFBound, F: Send + Sync + Fn(&I)->Vec<O> + 'static
{
    fn apply(&self, input: Box<dyn Any + Send + Sync>) -> Box<dyn Any + Send + Sync> {
        let v = *input.downcast::<Vec<I>>().expect("FlatMapOp input");
        let mut out: Vec<O> = Vec::new();
        for i in &v { out.extend(self.0(i)); }
        Box::new(out)
    }
}

impl<T: RFBound> PCollection<T> {
    pub fn map<O, F>(self, f: F) -> PCollection<O>
    where O: RFBound, F: 'static + Send + Sync + Fn(&T) -> O
    {
        let op: Arc<dyn DynOp> = Arc::new(MapOp::<T,O,F>(f, PhantomData));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        PCollection { pipeline: self.pipeline, id, _t: PhantomData }
    }

    pub fn filter<F>(self, pred: F) -> PCollection<T>
    where F: 'static + Send + Sync + Fn(&T) -> bool
    {
        let op: Arc<dyn DynOp> = Arc::new(FilterOp::<T,F>(pred, PhantomData));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        PCollection { pipeline: self.pipeline, id, _t: PhantomData }
    }

    pub fn flat_map<O, F>(self, f: F) -> PCollection<O>
    where O: RFBound, F: 'static + Send + Sync + Fn(&T) -> Vec<O>
    {
        let op: Arc<dyn DynOp> = Arc::new(FlatMapOp::<T,O,F>(f, PhantomData));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        PCollection { pipeline: self.pipeline, id, _t: PhantomData }
    }

    pub fn collect_seq(self) -> Result<Vec<T>> {
        let r = Runner { mode: ExecMode::Sequential, ..Default::default() };
        r.run_collect::<T>(&self.pipeline, self.id)
    }
    pub fn collect_par(self, threads: Option<usize>, partitions: Option<usize>) -> Result<Vec<T>> {
        let r = Runner { mode: ExecMode::Parallel { threads, partitions }, ..Default::default() };
        r.run_collect::<T>(&self.pipeline, self.id)
    }
}

// ---- keyed ops (String keys shown for simplicity) ----
impl<T: RFBound> PCollection<T> {
    pub fn key_by<F>(self, key_fn: F) -> PCollection<(String, T)>
    where F: 'static + Send + Sync + Fn(&T) -> String
    {
        self.map(move |t| (key_fn(t), t.clone()))
    }
}
impl<K, V> PCollection<(K, V)>
where
    K: RFBound + Eq + Hash,
    V: RFBound,
{
    pub fn map_values<O, F>(self, f: F) -> PCollection<(K, O)>
    where
        O: RFBound,
        F: 'static + Send + Sync + Fn(&V) -> O,
    {
        self.map(move |kv: &(K, V)| (kv.0.clone(), f(&kv.1)))
    }
}
impl<K: RFBound, V: RFBound> PCollection<(K, V)> {
    pub fn group_by_key(self) -> PCollection<(K, Vec<V>)> {
        // For now our runner supports String/u64 arms; keep K=String in examples/tests
        let id = self.pipeline.insert_node(Node::GroupByKey);
        self.pipeline.connect(self.id, id);
        PCollection { pipeline: self.pipeline, id, _t: PhantomData }
    }
}

// Combiner trait + Count as before:
pub trait CombineFn<V, A, O>: Send + Sync + 'static {
    fn create(&self) -> A; fn add_input(&self, acc: &mut A, v: V);
    fn merge(&self, acc: &mut A, other: A); fn finish(&self, acc: A) -> O;
}
#[derive(Clone, Default)]
pub struct Count;
impl<V> CombineFn<V, u64, u64> for Count {
    fn create(&self) -> u64 { 0 }
    fn add_input(&self, acc: &mut u64, _v: V) { *acc += 1; }
    fn merge(&self, acc: &mut u64, other: u64) { *acc += other; }
    fn finish(&self, acc: u64) -> u64 { acc }
}

impl<K: RFBound, V: RFBound> PCollection<(K, V)> {
    pub fn combine_values<C, A, O>(self, comb: C) -> PCollection<(K, O)>
    where
        C: CombineFn<V, A, O> + 'static,
        A: Send + 'static,
        O: RFBound,
    {
        let id = self
            .pipeline
            .insert_node(Node::CombineValues(Arc::new(comb)));
        self.pipeline.connect(self.id, id);
        PCollection { pipeline: self.pipeline, id, _t: PhantomData }
    }
}