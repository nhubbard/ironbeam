use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use serde::de::DeserializeOwned;
use serde::Serialize;
use crate::node_id::NodeId;
use crate::pipeline::Pipeline;

/// A typed handle to a node's output.
pub struct PCollection<T> {
    pipeline: Pipeline,
    id: NodeId,
    _t: PhantomData<T>,
}

impl<T> Clone for PCollection<T> {
    fn clone(&self) -> Self {
        Self {
            pipeline: Pipeline { inner: Arc::clone(&self.pipeline.inner) },
            id: self.id,
            _t: PhantomData,
        }
    }
}

pub trait RFBound: 'static + Send + Sync + Clone + Serialize + DeserializeOwned {}
impl<T> RFBound for T where T: 'static + Send + Sync + Clone + Serialize + DeserializeOwned {}

/// -------- Sources --------
pub fn from_vec<T>(p: &Pipeline, data: Vec<T>) -> PCollection<T>
where
    T: RFBound,
{
    let id = p.write_vec(data);
    PCollection { pipeline: p.clone(), id, _t: PhantomData }
}

/// -------- Transforms (eager execution into new node) --------
impl<T> PCollection<T>
where
    T: RFBound,
{
    /// Map T -> O
    pub fn map<O, F>(self, f: F) -> PCollection<O>
    where
        O: RFBound,
        F: 'static + Send + Sync + Fn(&T) -> O,
    {
        let input: Vec<T> = self.pipeline.read_vec(self.id)
            .expect("internal: source vector missing");
        let out: Vec<O> = input.iter().map(f).collect();
        let out_id = self.pipeline.write_vec(out);
        self.pipeline.connect(self.id, out_id);
        PCollection { pipeline: self.pipeline, id: out_id, _t: PhantomData }
    }

    /// Filter T with predicate(&T) -> bool
    pub fn filter<F>(self, pred: F) -> PCollection<T>
    where
        F: 'static + Send + Sync + Fn(&T) -> bool,
    {
        let input: Vec<T> = self.pipeline.read_vec(self.id)
            .expect("internal: source vector missing");
        let out: Vec<T> = input.into_iter().filter(|t| pred(t)).collect();
        let out_id = self.pipeline.write_vec(out);
        self.pipeline.connect(self.id, out_id);
        PCollection { pipeline: self.pipeline, id: out_id, _t: PhantomData }
    }

    /// FlatMap T -> Vec<O>
    pub fn flat_map<O, F>(self, f: F) -> PCollection<O>
    where
        O: RFBound,
        F: 'static + Send + Sync + Fn(&T) -> Vec<O>,
    {
        let input: Vec<T> = self.pipeline.read_vec(self.id)
            .expect("internal: source vector missing");
        let mut out: Vec<O> = Vec::new();
        for t in &input {
            out.extend(f(t));
        }
        let out_id = self.pipeline.write_vec(out);
        self.pipeline.connect(self.id, out_id);
        PCollection { pipeline: self.pipeline, id: out_id, _t: PhantomData }
    }

    /// Materialize result as Vec<T>.
    pub fn collect(self) -> anyhow::Result<Vec<T>> {
        self.pipeline.read_vec::<T>(self.id)
    }
}

/// -------- Keyed transforms --------
/// key_by: T -> (K, T)
impl<T> PCollection<T>
where
    T: RFBound,
{
    pub fn key_by<K, F>(self, key_fn: F) -> PCollection<(K, T)>
    where
        K: RFBound + Eq + Hash,
        F: 'static + Send + Sync + Fn(&T) -> K,
    {
        let input: Vec<T> = self.pipeline.read_vec(self.id)
            .expect("internal: source vector missing");
        let out: Vec<(K, T)> = input.into_iter().map(|t| (key_fn(&t), t)).collect();
        let out_id = self.pipeline.write_vec(out);
        self.pipeline.connect(self.id, out_id);
        PCollection {
            pipeline: self.pipeline,
            id: out_id,
            _t: PhantomData
        }
    }
}

/// map_values: (K,V) -> (K,O)
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
        let input: Vec<(K, V)> = self.pipeline.read_vec(self.id)
            .expect("internal: source vector missing");
        let out: Vec<(K, O)> = input.into_iter().map(|(k, v)| (k, f(&v))).collect();
        let out_id = self.pipeline.write_vec(out);
        self.pipeline.connect(self.id, out_id);
        PCollection { pipeline: self.pipeline, id: out_id, _t: PhantomData }
    }
}

/// group_by_key: (K,V) -> (K, Vec<V>)
impl<K, V> PCollection<(K, V)>
where
    K: RFBound + Eq + Hash,
    V: RFBound,
{
    pub fn group_by_key(self) -> PCollection<(K, Vec<V>)> {
        let input: Vec<(K, V)> = self.pipeline.read_vec(self.id)
            .expect("internal: source vector missing");
        let mut map: HashMap<K, Vec<V>> = HashMap::new();
        for (k, v) in input.into_iter() {
            map.entry(k).or_default().push(v);
        }
        let out: Vec<(K, Vec<V>)> = map.into_iter().collect();
        let out_id = self.pipeline.write_vec(out);
        self.pipeline.connect(self.id, out_id);
        PCollection { pipeline: self.pipeline, id: out_id, _t: PhantomData }
    }
}

/// -------- Combine (per key) --------
pub trait CombineFn<V, A, O>: Send + Sync + 'static {
    fn create(&self) -> A;
    fn add_input(&self, acc: &mut A, v: V);
    fn merge(&self, acc: &mut A, other: A);
    fn finish(&self, acc: A) -> O;
}

/// Built-in: Count values
pub struct Count;
impl<V> CombineFn<V, u64, u64> for Count {
    fn create(&self) -> u64 { 0 }
    fn add_input(&self, acc: &mut u64, _v: V) { *acc += 1; }
    fn merge(&self, acc: &mut u64, other: u64) { *acc += other; }
    fn finish(&self, acc: u64) -> u64 { acc }
}

impl<K, V> PCollection<(K, V)>
where
    K: RFBound + Eq + Hash,
    V: RFBound,
{
    /// combine_values: (K,V) -> (K,O) using provided CombineFn
    pub fn combine_values<C, A, O>(self, comb: C) -> PCollection<(K, O)>
    where
        C: CombineFn<V, A, O>,
        A: Send + 'static,
        O: RFBound,
    {
        let input: Vec<(K, V)> = self.pipeline.read_vec(self.id)
            .expect("internal: source vector missing");
        // pre-aggregate per key
        let mut accs: HashMap<K, A> = HashMap::new();
        for (k, v) in input {
            let entry = accs.entry(k).or_insert_with(|| comb.create());
            comb.add_input(entry, v);
        }
        // finalize
        let out: Vec<(K, O)> = accs
            .into_iter()
            .map(|(k, a)| (k, comb.finish(a)))
            .collect();
        let out_id = self.pipeline.write_vec(out);
        self.pipeline.connect(self.id, out_id);
        PCollection { pipeline: self.pipeline, id: out_id, _t: PhantomData }
    }
}