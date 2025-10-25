use std::collections::HashMap;
use std::hash::Hash;
use crate::node::{DynOp, Node};
use crate::pipeline::Pipeline;
use crate::type_token::{Partition, TypeTag, vec_ops_for};
use anyhow::Result;
use serde::{Serialize, de::DeserializeOwned};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use crate::{ExecMode, Runner};
use crate::io::{read_jsonl_vec, write_jsonl_vec};

// ----- RFBound as before -----
pub trait RFBound: 'static + Send + Sync + Clone + Serialize + DeserializeOwned {}
impl<T> RFBound for T where T: 'static + Send + Sync + Clone + Serialize + DeserializeOwned {}

#[derive(Clone)]
pub struct PCollection<T> {
    pub(crate) pipeline: Pipeline,
    pub(crate) id: crate::NodeId,
    _t: PhantomData<T>,
}

// ----- from_vec (TypeToken) -----
pub fn from_vec<T>(p: &Pipeline, data: Vec<T>) -> PCollection<T>
where T: RFBound
{
    let id = p.insert_node(Node::Source {
        payload: Arc::new(data),
        vec_ops: vec_ops_for::<T>(),
        elem_tag: TypeTag::of::<T>(),
    });
    PCollection { pipeline: p.clone(), id, _t: PhantomData }
}

/// Create a collection from any owned iterator (collects into Vec<T>).
pub fn from_iter<T, I>(p: &Pipeline, iter: I) -> PCollection<T>
where
    T: RFBound,
    I: IntoIterator<Item = T>,
{
    from_vec(p, iter.into_iter().collect::<Vec<T>>())
}

/// Read a JSONL file into a typed PCollection<T>.
pub fn read_jsonl<T>(p: &Pipeline, path: impl AsRef<Path>) -> Result<PCollection<T>>
where
    T: RFBound,
{
    let data: Vec<T> = read_jsonl_vec(path)?;
    Ok(from_vec(p, data))
}

// ----- stateless ops (DynOp) -----
struct MapOp<I,O,F>(F, PhantomData<(I,O)>);
impl<I,O,F> DynOp for MapOp<I,O,F>
where I: RFBound, O: RFBound, F: Send + Sync + Fn(&I)->O + 'static
{
    fn apply(&self, input: Partition) -> Partition {
        let v = *input.downcast::<Vec<I>>().expect("MapOp input type");
        let out: Vec<O> = v.iter().map(|i| self.0(i)).collect();
        Box::new(out) as Partition
    }
}
struct FilterOp<T,P>(P, PhantomData<T>);
impl<T,P> DynOp for FilterOp<T,P>
where T: RFBound, P: Send + Sync + Fn(&T)->bool + 'static
{
    fn apply(&self, input: Partition) -> Partition {
        let v = *input.downcast::<Vec<T>>().expect("FilterOp input type");
        let out: Vec<T> = v.into_iter().filter(|t| self.0(t)).collect();
        Box::new(out) as Partition
    }
}
struct FlatMapOp<I,O,F>(F, PhantomData<(I,O)>);
impl<I,O,F> DynOp for FlatMapOp<I,O,F>
where I: RFBound, O: RFBound, F: Send + Sync + Fn(&I)->Vec<O> + 'static
{
    fn apply(&self, input: Partition) -> Partition {
        let v = *input.downcast::<Vec<I>>().expect("FlatMapOp input type");
        let mut out: Vec<O> = Vec::new();
        for i in &v { out.extend(self.0(i)); }
        Box::new(out) as Partition
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
}

impl<T: RFBound> PCollection<T> {
    pub fn collect(self) -> Result<Vec<T>> { self.collect_seq() }
    pub fn collect_seq(self) -> Result<Vec<T>> {
        Runner { mode: ExecMode::Sequential, ..Default::default() }
            .run_collect::<T>(&self.pipeline, self.id)
    }
    pub fn collect_par(self, threads: Option<usize>, partitions: Option<usize>) -> Result<Vec<T>> {
        Runner { mode: ExecMode::Parallel { threads, partitions }, ..Default::default() }
            .run_collect::<T>(&self.pipeline, self.id)
    }
}

impl<T: RFBound> PCollection<T> {
    /// Execute the pipeline and write the result to a JSONL file.
    /// Returns number of records written.
    pub fn write_jsonl(self, path: impl AsRef<Path>) -> Result<usize> {
        let data = self.collect_seq()?; // keep it simple; you can add a _par variant later
        write_jsonl_vec(path, &data)
    }
}

// ---------- Keyed ops ----------

impl<T: RFBound> PCollection<T> {
    /// Derive a key and produce (K, T)
    pub fn key_by<K, F>(self, key_fn: F) -> PCollection<(K, T)>
    where
        K: RFBound + Eq + Hash,
        F: 'static + Send + Sync + Fn(&T) -> K,
    {
        self.map(move |t| (key_fn(t), t.clone()))
    }
}

impl<K: RFBound + Eq + Hash, V: RFBound> PCollection<(K, V)> {
    /// Map only the value: (K, V) -> (K, O)
    pub fn map_values<O, F>(self, f: F) -> PCollection<(K, O)>
    where
        O: RFBound,
        F: 'static + Send + Sync + Fn(&V) -> O,
    {
        // map receives & (K, V); clone K to keep (K, O), not (&K, O)
        self.map(move |kv: &(K, V)| (kv.0.clone(), f(&kv.1)))
    }

    /// Group values by key: (K, V) -> (K, Vec<V>)
    pub fn group_by_key(self) -> PCollection<(K, Vec<V>)> {
        // typed closures: no runtime type checks needed
        let local = Arc::new(|p: Partition| -> Partition {
            let kv = *p.downcast::<Vec<(K, V)>>().expect("GBK local: bad input");
            let mut m: HashMap<K, Vec<V>> = HashMap::new();
            for (k, v) in kv {
                m.entry(k).or_default().push(v);
            }
            Box::new(m) as Partition
        });

        let merge = Arc::new(|parts: Vec<Partition>| -> Partition {
            let mut acc: HashMap<K, Vec<V>> = HashMap::new();
            for p in parts {
                let m = *p.downcast::<HashMap<K, Vec<V>>>().expect("GBK merge: bad part");
                for (k, vs) in m {
                    acc.entry(k).or_default().extend(vs);
                }
            }
            Box::new(acc.into_iter().collect::<Vec<(K, Vec<V>)>>()) as Partition
        });

        let id = self.pipeline.insert_node(Node::GroupByKey { local, merge });
        self.pipeline.connect(self.id, id);
        PCollection { pipeline: self.pipeline, id, _t: PhantomData }
    }
}

// ---------- Combine (per key) ----------

pub trait CombineFn<V, A, O>: Send + Sync + 'static {
    fn create(&self) -> A;
    fn add_input(&self, acc: &mut A, v: V);
    fn merge(&self, acc: &mut A, other: A);
    fn finish(&self, acc: A) -> O;
}

/// Built-in combiner: counts values per key.
#[derive(Clone, Default)]
pub struct Count;
impl<V> CombineFn<V, u64, u64> for Count {
    fn create(&self) -> u64 { 0 }
    fn add_input(&self, acc: &mut u64, _v: V) { *acc += 1; }
    fn merge(&self, acc: &mut u64, other: u64) { *acc += other; }
    fn finish(&self, acc: u64) -> u64 { acc }
}

impl<K: RFBound + Eq + Hash, V: RFBound> PCollection<(K, V)> {
    /// Generic combine-by-key using a user-supplied `CombineFn`
    pub fn combine_values<C, A, O>(self, comb: C) -> PCollection<(K, O)>
    where
        C: CombineFn<V, A, O> + 'static,
        A: Send + 'static + Sync,
        O: RFBound,
    {
        let comb = Arc::new(comb);

        let local = {
            let comb = Arc::clone(&comb);
            Arc::new(move |p: Partition| -> Partition {
                let kv = *p.downcast::<Vec<(K, V)>>().expect("combine local: bad input");
                let mut map: HashMap<K, A> = HashMap::new();
                for (k, v) in kv {
                    comb.add_input(map.entry(k).or_insert_with(|| comb.create()), v);
                }
                Box::new(map) as Partition
            })
        };

        let merge = {
            let comb = Arc::clone(&comb);
            Arc::new(move |parts: Vec<Partition>| -> Partition {
                let mut accs: HashMap<K, A> = HashMap::new();
                for p in parts {
                    let m = *p.downcast::<HashMap<K, A>>().expect("combine merge: bad part");
                    for (k, a) in m {
                        comb.merge(accs.entry(k).or_insert_with(|| comb.create()), a);
                    }
                }
                let out: Vec<(K, O)> = accs.into_iter().map(|(k, a)| (k, comb.finish(a))).collect();
                Box::new(out) as Partition
            })
        };

        let id = self.pipeline.insert_node(Node::CombineValues { local, merge });
        self.pipeline.connect(self.id, id);
        PCollection { pipeline: self.pipeline, id, _t: PhantomData }
    }
}