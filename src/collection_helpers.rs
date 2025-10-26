use crate::collection::{LiftableCombiner, SideInput, SideMap};
use crate::node::{DynOp, Node};
use crate::type_token::{vec_ops_for, TypeTag};
use crate::{CombineFn, ExecMode, PCollection, Partition, Pipeline, RFBound, Runner};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use anyhow::Result;

#[cfg(feature = "io-csv")]
use crate::io::csv::{build_csv_shards, CsvShards, CsvVecOps};

#[cfg(feature = "io-jsonl")]
use crate::io::jsonl::{build_jsonl_shards, read_jsonl_vec, write_jsonl_vec, JsonlShards, JsonlVecOps};

#[cfg(all(feature = "io-jsonl", feature = "parallel-io"))]
use crate::io::jsonl::write_jsonl_par;

#[cfg(feature = "io-parquet")]
use crate::io::parquet::{build_parquet_shards, ParquetShards, ParquetVecOps};

// ----- from_vec -----
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
#[cfg(feature = "io-jsonl")]
pub fn read_jsonl<T>(p: &Pipeline, path: impl AsRef<Path>) -> Result<PCollection<T>>
where
    T: RFBound + DeserializeOwned,
{
    let data: Vec<T> = read_jsonl_vec(path)?;
    Ok(from_vec(p, data))
}

impl<T: RFBound> PCollection<T> {
    pub fn map<O, F>(self, f: F) -> PCollection<O>
    where O: RFBound, F: 'static + Send + Sync + Fn(&T) -> O
    {
        let op: Arc<dyn DynOp> = Arc::new(crate::collection::MapOp::<T, O, F>(f, PhantomData));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        PCollection { pipeline: self.pipeline, id, _t: PhantomData }
    }

    pub fn filter<F>(self, pred: F) -> PCollection<T>
    where F: 'static + Send + Sync + Fn(&T) -> bool
    {
        let op: Arc<dyn DynOp> = Arc::new(crate::collection::FilterOp::<T, F>(pred, PhantomData));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        PCollection { pipeline: self.pipeline, id, _t: PhantomData }
    }

    pub fn flat_map<O, F>(self, f: F) -> PCollection<O>
    where O: RFBound, F: 'static + Send + Sync + Fn(&T) -> Vec<O>
    {
        let op: Arc<dyn DynOp> = Arc::new(crate::collection::FlatMapOp::<T, O, F>(f, PhantomData));
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

#[cfg(feature = "io-jsonl")]
impl<T: RFBound + Serialize> PCollection<T> {
    /// Execute the pipeline and write the result to a JSONL file.
    /// Returns number of records written.
    pub fn write_jsonl(self, path: impl AsRef<Path>) -> Result<usize> {
        let data = self.collect_seq()?;
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
        // typed closures, so no runtime type checks needed
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

impl<K, V> PCollection<(K, Vec<V>)>
where
    K: RFBound + Eq + Hash,
    V: RFBound,
{
    /// Lifted combine: to be used right after `group_by_key()`.
    /// It skips the GBK barrier by creating accumulators from each group's full value slice.
    pub fn combine_values_lifted<C, A, O>(self, comb: C) -> PCollection<(K, O)>
    where
        C: CombineFn<V, A, O> + LiftableCombiner<V, A, O> + 'static,
        A: Send + Sync + 'static,
        O: RFBound,
    {
        let comb = Arc::new(comb);

        // local: input is Vec<(K, Vec<V>)> → produce HashMap<K, A> by calling C::from_group
        let local = {
            let comb = std::sync::Arc::clone(&comb);
            Arc::new(move |p: Partition| -> Partition {
                let kvv = *p.downcast::<Vec<(K, Vec<V>)>>()
                    .expect("lifted combine local: bad input");
                let mut map: HashMap<K, A> = HashMap::new();
                for (k, vs) in kvv {
                    let acc = comb.build_from_group(&vs); // note: instance call
                    map.insert(k, acc);
                }
                Box::new(map) as Partition
            })
        };

        // merge: identical to your existing combine_values merge — reuse comb to merge A's and finish
        let merge = {
            let comb = std::sync::Arc::clone(&comb);
            Arc::new(move |parts: Vec<Partition>| -> Partition {
                let mut accs: HashMap<K, A> = HashMap::new();
                for p in parts {
                    let m = *p.downcast::<HashMap<K, A>>()
                        .expect("lifted combine merge: bad part");
                    for (k, a) in m {
                        let entry = accs.entry(k).or_insert_with(|| comb.create());
                        comb.merge(entry, a);
                    }
                }
                let out: Vec<(K, O)> = accs
                    .into_iter()
                    .map(|(k, a)| (k, comb.finish(a)))
                    .collect();
                Box::new(out) as Partition
            })
        };

        let id = self.pipeline.insert_node(Node::CombineValues { local, merge });
        self.pipeline.connect(self.id, id);
        PCollection { pipeline: self.pipeline, id, _t: PhantomData }
    }
}

// --------- Sources: JSONL streaming ----------
#[cfg(feature = "io-jsonl")]
pub fn read_jsonl_streaming<T>(
    p: &Pipeline,
    path: impl AsRef<Path>,
    lines_per_shard: usize,
) -> Result<PCollection<T>>
where
    T: RFBound + DeserializeOwned,
{
    let shards: JsonlShards = build_jsonl_shards(path, lines_per_shard)?;
    let id = p.insert_node(Node::Source {
        payload: Arc::new(shards),
        vec_ops: JsonlVecOps::<T>::new(),
        elem_tag: TypeTag::of::<T>(),
    });
    Ok(PCollection { pipeline: p.clone(), id, _t: PhantomData })
}

// --------- Sources: CSV (vector + streaming) ----------
#[cfg(feature = "io-csv")]
pub fn read_csv<T>(p: &Pipeline, path: impl AsRef<Path>, has_headers: bool) -> Result<PCollection<T>>
where
    T: RFBound + DeserializeOwned,
{
    let v = crate::io::csv::read_csv_vec::<T>(path, has_headers)?;
    Ok(from_vec(p, v))
}

#[cfg(feature = "io-csv")]
impl<T: RFBound + Serialize> PCollection<T> {
    pub fn write_csv(self, path: impl AsRef<Path>, has_headers: bool) -> Result<usize> {
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
    ) -> Result<usize> {
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
) -> Result<PCollection<T>>
where
    T: RFBound + DeserializeOwned,
{
    let shards: CsvShards = build_csv_shards(path, has_headers, rows_per_shard)?;
    let id = p.insert_node(Node::Source {
        payload: Arc::new(shards),
        vec_ops: CsvVecOps::<T>::new(),
        elem_tag: TypeTag::of::<T>(),
    });
    Ok(PCollection { pipeline: p.clone(), id, _t: PhantomData })
}

// --------- Sink: parallel JSONL writer (stable order) ----------
#[cfg_attr(docsrs, doc(cfg(all(feature = "io-jsonl", feature = "parallel-io"))))]
#[cfg(all(feature = "io-jsonl", feature = "parallel-io"))]
impl<T: RFBound + Serialize> PCollection<T> {
    /// Execute sequentially and write JSONL in parallel (stable file order).
    pub fn write_jsonl_par(self, path: impl AsRef<Path>, shards: Option<usize>) -> Result<usize> {
        let data = self.collect_seq()?; // deterministic order of elements
        write_jsonl_par(path, &data, shards)
    }
}

#[cfg(feature = "io-parquet")]
impl<T: RFBound + DeserializeOwned + Serialize> PCollection<T> {
    pub fn write_parquet(self, path: impl AsRef<Path>) -> Result<usize> {
        let rows: Vec<T> = self.collect_seq()?;
        crate::io::parquet::write_parquet_vec(path, &rows)
    }
}

#[cfg(feature = "io-parquet")]
pub fn read_parquet_streaming<T>(
    p: &Pipeline,
    path: impl AsRef<Path>,
    groups_per_shard: usize,
) -> Result<PCollection<T>>
where
    T: RFBound + DeserializeOwned,
{
    let shards: ParquetShards = build_parquet_shards(path, groups_per_shard)?;
    let id = p.insert_node(Node::Source {
        payload: Arc::new(shards),
        vec_ops: ParquetVecOps::<T>::new(),
        elem_tag: TypeTag::of::<T>(),
    });
    Ok(PCollection { pipeline: p.clone(), id, _t: PhantomData })
}

impl<T: RFBound + Ord> PCollection<T> {
    /// Collect (seq) and sort to a total order.
    pub fn collect_seq_sorted(self) -> Result<Vec<T>> {
        let mut v = self.collect_seq()?;
        v.sort();
        Ok(v)
    }
}

impl<T: RFBound + Ord> PCollection<T> {
    /// Collect (par) and sort to a total order.
    pub fn collect_par_sorted(self, parts: Option<usize>, chunk: Option<usize>) -> Result<Vec<T>> {
        let mut v = self.collect_par(parts, chunk)?;
        v.sort();
        Ok(v)
    }
}

// Keyed convenience: sort by key only
impl<K: RFBound + Ord, V: RFBound> PCollection<(K, V)> {
    pub fn collect_par_sorted_by_key(self, parts: Option<usize>, chunk: Option<usize>) -> Result<Vec<(K, V)>> {
        let mut v = self.collect_par(parts, chunk)?;
        v.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(v)
    }
}

pub fn side_vec<T: RFBound>(v: Vec<T>) -> SideInput<T> {
    SideInput(Arc::new(v))
}

impl<T: RFBound> PCollection<T> {
    /// Map with read-only side input vector (e.g., lookup table)
    pub fn map_with_side<O, S, F>(self, side: SideInput<S>, f: F) -> PCollection<O>
    where
        O: RFBound, S: RFBound,
        F: 'static + Send + Sync + Fn(&T, &[S]) -> O
    {
        let side_arc = side.0.clone();
        self.map(move |t: &T| f(t, &side_arc))
    }

    /// Filter using side input
    pub fn filter_with_side<S, F>(self, side: SideInput<S>, pred: F) -> PCollection<T>
    where
        S: RFBound,
        F: 'static + Send + Sync + Fn(&T, &[S]) -> bool
    {
        let side_arc = side.0.clone();
        self.filter(move |t: &T| pred(t, &side_arc))
    }
}

pub fn side_hashmap<K: RFBound + Eq + Hash, V: RFBound>(pairs: Vec<(K, V)>) -> SideMap<K, V> {
    SideMap(Arc::new(pairs.into_iter().collect()))
}

impl<T: RFBound> PCollection<T> {
    pub fn map_with_side_map<O, K, V, F>(self, side: SideMap<K, V>, f: F) -> PCollection<O>
    where
        O: RFBound, K: RFBound + Eq + Hash, V: RFBound,
        F: 'static + Send + Sync + Fn(&T, &HashMap<K, V>) -> O
    {
        let side_map = side.0.clone();
        self.map(move |t: &T| f(t, &side_map))
    }
}

impl<T: RFBound> PCollection<T> {
    pub fn try_map<O, E, F>(self, f: F) -> PCollection<Result<O, E>>
    where
        O: RFBound,
        E: 'static + Send + Sync + Clone + std::fmt::Display,
        F: 'static + Send + Sync + Fn(&T) -> Result<O, E>,
    {
        // Result<O,E> now satisfies RFBound because E: Clone
        self.map(move |t| f(t))
    }

    pub fn try_flat_map<O, E, F>(self, f: F) -> PCollection<Result<Vec<O>, E>>
    where
        O: RFBound,
        E: 'static + Send + Sync + Clone + std::fmt::Display,
        F: 'static + Send + Sync + Fn(&T) -> Result<Vec<O>, E>,
    {
        self.map(move |t| f(t))
    }
}

// Fail-fast terminal (keeps errors ergonomic)
impl<T: RFBound, E> PCollection<Result<T, E>>
where
    E: 'static + Send + Sync + Clone + std::fmt::Display,
{
    pub fn collect_fail_fast(self) -> Result<Vec<T>> {
        let mut ok = Vec::new();
        for r in self.collect_seq()? {
            match r {
                Ok(v) => ok.push(v),
                Err(e) => return Err(anyhow::anyhow!("element failed: {}", e)),
            }
        }
        Ok(ok)
    }
}