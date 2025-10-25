use crate::node::{DynOp, Node};
use crate::pipeline::{Pipeline, PipelineInner};
use crate::NodeId;
use anyhow::{anyhow, bail, Result};
use rayon::prelude::*;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// Parallel-friendly boxed partition payload
pub(crate) type Partition = Box<dyn Any + Send + Sync>;

#[derive(Clone, Copy, Debug)]
pub enum ExecMode {
    Sequential,
    Parallel { threads: Option<usize>, partitions: Option<usize> },
}

pub struct Runner {
    pub mode: ExecMode,
    pub default_partitions: usize,
}

impl Default for Runner {
    fn default() -> Self {
        Self {
            mode: ExecMode::Parallel { threads: None, partitions: None },
            default_partitions: 2 * num_cpus::get().max(2),
        }
    }
}

impl Runner {
    pub fn run_collect<T: 'static + Send + Sync + Clone>(
        &self,
        p: &Pipeline,
        terminal: NodeId,
    ) -> Result<Vec<T>> {
        // Snapshot graph (Node is Clone)
        let (mut gnodes, edges) = {
            let locked: &PipelineInner = &p.inner.lock().unwrap();
            (locked.nodes.clone(), locked.edges.clone())
        };

        // Linear backwalk: terminal → … → source
        let mut chain: Vec<(NodeId, Node)> = Vec::new();
        let mut cur = terminal;
        loop {
            let n = gnodes
                .remove(&cur)
                .ok_or_else(|| anyhow!("missing node {cur:?}"))?;
            chain.push((cur, n));
            if let Some((from, _)) = edges.iter().find(|(_, to)| *to == cur).cloned() {
                cur = from;
            } else {
                break;
            }
        }
        chain.reverse();

        match self.mode {
            ExecMode::Sequential => exec_seq::<T>(chain),
            ExecMode::Parallel { threads, partitions } => {
                if let Some(t) = threads {
                    // ok() to ignore "already built" on repeated calls in tests
                    rayon::ThreadPoolBuilder::new().num_threads(t).build_global().ok();
                }
                let parts = partitions.unwrap_or(self.default_partitions);
                exec_parallel::<T>(chain, parts)
            }
        }
    }
}

/// Run a fused stateless stage
fn fuse_stateless(ops: Vec<Arc<dyn DynOp>>, input: Partition) -> Partition {
    ops.into_iter().fold(input, |acc, op| op.apply(acc))
}

/// Sequential executor (single partition in-process)
fn exec_seq<T: 'static + Send + Sync + Clone>(plan: Vec<(NodeId, Node)>) -> Result<Vec<T>> {
    let mut buf: Option<Partition> = None;

    for (_id, node) in plan {
        buf = Some(match node {
            Node::Source(payload) | Node::Materialized(payload) => {
                // Permit known concrete Vec payloads; terminal downcast enforces T
                if let Some(v) = payload.downcast_ref::<Vec<T>>() {
                    Box::new(v.clone()) as Partition
                } else if let Some(v) = payload.downcast_ref::<Vec<String>>() {
                    Box::new(v.clone()) as Partition
                } else if let Some(v) = payload.downcast_ref::<Vec<u8>>() {
                    Box::new(v.clone()) as Partition
                } else {
                    return Err(anyhow!("unsupported source payload type in sequential path"));
                }
            }
            Node::Stateless(ops) => fuse_stateless(ops, buf.take().expect("missing input")),
            Node::GroupByKey => gbk(buf.take().expect("missing input")),
            Node::CombineValues(comb) => combine_values(buf.take().expect("missing input"), comb),
        });
    }

    let out = buf.unwrap();
    let out_vec = *out
        .downcast::<Vec<T>>()
        .map_err(|_| anyhow!("terminal type mismatch"))?;
    Ok(out_vec)
}

/// Parallel executor (partition source → fuse stateless per partition → barriers)
fn exec_parallel<T: 'static + Send + Sync + Clone>(
    plan: Vec<(NodeId, Node)>,
    partitions: usize,
) -> Result<Vec<T>> {
    // Expect plan[0] is the only source/materialized node
    let (source_payload, rest) = match &plan[0].1 {
        Node::Source(p) | Node::Materialized(p) => (Arc::clone(p), &plan[1..]),
        _ => bail!("plan must start with a Source or Materialized node"),
    };

    let total_len = dyn_vec_len(source_payload.as_ref()).unwrap_or(0);
    let parts = partitions.max(1).min(total_len.max(1));

    let mut current_partitions: Vec<Partition> = split_any_vec(Arc::clone(&source_payload), parts);

    let mut i = 0usize;
    while i < rest.len() {
        match &rest[i].1 {
            Node::Stateless(_) => {
                // Collect contiguous stateless nodes and fuse them
                let mut ops: Vec<Arc<dyn DynOp>> = Vec::new();
                while i < rest.len() {
                    if let Node::Stateless(more) = &rest[i].1 {
                        ops.extend(more.iter().cloned());
                        i += 1;
                    } else {
                        break;
                    }
                }
                current_partitions = current_partitions
                    .into_par_iter() // move each partition to a worker
                    .map(|chunk| fuse_stateless(ops.clone(), chunk))
                    .collect();
            }
            Node::GroupByKey => {
                // Two-phase GBK (String keys)
                let maps: Vec<HashMap<String, Vec<Partition>>> = current_partitions
                    .iter() // borrow ok; we don't move partitions here
                    .map(|chunk| gbk_local(chunk))
                    .collect();
                let merged = merge_gbk_maps(maps);
                current_partitions = vec![merged];
                i += 1;
            }
            Node::CombineValues(comb) => {
                // Two-phase combine (owned per partition → merge)
                let locals: Vec<Partition> = current_partitions
                    .into_par_iter()
                    .map(|chunk| combine_local_owned(chunk, comb.as_ref()))
                    .collect();
                let merged = merge_combined_maps(locals, comb.as_ref());
                current_partitions = vec![merged];
                i += 1;
            }
            Node::Source(_) | Node::Materialized(_) => {
                bail!("unexpected additional source in plan")
            }
        }
    }

    // Materialize terminal Vec<T>
    if current_partitions.len() == 1 {
        let one = current_partitions.into_iter().next().unwrap();
        let v = *one
            .downcast::<Vec<T>>()
            .map_err(|_| anyhow!("terminal type mismatch"))?;
        Ok(v)
    } else {
        let mut out = Vec::<T>::new();
        for part in current_partitions {
            let v = *part
                .downcast::<Vec<T>>()
                .map_err(|_| anyhow!("terminal type mismatch"))?;
            out.extend(v);
        }
        Ok(out)
    }
}

/// Length helper for known Vec types
fn dyn_vec_len(a: &dyn Any) -> Option<usize> {
    if let Some(v) = a.downcast_ref::<Vec<u8>>() {
        return Some(v.len());
    }
    if let Some(v) = a.downcast_ref::<Vec<String>>() {
        return Some(v.len());
    }
    None
}

/// Split a known Vec payload (String/u8) into N partitions
fn split_any_vec(src: Arc<dyn Any + Send + Sync>, n: usize) -> Vec<Partition> {
    if let Some(v) = src.downcast_ref::<Vec<u8>>() {
        return split_vec(v.clone(), n)
            .into_iter()
            .map(|c| Box::new(c) as Partition)
            .collect();
    }
    if let Some(v) = src.downcast_ref::<Vec<String>>() {
        return split_vec(v.clone(), n)
            .into_iter()
            .map(|c| Box::new(c) as Partition)
            .collect();
    }
    // Fallback: one inert partition; any op expecting Vec<_> will error,
    // which is still a nicer failure than moving an unknown Any.
    vec![Box::new(()) as Partition]
}

fn split_vec<T: Send + Clone>(v: Vec<T>, n: usize) -> Vec<Vec<T>> {
    let len = v.len();
    if n <= 1 || len <= 1 {
        return vec![v];
    }
    let mut out = Vec::with_capacity(n);
    let chunk = len.div_ceil(n);
    for c in v.chunks(chunk) {
        out.push(c.to_vec());
    }
    out
}

/// ---------------- GBK / Combine (String keys supported) ----------------
fn gbk(input: Partition) -> Partition {
    // Try (String, String)
    let input = match input.downcast::<Vec<(String, String)>>() {
        Ok(kv) => {
            let mut m: HashMap<String, Vec<String>> = HashMap::new();
            for (k, v) in *kv {
                m.entry(k).or_default().push(v);
            }
            return Box::new(m.into_iter().collect::<Vec<_>>()) as Partition;
        }
        Err(boxed) => boxed,
    };
    // Try (String, u64)
    let kv = input
        .downcast::<Vec<(String, u64)>>()
        .expect("GBK: unsupported input type");
    let mut m: HashMap<String, Vec<u64>> = HashMap::new();
    for (k, v) in *kv {
        m.entry(k).or_default().push(v);
    }
    Box::new(m.into_iter().collect::<Vec<_>>()) as Partition
}

fn gbk_local(input: &Partition) -> HashMap<String, Vec<Partition>> {
    if let Some(kv) = input.downcast_ref::<Vec<(String, String)>>() {
        let mut m: HashMap<String, Vec<Partition>> = HashMap::new();
        for (k, v) in kv.iter() {
            m.entry(k.clone())
                .or_default()
                .push(Box::new(v.clone()) as Partition);
        }
        return m;
    }
    if let Some(kv) = input.downcast_ref::<Vec<(String, u64)>>() {
        let mut m: HashMap<String, Vec<Partition>> = HashMap::new();
        for (k, v) in kv.iter() {
            m.entry(k.clone())
                .or_default()
                .push(Box::new(*v) as Partition);
        }
        return m;
    }
    panic!("gbk_local: unsupported input type");
}

fn merge_gbk_maps(maps: Vec<HashMap<String, Vec<Partition>>>) -> Partition {
    let mut merged: HashMap<String, Vec<Partition>> = HashMap::new();
    for m in maps {
        for (k, vs) in m {
            merged.entry(k).or_default().extend(vs);
        }
    }
    // Try to materialize to (String, Vec<String>) first, else (String, Vec<u64>)
    if merged
        .values()
        .next()
        .and_then(|v| v.first())
        .map(|x| x.is::<String>())
        == Some(true)
    {
        let out: Vec<(String, Vec<String>)> = merged
            .into_iter()
            .map(|(k, vs)| {
                let vv = vs
                    .into_iter()
                    .map(|b| *b.downcast::<String>().unwrap())
                    .collect();
                (k, vv)
            })
            .collect();
        Box::new(out) as Partition
    } else {
        let out: Vec<(String, Vec<u64>)> = merged
            .into_iter()
            .map(|(k, vs)| {
                let vv = vs
                    .into_iter()
                    .map(|b| *b.downcast::<u64>().unwrap())
                    .collect();
                (k, vv)
            })
            .collect();
        Box::new(out) as Partition
    }
}

fn combine_values(input: Partition, comb: Arc<dyn Any + Send + Sync>) -> Partition {
    if comb.is::<crate::collection::Count>() {
        // Try (String, u64)
        let input = match input.downcast::<Vec<(String, u64)>>() {
            Ok(kv) => {
                let mut acc: HashMap<String, u64> = HashMap::new();
                for (k, v) in *kv {
                    *acc.entry(k).or_insert(0) += v;
                }
                return Box::new(acc.into_iter().collect::<Vec<_>>()) as Partition;
            }
            Err(boxed) => boxed,
        };
        // Else (String, String) → count values
        let kv = input
            .downcast::<Vec<(String, String)>>()
            .expect("combine_values(Count): unsupported input type");
        let mut acc: HashMap<String, u64> = HashMap::new();
        for (k, _) in *kv {
            *acc.entry(k).or_insert(0) += 1;
        }
        return Box::new(acc.into_iter().collect::<Vec<_>>()) as Partition;
    }
    panic!("combine_values: unsupported combiner/type");
}

/// Owned per-partition combine (used with into_par_iter)
fn combine_local_owned(input: Partition, comb: &dyn Any) -> Partition {
    if comb.is::<crate::collection::Count>() {
        // First try (String, u64)
        let input = match input.downcast::<Vec<(String, u64)>>() {
            Ok(kv) => {
                let mut acc: HashMap<String, u64> = HashMap::new();
                for (k, v) in *kv {
                    *acc.entry(k).or_insert(0) += v;
                }
                return Box::new(acc) as Partition;
            }
            Err(boxed) => boxed, // recover ownership to try next type
        };

        // Then try (String, String)
        let kv = input
            .downcast::<Vec<(String, String)>>()
            .expect("combine_local_owned(Count): unsupported input type");
        let mut acc: HashMap<String, u64> = HashMap::new();
        for (k, _) in *kv {
            *acc.entry(k).or_insert(0) += 1;
        }
        return Box::new(acc) as Partition;
    }
    panic!("combine_local_owned: unsupported combiner/type");
}

fn merge_combined_maps(parts: Vec<Partition>, comb: &dyn Any) -> Partition {
    if comb.is::<crate::collection::Count>() {
        let mut acc: HashMap<String, u64> = HashMap::new();
        for p in parts {
            let m = *p
                .downcast::<HashMap<String, u64>>()
                .expect("combine merge type");
            for (k, v) in m {
                *acc.entry(k).or_insert(0) += v;
            }
        }
        return Box::new(acc.into_iter().collect::<Vec<_>>()) as Partition;
    }
    panic!("merge_combined_maps: unsupported");
}