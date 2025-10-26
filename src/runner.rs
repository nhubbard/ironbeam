// src/runner.rs

use crate::node::Node;
use crate::pipeline::Pipeline;
use crate::NodeId;
use anyhow::{anyhow, bail, Result};
use rayon::prelude::*;
use std::sync::Arc;
use crate::type_token::Partition;

pub struct Shard { idx: usize, data: Partition }

#[derive(Clone, Copy, Debug)]
pub enum ExecMode { Sequential, Parallel { threads: Option<usize>, partitions: Option<usize> } }

pub struct Runner {
    pub mode: ExecMode,
    pub default_partitions: usize,
}

impl Default for Runner {
    fn default() -> Self {
        Self { mode: ExecMode::Parallel { threads: None, partitions: None }, default_partitions: 2 * num_cpus::get().max(2) }
    }
}

impl Runner {
    pub fn run_collect<T: 'static + Send + Sync + Clone>(&self, p: &Pipeline, terminal: NodeId) -> Result<Vec<T>> {
        let (mut nodes, edges) = p.snapshot();

        // linear backwalk
        let mut chain: Vec<Node> = Vec::new();
        let mut cur = terminal;
        loop {
            let n = nodes.remove(&cur).ok_or_else(|| anyhow!("missing node {cur:?}"))?;
            chain.push(n);
            if let Some((from, _)) = edges.iter().find(|(_, to)| *to == cur).cloned() { cur = from; } else { break; }
        }
        chain.reverse();

        match self.mode {
            ExecMode::Sequential => exec_seq::<T>(chain),
            ExecMode::Parallel { threads, partitions } => {
                if let Some(t) = threads { rayon::ThreadPoolBuilder::new().num_threads(t).build_global().ok(); }
                exec_par::<T>(chain, partitions.unwrap_or(self.default_partitions))
            }
        }
    }
}

fn exec_seq<T: 'static + Send + Sync + Clone>(chain: Vec<Node>) -> Result<Vec<T>> {
    let mut buf: Option<Partition> = None;

    for node in chain {
        buf = Some(match node {
            Node::Source { payload, vec_ops, .. } => {
                vec_ops.clone_any(payload.as_ref()).ok_or_else(|| anyhow!("unsupported source vec type"))?
            }
            Node::Stateless(ops) => ops.into_iter().fold(buf.take().unwrap(), |acc, op| op.apply(acc)),
            Node::GroupByKey { local, merge } => {
                let mid = local(buf.take().unwrap());
                merge(vec![mid])
            }
            Node::CombineValues { local, merge } => {
                let mid = local(buf.take().unwrap());
                merge(vec![mid])
            }
            Node::Materialized(p) => {
                // Keep for convenience if you have tests that insert Materialized later in the chain.
                // We only support terminal Vec<T> here.
                Box::new(p.downcast_ref::<Vec<T>>().cloned().ok_or_else(|| anyhow!("terminal type mismatch"))?) as Partition
            }
        });
    }

    let out = buf.unwrap();
    let v = *out.downcast::<Vec<T>>().map_err(|_| anyhow!("terminal type mismatch"))?;
    Ok(v)
}

fn exec_par<T: 'static + Send + Sync + Clone>(chain: Vec<Node>, partitions: usize) -> Result<Vec<T>> {
    // Require a Source as the first node (keeps partitioning generic & simple)
    let (payload, vec_ops, rest) = match &chain[0] {
        Node::Source { payload, vec_ops, .. } => (Arc::clone(payload), Arc::clone(vec_ops), &chain[1..]),
        _ => bail!("execution plan must start with a Source node"),
    };

    // Ask the source for a length hint and split into at most `partitions` pieces.
    let total_len = vec_ops.len(payload.as_ref()).unwrap_or(0);
    let target_parts = partitions.max(1).min(total_len.max(1));

    // Split if possible; otherwise fall back to a single clone.
    let initial_parts: Vec<Partition> = vec_ops
        .split(payload.as_ref(), target_parts)
        .unwrap_or_else(|| vec![vec_ops.clone_any(payload.as_ref()).expect("cloneable source")]);

    // Wrap partitions with stable indices so we can maintain deterministic order.
    let mut curr: Vec<Shard> = initial_parts
        .into_iter()
        .enumerate()
        .map(|(idx, data)| Shard { idx, data })
        .collect();

    // Walk the rest of the chain. Fuse stateless ops; collapse at barriers (GBK/Combine).
    let mut i = 0usize;
    while i < rest.len() {
        match &rest[i] {
            Node::Stateless(_) => {
                // Collect a fused block of stateless ops
                let mut ops = Vec::new();
                while i < rest.len() {
                    if let Node::Stateless(more) = &rest[i] {
                        ops.extend(more.iter().cloned());
                        i += 1;
                    } else {
                        break;
                    }
                }
                // Apply the fused block per-shard (can run in parallel safely)
                curr = curr
                    .into_par_iter()
                    .map(|Shard { idx, data }| {
                        let data2 = ops.iter().fold(data, |acc, op| op.apply(acc));
                        Shard { idx, data: data2 }
                    })
                    .collect();
            }
            Node::GroupByKey { local, merge } => {
                // Local per-shard aggregation, then global merge as a barrier
                let mids: Vec<Partition> = curr
                    .into_par_iter()
                    .map(|s| local(s.data))
                    .collect();
                let merged = merge(mids);
                // After a barrier, we reset to a single shard with idx=0
                curr = vec![Shard { idx: 0, data: merged }];
                i += 1;
            }
            Node::CombineValues { local, merge } => {
                // Local per-shard combine, then global merge as a barrier
                let mids: Vec<Partition> = curr
                    .into_par_iter()
                    .map(|s| local(s.data))
                    .collect();
                let merged = merge(mids);
                curr = vec![Shard { idx: 0, data: merged }];
                i += 1;
            }
            Node::Source { .. } | Node::Materialized(_) => bail!("unexpected additional source/materialized"),
        }
    }

    // Terminal: concatenate deterministically in shard-index order.
    if curr.len() == 1 {
        let one = curr.into_iter().next().unwrap().data;
        let v = *one.downcast::<Vec<T>>().map_err(|_| anyhow!("terminal type mismatch"))?;
        Ok(v)
    } else {
        // Make order explicit & robust to future refactors
        curr.sort_by_key(|s| s.idx);
        let mut out = Vec::<T>::new();
        for Shard { data, .. } in curr {
            let v = *data
                .downcast::<Vec<T>>()
                .map_err(|_| anyhow!("terminal type mismatch"))?;
            out.extend(v);
        }
        Ok(out)
    }
}