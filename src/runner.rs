use crate::node::Node;
use crate::pipeline::Pipeline;
use crate::planner::build_plan;
use crate::type_token::Partition;
use crate::NodeId;
use anyhow::{anyhow, bail, Result};
use rayon::prelude::*;
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
pub enum ExecMode {
    Sequential,
    Parallel {
        threads: Option<usize>,
        partitions: Option<usize>,
    },
}

pub struct Runner {
    pub mode: ExecMode,
    pub default_partitions: usize,
}

impl Default for Runner {
    fn default() -> Self {
        Self {
            mode: ExecMode::Parallel {
                threads: None,
                partitions: None,
            },
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
        // Get the optimized plan
        let plan = build_plan(p, terminal)?;
        let chain = plan.chain;
        let suggested_parts = plan.suggested_partitions;

        match self.mode {
            ExecMode::Sequential => exec_seq::<T>(chain),
            ExecMode::Parallel {
                threads,
                partitions,
            } => {
                if let Some(t) = threads {
                    rayon::ThreadPoolBuilder::new()
                        .num_threads(t)
                        .build_global()
                        .ok();
                }
                let parts = partitions
                    .or(suggested_parts)
                    .unwrap_or(self.default_partitions);
                exec_par::<T>(chain, parts)
            }
        }
    }
}

fn exec_seq<T: 'static + Send + Sync + Clone>(chain: Vec<Node>) -> Result<Vec<T>> {
    let mut buf: Option<Partition> = None;

    fn run_subplan_seq(mut chain: Vec<Node>) -> Result<Vec<Partition>> {
        let mut curr: Option<Partition> = None;
        for node in chain.drain(..) {
            curr = Some(match node {
                Node::Source {
                    payload, vec_ops, ..
                } => vec_ops
                    .clone_any(payload.as_ref())
                    .ok_or_else(|| anyhow!("unsupported source vec type"))?,
                Node::Stateless(ops) => ops
                    .into_iter()
                    .fold(curr.take().unwrap(), |acc, op| op.apply(acc)),
                Node::GroupByKey { local, merge } => {
                    let mid = local(curr.take().unwrap());
                    merge(vec![mid])
                }
                Node::CombineValues {
                    local_pairs,
                    local_groups,
                    merge,
                } => {
                    let local = if let Some(lg) = local_groups {
                        lg
                    } else {
                        local_pairs
                    };
                    let mid = local(curr.take().unwrap());
                    merge(vec![mid])
                }
                Node::Materialized(p) => Box::new(p) as Partition,
                Node::CoGroup { .. } => bail!("nested CoGroup not supported in subplan"),
            });
        }
        Ok(vec![curr.unwrap()])
    }

    for node in chain {
        buf = Some(match node {
            Node::CoGroup {
                left_chain,
                right_chain,
                coalesce_left,
                coalesce_right,
                exec,
            } => {
                let mut left_parts = run_subplan_seq((*left_chain).clone())?;
                let mut right_parts = run_subplan_seq((*right_chain).clone())?;

                let left_single: Partition = if left_parts.len() == 1 {
                    left_parts.pop().unwrap()
                } else {
                    coalesce_left(left_parts)
                };
                let right_single: Partition = if right_parts.len() == 1 {
                    right_parts.pop().unwrap()
                } else {
                    coalesce_right(right_parts)
                };

                exec(left_single, right_single)
            }
            Node::Source {
                payload, vec_ops, ..
            } => vec_ops
                .clone_any(payload.as_ref())
                .ok_or_else(|| anyhow!("unsupported source vec type"))?,
            Node::Stateless(ops) => ops
                .into_iter()
                .fold(buf.take().unwrap(), |acc, op| op.apply(acc)),
            Node::GroupByKey { local, merge } => {
                let mid = local(buf.take().unwrap());
                merge(vec![mid])
            }
            Node::CombineValues {
                local_pairs,
                local_groups,
                merge,
            } => {
                let local = if let Some(lg) = local_groups {
                    lg
                } else {
                    local_pairs
                };
                let mid = local(buf.take().unwrap());
                merge(vec![mid])
            }
            Node::Materialized(p) => Box::new(
                p.downcast_ref::<Vec<T>>()
                    .cloned()
                    .ok_or_else(|| anyhow!("terminal type mismatch"))?,
            ) as Partition,
        });
    }

    let out = buf.unwrap();
    let v = *out
        .downcast::<Vec<T>>()
        .map_err(|_| anyhow!("terminal type mismatch"))?;
    Ok(v)
}

fn exec_par<T: 'static + Send + Sync + Clone>(
    chain: Vec<Node>,
    partitions: usize,
) -> Result<Vec<T>> {
    // helper: run a subplan in parallel; returns vector of partitions (may be >1)
    fn run_subplan_par(chain: Vec<Node>, partitions: usize) -> Result<Vec<Partition>> {
        // must start with source
        let (payload, vec_ops, rest) = match &chain[0] {
            Node::Source {
                payload, vec_ops, ..
            } => (payload.clone(), vec_ops.clone(), &chain[1..]),
            _ => bail!("subplan must start with a Source"),
        };
        let total_len = vec_ops.len(payload.as_ref()).unwrap_or(0);
        let parts = partitions.max(1).min(total_len.max(1));
        let mut curr = vec_ops.split(payload.as_ref(), parts).unwrap_or_else(|| {
            vec![
                vec_ops
                    .clone_any(payload.as_ref())
                    .expect("cloneable source"),
            ]
        });

        let mut i = 0usize;
        while i < rest.len() {
            match &rest[i] {
                Node::Stateless(_) => {
                    let mut ops = Vec::new();
                    while i < rest.len() {
                        if let Node::Stateless(more) = &rest[i] {
                            ops.extend(more.iter().cloned());
                            i += 1;
                        } else {
                            break;
                        }
                    }
                    curr = curr
                        .into_par_iter()
                        .map(|p| ops.iter().fold(p, |acc, op| op.apply(acc)))
                        .collect();
                }
                Node::GroupByKey { local, merge } => {
                    let mids: Vec<Partition> = curr.into_par_iter().map(|p| local(p)).collect();
                    curr = vec![merge(mids)];
                    i += 1;
                }
                Node::CombineValues {
                    local_pairs,
                    local_groups,
                    merge,
                } => {
                    let local = if let Some(lg) = local_groups {
                        lg.clone()
                    } else {
                        local_pairs.clone()
                    };
                    let mids: Vec<Partition> = curr.into_par_iter().map(|p| local(p)).collect();
                    curr = vec![merge(mids)];
                    i += 1;
                }
                Node::Source { .. } | Node::Materialized(_) => {
                    bail!("unexpected source/materialized in subplan")
                }
                Node::CoGroup { .. } => bail!("nested CoGroup not supported in subplan"),
            }
        }
        Ok(curr)
    }

    // original head Source
    let (payload, vec_ops, rest) = match &chain[0] {
        Node::Source {
            payload, vec_ops, ..
        } => (Arc::clone(payload), Arc::clone(vec_ops), &chain[1..]),
        _ => bail!("execution plan must start with a Source node"),
    };

    let total_len = vec_ops.len(payload.as_ref()).unwrap_or(0);
    let parts = partitions.max(1).min(total_len.max(1));
    let mut curr = vec_ops.split(payload.as_ref(), parts).unwrap_or_else(|| {
        vec![
            vec_ops
                .clone_any(payload.as_ref())
                .expect("cloneable source"),
        ]
    });

    let mut i = 0usize;
    while i < rest.len() {
        match &rest[i] {
            Node::Stateless(_) => {
                let mut ops = Vec::new();
                while i < rest.len() {
                    if let Node::Stateless(more) = &rest[i] {
                        ops.extend(more.iter().cloned());
                        i += 1;
                    } else {
                        break;
                    }
                }
                curr = curr
                    .into_par_iter()
                    .map(|p| ops.iter().fold(p, |acc, op| op.apply(acc)))
                    .collect();
            }
            Node::GroupByKey { local, merge } => {
                let mids: Vec<Partition> = curr.into_par_iter().map(|p| local(p)).collect();
                curr = vec![merge(mids)];
                i += 1;
            }
            Node::CombineValues {
                local_pairs,
                local_groups,
                merge,
            } => {
                let local = if let Some(lg) = local_groups {
                    lg.clone()
                } else {
                    local_pairs.clone()
                };
                let mids: Vec<Partition> = curr.into_par_iter().map(|p| local(p)).collect();
                curr = vec![merge(mids)];
                i += 1;
            }
            Node::CoGroup {
                left_chain,
                right_chain,
                coalesce_left,
                coalesce_right,
                exec,
            } => {
                let left_parts = run_subplan_par((**left_chain).clone(), partitions)?;
                let right_parts = run_subplan_par((**right_chain).clone(), partitions)?;

                let left_single = if left_parts.len() == 1 {
                    left_parts.into_iter().next().unwrap()
                } else {
                    coalesce_left(left_parts)
                };
                let right_single = if right_parts.len() == 1 {
                    right_parts.into_iter().next().unwrap()
                } else {
                    coalesce_right(right_parts)
                };

                curr = vec![exec(left_single, right_single)];
                i += 1;
            }
            Node::Source { .. } | Node::Materialized(_) => {
                bail!("unexpected additional source/materialized")
            }
        }
    }

    if curr.len() == 1 {
        let one = curr.into_iter().next().unwrap();
        let v = *one
            .downcast::<Vec<T>>()
            .map_err(|_| anyhow!("terminal type mismatch"))?;
        Ok(v)
    } else {
        let mut out = Vec::<T>::new();
        for part in curr {
            let v = *part
                .downcast::<Vec<T>>()
                .map_err(|_| anyhow!("terminal type mismatch"))?;
            out.extend(v);
        }
        Ok(out)
    }
}
