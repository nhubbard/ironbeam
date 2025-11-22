//! Join helpers built on top of a co-group plan.
//!
//! These helpers construct a `CoGroup` node that:
//! 1) snapshots the left and right subplans ending at the provided `PCollection`s,
//! 2) replays each subplan into intermediate, coalesced `Vec<(K, V)>` / `Vec<(K, W)>` buffers,
//! 3) executes a join-specific closure over those buffers to emit the final joined rows.
//!
//! All joins are **key-based** and require `K: Eq + Hash + RFBound`. The left and right value
//! types must satisfy `RFBound` as usual. The resulting collection is another `PCollection` in
//! the same pipeline.
//!
//! ## Available operations
//! - [`PCollection::join_inner`](crate::PCollection::join_inner) - Inner join on key
//! - [`PCollection::join_left`](crate::PCollection::join_left) - Left outer join on key
//! - [`PCollection::join_right`](crate::PCollection::join_right) - Right outer join on key
//! - [`PCollection::join_full`](crate::PCollection::join_full) - Full outer join on key
//!
//! ### Notes
//! - The co-group strategy avoids materializing the entire pipeline at once; each subplan is run
//!   to a single partition, then joined.
//! - Ordering of output rows is not guaranteed; if you need deterministic ordering for assertions,
//!   call `.collect_par_sorted_by_key(None, None)` or `.collect_seq_sorted()` where appropriate.
//! - For very large datasets, consider splitting upstream with meaningful partitioning and relying
//!   on any later combiners/GBK to reduce volume prior to any other joins.
//!
//! ## Examples
//! Inner / left / right / full joins:
//! ```no_run
//! use ironbeam::*;
//! use anyhow::Result;
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//! let left  = from_vec(&p, vec![("a".to_string(), 1u32), ("a".to_string(), 2), ("b".to_string(), 3)]);
//! let right = from_vec(&p, vec![("a".to_string(), "x".to_string()), ("c".to_string(), "y".to_string())]);
//!
//! let j_inner = left.clone().join_inner(&right);
//! let j_left  = left.clone().join_left(&right);
//! let j_right = left.clone().join_right(&right);
//! let j_full  = left.clone().join_full(&right);
//!
//! // materialize (optionally sort for stable assertions)
//! let _ = j_inner.collect_par_sorted_by_key(None, None)?;
//! let _ = j_left.collect_par_sorted_by_key(None, None)?;
//! let _ = j_right.collect_par_sorted_by_key(None, None)?;
//! let _ = j_full.collect_par_sorted_by_key(None, None)?;
//! # Ok(()) }
//! ```

use crate::node::Node;
use crate::type_token::{TypeTag, vec_ops_for};
use crate::{NodeId, PCollection, Partition, Pipeline, RFBound};
use anyhow::{Result, anyhow};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

/// Build a linear execution chain ending at `terminal` by snapshotting the pipeline
/// and walking backwards through single-input edges.
///
/// This is an internal helper used by joins to capture each side's subplan so that
/// the runner can execute them as independent "subplans" before the co-group step.
///
/// # Errors
/// Returns an error if the pipeline snapshot is missing a referenced node.
fn chain_from(p: &Pipeline, terminal: NodeId) -> Result<Vec<Node>> {
    let (mut nodes, edges) = p.snapshot();
    let mut chain = Vec::<Node>::new();
    let mut cur = terminal;
    loop {
        let n = nodes
            .remove(&cur)
            .ok_or_else(|| anyhow!("missing node {cur:?}"))?;
        chain.push(n);
        if let Some((from, _)) = edges.iter().find(|(_, to)| *to == cur).copied() {
            cur = from;
        } else {
            break;
        }
    }
    chain.reverse();
    Ok(chain)
}

/// Insert a tiny dummy `Source` so the outer co-group plan always starts with a
/// `Source` node (as expected by the runner).
///
/// The dummy payload is a `Vec<u8>` of length 1. It does not participate in the
/// join semantics--it's only a structural anchor for the execution plan.
fn insert_dummy_source(p: &Pipeline) -> NodeId {
    p.insert_node(Node::Source {
        payload: Arc::new(vec![0u8]),
        vec_ops: vec_ops_for::<u8>(),
        elem_tag: TypeTag::of::<u8>(),
    })
}

impl<K, V> PCollection<(K, V)>
where
    K: RFBound + Eq + Hash,
    V: RFBound,
{
    /// Inner join on a key with another `(K, W)` -> `(K, (V, W))`.
    ///
    /// Emits one row for every `(k, v)` on the left and `(k, w)` on the right with the same `k`.
    ///
    /// # Example
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let left  = from_vec(&p, vec![("a".to_string(), 1u32), ("b".to_string(), 3u32)]);
    /// let right = from_vec(&p, vec![("a".to_string(), "x".to_string())]);
    ///
    /// let joined = left.join_inner(&right);
    /// let _ = joined.collect_par_sorted_by_key(None, None)?;
    /// # Ok(()) }
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if types are mismatched or if the chain building operation fails.
    #[must_use]
    pub fn join_inner<W>(&self, right: &PCollection<(K, W)>) -> PCollection<(K, (V, W))>
    where
        W: RFBound,
    {
        // Build subchains
        let left_chain = chain_from(&self.pipeline, self.id).expect("left chain build");
        let right_chain = chain_from(&right.pipeline, right.id).expect("right chain build");

        // Exec closure: Vec<(K,V)>, Vec<(K,W)> -> Vec<(K,(V,W))>
        let exec = Arc::new(|left_part: Partition, right_part: Partition| {
            let left_rows = *left_part
                .downcast::<Vec<(K, V)>>()
                .expect("cogroup exec: left type Vec<(K,V)>");
            let right_rows = *right_part
                .downcast::<Vec<(K, W)>>()
                .expect("cogroup exec: right type Vec<(K,W)>");

            let mut lm: HashMap<K, Vec<V>> = HashMap::new();
            for (k, v) in left_rows {
                lm.entry(k).or_default().push(v);
            }
            let mut rm: HashMap<K, Vec<W>> = HashMap::new();
            for (k, w) in right_rows {
                rm.entry(k).or_default().push(w);
            }

            let mut out: Vec<(K, (V, W))> = Vec::new();
            for (k, vs) in lm {
                if let Some(ws) = rm.get(&k) {
                    for v in &vs {
                        for w in ws {
                            out.push((k.clone(), (v.clone(), w.clone())));
                        }
                    }
                }
            }
            Box::new(out) as Partition
        });

        // Insert dummy source + CoGroup
        let source_id = insert_dummy_source(&self.pipeline);
        let coalesce_left = Arc::new(|parts: Vec<Partition>| -> Partition {
            let mut out: Vec<(K, V)> = Vec::new();
            for p in parts {
                let mut v = *p
                    .downcast::<Vec<(K, V)>>()
                    .expect("coalesce_left: wrong type");
                out.append(&mut v);
            }
            Box::new(out) as Partition
        });

        let coalesce_right = Arc::new(|parts: Vec<Partition>| -> Partition {
            let mut out: Vec<(K, W)> = Vec::new();
            for p in parts {
                let mut v = *p
                    .downcast::<Vec<(K, W)>>()
                    .expect("coalesce_right: wrong type");
                out.append(&mut v);
            }
            Box::new(out) as Partition
        });

        let id = self.pipeline.insert_node(Node::CoGroup {
            left_chain: left_chain.into(),
            right_chain: right_chain.into(),
            coalesce_left,
            coalesce_right,
            exec,
        });
        self.pipeline.connect(source_id, id);
        PCollection {
            pipeline: self.pipeline.clone(),
            id,
            _t: PhantomData,
        }
    }

    /// Left outer join on a key with `(K, W)` -> `(K, (V, Option<W>))`.
    ///
    /// Emits all left rows; missing right values appear as `None`.
    ///
    /// # Example
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let left  = from_vec(&p, vec![("a".to_string(), 1u32), ("b".to_string(), 3u32)]);
    /// let right = from_vec(&p, vec![("a".to_string(), "x".to_string())]);
    ///
    /// let joined = left.join_left(&right);
    /// let _ = joined.collect_par_sorted_by_key(None, None)?;
    /// # Ok(()) }
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if types are mismatched or chain building fails.
    #[must_use]
    pub fn join_left<W>(&self, right: &PCollection<(K, W)>) -> PCollection<(K, (V, Option<W>))>
    where
        W: RFBound,
    {
        let left_chain = chain_from(&self.pipeline, self.id).expect("left chain build");
        let right_chain = chain_from(&right.pipeline, right.id).expect("right chain build");

        let exec = Arc::new(|left_part: Partition, right_part: Partition| {
            let left_rows = *left_part
                .downcast::<Vec<(K, V)>>()
                .expect("cogroup exec: left type Vec<(K,V)>");
            let right_rows = *right_part
                .downcast::<Vec<(K, W)>>()
                .expect("cogroup exec: right type Vec<(K,W)>");

            let mut lm: HashMap<K, Vec<V>> = HashMap::new();
            for (k, v) in left_rows {
                lm.entry(k).or_default().push(v);
            }
            let mut rm: HashMap<K, Vec<W>> = HashMap::new();
            for (k, w) in right_rows {
                rm.entry(k).or_default().push(w);
            }

            let mut out: Vec<(K, (V, Option<W>))> = Vec::new();
            for (k, vs) in lm {
                match rm.get(&k) {
                    Some(ws) => {
                        for v in &vs {
                            for w in ws {
                                out.push((k.clone(), (v.clone(), Some(w.clone()))));
                            }
                        }
                    }
                    None => {
                        for v in vs {
                            out.push((k.clone(), (v, None)));
                        }
                    }
                }
            }
            Box::new(out) as Partition
        });

        let source_id = insert_dummy_source(&self.pipeline);
        let coalesce_left = Arc::new(|parts: Vec<Partition>| -> Partition {
            let mut out: Vec<(K, V)> = Vec::new();
            for p in parts {
                let mut v = *p
                    .downcast::<Vec<(K, V)>>()
                    .expect("coalesce_left: wrong type");
                out.append(&mut v);
            }
            Box::new(out) as Partition
        });

        let coalesce_right = Arc::new(|parts: Vec<Partition>| -> Partition {
            let mut out: Vec<(K, W)> = Vec::new();
            for p in parts {
                let mut v = *p
                    .downcast::<Vec<(K, W)>>()
                    .expect("coalesce_right: wrong type");
                out.append(&mut v);
            }
            Box::new(out) as Partition
        });

        let id = self.pipeline.insert_node(Node::CoGroup {
            left_chain: left_chain.into(),
            right_chain: right_chain.into(),
            coalesce_left,
            coalesce_right,
            exec,
        });
        self.pipeline.connect(source_id, id);
        PCollection {
            pipeline: self.pipeline.clone(),
            id,
            _t: PhantomData,
        }
    }

    /// Right outer join on a key with `(K, W)` -> `(K, (Option<V>, W))`.
    ///
    /// Emits all right rows; missing left values appear as `None`.
    ///
    /// # Example
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let left  = from_vec(&p, vec![("a".to_string(), 1u32)]);
    /// let right = from_vec(&p, vec![("a".to_string(), "x".to_string()), ("c".to_string(), "y".to_string())]);
    ///
    /// let joined = left.join_right(&right);
    /// let _ = joined.collect_par_sorted_by_key(None, None)?;
    /// # Ok(()) }
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if types are mismatched or chain building fails.
    #[must_use]
    pub fn join_right<W>(&self, right: &PCollection<(K, W)>) -> PCollection<(K, (Option<V>, W))>
    where
        W: RFBound,
    {
        let left_chain = chain_from(&self.pipeline, self.id).expect("left chain build");
        let right_chain = chain_from(&right.pipeline, right.id).expect("right chain build");

        let exec = Arc::new(|left_part: Partition, right_part: Partition| {
            let left_rows = *left_part
                .downcast::<Vec<(K, V)>>()
                .expect("cogroup exec: left type Vec<(K,V)>");
            let right_rows = *right_part
                .downcast::<Vec<(K, W)>>()
                .expect("cogroup exec: right type Vec<(K,W)>");

            let mut lm: HashMap<K, Vec<V>> = HashMap::new();
            for (k, v) in left_rows {
                lm.entry(k).or_default().push(v);
            }
            let mut rm: HashMap<K, Vec<W>> = HashMap::new();
            for (k, w) in right_rows {
                rm.entry(k).or_default().push(w);
            }

            let mut out: Vec<(K, (Option<V>, W))> = Vec::new();

            for (k, ws) in rm {
                match lm.get(&k) {
                    Some(vs) => {
                        for w in &ws {
                            for v in vs {
                                out.push((k.clone(), (Some(v.clone()), w.clone())));
                            }
                        }
                    }
                    None => {
                        for w in ws {
                            out.push((k.clone(), (None, w)));
                        }
                    }
                }
            }
            Box::new(out) as Partition
        });

        let source_id = insert_dummy_source(&self.pipeline);
        let coalesce_left = Arc::new(|parts: Vec<Partition>| -> Partition {
            let mut out: Vec<(K, V)> = Vec::new();
            for p in parts {
                let mut v = *p
                    .downcast::<Vec<(K, V)>>()
                    .expect("coalesce_left: wrong type");
                out.append(&mut v);
            }
            Box::new(out) as Partition
        });

        let coalesce_right = Arc::new(|parts: Vec<Partition>| -> Partition {
            let mut out: Vec<(K, W)> = Vec::new();
            for p in parts {
                let mut v = *p
                    .downcast::<Vec<(K, W)>>()
                    .expect("coalesce_right: wrong type");
                out.append(&mut v);
            }
            Box::new(out) as Partition
        });

        let id = self.pipeline.insert_node(Node::CoGroup {
            left_chain: left_chain.into(),
            right_chain: right_chain.into(),
            coalesce_left,
            coalesce_right,
            exec,
        });
        self.pipeline.connect(source_id, id);
        PCollection {
            pipeline: self.pipeline.clone(),
            id,
            _t: PhantomData,
        }
    }

    /// Full outer join on a key with `(K, W)` -> `(K, (Option<V>, Option<W>))`.
    ///
    /// Emits rows for the union of keys found on either side. Missing values are `None`.
    ///
    /// # Example
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let left  = from_vec(&p, vec![("a".to_string(), 1u32)]);
    /// let right = from_vec(&p, vec![("a".to_string(), "x".to_string()), ("c".to_string(), "y".to_string())]);
    ///
    /// let joined = left.join_full(&right);
    /// let _ = joined.collect_par_sorted_by_key(None, None)?;
    /// # Ok(()) }
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if types are mismatched or chain building fails.
    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn join_full<W>(
        &self,
        right: &PCollection<(K, W)>,
    ) -> PCollection<(K, (Option<V>, Option<W>))>
    where
        W: RFBound,
    {
        let left_chain = chain_from(&self.pipeline, self.id).expect("left chain build");
        let right_chain = chain_from(&right.pipeline, right.id).expect("right chain build");

        let exec = Arc::new(|left_part: Partition, right_part: Partition| {
            let left_rows = *left_part
                .downcast::<Vec<(K, V)>>()
                .expect("cogroup exec: left type Vec<(K,V)>");
            let right_rows = *right_part
                .downcast::<Vec<(K, W)>>()
                .expect("cogroup exec: right type Vec<(K,W)>");

            let mut lm: HashMap<K, Vec<V>> = HashMap::new();
            for (k, v) in left_rows {
                lm.entry(k).or_default().push(v);
            }
            let mut rm: HashMap<K, Vec<W>> = HashMap::new();
            for (k, w) in right_rows {
                rm.entry(k).or_default().push(w);
            }

            let mut out: Vec<(K, (Option<V>, Option<W>))> = Vec::new();
            let mut keys: HashSet<K> = HashSet::new();
            keys.extend(lm.keys().cloned());
            keys.extend(rm.keys().cloned());

            for k in keys {
                match (lm.get(&k), rm.get(&k)) {
                    (Some(vs), Some(ws)) => {
                        for v in vs {
                            for w in ws {
                                out.push((k.clone(), (Some(v.clone()), Some(w.clone()))));
                            }
                        }
                    }
                    (Some(vs), None) => {
                        for v in vs {
                            out.push((k.clone(), (Some(v.clone()), None)));
                        }
                    }
                    (None, Some(ws)) => {
                        for w in ws {
                            out.push((k.clone(), (None, Some(w.clone()))));
                        }
                    }
                    (None, None) => {} // unreachable
                }
            }
            Box::new(out) as Partition
        });

        let source_id = insert_dummy_source(&self.pipeline);
        let coalesce_left = Arc::new(|parts: Vec<Partition>| -> Partition {
            let mut out: Vec<(K, V)> = Vec::new();
            for p in parts {
                let mut v = *p
                    .downcast::<Vec<(K, V)>>()
                    .expect("coalesce_left: wrong type");
                out.append(&mut v);
            }
            Box::new(out) as Partition
        });

        let coalesce_right = Arc::new(|parts: Vec<Partition>| -> Partition {
            let mut out: Vec<(K, W)> = Vec::new();
            for p in parts {
                let mut v = *p
                    .downcast::<Vec<(K, W)>>()
                    .expect("coalesce_right: wrong type");
                out.append(&mut v);
            }
            Box::new(out) as Partition
        });

        let id = self.pipeline.insert_node(Node::CoGroup {
            left_chain: left_chain.into(),
            right_chain: right_chain.into(),
            coalesce_left,
            coalesce_right,
            exec,
        });
        self.pipeline.connect(source_id, id);
        PCollection {
            pipeline: self.pipeline.clone(),
            id,
            _t: PhantomData,
        }
    }
}
