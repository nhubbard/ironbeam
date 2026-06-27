//! Join helpers built on top of a co-group plan.
//!
//! These helpers construct a `CoGroup` node that:
//! 1) snapshots the left and right subplans ending at the provided `PCollection`s,
//! 2) replays each subplan into intermediate, coalesced `Vec<(K, V)>` / `Vec<(K, W)>` buffers,
//! 3) applies a Bloom semi-join pre-filter where semantically safe (see below), and
//! 4) executes a join-specific closure over those buffers to emit the final joined rows.
//!
//! All joins are **key-based** and require `K: Eq + Hash + Element`. The left and right value
//! types must satisfy `Element` as usual. The resulting collection is another `PCollection` in
//! the same pipeline.
//!
//! ## Bloom Semi-Join Pre-Filter
//!
//! Before the hash-join phase, each `exec` closure builds a Bloom filter from the keys of one
//! side and discards elements from the other side whose key is *definitively* absent:
//!
//! | Join type   | Build side | Filter side | Rationale                                                 |
//! |-------------|------------|-------------|-----------------------------------------------------------|
//! | Inner       | Smaller    | Larger      | Unmatched elements on either side are dropped             |
//! | Left outer  | Left       | Right       | All left rows appear; right rows not in left are dropped  |
//! | Right outer | Right      | Left        | All right rows appear; left rows not in right are dropped |
//! | Full outer  | —          | —           | Both sides appear completely; no safe filtering           |
//!
//! False positives in the Bloom filter are harmless (a few extra elements reach the
//! hash-join step); false negatives are impossible, so join correctness is guaranteed.
//!
//! ## Available operations
//! - [`PCollection::join_inner`](crate::PCollection::join_inner) - Inner join on the key
//! - [`PCollection::join_left`](crate::PCollection::join_left) - Left outer join on the key
//! - [`PCollection::join_right`](crate::PCollection::join_right) - Right outer join on the key
//! - [`PCollection::join_full`](crate::PCollection::join_full) - Full outer join on the key
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

use crate::bloom_filter::BloomFilter;
use crate::node::Node;
use crate::type_token::{TypeTag, vec_ops_for};
use crate::{Element, NodeId, PCollection, Partition, Pipeline};
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
    K: Element + Eq + Hash,
    V: Element,
{
    /// Inner join on a key with another `(K, W)` -> `(K, (V, W))`.
    ///
    /// Emits one row for every `(k, v)` on the left and `(k, w)` on the right with the same `k`.
    ///
    /// Internally applies a Bloom semi-join pre-filter: the smaller side's keys are loaded into
    /// a Bloom filter, and elements on the larger side whose key is definitively absent are
    /// discarded before the hash-join step.
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
        W: Element,
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

            // Bloom semi-join: build filter from the smaller side and pre-filter the larger.
            // Both sides can safely be filtered for an inner join (unmatched rows on either
            // side never appear in the output).
            let (left_rows, right_rows) = if left_rows.len() <= right_rows.len() {
                let mut filter = BloomFilter::new(left_rows.len());
                for (k, _) in &left_rows {
                    filter.insert(k);
                }
                let right_filtered = right_rows
                    .into_iter()
                    .filter(|(k, _)| filter.might_contain(k))
                    .collect::<Vec<_>>();
                (left_rows, right_filtered)
            } else {
                let mut filter = BloomFilter::new(right_rows.len());
                for (k, _) in &right_rows {
                    filter.insert(k);
                }
                let left_filtered = left_rows
                    .into_iter()
                    .filter(|(k, _)| filter.might_contain(k))
                    .collect::<Vec<_>>();
                (left_filtered, right_rows)
            };

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
            uses_bloom_semi_join: true,
        });
        self.pipeline.connect(source_id, id);
        // CoGroup inputs are read as `kv<lp, lp>`; upgrade both predecessors
        // (mirrors `group_by_key`). The join's own output is the joined tuple.
        self.pipeline.set_kv_coder::<K, V>(self.id);
        self.pipeline.set_kv_coder::<K, W>(right.id);
        self.pipeline.set_coder::<(K, (V, W))>(id);
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
    /// Applies a Bloom semi-join pre-filter on the **right** side: left keys are loaded into
    /// a Bloom filter, and right elements whose key is definitively absent from the left are
    /// discarded before the hash-join step. All left rows are preserved unconditionally.
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
        W: Element,
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

            // Bloom semi-join on the right side only.
            // All left rows must appear in the output, so only the right side can be
            // pre-filtered. Right elements whose key is absent from the left will never
            // appear in the left-outer-join output, so discarding them is safe.
            let right_rows = {
                let mut filter = BloomFilter::new(left_rows.len());
                for (k, _) in &left_rows {
                    filter.insert(k);
                }
                right_rows
                    .into_iter()
                    .filter(|(k, _)| filter.might_contain(k))
                    .collect::<Vec<_>>()
            };

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
            uses_bloom_semi_join: true,
        });
        self.pipeline.connect(source_id, id);
        // CoGroup inputs are read as `kv<lp, lp>`; upgrade both predecessors
        // (mirrors `group_by_key`). The join's own output is the joined tuple.
        self.pipeline.set_kv_coder::<K, V>(self.id);
        self.pipeline.set_kv_coder::<K, W>(right.id);
        self.pipeline.set_coder::<(K, (V, Option<W>))>(id);
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
    /// Applies a Bloom semi-join pre-filter on the **left** side: right keys are loaded into
    /// a Bloom filter, and left elements whose key is definitively absent from the right are
    /// discarded before the hash-join step. All right rows are preserved unconditionally.
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
        W: Element,
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

            // Bloom semi-join on the left side only.
            // All right rows must appear in the output, so only the left side can be
            // pre-filtered. Left elements whose key is absent from the right will never
            // appear in the right-outer-join output, so discarding them is safe.
            let left_rows = {
                let mut filter = BloomFilter::new(right_rows.len());
                for (k, _) in &right_rows {
                    filter.insert(k);
                }
                left_rows
                    .into_iter()
                    .filter(|(k, _)| filter.might_contain(k))
                    .collect::<Vec<_>>()
            };

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
            uses_bloom_semi_join: true,
        });
        self.pipeline.connect(source_id, id);
        // CoGroup inputs are read as `kv<lp, lp>`; upgrade both predecessors
        // (mirrors `group_by_key`). The join's own output is the joined tuple.
        self.pipeline.set_kv_coder::<K, V>(self.id);
        self.pipeline.set_kv_coder::<K, W>(right.id);
        self.pipeline.set_coder::<(K, (Option<V>, W))>(id);
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
    /// No Bloom semi-join pre-filter is applied: both sides must be preserved in full because
    /// every key on either side appears in the output (potentially with `None` on the missing
    /// side).
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
        W: Element,
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

            // No Bloom semi-join for full outer join: every key on both sides must appear
            // in the output (with None for the absent side), so neither side can be filtered.

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
            uses_bloom_semi_join: false,
        });
        self.pipeline.connect(source_id, id);
        // CoGroup inputs are read as `kv<lp, lp>`; upgrade both predecessors
        // (mirrors `group_by_key`). The join's own output is the joined tuple.
        self.pipeline.set_kv_coder::<K, V>(self.id);
        self.pipeline.set_kv_coder::<K, W>(right.id);
        self.pipeline.set_coder::<(K, (Option<V>, Option<W>))>(id);
        PCollection {
            pipeline: self.pipeline.clone(),
            id,
            _t: PhantomData,
        }
    }
}
