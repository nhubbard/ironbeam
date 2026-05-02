//! Reshuffle: a true graph-level shuffle barrier for [`PCollection`].
//!
//! # Overview
//!
//! [`PCollection::reshuffle`] inserts a [`Node::Reshuffle`] barrier that:
//!
//! 1. **Forces materialization** — all elements are collected from every current partition
//!    before any downstream operator runs.
//! 2. **Re-distributes elements** — elements are re-partitioned evenly across the runner's
//!    configured partition count (one or more partitions per CPU core by default), restoring
//!    full parallelism for downstream transforms.
//! 3. **Prevents op fusion** — [`Node::Reshuffle`] is a
//!    non-[`Stateless`](Node::Stateless) node; the planner and runner cannot
//!    fuse stateless ops across it.
//!
//! This is analogous to `Reshuffle` in Apache Beam or `repartition()` in Apache Spark.
//! The primary use cases in a local framework are:
//!
//! - **Breaking downstream skew** after a large [`group_by_key`](crate::PCollection::group_by_key)
//!   or [`combine_values`](crate::PCollection::combine_values) that produces unevenly sized groups:
//!   a `reshuffle()` after the barrier re-spreads elements so the next stateless map/filter stage
//!   runs in parallel across all cores.
//! - **Forcing a stage boundary** to prevent the planner from fusing surrounding ops.
//! - **Checkpointing** — `reshuffle` counts as a barrier for checkpoint purposes.
//!
//! # Performance Note
//!
//! `reshuffle` is O(N) in time and space: it collects every element once, then copies them
//! into evenly sized output partitions using `div_ceil` chunking — the same strategy as the
//! source splitter. There is no `HashMap` allocation or key-assignment overhead.

use crate::node::Node;
use crate::{PCollection, Partition, RFBound};
use std::marker::PhantomData;
use std::sync::Arc;

impl<T: RFBound> PCollection<T> {
    /// Insert a shuffle barrier, re-distributing elements evenly across output partitions.
    ///
    /// This is a true graph-level barrier backed by [`Node::Reshuffle`]:
    /// elements are fully materialized, then re-partitioned into *N* evenly sized output
    /// partitions where *N* matches the runner's configured parallelism. Downstream stateless
    /// transforms subsequently run in parallel across all *N* partitions.
    ///
    /// Use `reshuffle` to:
    ///
    /// - **Break skew** after a [`group_by_key`](Self::group_by_key) or combine that produces
    ///   unevenly sized groups; the reshuffle re-spreads elements for parallel downstream work.
    /// - **Force a stage boundary**, preventing the planner from fusing surrounding ops.
    /// - **Restore parallelism** in a reduce-then-expand pattern where a preceding barrier
    ///   collapsed all partitions into one.
    ///
    /// # Panics
    ///
    /// Panics if a partition holds a type other than `Vec<T>`. This cannot occur in
    /// normal usage because the closure is constructed from a typed `PCollection<T>`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let data = from_vec(&p, vec![1u32, 2, 3, 4, 5]);
    /// let reshuffled = data.reshuffle();
    /// let mut out = reshuffled.collect_seq()?;
    /// out.sort();
    /// assert_eq!(out, vec![1u32, 2, 3, 4, 5]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn reshuffle(self) -> Self {
        let reshuffle_fn: Arc<dyn Fn(Vec<Partition>, usize) -> Vec<Partition> + Send + Sync> =
            Arc::new(|parts: Vec<Partition>, n: usize| {
                // Collect all elements from every input partition.
                let mut all: Vec<T> = Vec::new();
                for p in parts {
                    #[allow(clippy::expect_used)]
                    let v = *p
                        .downcast::<Vec<T>>()
                        .expect("Reshuffle: partition held unexpected element type");
                    all.extend(v);
                }
                if all.is_empty() || n <= 1 {
                    return vec![Box::new(all) as Partition];
                }
                // Split evenly using div_ceil chunking — same strategy as VecOps::split.
                let chunk_size = all.len().div_ceil(n);
                all.chunks(chunk_size)
                    .map(|c| Box::new(c.to_vec()) as Partition)
                    .collect()
            });
        let id = self.pipeline.insert_node(Node::Reshuffle {
            reshuffle: reshuffle_fn,
        });
        self.pipeline.connect(self.id, id);
        Self {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }
}
