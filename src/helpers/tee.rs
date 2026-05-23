//! Pipeline fan-out helpers for [`PCollection`].
//!
//! Ironbeam's transforms consume their input by value (`self`), so to feed a
//! single upstream stream into N independent downstream branches, you need
//! to duplicate the [`PCollection`] handle. [`PCollection::tee`] and
//! [`PCollection::tee_n`] provide a concise, intention-revealing way to do
//! that.
//!
//! ## How fan-out is actually executed
//!
//! A [`PCollection`] is just a `(pipeline, node_id, _t)` triple, with the
//! pipeline shared via `Arc`. Cloning it does **not** rerun any upstream
//! work; it only produces a second handle pointing at the same graph node.
//! When each branch attaches its own downstream transforms, the source node
//! gains multiple outgoing edges (a fan-out / diamond topology).
//!
//! The planner's dominator-based cache placement (introduced in v3.0.0 —
//! feature 3.13 in `FEATURE_PARITY_PLAN.md`) detects this topology and
//! inserts a cache so the source transform runs **once** and all branches
//! consume the materialized output. So calling `tee` is functionally
//! equivalent to two manual `clone` calls but communicates intent more clearly.

use crate::{PCollection, RFBound};

impl<T: RFBound> PCollection<T> {
    /// Split this collection into two independent downstream branches.
    ///
    /// Returns two handles `(a, b)` that both reference the same upstream
    /// node. Each branch can independently apply transforms and trigger
    /// execution; the planner's cache placement ensures upstream work is
    /// materialized exactly once.
    ///
    /// This is the Ironbeam equivalent of Apache Beam's `Tee`. See the
    /// module-level docs for details on the execution semantics.
    ///
    /// # Example
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let source = from_vec(&p, vec![1u32, 2, 3, 4, 5]);
    ///
    /// let (evens, odds) = source.tee();
    /// let even_only = evens.filter(|x| x % 2 == 0).collect_seq_sorted()?;
    /// let odd_only = odds.filter(|x| x % 2 == 1).collect_seq_sorted()?;
    ///
    /// assert_eq!(even_only, vec![2u32, 4]);
    /// assert_eq!(odd_only, vec![1u32, 3, 5]);
    /// # Ok(()) }
    /// ```
    #[must_use]
    pub fn tee(self) -> (Self, Self) {
        let other = self.clone();
        (self, other)
    }

    /// Split this collection into `n` independent downstream branches.
    ///
    /// Returns a `Vec<PCollection<T>>` of length `n`. All branches reference
    /// the same upstream node. Calling `tee_n(0)` returns an empty vector;
    /// calling `tee_n(1)` returns a vector containing only `self`. The
    /// planner's cache placement ensures upstream work is materialized once
    /// regardless of fan-out width.
    ///
    /// # Example
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let source = from_vec(&p, vec![1u32, 2, 3, 4, 5]);
    ///
    /// let branches = source.tee_n(3);
    /// let counts: Vec<usize> = branches
    ///     .into_iter()
    ///     .map(|b| b.collect_seq().unwrap().len())
    ///     .collect();
    /// assert_eq!(counts, vec![5usize, 5, 5]);
    /// # Ok(()) }
    /// ```
    #[must_use]
    pub fn tee_n(self, n: usize) -> Vec<Self> {
        if n == 0 {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(n);
        for _ in 0..(n - 1) {
            out.push(self.clone());
        }
        out.push(self);
        out
    }
}
