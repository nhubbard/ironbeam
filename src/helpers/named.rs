//! Fluent labeling helper for [`PCollection`].
//!
//! [`PCollection::with_name`] attaches a human-readable name to the node that
//! produced this collection. The name is pure metadata: planning and execution
//! are completely unaffected, so adding (or omitting) names never changes the
//! result of a pipeline.
//!
//! Names are intended for **observability and external backends**. Apache Beam
//! lets every applied transform carry a label (`data | "Split" >>
//! beam.FlatMap(...)`, `data.apply("Split", ...)`); that label is the
//! identifier Beam's UI, logs, and metrics show. Ironbeam exposes the same
//! concept so that:
//!
//! - External translators (e.g., the community Google Cloud Dataflow backend)
//!   can forward meaningful step names to the remote runner instead of the
//!   generic op category;
//! - The local
//!   [`ExecutionExplanation`](crate::planner::ExecutionExplanation) view can
//!   render the user-chosen label next to each step.
//!
//! ## Why "post-fix"?
//!
//! None of Ironbeam's existing transform helpers (`map`, `filter`,
//! `key_by`, …) take a name parameter. Adding one would be a breaking change
//! across the entire `PCollection` surface. `with_name` sidesteps that by
//! labeling the *result* of a transform after the fact — `self.id` is the
//! [`NodeId`](crate::NodeId) of the node the immediately preceding transform
//! created, so attaching the name there is equivalent to having named the
//! transform itself.
//!
//! ## Behavior
//!
//! - The method returns `Self` so it chains naturally between transforms.
//! - Calling it twice on the same collection (or on two collections sharing a
//!   node id) is **last-write-wins** — see
//!   [`Pipeline::set_node_name`](crate::Pipeline::set_node_name).
//! - It only labels the node `self.id` points at. Composite helpers that
//!   internally fan out into several nodes (`combine_values_lifted`,
//!   `join_*`, …) name only their terminal node; per-subnode naming would
//!   require a scope helper, which is a planned follow-up.
//!
//! ## Example
//!
//! ```no_run
//! # use anyhow::Result;
//! use ironbeam::*;
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//!
//! let counts = from_vec(&p, vec!["the quick brown fox".to_string()])
//!     .with_name("Source")
//!     .flat_map(|line: &String| {
//!         line.split_whitespace().map(String::from).collect()
//!     })
//!     .with_name("Split")
//!     .key_by(|w: &String| w.clone())
//!     .with_name("KeyByWord")
//!     .map_values(|_| 1u64)
//!     .with_name("AssignOne")
//!     .combine_values(Sum::<u64>::default())
//!     .with_name("Sum");
//!
//! assert_eq!(p.node_name(counts.node_id()).as_deref(), Some("Sum"));
//! # Ok(()) }
//! ```

use crate::{Element, PCollection};

impl<T: Element> PCollection<T> {
    /// Attach a human-readable name to the node backing this collection.
    ///
    /// This labels `self.id` — the [`NodeId`](crate::NodeId) of the most
    /// recently inserted transform that produced this `PCollection`. The
    /// method returns `self` unchanged so it can be chained between
    /// transforms; no graph edges or planner state are modified.
    ///
    /// See the module-level docs of [`crate::helpers::named`] for the
    /// rationale behind the post-fix shape and the full behavioral
    /// contract.
    ///
    /// # Examples
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let evens = from_vec(&p, vec![1u32, 2, 3, 4, 5])
    ///     .filter(|x| x % 2 == 0)
    ///     .with_name("KeepEven");
    /// assert_eq!(p.node_name(evens.node_id()).as_deref(), Some("KeepEven"));
    /// # Ok(()) }
    /// ```
    #[must_use]
    pub fn with_name(self, name: impl Into<String>) -> Self {
        let qualified = self.pipeline.qualify_with_scope(name);
        self.pipeline.set_node_name(self.id, qualified);
        self
    }
}
