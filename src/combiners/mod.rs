//! Built-in combiners for `combine_values` and `combine_values_lifted`.
//!
//! These are reusable implementations of [`CombineFn`] (and many also implement
//! [`LiftableCombiner`]) that operate over per-key value streams:
//!
//! - [`Sum<T>`] -- sum of values.
//! - [`Min<T>`] -- minimum value.
//! - [`Max<T>`] -- maximum value.
//! - [`AverageF64`] -- average as `f64` (values convertible to `f64`).
//! - [`DistinctCount<T>`] -- count of distinct values.
//! - [`TopK<T>`] -- the top-K largest values.
//!
//! Each combiner specifies its accumulator type (`A`) and output type (`O`).
//! Many provide a `build_from_group` optimization via [`LiftableCombiner`],
//! enabling efficient `group_by_key().combine_values_lifted(...)` plans.
//!
//! # Examples
//! ```ignore
//! use rustflow::*;
//! use rustflow::combiners::{Sum, Min, Max, AverageF64, DistinctCount, TopK};
//!
//! let p = Pipeline::default();
//!
//! // Sum
//! let s = from_vec(&p, vec![("a", 1u64), ("a", 2), ("b", 10)])
//!     .combine_values(Sum::<u64>::default())
//!     .collect_seq_sorted()?;
//!
//! // Min / Max (require Ord)
//! let mn = from_vec(&p, vec![("a", 3u64), ("a", 2), ("a", 5)])
//!     .combine_values(Min::<u64>::default())
//!     .collect_seq()?;
//! let mx = from_vec(&p, vec![("a", 3u64), ("a", 2), ("a", 5)])
//!     .combine_values(Max::<u64>::default())
//!     .collect_seq()?;
//!
//! // AverageF64 (values must be Into<f64>)
//! let avg = from_vec(&p, vec![("a", 1u32), ("a", 2), ("a", 3)])
//!     .combine_values(AverageF64::default())
//!     .collect_seq()?;
//!
//! // DistinctCount (values must be Eq + Hash)
//! let dc = from_vec(&p, vec![("a", 1u32), ("a", 1), ("a", 2)])
//!     .combine_values(DistinctCount::<u32>::default())
//!     .collect_seq()?;
//!
//! // TopK (values must be Ord)
//! let top = from_vec(&p, vec![("a", 3u32), ("a", 7), ("a", 5)])
//!     .combine_values(TopK::<u32>::new(2))
//!     .collect_seq()?;
//!
//! # anyhow::Result::<()>::Ok(())
//! ```

mod basic;
mod distinct;
mod sampling;
mod statistical;
mod topk;

// Re-export all public combiners
pub use basic::{Max, Min, Sum};
pub use distinct::{DistinctCount, DistinctSet, KMVApproxDistinctCount};
pub use sampling::PriorityReservoir;
pub use statistical::AverageF64;
pub use topk::TopK;