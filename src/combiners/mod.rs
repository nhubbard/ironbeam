//! Built-in combiners for `combine_values` and `combine_values_lifted`.
//!
//! These are reusable implementations of [`crate::collection::CombineFn`] (and many also implement
//! that operate over per-key value streams:
//!
//! - [`Sum<T>`] -- sum of values.
//! - [`Min<T>`] -- minimum value.
//! - [`Max<T>`] -- maximum value.
//! - [`Count<T>`] -- count of values.
//! - [`AverageF64`] -- average as `f64` (values convertible to `f64`).
//! - [`Mean<O>`] -- arithmetic mean with caller-chosen floating-point output (`f32` or `f64`).
//! - [`DistinctCount<T>`] -- count of distinct values.
//! - [`ToList<T>`] -- collect all values into a `Vec<T>`.
//! - [`ToSet<T>`] -- collect unique values into a `HashSet<T>`.
//! - [`ToDict<K, V>`] -- collect `(K, V)` pairs into a `HashMap<K, V>`.
//! - [`Latest<T>`] -- select the value with the latest timestamp.
//! - [`TopK<T>`] -- the top-K largest values.
//! - [`BottomK<T>`] -- the bottom-K smallest values.
//! - [`ApproxQuantiles<T>`] -- approximate quantiles/percentiles using t-digest.
//! - [`ApproxMedian<T>`] -- approximate median using t-digest.
//!
//! Each combiner specifies its accumulator type (`A`) and output type (`O`).
//!
//! # Examples
//! ```no_run
//! # use anyhow::Result;
//! use ironbeam::*;
//! use ironbeam::combiners::{Sum, Min, Max, Count, AverageF64, Mean, DistinctCount, ToList, ToSet, ToDict, Latest, TopK, BottomK, ApproxQuantiles, ApproxMedian};
//! use ironbeam::window::Timestamped;
//!
//! let p = Pipeline::default();
//!
//! // Sum
//! let s = from_vec(&p, vec![("a".to_string(), 1u64), ("a".to_string(), 2), ("b".to_string(), 10)])
//!     .combine_values(Sum::<u64>::default())
//!     .collect_seq_sorted()?;
//!
//! // Min / Max (require Ord)
//! let mn = from_vec(&p, vec![("a".to_string(), 3u64), ("a".to_string(), 2), ("a".to_string(), 5)])
//!     .combine_values(Min::<u64>::default())
//!     .collect_seq()?;
//! let mx = from_vec(&p, vec![("a".to_string(), 3u64), ("a".to_string(), 2), ("a".to_string(), 5)])
//!     .combine_values(Max::<u64>::default())
//!     .collect_seq()?;
//!
//! // Count
//! let cnt = from_vec(&p, vec![("a".to_string(), 1u64), ("a".to_string(), 2), ("a".to_string(), 3)])
//!     .combine_values(Count::new())
//!     .collect_seq()?;
//!
//! // AverageF64 (values must be Into<f64>)
//! let avg = from_vec(&p, vec![("a".to_string(), 1u32), ("a".to_string(), 2), ("a".to_string(), 3)])
//!     .combine_values(AverageF64::default())
//!     .collect_seq()?;
//!
//! // Mean<O> — choose the output floating-point precision (f32 or f64)
//! let mean_f32 = from_vec(&p, vec![("a".to_string(), 1i16), ("a".to_string(), 2i16), ("a".to_string(), 3i16)])
//!     .combine_values(Mean::<f32>::new())
//!     .collect_seq()?;
//! let mean_f64 = from_vec(&p, vec![("a".to_string(), 1i16), ("a".to_string(), 2i16), ("a".to_string(), 3i16)])
//!     .combine_values(Mean::<f64>::new())
//!     .collect_seq()?;
//!
//! // DistinctCount (values must be Eq + Hash)
//! let dc = from_vec(&p, vec![("a".to_string(), 1u32), ("a".to_string(), 1), ("a".to_string(), 2)])
//!     .combine_values(DistinctCount::<u32>::default())
//!     .collect_seq()?;
//!
//! // ToList - collect all values into a Vec
//! let lst = from_vec(&p, vec![("a".to_string(), 1u32), ("a".to_string(), 2), ("b".to_string(), 3)])
//!     .combine_values(ToList::new())
//!     .collect_seq()?;
//!
//! // ToSet - collect unique values into a HashSet
//! let set = from_vec(&p, vec![("a".to_string(), 1u32), ("a".to_string(), 1), ("a".to_string(), 2)])
//!     .combine_values(ToSet::new())
//!     .collect_seq()?;
//!
//! // ToDict - materialize a keyed collection as a single HashMap
//! let dict = from_vec(&p, vec![("a".to_string(), 1u32), ("b".to_string(), 2)])
//!     .combine_globally(ToDict::new(), None)
//!     .collect_seq()?;
//!
//! // Latest - select value with latest timestamp
//! let latest = from_vec(&p, vec![
//!     ("user".to_string(), Timestamped::new(100, "login".to_string())),
//!     ("user".to_string(), Timestamped::new(200, "click".to_string()))
//! ])
//!     .combine_values(Latest::new())
//!     .collect_seq()?;
//!
//! // TopK (values must be Ord)
//! let top = from_vec(&p, vec![("a".to_string(), 3u32), ("a".to_string(), 7), ("a".to_string(), 5)])
//!     .combine_values(TopK::<u32>::new(2))
//!     .collect_seq()?;
//!
//! // BottomK (values must be Ord)
//! let bot = from_vec(&p, vec![("a".to_string(), 3u32), ("a".to_string(), 7), ("a".to_string(), 5)])
//!     .combine_values(BottomK::<u32>::new(2))
//!     .collect_seq()?;
//!
//! // Approximate quantiles (values must be Into<f64>)
//! let quantiles = from_vec(&p, vec![("a".to_string(), 1.0), ("a".to_string(), 2.0), ("a".to_string(), 3.0), ("a".to_string(), 4.0)])
//!     .combine_values(ApproxQuantiles::<f64>::new(vec![0.25, 0.5, 0.75], 100.0))
//!     .collect_seq()?;
//!
//! // Approximate median
//! let median = from_vec(&p, vec![("a".to_string(), 1.0), ("a".to_string(), 2.0), ("a".to_string(), 3.0)])
//!     .combine_values(ApproxMedian::<f64>::default())
//!     .collect_seq()?;
//!
//! # Result::<()>::Ok(())
//! ```

mod basic;
mod collect;
mod count;
mod distinct;
mod latest;
mod quantiles;
mod sampling;
mod statistical;
mod topk;

// Re-export all public combiners
pub use basic::{Max, Min, Sum};
pub use collect::{ToDict, ToList, ToSet};
pub use count::Count;
pub use distinct::{DistinctCount, DistinctSet, HllApproxDistinctCount, KMVApproxDistinctCount};
pub use latest::Latest;
pub use quantiles::{ApproxMedian, ApproxQuantiles, TDigest};
pub use sampling::PriorityReservoir;
pub use statistical::{AverageF64, Mean};
pub use topk::{BottomK, TopK};
