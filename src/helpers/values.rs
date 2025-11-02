//! Value-only transforms for keyed collections.
//!
//! This module provides ergonomic helpers to manipulate only the *values* of
//! key–value pairs while preserving their keys. They're direct analogs to
//! [`map`] and [`filter`], but operating over `(K, V)` pairs instead of single
//! values.
//!
//! ## Provided methods
//! - [`map_values`] -- apply a function `&V -> O`, producing `(K, O)`
//! - [`filter_values`] -- retain only entries where `pred(&V)` is true
//!
//! ## Example
//! ```no_run
//! use rustflow::*;
//!
//! let p = Pipeline::default();
//!
//! let kv = from_vec(&p, vec![
//!     ("a", 1u32),
//!     ("b", 5),
//!     ("c", 8),
//! ]);
//!
//! // Double all values
//! let doubled = kv.map_values(|v| v * 2);
//!
//! // Keep only even results
//! let evens = doubled.filter_values(|v| v % 2 == 0);
//!
//! let out = evens.collect_seq()?;
//! assert_eq!(out, vec![("a", 2u32), ("c", 16u32)]);
//! # anyhow::Result::<()>::Ok(())
//! ```

use crate::collection::{FilterValuesOp, MapValuesOp};
use crate::node::{DynOp, Node};
use crate::{PCollection, RFBound};
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

impl<K: RFBound + Eq + Hash, V: RFBound> PCollection<(K, V)> {
    /// Transform only the *value* component of each key–value pair.
    ///
    /// Applies a function `f: &V -> O` to every value in `(K, V)`, keeping
    /// the same key and outputting `(K, O)`.
    ///
    /// ### Arguments
    /// - `f`: A pure function or closure that maps each value.
    ///
    /// ### Returns
    /// `PCollection<(K, O)>`
    ///
    /// ### Example
    /// ```no_run
    /// use rustflow::*;
    ///
    /// let p = Pipeline::default();
    /// let kv = from_vec(&p, vec![("x", 1u32), ("y", 2u32)]);
    ///
    /// let out = kv.map_values(|v| v + 1).collect_seq()?;
    /// assert_eq!(out, vec![("x", 2u32), ("y", 3u32)]);
    /// # anyhow::Result::<()>::Ok(())
    /// ```
    pub fn map_values<O, F>(self, f: F) -> PCollection<(K, O)>
    where
        O: RFBound,
        F: 'static + Send + Sync + Fn(&V) -> O,
    {
        let op: Arc<dyn DynOp> = Arc::new(MapValuesOp::<K, V, O, F>(f, PhantomData));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }

    /// Filter elements based on their *value* component.
    ///
    /// Keeps only key–value pairs `(K, V)` where `pred(&V)` returns true.
    ///
    /// ### Arguments
    /// - `pred`: A predicate function evaluated for each value.
    ///
    /// ### Returns
    /// A filtered `PCollection<(K, V)>`.
    ///
    /// ### Example
    /// ```no_run
    /// use rustflow::*;
    ///
    /// let p = Pipeline::default();
    /// let kv = from_vec(&p, vec![("x", 3u32), ("y", 8u32)]);
    ///
    /// let out = kv.filter_values(|v| *v > 5).collect_seq()?;
    /// assert_eq!(out, vec![("y", 8u32)]);
    /// # anyhow::Result::<()>::Ok(())
    /// ```
    pub fn filter_values<F>(self, pred: F) -> PCollection<(K, V)>
    where
        F: 'static + Send + Sync + Fn(&V) -> bool,
    {
        let op: Arc<dyn DynOp> = Arc::new(FilterValuesOp::<K, V, F>(pred, PhantomData));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }
}
