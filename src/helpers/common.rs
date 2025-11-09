//! Common elementwise transforms and collection utilities for [`PCollection`].
//!
//! This module defines the core functional operators used throughout Rustflow:
//!
//! - [`PCollection::map`] -- one-to-one element transformation.
//! - [`PCollection::filter`] -- element selection by predicate.
//! - [`PCollection::flat_map`] -- one-to-many expansion.
//!
//! It also includes some collection materialization helpers:
//!
//! - [`PCollection::collect`] -- collects sequentially by default.
//! - [`PCollection::collect_seq`] -- explicit sequential collection.
//! - [`PCollection::collect_par`] -- parallel collection with configurable concurrency.
//!
//! These operations form the foundation of the dataflow API, similar to Apache Beam's
//! elementwise transforms (`Map`, `Filter`, `FlatMap`).

use crate::collection::{FilterOp, FlatMapOp, MapOp};
use crate::node::{DynOp, Node};
use crate::{ExecMode, PCollection, RFBound, Runner};
use anyhow::Result;
use std::marker::PhantomData;
use std::sync::Arc;

impl<T: RFBound> PCollection<T> {
    /// Apply a function to each element of the collection.
    ///
    /// This is the simplest transform -- it applies `f(&T) -> O` to each element independently,
    /// producing a new [`PCollection<O>`].
    ///
    /// # Type Parameters
    /// - `O`: Output type.
    /// - `F`: Closure type implementing `Fn(&T) -> O`.
    ///
    /// # Example
    /// ```no_run
    /// use rustflow::*;
    ///
    /// let p = Pipeline::default();
    /// let nums = from_vec(&p, vec![1, 2, 3]);
    /// let doubled = nums.map(|x| x * 2).collect_seq_sorted().unwrap();
    /// assert_eq!(doubled, vec![2, 4, 6]);
    /// ```
    pub fn map<O, F>(self, f: F) -> PCollection<O>
    where
        O: RFBound,
        F: 'static + Send + Sync + Fn(&T) -> O,
    {
        let op: Arc<dyn DynOp> = Arc::new(MapOp::<T, O, F>(f, PhantomData));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }

    /// Retain only elements that satisfy the given predicate.
    ///
    /// Evaluates `pred(&T) -> bool` for each element and passes through only those
    /// for which the predicate returns `true`.
    ///
    /// # Example
    /// ```no_run
    /// use rustflow::*;
    ///
    /// let p = Pipeline::default();
    /// let data = from_vec(&p, vec![1, 2, 3, 4, 5]);
    /// let evens = data.filter(|x| x % 2 == 0).collect_seq_sorted().unwrap();
    /// assert_eq!(evens, vec![2, 4]);
    /// ```
    #[must_use]
    pub fn filter<F>(self, pred: F) -> Self
    where
        F: 'static + Send + Sync + Fn(&T) -> bool,
    {
        let op: Arc<dyn DynOp> = Arc::new(FilterOp::<T, F>(pred, PhantomData));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        Self {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }

    /// Apply a one-to-many transformation, expanding each element into zero or more outputs.
    ///
    /// Each element is passed to `f(&T) -> Vec<O>`, and the resulting vectors are concatenated
    /// into a single flattened stream.
    ///
    /// This is analogous to a `flatMap` or `SelectMany` operation in other dataflow APIs.
    ///
    /// # Example
    /// ```no_run
    /// use rustflow::*;
    ///
    /// let p = Pipeline::default();
    /// let words = from_vec(&p, vec!["a b", "c d"]);
    /// let split = words.flat_map(|s| s.split_whitespace().map(String::from).collect());
    /// let out = split.collect_seq_sorted().unwrap();
    /// assert_eq!(out, vec!["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()]);
    /// ```
    pub fn flat_map<O, F>(self, f: F) -> PCollection<O>
    where
        O: RFBound,
        F: 'static + Send + Sync + Fn(&T) -> Vec<O>,
    {
        let op: Arc<dyn DynOp> = Arc::new(FlatMapOp::<T, O, F>(f, PhantomData));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }
}

impl<T: RFBound> PCollection<T> {
    /// Collect elements from this collection using the default (sequential) mode.
    ///
    /// Equivalent to calling [`PCollection::collect_seq`].
    ///
    /// # Example
    /// ```no_run
    /// use rustflow::*;
    ///
    /// let p = Pipeline::default();
    /// let col = from_vec(&p, vec![10, 20, 30]);
    /// assert_eq!(col.collect().unwrap(), vec![10, 20, 30]);
    /// ```
    ///
    /// # Errors
    ///
    /// If an error is encountered, it is returned in a [`Result`] wrapper.
    pub fn collect(self) -> Result<Vec<T>> {
        self.collect_seq()
    }

    /// Collect elements **sequentially** into a local vector.
    ///
    /// Runs the pipeline in [`ExecMode::Sequential`], executing each transform in a
    /// single-threaded context and materializing the results into a `Vec<T>`.
    ///
    /// # Example
    /// ```no_run
    /// use rustflow::*;
    ///
    /// let p = Pipeline::default();
    /// let col = from_vec(&p, vec![1, 2, 3]);
    /// let out = col.collect_seq().unwrap();
    /// assert_eq!(out, vec![1, 2, 3]);
    /// ```
    ///
    /// # Errors
    ///
    /// Returns any errors in a [`Result`] container.
    pub fn collect_seq(self) -> Result<Vec<T>> {
        Runner {
            mode: ExecMode::Sequential,
            ..Default::default()
        }
        .run_collect::<T>(&self.pipeline, self.id)
    }

    /// Collect elements **in parallel** using the specified number of threads and partitions.
    ///
    /// This executes the pipeline with [`ExecMode::Parallel`], splitting data across partitions
    /// and threads for concurrent processing. The resulting partitions are merged into a
    /// single `Vec<T>`.
    ///
    /// # Arguments
    /// - `threads`: Optional number of worker threads (defaults to runtime detection).
    /// - `partitions`: Optional number of partitions per operator (defaults to auto-chosen).
    ///
    /// # Example
    /// ```no_run
    /// use rustflow::*;
    ///
    /// let p = Pipeline::default();
    /// let data = from_vec(&p, vec![1, 2, 3]);
    /// let out = data.collect_par(Some(4), Some(2)).unwrap();
    /// assert_eq!(out.len(), 3);
    /// ```
    ///
    /// # Errors
    ///
    /// Errors are returned in a [`Result`] wrapper.
    pub fn collect_par(self, threads: Option<usize>, partitions: Option<usize>) -> Result<Vec<T>> {
        Runner {
            mode: ExecMode::Parallel {
                threads,
                partitions,
            },
            ..Default::default()
        }
        .run_collect::<T>(&self.pipeline, self.id)
    }
}
