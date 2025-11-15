//! Debug utilities for inspecting pipelines during test execution.
//!
//! This module provides extension traits that add debugging methods to
//! [`PCollection`] for use during testing.

use crate::collection::{PCollection, RFBound};
use crate::node::{DynOp, Node};
use crate::type_token::Partition;
use std::marker::PhantomData;
use std::sync::Arc;

/// Debug operator that prints elements as they flow through the pipeline.
pub(crate) struct DebugInspectOp<T, F> {
    label: String,
    inspector: F,
    _phantom: PhantomData<T>,
}

impl<T, F> DebugInspectOp<T, F> {
    pub const fn new(label: String, inspector: F) -> Self {
        Self {
            label,
            inspector,
            _phantom: PhantomData,
        }
    }
}

impl<T, F> DynOp for DebugInspectOp<T, F>
where
    T: RFBound + std::fmt::Debug,
    F: Fn(&T) + Send + Sync + 'static,
{
    fn apply(&self, input: Partition) -> Partition {
        let v = input
            .downcast::<Vec<T>>()
            .expect("DebugInspectOp input type");

        eprintln!("[Debug: {}] Processing {} elements", self.label, v.len());

        for (i, item) in v.iter().enumerate() {
            (self.inspector)(item);
            if i < 10 {
                // Only print first 10 items to avoid spam
                eprintln!("[Debug: {}] [{}]: {:?}", self.label, i, item);
            }
        }

        if v.len() > 10 {
            eprintln!("[Debug: {}] ... ({} more elements)", self.label, v.len() - 10);
        }

        Box::new(*v) as Partition
    }
}

/// Debug operator that counts elements flowing through the pipeline.
pub(crate) struct DebugCountOp<T> {
    label: String,
    _phantom: PhantomData<T>,
}

impl<T> DebugCountOp<T> {
    pub const fn new(label: String) -> Self {
        Self {
            label,
            _phantom: PhantomData,
        }
    }
}

impl<T: RFBound> DynOp for DebugCountOp<T> {
    fn apply(&self, input: Partition) -> Partition {
        let v = input
            .downcast::<Vec<T>>()
            .expect("DebugCountOp input type");

        eprintln!("[Debug: {}] Count: {} elements", self.label, v.len());

        Box::new(*v) as Partition
    }
}

/// Debug operator that samples N elements from the collection.
pub(crate) struct DebugSampleOp<T> {
    label: String,
    n: usize,
    _phantom: PhantomData<T>,
}

impl<T> DebugSampleOp<T> {
    pub const fn new(label: String, n: usize) -> Self {
        Self {
            label,
            n,
            _phantom: PhantomData,
        }
    }
}

impl<T: RFBound + std::fmt::Debug> DynOp for DebugSampleOp<T> {
    fn apply(&self, input: Partition) -> Partition {
        let v = input
            .downcast::<Vec<T>>()
            .expect("DebugSampleOp input type");

        eprintln!(
            "[Debug: {}] Sampling first {} of {} elements:",
            self.label,
            self.n.min(v.len()),
            v.len()
        );

        for (i, item) in v.iter().take(self.n).enumerate() {
            eprintln!("[Debug: {}] [{}]: {:?}", self.label, i, item);
        }

        Box::new(*v) as Partition
    }
}

/// Extension trait for adding debug methods to [`PCollection`].
///
/// These methods allow you to inspect data flowing through your pipeline
/// during test execution without consuming the collection.
pub trait PCollectionDebugExt<T: RFBound> {
    /// Insert a debug inspection point that prints elements to stderr.
    ///
    /// This is a pass-through operation that logs elements as they flow through
    /// the pipeline. Useful for debugging test failures.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ironbeam::*;
    /// use ironbeam::testing::PCollectionDebugExt;
    ///
    /// # fn main() -> anyhow::Result<()> {
    /// let p = Pipeline::default();
    /// let result = from_vec(&p, vec![1, 2, 3])
    ///     .debug_inspect("after source")
    ///     .map(|x: &i32| x * 2)
    ///     .debug_inspect("after map")
    ///     .collect_seq()?;
    /// # Ok(())
    /// # }
    /// ```
    fn debug_inspect(&self, label: &str) -> PCollection<T>
    where
        T: std::fmt::Debug;

    /// Insert a debug inspection point with a custom inspector function.
    ///
    /// Similar to `debug_inspect` but allows you to provide a custom function
    /// to inspect each element.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ironbeam::*;
    /// use ironbeam::testing::PCollectionDebugExt;
    ///
    /// # fn main() -> anyhow::Result<()> {
    /// let p = Pipeline::default();
    /// let result = from_vec(&p, vec![1, 2, 3])
    ///     .debug_inspect_with("custom", |x: &i32| {
    ///         eprintln!("Custom inspect: {}", x * 10);
    ///     })
    ///     .collect_seq()?;
    /// # Ok(())
    /// # }
    /// ```
    fn debug_inspect_with<F>(&self, label: &str, inspector: F) -> PCollection<T>
    where
        T: std::fmt::Debug,
        F: Fn(&T) + Send + Sync + 'static;

    /// Insert a debug count point that prints the number of elements.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ironbeam::*;
    /// use ironbeam::testing::PCollectionDebugExt;
    ///
    /// # fn main() -> anyhow::Result<()> {
    /// let p = Pipeline::default();
    /// let result = from_vec(&p, vec![1, 2, 3, 4, 5])
    ///     .filter(|x: &i32| x % 2 == 0)
    ///     .debug_count("after filter")
    ///     .collect_seq()?;
    /// # Ok(())
    /// # }
    /// ```
    fn debug_count(&self, label: &str) -> PCollection<T>;

    /// Insert a debug sample point that prints the first N elements.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ironbeam::*;
    /// use ironbeam::testing::PCollectionDebugExt;
    ///
    /// # fn main() -> anyhow::Result<()> {
    /// let p = Pipeline::default();
    /// let result = from_vec(&p, (1..=100).collect::<Vec<_>>())
    ///     .debug_sample(5, "first 5 elements")
    ///     .collect_seq()?;
    /// # Ok(())
    /// # }
    /// ```
    fn debug_sample(&self, n: usize, label: &str) -> PCollection<T>
    where
        T: std::fmt::Debug;
}

impl<T: RFBound> PCollectionDebugExt<T> for PCollection<T> {
    fn debug_inspect(&self, label: &str) -> Self
    where
        T: std::fmt::Debug,
    {
        self.debug_inspect_with(label, |_| {})
    }

    fn debug_inspect_with<F>(&self, label: &str, inspector: F) -> Self
    where
        T: std::fmt::Debug,
        F: Fn(&T) + Send + Sync + 'static,
    {
        let op = DebugInspectOp::new(label.to_string(), inspector);
        let id = self
            .pipeline
            .insert_node(Node::Stateless(vec![Arc::new(op)]));
        self.pipeline.connect(self.id, id);

        Self {
            pipeline: self.pipeline.clone(),
            id,
            _t: PhantomData,
        }
    }

    fn debug_count(&self, label: &str) -> Self {
        let op: DebugCountOp<T> = DebugCountOp::new(label.to_string());
        let id = self
            .pipeline
            .insert_node(Node::Stateless(vec![Arc::new(op)]));
        self.pipeline.connect(self.id, id);

        Self {
            pipeline: self.pipeline.clone(),
            id,
            _t: PhantomData,
        }
    }

    fn debug_sample(&self, n: usize, label: &str) -> Self
    where
        T: std::fmt::Debug,
    {
        let op: DebugSampleOp<T> = DebugSampleOp::new(label.to_string(), n);
        let id = self
            .pipeline
            .insert_node(Node::Stateless(vec![Arc::new(op)]));
        self.pipeline.connect(self.id, id);

        Self {
            pipeline: self.pipeline.clone(),
            id,
            _t: PhantomData,
        }
    }
}
