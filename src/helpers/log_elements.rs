//! Passthrough debug-tap helpers for [`PCollection`].
//!
//! [`PCollection::log_elements`] and [`PCollection::log_elements_with`] are
//! the Ironbeam equivalent of Apache Beam's `LogElements`: each transform
//! observes every element as it flows through, writes a rendered form to
//! standard output, and re-emits the element unchanged. They are
//! intentionally analogous to the `tap` combinator in iterator-style
//! pipelines and are primarily a debugging aid.
//!
//! Both methods are pure passthroughs: the downstream type, value, and
//! ordering match the input exactly. Only the side effect (writing to
//! `stdout`) differs from a no-op.
//!
//! For a more configurable, label-prefixed and capped variant intended for
//! test inspection, see [`crate::testing::PCollectionDebugExt::debug_inspect`].
//!
//! # Choosing between `log_elements` and `log_elements_with`
//!
//! - Use [`PCollection::log_elements`] when `T: Debug` and the default
//!   `{value:?}` rendering is enough — typically during ad-hoc inspection.
//! - Use [`PCollection::log_elements_with`] when you need control over the
//!   rendered representation (truncation, structured prefixes, redaction of
//!   sensitive fields, …) or when `T` does not implement [`Debug`].

use crate::collection::{Element, PCollection};
use crate::node::{DynOp, Node};
use crate::type_token::Partition;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

/// Internal [`DynOp`] backing [`PCollection::log_elements_with`] (and, by
/// composition, [`PCollection::log_elements`]).
///
/// For each element of the incoming partition the configured `formatter` is
/// invoked, and its return value is written to `stdout`, terminated by a
/// newline. The partition is then re-emitted unchanged.
pub(crate) struct LogElementsOp<T, F> {
    formatter: F,
    _phantom: PhantomData<T>,
}

impl<T, F> LogElementsOp<T, F> {
    pub(crate) const fn new(formatter: F) -> Self {
        Self {
            formatter,
            _phantom: PhantomData,
        }
    }
}

impl<T, F> DynOp for LogElementsOp<T, F>
where
    T: Element,
    F: Fn(&T) -> String + Send + Sync + 'static,
{
    fn apply(&self, input: Partition) -> Partition {
        let v = *input
            .downcast::<Vec<T>>()
            .expect("LogElementsOp: expected Vec<T> input");
        for item in &v {
            println!("{}", (self.formatter)(item));
        }
        Box::new(v) as Partition
    }
}

impl<T: Element> PCollection<T> {
    /// Print each element to standard output using its [`Debug`] representation,
    /// passing the collection through unchanged.
    ///
    /// This is the Ironbeam equivalent of Apache Beam's `LogElements` and is
    /// analogous to `tap` in iterator-style pipelines. The downstream
    /// collection is identical in type, value, and ordering to `self`; the
    /// only effect is that each element is written to `stdout` (one per
    /// line, formatted via `{value:?}`) as the pipeline executes.
    ///
    /// For custom formatting (including for types that do not implement
    /// [`Debug`]), use [`PCollection::log_elements_with`].
    ///
    /// # Example
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// // Prints `1`, `2`, `3` to stdout, then yields the original elements.
    /// let out = from_vec(&p, vec![1u32, 2, 3])
    ///     .log_elements()
    ///     .collect_seq()?;
    /// assert_eq!(out, vec![1u32, 2, 3]);
    /// # Ok(()) }
    /// ```
    #[must_use]
    pub fn log_elements(self) -> Self
    where
        T: Debug,
    {
        self.log_elements_with(|t| format!("{t:?}"))
    }

    /// Apply a user-supplied formatter to each element, write the resulting
    /// `String` to standard output, then pass the element through unchanged.
    ///
    /// `formatter` is invoked exactly once per element in the pipeline order
    /// within each partition; it does not affect the downstream type.
    ///
    /// Unlike [`PCollection::log_elements`], the element type does not need to
    /// implement [`Debug`] — only the formatter must produce a `String`.
    ///
    /// # Example
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// // Renders `[event] 1`, `[event] 2`, … to stdout.
    /// let out = from_vec(&p, vec![1u32, 2, 3])
    ///     .log_elements_with(|x| format!("[event] {x}"))
    ///     .collect_seq()?;
    /// assert_eq!(out, vec![1u32, 2, 3]);
    /// # Ok(()) }
    /// ```
    #[must_use]
    pub fn log_elements_with<F>(self, formatter: F) -> Self
    where
        F: Fn(&T) -> String + Send + Sync + 'static,
    {
        let op: Arc<dyn DynOp> = Arc::new(LogElementsOp::<T, F>::new(formatter));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        Self {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }
}
