//! Type tags and type-erased vector helpers.
//!
//! This module provides:
//! - [`TypeTag`]: a lightweight runtime type identifier used by the planner/runner
//!   to assert element types across node boundaries without carrying generic types.
//! - [`VecOps`]: a type-erased interface for common `Vec<T>` operations required by
//!   the runner (length, splitting, cloning). Concrete implementations are produced
//!   via [`vec_ops_for`].
//!
//! The runner relies on `VecOps` to handle `Source` payloads without knowing `T` at
//! compile time. Splitting is used to create per-partition chunks for parallel
//! execution. All operations are safe and return `None` if the dynamic type does not
//! match the expected `Vec<T>`.

use std::any::{type_name, Any, TypeId};
use std::marker::PhantomData;
use std::sync::Arc;

/// A partition buffer carried between nodes at runtime.
///
/// The execution engine materializes intermediate results as opaque, type-erased
/// partitions. Nodes downcast these to the expected type (e.g., `Vec<T>`) when
/// applying their work.
pub type Partition = Box<dyn Any + Send + Sync>;

/// A lightweight runtime type tag for debugging and assertions.
///
/// `TypeTag` carries the `TypeId` and a readable type name. It is attached to
/// `Source` nodes so the runner and planner can reason about the element type
/// without a generic parameter.
///
/// ```ignore
/// use rustflow::type_token::TypeTag;
/// let tag = TypeTag::of::<u32>();
/// assert_eq!(tag.name, "u32");
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TypeTag {
    /// Stable Rust type identifier.
    pub id: TypeId,
    /// Human-readable type name (best-effort).
    pub name: &'static str,
}

impl TypeTag {
    /// Construct a tag for `T`.
    pub fn of<T: 'static>() -> Self {
        Self {
            id: TypeId::of::<T>(),
            name: type_name::<T>(),
        }
    }
}

/// Type-erased helpers for `Vec<T>`.
///
/// The runner uses `VecOps` to:
/// - compute the logical size of the source (`len`)
/// - split the source into `n` partitions (`split`)
/// - clone the entire source when executing sequentially (`clone_any`)
///
/// Implementations must return `None` when the provided `data` does not match
/// the concrete `Vec<T>` the implementor expects.
pub trait VecOps: Send + Sync {
    /// Return the number of elements if `data` is a `Vec<T>`, otherwise `None`.
    fn len(&self, data: &dyn Any) -> Option<usize>;

    /// Split `data` (a `Vec<T>`) into up to `n` contiguous partitions.
    ///
    /// Implementations should:
    /// - gracefully handle `n <= 1` or very small inputs by returning a single chunk
    /// - preserve element order within each returned chunk
    fn split(&self, data: &dyn Any, n: usize) -> Option<Vec<Partition>>;

    /// Clone the entire `Vec<T>` behind `data` and return it boxed as a [`Partition`].
    fn clone_any(&self, data: &dyn Any) -> Option<Partition>;
}

/// Concrete `VecOps` for a specific `T`.
///
/// This wraps a phantom type to bind the implementation to `T` while remaining
/// type-erased via the `VecOps` trait object at call sites.
pub struct VecOpsImpl<T: Clone + Send + Sync + 'static>(PhantomData<T>);

impl<T: Clone + Send + Sync + 'static> VecOps for VecOpsImpl<T> {
    fn len(&self, data: &dyn Any) -> Option<usize> {
        data.downcast_ref::<Vec<T>>().map(|v| v.len())
    }

    fn split(&self, data: &dyn Any, n: usize) -> Option<Vec<Partition>> {
        let v = data.downcast_ref::<Vec<T>>()?;
        let len = v.len();

        // Degenerate cases: one chunk is fine.
        if n <= 1 || len <= 1 {
            return Some(vec![Box::new(v.clone())]);
        }

        // Split into contiguous chunks of ~len/n each (last chunk may be shorter).
        let chunk = len.div_ceil(n);
        let parts = v
            .chunks(chunk)
            .map(|c| Box::new(c.to_vec()) as Partition)
            .collect();
        Some(parts)
    }

    fn clone_any(&self, data: &dyn Any) -> Option<Partition> {
        data.downcast_ref::<Vec<T>>()
            .map(|v| Box::new(v.clone()) as Partition)
    }
}

/// Create a type-erased `VecOps` for `Vec<T>`.
///
/// The returned trait object is used by `Source` nodes to support length queries,
/// partition splitting, and cloning at runtime.
///
/// ```ignore
/// use rustflow::type_token::{vec_ops_for, VecOps};
/// use std::any::Any;
///
/// let ops: std::sync::Arc<dyn VecOps> = vec_ops_for::<i64>();
/// let data: Box<dyn Any + Send + Sync> = Box::new(vec![1i64, 2, 3]);
/// assert_eq!(ops.len(data.as_ref()), Some(3));
/// ```
pub fn vec_ops_for<T: Clone + Send + Sync + 'static>() -> Arc<dyn VecOps> {
    Arc::new(VecOpsImpl::<T>(PhantomData))
}