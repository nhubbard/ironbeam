use std::any::Any;
use std::sync::Arc;
use crate::type_token::{Partition, TypeTag, VecOps};

pub trait DynOp: Send + Sync {
    fn apply(&self, input: Partition) -> Partition;
}

#[derive(Clone)]
pub enum Node {
    /// Source keeps payload and VecOps (no more hardcoded types)
    Source {
        payload: Arc<dyn Any + Send + Sync>,
        vec_ops: Arc<dyn VecOps>,
        elem_tag: TypeTag,
    },

    /// Fused stateless ops (map/filter/flat_map)
    Stateless(Vec<Arc<dyn DynOp>>),

    /// Barrier with typed closures:
    /// - local: per-partition transform
    /// - merge: merge partitions into a single partition
    GroupByKey {
        local: Arc<dyn Fn(Partition) -> Partition + Send + Sync>,
        merge: Arc<dyn Fn(Vec<Partition>) -> Partition + Send + Sync>,
    },

    CombineValues {
        local: Arc<dyn Fn(Partition) -> Partition + Send + Sync>,
        merge: Arc<dyn Fn(Vec<Partition>) -> Partition + Send + Sync>,
    },

    /// (Optional) Materialized for tests (rarely needed now)
    Materialized(Arc<dyn Any + Send + Sync>),
}