use std::any::Any;
use std::sync::Arc;

pub trait DynOp: Send + Sync {
    fn apply(&self, input: super::runner::Partition) -> super::runner::Partition;
}

#[derive(Clone)]
pub enum Node {
    /// Materialized or logical payloads are Arc so Node can be Clone
    Materialized(Arc<dyn Any + Send + Sync>),
    Source(Arc<dyn Any + Send + Sync>),

    /// Fused stateless ops (map/filter/flat_map)
    Stateless(Vec<Arc<dyn DynOp>>),

    /// Barriers
    GroupByKey,
    /// Type-erased combiner (e.g., Count)
    CombineValues(Arc<dyn Any + Send + Sync>),
}