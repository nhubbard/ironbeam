use crate::type_token::{Partition, TypeTag, VecOps};
use std::any::Any;
use std::sync::Arc;

pub trait DynOp: Send + Sync {
    fn apply(&self, input: Partition) -> Partition;

    /* ---- Capability flags (planner uses these) ---- */

    /// True if the op never mutates the key when acting on (K, V).
    fn key_preserving(&self) -> bool {
        false
    }

    /// True if the op reads/writes only the value part of (K, V) and never inspects/modifies the key.
    fn value_only(&self) -> bool {
        false
    }

    /// True if reordering this op earlier across other value-only ops is semantics-preserving.
    fn reorder_safe_with_value_only(&self) -> bool {
        false
    }

    /// A tiny heuristic cost (lower is cheaper). Planner uses to order value-only ops.
    fn cost_hint(&self) -> u8 {
        10
    }
}

#[derive(Clone)]
pub enum Node {
    Source {
        payload: Arc<dyn Any + Send + Sync>,
        vec_ops: Arc<dyn VecOps>,
        elem_tag: TypeTag,
    },
    Stateless(Vec<Arc<dyn DynOp>>),

    /// Combine-by-key
    /// - `local_pairs`: consumes Vec<(K, V)>  → HashMap<K, A>
    /// - `local_groups`: optional lifted local that consumes Vec<(K, Vec<V>)> → HashMap<K, A>
    /// - `merge`: merges Vec<HashMap<K, A>> → Vec<(K, O)>
    CombineValues {
        local_pairs: Arc<dyn Fn(Partition) -> Partition + Send + Sync>,
        local_groups: Option<Arc<dyn Fn(Partition) -> Partition + Send + Sync>>,
        merge: Arc<dyn Fn(Vec<Partition>) -> Partition + Send + Sync>,
    },

    /// GroupByKey: local partitions Vec<(K,V)> → HashMap<K, Vec<V>> ; merge: Vec<HashMap<K,Vec<V>>> → Vec<(K,Vec<V>)>
    GroupByKey {
        local: Arc<dyn Fn(Partition) -> Partition + Send + Sync>,
        merge: Arc<dyn Fn(Vec<Partition>) -> Partition + Send + Sync>,
    },

    Materialized(Arc<dyn Any + Send + Sync>),
}
