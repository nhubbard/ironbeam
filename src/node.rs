use crate::type_token::{Partition, TypeTag, VecOps};
use std::any::Any;
use std::sync::Arc;

pub trait DynOp: Send + Sync {
    fn apply(&self, input: Partition) -> Partition;

    fn key_preserving(&self) -> bool {
        false
    }
    fn value_only(&self) -> bool {
        false
    }
    fn reorder_safe_with_value_only(&self) -> bool {
        false
    }
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

    /// GroupByKey: local partitions Vec<(K,V)> → HashMap<K, Vec<V>>
    /// merge: Vec<HashMap<K,Vec<V>>> → Vec<(K,Vec<V>)>
    GroupByKey {
        local: Arc<dyn Fn(Partition) -> Partition + Send + Sync>,
        merge: Arc<dyn Fn(Vec<Partition>) -> Partition + Send + Sync>,
    },

    /// Binary co-group:
    /// `left_chain` and `right_chain` are full sub-plans that each produce a Vec<(K, V)> and Vec<(K, W)>
    /// The `exec` closure receives their *materialized* Partitions and returns a Partition of
    /// Vec<(K, (Vec<V>, Vec<W>))> (type-erased).
    CoGroup {
        left_chain: Arc<Vec<Node>>,
        right_chain: Arc<Vec<Node>>,
        coalesce_left: Arc<dyn Fn(Vec<Partition>) -> Partition + Send + Sync>,
        coalesce_right: Arc<dyn Fn(Vec<Partition>) -> Partition + Send + Sync>,
        exec: Arc<dyn Fn(Partition, Partition) -> Partition + Send + Sync>,
    },

    /// Pre-materialized payload (type-erased)
    Materialized(Arc<dyn Any + Send + Sync>),
}
