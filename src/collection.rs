use crate::node::DynOp;
use crate::pipeline::Pipeline;
use crate::type_token::Partition;
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

pub trait RFBound: 'static + Send + Sync + Clone {}
impl<T> RFBound for T where T: 'static + Send + Sync + Clone {}

#[derive(Clone)]
pub struct PCollection<T> {
    pub(crate) pipeline: Pipeline,
    pub(crate) id: crate::NodeId,
    pub(crate) _t: PhantomData<T>,
}

// ----- stateless ops (DynOp) -----
pub(crate) struct MapOp<I, O, F>(pub F, pub PhantomData<(I, O)>);

impl<I, O, F> DynOp for MapOp<I, O, F>
where
    I: RFBound,
    O: RFBound,
    F: Send + Sync + Fn(&I) -> O + 'static,
{
    fn apply(&self, input: Partition) -> Partition {
        let v = *input.downcast::<Vec<I>>().expect("MapOp input type");
        let out: Vec<O> = v.iter().map(|i| self.0(i)).collect();
        Box::new(out) as Partition
    }
}

pub(crate) struct MapValuesOp<K, V, O, F>(pub F, pub PhantomData<(K, V, O)>);

impl<K, V, O, F> DynOp for MapValuesOp<K, V, O, F>
where
    K: RFBound,
    V: RFBound,
    O: RFBound,
    F: 'static + Send + Sync + Fn(&V) -> O,
{
    fn apply(&self, p: Partition) -> Partition {
        let f = &self.0;
        let kv = *p
            .downcast::<Vec<(K, V)>>()
            .expect("MapValuesOp: expected Vec<(K,V)>");
        let out: Vec<(K, O)> = kv.into_iter().map(|(k, v)| (k, f(&v))).collect();
        Box::new(out) as Partition
    }

    // Planner capability flags:
    fn key_preserving(&self) -> bool {
        true
    }
    fn value_only(&self) -> bool {
        true
    }
    fn reorder_safe_with_value_only(&self) -> bool {
        true
    }
    fn cost_hint(&self) -> u8 {
        3
    } // cheap, but keep filters before it
}

pub(crate) struct FilterOp<T, P>(pub P, pub PhantomData<T>);

impl<T, P> DynOp for FilterOp<T, P>
where
    T: RFBound,
    P: Send + Sync + Fn(&T) -> bool + 'static,
{
    fn apply(&self, input: Partition) -> Partition {
        let v = *input.downcast::<Vec<T>>().expect("FilterOp input type");
        let out: Vec<T> = v.into_iter().filter(|t| self.0(t)).collect();
        Box::new(out) as Partition
    }
}

pub(crate) struct FilterValuesOp<K, V, F>(pub F, pub PhantomData<(K, V)>);

impl<K, V, F> DynOp for FilterValuesOp<K, V, F>
where
    K: RFBound,
    V: RFBound,
    F: 'static + Send + Sync + Fn(&V) -> bool,
{
    fn apply(&self, p: Partition) -> Partition {
        let pred = &self.0;
        let kv = *p
            .downcast::<Vec<(K, V)>>()
            .expect("FilterValuesOp: expected Vec<(K,V)>");
        let out: Vec<(K, V)> = kv.into_iter().filter(|(_, v)| pred(v)).collect();
        Box::new(out) as Partition
    }

    // Planner capability flags:
    fn key_preserving(&self) -> bool {
        true
    }
    fn value_only(&self) -> bool {
        true
    }
    fn reorder_safe_with_value_only(&self) -> bool {
        true
    }
    fn cost_hint(&self) -> u8 {
        1
    } // filters are "cheapest" -> push earlier
}

pub(crate) struct FlatMapOp<I, O, F>(pub F, pub PhantomData<(I, O)>);

impl<I, O, F> DynOp for FlatMapOp<I, O, F>
where
    I: RFBound,
    O: RFBound,
    F: Send + Sync + Fn(&I) -> Vec<O> + 'static,
{
    fn apply(&self, input: Partition) -> Partition {
        let v = *input.downcast::<Vec<I>>().expect("FlatMapOp input type");
        let mut out: Vec<O> = Vec::new();
        for i in &v {
            out.extend(self.0(i));
        }
        Box::new(out) as Partition
    }
}

// ---------- Combine (per key) ----------

pub trait CombineFn<V, A, O>: Send + Sync + 'static {
    fn create(&self) -> A;
    fn add_input(&self, acc: &mut A, v: V);
    fn merge(&self, acc: &mut A, other: A);
    fn finish(&self, acc: A) -> O;
}

/// Built-in combiner: counts values per key.
#[derive(Clone, Default)]
pub struct Count;

impl<V> CombineFn<V, u64, u64> for Count {
    fn create(&self) -> u64 {
        0
    }
    fn add_input(&self, acc: &mut u64, _v: V) {
        *acc += 1;
    }
    fn merge(&self, acc: &mut u64, other: u64) {
        *acc += other;
    }
    fn finish(&self, acc: u64) -> u64 {
        acc
    }
}

/// A combiner that can construct its accumulator directly from a group of values.
/// Default implementation folds via `add_input`, but specific combiners can override.
pub trait LiftableCombiner<V, A, O>: CombineFn<V, A, O>
where
    V: RFBound,
{
    fn build_from_group(&self, values: &[V]) -> A {
        let mut acc = self.create();
        for v in values {
            self.add_input(&mut acc, v.clone());
        }
        acc
    }
}

impl<V: RFBound> LiftableCombiner<V, u64, u64> for Count {
    fn build_from_group(&self, values: &[V]) -> u64 {
        values.len() as u64
    }
}

#[derive(Clone)]
pub struct SideInput<T: RFBound>(pub Arc<Vec<T>>);

#[derive(Clone)]
pub struct SideMap<K: RFBound + Eq + Hash, V: RFBound>(pub Arc<HashMap<K, V>>);
