use std::marker::PhantomData;
use std::sync::Arc;
use crate::{PCollection, Pipeline, RFBound};
use crate::node::Node;
use crate::type_token::{vec_ops_for, TypeTag};

// ----- from_vec -----
pub fn from_vec<T>(p: &Pipeline, data: Vec<T>) -> PCollection<T>
where
    T: RFBound,
{
    let id = p.insert_node(Node::Source {
        payload: Arc::new(data),
        vec_ops: vec_ops_for::<T>(),
        elem_tag: TypeTag::of::<T>(),
    });
    PCollection {
        pipeline: p.clone(),
        id,
        _t: PhantomData,
    }
}

/// Create a collection from any owned iterator (collects into Vec<T>).
pub fn from_iter<T, I>(p: &Pipeline, iter: I) -> PCollection<T>
where
    T: RFBound,
    I: IntoIterator<Item = T>,
{
    from_vec(p, iter.into_iter().collect::<Vec<T>>())
}