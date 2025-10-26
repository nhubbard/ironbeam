use std::any::{type_name, Any, TypeId};
use std::sync::Arc;

pub type Partition = Box<dyn Any + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TypeTag {
    pub id: TypeId,
    pub name: &'static str,
}
impl TypeTag {
    pub fn of<T: 'static>() -> Self {
        Self {
            id: TypeId::of::<T>(),
            name: type_name::<T>(),
        }
    }
}

/// Type-erased helpers for Vec<T>
pub trait VecOps: Send + Sync {
    fn len(&self, data: &dyn Any) -> Option<usize>;
    fn split(&self, data: &dyn Any, n: usize) -> Option<Vec<Partition>>;
    fn clone_any(&self, data: &dyn Any) -> Option<Partition>;
}

pub struct VecOpsImpl<T: Clone + Send + Sync + 'static>(std::marker::PhantomData<T>);
impl<T: Clone + Send + Sync + 'static> VecOps for VecOpsImpl<T> {
    fn len(&self, data: &dyn Any) -> Option<usize> {
        data.downcast_ref::<Vec<T>>().map(|v| v.len())
    }
    fn split(&self, data: &dyn Any, n: usize) -> Option<Vec<Partition>> {
        let v = data.downcast_ref::<Vec<T>>()?;
        let len = v.len();
        if n <= 1 || len <= 1 {
            return Some(vec![Box::new(v.clone())]);
        }
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

pub fn vec_ops_for<T: Clone + Send + Sync + 'static>() -> Arc<dyn VecOps> {
    Arc::new(VecOpsImpl::<T>(std::marker::PhantomData))
}
