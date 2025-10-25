use std::any::Any;

pub enum Node {
    /// Holds a materialized Vec<T> as type-erased payload.
    Materialized(Box<dyn Any + Send + Sync>),
}