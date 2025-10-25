use crate::io::{read_csv_range, read_jsonl_range, CsvShards, JsonlShards};
use crate::type_token::{Partition, VecOps};
use serde::de::DeserializeOwned;
use std::any::Any;
use std::sync::Arc;

/// VecOps for JSONL shards, typed over T.
pub struct JsonlVecOps<T>(std::marker::PhantomData<T>);
impl<T> JsonlVecOps<T> { pub fn new() -> Arc<Self> { Arc::new(Self(std::marker::PhantomData)) } }

impl<T> VecOps for JsonlVecOps<T>
where
    T: DeserializeOwned + Send + Sync + Clone + 'static,
{
    fn len(&self, data: &dyn Any) -> Option<usize> {
        let s = data.downcast_ref::<JsonlShards>()?;
        Some(s.total_lines as usize)
    }
    fn split(&self, data: &dyn Any, _n: usize) -> Option<Vec<Partition>> {
        let s = data.downcast_ref::<JsonlShards>()?;
        let mut parts = Vec::<Partition>::with_capacity(s.ranges.len());
        for &(start, end) in &s.ranges {
            let v: Vec<T> = read_jsonl_range::<T>(s, start, end).ok()?;
            parts.push(Box::new(v) as Partition);
        }
        Some(parts)
    }
    fn clone_any(&self, data: &dyn Any) -> Option<Partition> {
        // read full file if sequential
        let s = data.downcast_ref::<JsonlShards>()?;
        let v: Vec<T> = read_jsonl_range::<T>(s, 0, s.total_lines).ok()?;
        Some(Box::new(v) as Partition)
    }
}

/// VecOps for CSV shards, typed over T.
pub struct CsvVecOps<T>(std::marker::PhantomData<T>);
impl<T> CsvVecOps<T> { pub fn new() -> Arc<Self> { Arc::new(Self(std::marker::PhantomData)) } }

impl<T> VecOps for CsvVecOps<T>
where
    T: DeserializeOwned + Send + Sync + Clone + 'static,
{
    fn len(&self, data: &dyn Any) -> Option<usize> {
        let s = data.downcast_ref::<CsvShards>()?;
        Some(s.total_rows as usize)
    }
    fn split(&self, data: &dyn Any, _n: usize) -> Option<Vec<Partition>> {
        let s = data.downcast_ref::<CsvShards>()?;
        let mut parts = Vec::<Partition>::with_capacity(s.ranges.len());
        for &(start, end) in &s.ranges {
            let v: Vec<T> = read_csv_range::<T>(s, start, end).ok()?;
            parts.push(Box::new(v) as Partition);
        }
        Some(parts)
    }
    fn clone_any(&self, data: &dyn Any) -> Option<Partition> {
        let s = data.downcast_ref::<CsvShards>()?;
        let v: Vec<T> = read_csv_range::<T>(s, 0, s.total_rows).ok()?;
        Some(Box::new(v) as Partition)
    }
}