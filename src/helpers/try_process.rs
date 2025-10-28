use std::fmt::Display;
use crate::{PCollection, RFBound};
use anyhow::{anyhow, Result};

impl<T: RFBound> PCollection<T> {
    pub fn try_map<O, E, F>(self, f: F) -> PCollection<Result<O, E>>
    where
        O: RFBound,
        E: 'static + Send + Sync + Clone + Display,
        F: 'static + Send + Sync + Fn(&T) -> Result<O, E>,
    {
        // Result<O,E> now satisfies RFBound because E: Clone
        self.map(move |t| f(t))
    }

    pub fn try_flat_map<O, E, F>(self, f: F) -> PCollection<Result<Vec<O>, E>>
    where
        O: RFBound,
        E: 'static + Send + Sync + Clone + Display,
        F: 'static + Send + Sync + Fn(&T) -> Result<Vec<O>, E>,
    {
        self.map(move |t| f(t))
    }
}

// Fail-fast terminal (keeps errors ergonomic)
impl<T: RFBound, E> PCollection<Result<T, E>>
where
    E: 'static + Send + Sync + Clone + Display,
{
    pub fn collect_fail_fast(self) -> Result<Vec<T>> {
        let mut ok = Vec::new();
        for r in self.collect_seq()? {
            match r {
                Ok(v) => ok.push(v),
                Err(e) => return Err(anyhow!("element failed: {}", e)),
            }
        }
        Ok(ok)
    }
}