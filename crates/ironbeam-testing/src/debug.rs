//! Local test inspection helpers for v5 graphs.
use ironbeam_core::{Element, Error, PCollection};
use ironbeam_direct::{DirectRunner, JobConfig};
use std::fmt::Debug;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

pub trait PCollectionDebugExt<T: Element> {
    fn debug_inspect_named(&self, key: &str, label: &str) -> Result<PCollection<T>, Error>
    where
        T: Debug;
    fn debug_inspect_with_named(
        &self,
        key: &str,
        label: &str,
        inspector: impl Fn(&T) + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error>
    where
        T: Debug;
    fn debug_sample_named(&self, key: &str, label: &str, n: usize) -> Result<PCollection<T>, Error>
    where
        T: Debug;
    fn collect_debug_count_named(
        &self,
        key: &str,
        label: &str,
        config: &JobConfig,
    ) -> Result<Vec<T>, Error>;
}
impl<T: Element> PCollectionDebugExt<T> for PCollection<T> {
    fn debug_inspect_named(&self, key: &str, label: &str) -> Result<Self, Error>
    where
        T: Debug,
    {
        self.debug_inspect_with_named(key, label, |_| {})
    }
    fn debug_inspect_with_named(
        &self,
        key: &str,
        label: &str,
        inspector: impl Fn(&T) + Send + Sync + 'static,
    ) -> Result<Self, Error>
    where
        T: Debug,
    {
        let label = label.to_owned();
        let seen = AtomicUsize::new(0);
        self.map_named(key, move |value| {
            inspector(&value);
            let index = seen.fetch_add(1, Ordering::Relaxed);
            if index < 10 {
                eprintln!("[Debug: {label}] [{index}]: {value:?}");
            }
            value
        })
    }
    fn debug_sample_named(&self, key: &str, label: &str, n: usize) -> Result<Self, Error>
    where
        T: Debug,
    {
        let label = Arc::new(label.to_owned());
        let seen = AtomicUsize::new(0);
        self.map_named(key, move |value| {
            let index = seen.fetch_add(1, Ordering::Relaxed);
            if index < n {
                eprintln!("[Debug: {label}] [{index}]: {value:?}");
            }
            value
        })
    }
    fn collect_debug_count_named(
        &self,
        key: &str,
        label: &str,
        config: &JobConfig,
    ) -> Result<Vec<T>, Error> {
        let output = self.output(key)?;
        let values = DirectRunner.run_collect(&output, config)?.0;
        eprintln!("[Debug: {label}] Count: {} elements", values.len());
        Ok(values)
    }
}
