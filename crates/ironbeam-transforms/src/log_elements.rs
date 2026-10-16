//! Debug logging passthroughs. Writes happen when a runner executes the node.
use ironbeam_core::{Element, Error, PCollection};
use std::fmt::Debug;

pub trait LogElementsExt<T: Element> {
    /// Print each value with its `Debug` representation and pass it through.
    fn log_elements_named(&self, key: &str) -> Result<PCollection<T>, Error>
    where
        T: Debug;
    /// Print each value using `formatter` and pass it through.
    fn log_elements_with_named(
        &self,
        key: &str,
        formatter: impl Fn(&T) -> String + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error>;
}

impl<T: Element> LogElementsExt<T> for PCollection<T> {
    fn log_elements_named(&self, key: &str) -> Result<Self, Error>
    where
        T: Debug,
    {
        self.log_elements_with_named(key, |value| format!("{value:?}"))
    }

    fn log_elements_with_named(
        &self,
        key: &str,
        formatter: impl Fn(&T) -> String + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        self.map_named(key, move |value| {
            println!("{}", formatter(&value));
            value
        })
    }
}
