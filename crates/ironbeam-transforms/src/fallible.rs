//! Per-element failures carried as graph values until a terminal decides policy.
use ironbeam_core::{Element, Error, PCollection};

/// Preserve per-element failures as `Result` values in a collection.
pub trait TryProcessExt<T: Element> {
    /// Map each input to a `Result` retained as a collection element.
    fn try_map_named<O: Element, E: Element>(
        &self,
        key: &str,
        f: impl Fn(&T) -> Result<O, E> + Send + Sync + 'static,
    ) -> Result<PCollection<Result<O, E>>, Error>;
    /// Map each input to a `Result<Vec<_>, _>` retained as one element.
    fn try_flat_map_named<O: Element, E: Element>(
        &self,
        key: &str,
        f: impl Fn(&T) -> Result<Vec<O>, E> + Send + Sync + 'static,
    ) -> Result<PCollection<Result<Vec<O>, E>>, Error>;
}
impl<T: Element> TryProcessExt<T> for PCollection<T> {
    fn try_map_named<O: Element, E: Element>(
        &self,
        key: &str,
        f: impl Fn(&T) -> Result<O, E> + Send + Sync + 'static,
    ) -> Result<PCollection<Result<O, E>>, Error> {
        self.map_named(key, move |value| f(&value))
    }
    fn try_flat_map_named<O: Element, E: Element>(
        &self,
        key: &str,
        f: impl Fn(&T) -> Result<Vec<O>, E> + Send + Sync + 'static,
    ) -> Result<PCollection<Result<Vec<O>, E>>, Error> {
        self.map_named(key, move |value| f(&value))
    }
}
