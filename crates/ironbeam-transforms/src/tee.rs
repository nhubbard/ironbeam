//! Graph handle fan-out for independently constructed branches.
use ironbeam_core::{Element, PCollection};

pub trait TeeExt<T: Element> {
    /// Clone this graph handle for two independently built branches.
    fn tee(&self) -> (PCollection<T>, PCollection<T>);
    /// Clone this graph handle for `count` independently built branches.
    fn tee_n(&self, count: usize) -> Vec<PCollection<T>>;
}
impl<T: Element> TeeExt<T> for PCollection<T> {
    fn tee(&self) -> (Self, Self) {
        (self.clone(), self.clone())
    }
    fn tee_n(&self, count: usize) -> Vec<Self> {
        (0..count).map(|_| self.clone()).collect()
    }
}
