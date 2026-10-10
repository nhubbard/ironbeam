//! Graph handle fan-out for independently constructed branches.
use ironbeam_core::{Element, PCollection};

pub trait TeeExt<T: Element> {
    fn tee(&self) -> (PCollection<T>, PCollection<T>);
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
