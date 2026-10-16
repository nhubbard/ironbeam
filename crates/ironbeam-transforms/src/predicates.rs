//! Predicate conveniences composed over the core filter node.
use ironbeam_core::{Element, Error, PCollection};

pub trait PredicateExt<T: Element> {
    /// Keep values equal to `value`.
    fn filter_eq_named(&self, key: &str, value: &T) -> Result<PCollection<T>, Error>
    where
        T: PartialEq + Clone + Sync;
    /// Keep values unequal to `value`.
    fn filter_ne_named(&self, key: &str, value: &T) -> Result<PCollection<T>, Error>
    where
        T: PartialEq + Clone + Sync;
    /// Keep values less than `value`.
    fn filter_lt_named(&self, key: &str, value: &T) -> Result<PCollection<T>, Error>
    where
        T: PartialOrd + Clone + Sync;
    /// Keep values less than or equal to `value`.
    fn filter_le_named(&self, key: &str, value: &T) -> Result<PCollection<T>, Error>
    where
        T: PartialOrd + Clone + Sync;
    /// Keep values greater than `value`.
    fn filter_gt_named(&self, key: &str, value: &T) -> Result<PCollection<T>, Error>
    where
        T: PartialOrd + Clone + Sync;
    /// Keep values greater than or equal to `value`.
    fn filter_ge_named(&self, key: &str, value: &T) -> Result<PCollection<T>, Error>
    where
        T: PartialOrd + Clone + Sync;
    /// Keep values in the half-open range `[min, max)`.
    fn filter_range_named(&self, key: &str, min: &T, max: &T) -> Result<PCollection<T>, Error>
    where
        T: PartialOrd + Clone + Sync;
    /// Keep values in the closed range `[min, max]`.
    fn filter_range_inclusive_named(
        &self,
        key: &str,
        min: &T,
        max: &T,
    ) -> Result<PCollection<T>, Error>
    where
        T: PartialOrd + Clone + Sync;
    /// Keep values whose extracted field satisfies `predicate`.
    fn filter_by_named<V>(
        &self,
        key: &str,
        extract: impl Fn(&T) -> V + Send + Sync + 'static,
        predicate: impl Fn(&V) -> bool + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error>;
}

impl<T: Element> PredicateExt<T> for PCollection<T> {
    fn filter_eq_named(&self, key: &str, value: &T) -> Result<Self, Error>
    where
        T: PartialEq + Clone + Sync,
    {
        let value = value.clone();
        self.filter_named(key, move |element| element == &value)
    }
    fn filter_ne_named(&self, key: &str, value: &T) -> Result<Self, Error>
    where
        T: PartialEq + Clone + Sync,
    {
        let value = value.clone();
        self.filter_named(key, move |element| element != &value)
    }
    fn filter_lt_named(&self, key: &str, value: &T) -> Result<Self, Error>
    where
        T: PartialOrd + Clone + Sync,
    {
        let value = value.clone();
        self.filter_named(key, move |element| element < &value)
    }
    fn filter_le_named(&self, key: &str, value: &T) -> Result<Self, Error>
    where
        T: PartialOrd + Clone + Sync,
    {
        let value = value.clone();
        self.filter_named(key, move |element| element <= &value)
    }
    fn filter_gt_named(&self, key: &str, value: &T) -> Result<Self, Error>
    where
        T: PartialOrd + Clone + Sync,
    {
        let value = value.clone();
        self.filter_named(key, move |element| element > &value)
    }
    fn filter_ge_named(&self, key: &str, value: &T) -> Result<Self, Error>
    where
        T: PartialOrd + Clone + Sync,
    {
        let value = value.clone();
        self.filter_named(key, move |element| element >= &value)
    }
    fn filter_range_named(&self, key: &str, min: &T, max: &T) -> Result<Self, Error>
    where
        T: PartialOrd + Clone + Sync,
    {
        let min = min.clone();
        let max = max.clone();
        self.filter_named(key, move |element| element >= &min && element < &max)
    }
    fn filter_range_inclusive_named(&self, key: &str, min: &T, max: &T) -> Result<Self, Error>
    where
        T: PartialOrd + Clone + Sync,
    {
        let min = min.clone();
        let max = max.clone();
        self.filter_named(key, move |element| element >= &min && element <= &max)
    }
    fn filter_by_named<V>(
        &self,
        key: &str,
        extract: impl Fn(&T) -> V + Send + Sync + 'static,
        predicate: impl Fn(&V) -> bool + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        self.filter_named(key, move |element| predicate(&extract(element)))
    }
}
