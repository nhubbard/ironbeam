use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use crate::{PCollection, RFBound};
use crate::collection::{SideInput, SideMap};

pub fn side_vec<T: RFBound>(v: Vec<T>) -> SideInput<T> {
    SideInput(Arc::new(v))
}

impl<T: RFBound> PCollection<T> {
    /// Map with read-only side input vector (e.g., lookup table)
    pub fn map_with_side<O, S, F>(self, side: SideInput<S>, f: F) -> PCollection<O>
    where
        O: RFBound,
        S: RFBound,
        F: 'static + Send + Sync + Fn(&T, &[S]) -> O,
    {
        let side_arc = side.0.clone();
        self.map(move |t: &T| f(t, &side_arc))
    }

    /// Filter using side input
    pub fn filter_with_side<S, F>(self, side: SideInput<S>, pred: F) -> PCollection<T>
    where
        S: RFBound,
        F: 'static + Send + Sync + Fn(&T, &[S]) -> bool,
    {
        let side_arc = side.0.clone();
        self.filter(move |t: &T| pred(t, &side_arc))
    }
}

pub fn side_hashmap<K: RFBound + Eq + Hash, V: RFBound>(pairs: Vec<(K, V)>) -> SideMap<K, V> {
    SideMap(Arc::new(pairs.into_iter().collect()))
}

impl<T: RFBound> PCollection<T> {
    pub fn map_with_side_map<O, K, V, F>(self, side: SideMap<K, V>, f: F) -> PCollection<O>
    where
        O: RFBound,
        K: RFBound + Eq + Hash,
        V: RFBound,
        F: 'static + Send + Sync + Fn(&T, &HashMap<K, V>) -> O,
    {
        let side_map = side.0.clone();
        self.map(move |t: &T| f(t, &side_map))
    }
}