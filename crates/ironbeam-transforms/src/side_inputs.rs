//! Immutable constant side inputs for small local lookup data.
use ironbeam_core::{Element, Error, PCollection};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

#[derive(Clone)]
pub struct SideVec<T>(Arc<Vec<T>>);
#[derive(Clone)]
pub struct SideMap<K, V>(Arc<HashMap<K, V>>);
#[derive(Clone)]
pub struct SideSingleton<T>(Arc<T>);
#[derive(Clone)]
pub struct SideMultimap<K, V>(Arc<HashMap<K, Vec<V>>>);

#[must_use]
pub fn side_vec<T: Element>(values: Vec<T>) -> SideVec<T> {
    SideVec(Arc::new(values))
}

#[must_use]
pub fn side_hashmap<K: Element + Eq + Hash + Sync, V: Element + Sync>(
    pairs: Vec<(K, V)>,
) -> SideMap<K, V> {
    SideMap(Arc::new(pairs.into_iter().collect()))
}

#[must_use]
pub fn side_singleton<T: Element>(value: T) -> SideSingleton<T> {
    SideSingleton(Arc::new(value))
}

#[must_use]
pub fn side_multimap<K: Element + Eq + Hash + Sync, V: Element + Sync>(
    pairs: Vec<(K, V)>,
) -> SideMultimap<K, V> {
    let mut map: HashMap<K, Vec<V>> = HashMap::new();
    for (key, value) in pairs {
        map.entry(key).or_default().push(value);
    }
    SideMultimap(Arc::new(map))
}

pub trait ConstantSideInputExt<T: Element> {
    fn map_with_side_vec_named<S: Element + Sync, U: Element>(
        &self,
        key: &str,
        side: &SideVec<S>,
        f: impl Fn(&T, &[S]) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error>;
    fn filter_with_side_vec_named<S: Element + Sync>(
        &self,
        key: &str,
        side: &SideVec<S>,
        f: impl Fn(&T, &[S]) -> bool + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error>;
    fn map_with_side_map_named<K: Element + Eq + Hash + Sync, V: Element + Sync, U: Element>(
        &self,
        key: &str,
        side: &SideMap<K, V>,
        f: impl Fn(&T, &HashMap<K, V>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error>;
    fn filter_with_side_map_named<K: Element + Eq + Hash + Sync, V: Element + Sync>(
        &self,
        key: &str,
        side: &SideMap<K, V>,
        f: impl Fn(&T, &HashMap<K, V>) -> bool + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error>;
    fn map_with_singleton_named<S: Element + Sync, U: Element>(
        &self,
        key: &str,
        side: &SideSingleton<S>,
        f: impl Fn(&T, &S) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error>;
    fn filter_with_singleton_named<S: Element + Sync>(
        &self,
        key: &str,
        side: &SideSingleton<S>,
        f: impl Fn(&T, &S) -> bool + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error>;
    fn map_with_side_multimap_named<K: Element + Eq + Hash + Sync, V: Element + Sync, U: Element>(
        &self,
        key: &str,
        side: &SideMultimap<K, V>,
        f: impl Fn(&T, &HashMap<K, Vec<V>>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error>;
    fn filter_with_side_multimap_named<K: Element + Eq + Hash + Sync, V: Element + Sync>(
        &self,
        key: &str,
        side: &SideMultimap<K, V>,
        f: impl Fn(&T, &HashMap<K, Vec<V>>) -> bool + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error>;
}

impl<T: Element> ConstantSideInputExt<T> for PCollection<T> {
    fn map_with_side_vec_named<S: Element + Sync, U: Element>(
        &self,
        key: &str,
        side: &SideVec<S>,
        f: impl Fn(&T, &[S]) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        let side = Arc::clone(&side.0);
        self.map_named(key, move |value| f(&value, &side))
    }
    fn filter_with_side_vec_named<S: Element + Sync>(
        &self,
        key: &str,
        side: &SideVec<S>,
        f: impl Fn(&T, &[S]) -> bool + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error> {
        let side = Arc::clone(&side.0);
        self.filter_named(key, move |value| f(value, &side))
    }
    fn map_with_side_map_named<K: Element + Eq + Hash + Sync, V: Element + Sync, U: Element>(
        &self,
        key: &str,
        side: &SideMap<K, V>,
        f: impl Fn(&T, &HashMap<K, V>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        let side = Arc::clone(&side.0);
        self.map_named(key, move |value| f(&value, &side))
    }
    fn filter_with_side_map_named<K: Element + Eq + Hash + Sync, V: Element + Sync>(
        &self,
        key: &str,
        side: &SideMap<K, V>,
        f: impl Fn(&T, &HashMap<K, V>) -> bool + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error> {
        let side = Arc::clone(&side.0);
        self.filter_named(key, move |value| f(value, &side))
    }
    fn map_with_singleton_named<S: Element + Sync, U: Element>(
        &self,
        key: &str,
        side: &SideSingleton<S>,
        f: impl Fn(&T, &S) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        let side = Arc::clone(&side.0);
        self.map_named(key, move |value| f(&value, &side))
    }
    fn filter_with_singleton_named<S: Element + Sync>(
        &self,
        key: &str,
        side: &SideSingleton<S>,
        f: impl Fn(&T, &S) -> bool + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error> {
        let side = Arc::clone(&side.0);
        self.filter_named(key, move |value| f(value, &side))
    }
    fn map_with_side_multimap_named<
        K: Element + Eq + Hash + Sync,
        V: Element + Sync,
        U: Element,
    >(
        &self,
        key: &str,
        side: &SideMultimap<K, V>,
        f: impl Fn(&T, &HashMap<K, Vec<V>>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        let side = Arc::clone(&side.0);
        self.map_named(key, move |value| f(&value, &side))
    }
    fn filter_with_side_multimap_named<K: Element + Eq + Hash + Sync, V: Element + Sync>(
        &self,
        key: &str,
        side: &SideMultimap<K, V>,
        f: impl Fn(&T, &HashMap<K, Vec<V>>) -> bool + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error> {
        let side = Arc::clone(&side.0);
        self.filter_named(key, move |value| f(value, &side))
    }
}

/// Side views computed from bounded collections in the same pipeline.
pub trait DynamicSideInputExt<T: Element> {
    fn map_with_dynamic_singleton_named<S: Element, U: Element>(
        &self,
        key: &str,
        side: &PCollection<S>,
        f: impl Fn(T, &S) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error>;
    fn map_with_dynamic_map_named<K: Element + Eq + Hash, V: Element, U: Element>(
        &self,
        key: &str,
        side: &PCollection<(K, V)>,
        f: impl Fn(T, &HashMap<K, V>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error>;
    fn map_with_dynamic_multimap_named<K: Element + Eq + Hash, V: Element, U: Element>(
        &self,
        key: &str,
        side: &PCollection<(K, V)>,
        f: impl Fn(T, &HashMap<K, Vec<V>>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error>;
    fn filter_with_dynamic_singleton_named<S: Element>(
        &self,
        key: &str,
        side: &PCollection<S>,
        f: impl Fn(&T, &S) -> bool + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error>;
    fn filter_with_dynamic_map_named<K: Element + Eq + Hash, V: Element>(
        &self,
        key: &str,
        side: &PCollection<(K, V)>,
        f: impl Fn(&T, &HashMap<K, V>) -> bool + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error>;
    fn filter_with_dynamic_multimap_named<K: Element + Eq + Hash, V: Element>(
        &self,
        key: &str,
        side: &PCollection<(K, V)>,
        f: impl Fn(&T, &HashMap<K, Vec<V>>) -> bool + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error>;
}

impl<T: Element> DynamicSideInputExt<T> for PCollection<T> {
    fn map_with_dynamic_singleton_named<S: Element, U: Element>(
        &self,
        key: &str,
        side: &PCollection<S>,
        f: impl Fn(T, &S) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        let name = key.to_owned();
        self.map_with_materialized_side_named(
            key,
            side,
            move |mut values| {
                if values.len() != 1 {
                    return Err(Error::execution(
                        &name,
                        "direct",
                        format!(
                            "singleton side input requires one value; received {}",
                            values.len()
                        ),
                    ));
                }
                Ok(values.remove(0))
            },
            move |value, side| Ok(f(value, side)),
        )
    }
    fn map_with_dynamic_map_named<K: Element + Eq + Hash, V: Element, U: Element>(
        &self,
        key: &str,
        side: &PCollection<(K, V)>,
        f: impl Fn(T, &HashMap<K, V>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        self.map_with_materialized_side_named(
            key,
            side,
            |values| Ok(values.into_iter().collect::<HashMap<K, V>>()),
            move |value, side| Ok(f(value, side)),
        )
    }
    fn map_with_dynamic_multimap_named<K: Element + Eq + Hash, V: Element, U: Element>(
        &self,
        key: &str,
        side: &PCollection<(K, V)>,
        f: impl Fn(T, &HashMap<K, Vec<V>>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        self.map_with_materialized_side_named(
            key,
            side,
            |values| {
                let mut map: HashMap<K, Vec<V>> = HashMap::new();
                for (key, value) in values {
                    map.entry(key).or_default().push(value);
                }
                Ok(map)
            },
            move |value, side| Ok(f(value, side)),
        )
    }
    fn filter_with_dynamic_singleton_named<S: Element>(
        &self,
        key: &str,
        side: &PCollection<S>,
        f: impl Fn(&T, &S) -> bool + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error> {
        self.map_with_dynamic_singleton_named(
            &format!("{key}/evaluate"),
            side,
            move |value, side| f(&value, side).then_some(value),
        )?
        .flat_map_named(key, |value| value.into_iter().collect())
    }
    fn filter_with_dynamic_map_named<K: Element + Eq + Hash, V: Element>(
        &self,
        key: &str,
        side: &PCollection<(K, V)>,
        f: impl Fn(&T, &HashMap<K, V>) -> bool + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error> {
        self.map_with_dynamic_map_named(&format!("{key}/evaluate"), side, move |value, side| {
            f(&value, side).then_some(value)
        })?
        .flat_map_named(key, |value| value.into_iter().collect())
    }
    fn filter_with_dynamic_multimap_named<K: Element + Eq + Hash, V: Element>(
        &self,
        key: &str,
        side: &PCollection<(K, V)>,
        f: impl Fn(&T, &HashMap<K, Vec<V>>) -> bool + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error> {
        self.map_with_dynamic_multimap_named(
            &format!("{key}/evaluate"),
            side,
            move |value, side| f(&value, side).then_some(value),
        )?
        .flat_map_named(key, |value| value.into_iter().collect())
    }
}
