impl<K: Element, V: Element> KeyedExt<K, V> for PCollection<(K, V)> {
    fn keys_named(&self, key: &str) -> Result<PCollection<K>, Error> {
        self.map_named(key, |(key, _)| key)
    }
    fn values_named(&self, key: &str) -> Result<PCollection<V>, Error> {
        self.map_named(key, |(_, value)| value)
    }
    fn map_values_named<W: Element>(
        &self,
        key: &str,
        f: impl Fn(V) -> W + Send + Sync + 'static,
    ) -> Result<PCollection<(K, W)>, Error> {
        self.map_named(key, move |(group, value)| (group, f(value)))
    }
    fn filter_values_named(
        &self,
        key: &str,
        f: impl Fn(&V) -> bool + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        self.filter_named(key, move |(_, value)| f(value))
    }
    fn count_per_key_named(
        &self,
        group_key: &str,
        count_key: &str,
    ) -> Result<PCollection<(K, u64)>, Error>
    where
        K: Eq + Hash + Clone,
    {
        self.combine_values_lifted_named(group_key, Count)?
            .map_named(count_key, |row| row)
    }
    fn distinct_per_key_named(&self, group_key: &str, key: &str) -> Result<Self, Error>
    where
        K: Eq + Hash + Clone,
        V: Eq + Hash + Clone,
    {
        self.group_by_key_named(group_key)?
            .flat_map_named(key, |(group, values)| {
                let mut seen = HashSet::new();
                values
                    .into_iter()
                    .filter(|value| seen.insert(value.clone()))
                    .map(|value| (group.clone(), value))
                    .collect()
            })
    }
    fn distinct_count_per_key_named(
        &self,
        group_key: &str,
        key: &str,
    ) -> Result<PCollection<(K, u64)>, Error>
    where
        K: Eq + Hash + Clone,
        V: Eq + Hash,
    {
        self.combine_values_lifted_named(group_key, DistinctCount)?
            .map_named(key, |row| row)
    }
    fn swap_named(&self, key: &str) -> Result<PCollection<(V, K)>, Error> {
        self.map_named(key, |(group, value)| (value, group))
    }
    fn sum_per_key_named(&self, key: &str) -> Result<Self, Error>
    where
        K: Eq + Hash + Clone,
        V: Add<Output = V> + Default,
    {
        self.combine_values_lifted_named(&format!("{key}/group"), Sum)?
            .map_named(key, |row| row)
    }
    fn min_per_key_named(&self, group_key: &str, key: &str) -> Result<Self, Error>
    where
        K: Eq + Hash + Clone,
        V: Ord,
    {
        self.combine_values_lifted_named(group_key, Minimum)?
            .flat_map_named(key, |(group, values)| {
                values
                    .into_iter()
                    .next()
                    .map(|value| (group, value))
                    .into_iter()
                    .collect()
            })
    }
    fn max_per_key_named(&self, group_key: &str, key: &str) -> Result<Self, Error>
    where
        K: Eq + Hash + Clone,
        V: Ord,
    {
        self.combine_values_lifted_named(group_key, Maximum)?
            .flat_map_named(key, |(group, values)| {
                values
                    .into_iter()
                    .next()
                    .map(|value| (group, value))
                    .into_iter()
                    .collect()
            })
    }
    fn average_per_key_named(
        &self,
        group_key: &str,
        key: &str,
    ) -> Result<PCollection<(K, f64)>, Error>
    where
        K: Eq + Hash + Clone,
        V: Into<f64>,
    {
        self.combine_values_lifted_named(group_key, Average)?
            .map_named(key, |row| row)
    }
    fn mean_per_key_named<O: MeanOutput>(
        &self,
        group_key: &str,
        key: &str,
    ) -> Result<PCollection<(K, O)>, Error>
    where
        K: Eq + Hash + Clone,
        V: Into<O>,
    {
        self.combine_values_lifted_named(group_key, Mean::<O>(PhantomData))?
            .map_named(key, |row| row)
    }
}
