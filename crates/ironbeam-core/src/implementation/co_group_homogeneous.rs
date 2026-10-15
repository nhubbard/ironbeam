/// One key with a value vector for each homogeneous co-group input.
pub type HomogeneousGroups<K, V> = PCollection<(K, Vec<Vec<V>>)>;

impl<K: Element + Eq + Hash + Clone, V: Element> PCollection<(K, V)> {
    /// Co-group collections with the same key and value types.
    /// Bucket indices follow input order: `self` is zero, then `others`.
    /// Values within a bucket have no runner-independent order.
    /// Bounded and unbounded inputs may be mixed when their window declarations match.
    pub fn co_group_homogeneous_named(
        &self,
        key: &str,
        others: &[Self],
    ) -> Result<HomogeneousGroups<K, V>, Error> {
        let count = others
            .len()
            .checked_add(1)
            .and_then(|count| u32::try_from(count).ok())
            .ok_or_else(|| Error::resource(key, "too many co-group inputs"))?;
        let main = self.parent()?;
        let mut unbounded = main.boundedness == Boundedness::Unbounded;
        for other in others {
            if !Arc::ptr_eq(&self.pipeline.inner, &other.pipeline.inner) {
                return Err(Error::graph(key, "co-group inputs belong to different pipelines"));
            }
            let node = other.parent()?;
            unbounded |= node.boundedness == Boundedness::Unbounded;
            if node.time != main.time {
                return Err(Error::graph(
                    key,
                    "co-group inputs must share time/window declarations",
                ));
            }
        }
        if unbounded
            && !matches!(
                main.time.window,
                Window::Fixed { .. }
                    | Window::FixedOffset { .. }
                    | Window::Sliding { .. }
                    | Window::Session { .. }
            )
            && !(main.time.window == Window::Global
                && main.time.trigger.early_count().is_some())
        {
            return Err(Error::graph(
                key,
                "unbounded homogeneous co-group requires a finite window or early count trigger",
            ));
        }
        let tagged = std::iter::once(self)
            .chain(others.iter())
            .enumerate()
            .map(|(index, input)| {
                let index = u32::try_from(index)
                    .map_err(|_| Error::resource(key, "too many co-group inputs"))?;
                input.map_named(&format!("{key}/tag-{index}"), move |(row_key, value)| {
                    (row_key, (index, value))
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let Some((first, rest)) = tagged.split_first() else {
            return Err(Error::graph(key, "co-group requires an input"));
        };
        let apply_key = key.to_owned();
        first
            .flatten_named(&format!("{key}/flatten"), rest)?
            .group_by_key_named(&format!("{key}/group"))?
            .flat_map_fallible_named(key, move |(row_key, values)| {
                let mut buckets = (0..count).map(|_| Vec::new()).collect::<Vec<Vec<V>>>();
                for (index, value) in values {
                    let bucket = buckets.get_mut(index as usize).ok_or_else(|| {
                        Error::execution(&apply_key, "co-group", "input tag outside declared range")
                    })?;
                    bucket.push(value);
                }
                Ok(vec![(row_key, buckets)])
            })
    }
}
