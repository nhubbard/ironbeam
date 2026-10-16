impl<T: Element> PCollection<T> {
    /// Apply a versioned offset-restricted callback to a bounded collection.
    ///
    /// The callback must claim every position it processes. The output is
    /// discarded if it returns before completing the primary restriction.
    /// Direct executes this contract locally; portable runner execution is
    /// enabled separately when its registered worker handler is available.
    pub fn splittable_flat_map_named<U: Element>(
        &self,
        key: &str,
        semantic_id: &str,
        version: Version,
        initial_restriction: impl Fn(&T) -> Result<OffsetRange, Error> + Send + Sync + 'static,
        process: impl Fn(&T, &mut OffsetProcessContext<U>) -> Result<(), Error> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        Self::validate_semantic_handler(key, semantic_id, version)?;
        if self.parent()?.boundedness != Boundedness::Bounded {
            return Err(Error::unsupported(
                key,
                "direct",
                "unbounded-splittable-pardo",
            ));
        }
        let key_owned = key.to_owned();
        let transform = Transform::Multi(Arc::new(move |event| {
            let Envelope {
                pane,
                value,
                window,
                assigned_window,
                ..
            } = event;
            let value = value.downcast::<T>().map_err(|_| {
                Error::execution(&key_owned, "direct", "splittable input type mismatch")
            })?;
            let restriction = initial_restriction(&value)?;
            let tracker = OffsetRangeTracker::new(restriction)?;
            let mut context =
                OffsetProcessContext::new(tracker.clone(), OffsetWatermarkEstimator::new(None));
            process(&value, &mut context)?;
            if tracker.restriction()? != restriction {
                return Err(Error::unsupported(
                    &key_owned,
                    "direct",
                    "runner-owned-sdf-split",
                ));
            }
            Ok(context
                .finish()?
                .into_iter()
                .map(|output| Envelope {
                    pane,
                    value: Box::new(output.value),
                    timestamp_ns: output.timestamp_ns,
                    window: window.clone(),
                    assigned_window,
                })
                .collect())
        }));
        self.next_with_semantic(
            key,
            Operation::SplittableParDo,
            Some(transform),
            None,
            Some((semantic_id, version)),
        )
    }
}
