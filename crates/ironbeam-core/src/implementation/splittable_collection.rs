impl<T: Element> PCollection<T> {
    /// Apply a versioned offset-restricted callback to each input element.
    ///
    /// The callback must claim every position it processes. The output is
    /// discarded if it returns before completing the primary restriction.
    /// Direct processes callback-created residuals with the last reported
    /// watermark before returning the element's output.
    pub fn splittable_flat_map_named<U: Element>(
        &self,
        key: &str,
        semantic_id: &str,
        version: Version,
        initial_restriction: impl Fn(&T) -> Result<OffsetRange, Error> + Send + Sync + 'static,
        process: impl Fn(&T, &mut OffsetProcessContext<U>) -> Result<(), Error> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        Self::validate_semantic_handler(key, semantic_id, version)?;
        if !T::supports_coder() || !U::supports_coder() {
            return Err(Error::graph(
                key,
                "splittable input and output require registered element coders",
            ));
        }
        let initial_restriction = Arc::new(initial_restriction);
        let process = Arc::new(process);
        let direct_initial = Arc::clone(&initial_restriction);
        let direct_process = Arc::clone(&process);
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
            let mut restriction = direct_initial(&value)?;
            let mut watermark = None;
            let mut outputs = Vec::new();
            loop {
                let tracker = OffsetRangeTracker::new(restriction)?;
                let estimator = OffsetWatermarkEstimator::new(watermark);
                let mut context = OffsetProcessContext::new(tracker.clone(), estimator.clone());
                direct_process(&value, &mut context)?;
                let primary = tracker.restriction()?;
                outputs.extend(context.finish()?.into_iter().map(|output| Envelope {
                    pane,
                    value: Box::new(output.value),
                    timestamp_ns: output.timestamp_ns,
                    window: window.clone(),
                    assigned_window,
                }));
                if primary.stop == restriction.stop {
                    return Ok(outputs);
                }
                if primary.stop == restriction.start {
                    return Err(Error::execution(
                        &key_owned,
                        "direct",
                        "splittable callback made no progress before splitting",
                    ));
                }
                watermark = estimator.current()?;
                restriction = OffsetRange::new(primary.stop, restriction.stop)?;
            }
        }));
        let output = self.next_with_semantic(
            key,
            Operation::SplittableParDo,
            Some(transform),
            None,
            Some((semantic_id, version)),
        )?;
        self.pipeline.add_splittable(
            key,
            SplittableRegistration {
                initial: Arc::new(move |bytes| initial_restriction(&T::decode(bytes)?)),
                process: Arc::new(move |bytes, tracker, estimator| {
                    let value = T::decode(bytes)?;
                    let mut context = OffsetProcessContext::new(tracker, estimator);
                    process(&value, &mut context)?;
                    context
                        .finish()?
                        .into_iter()
                        .map(|output| {
                            Ok(Timestamped {
                                value: output.value.encode()?,
                                timestamp_ns: output.timestamp_ns,
                            })
                        })
                        .collect()
                }),
            },
        )?;
        Ok(output)
    }
}
