impl<T: Element> PCollection<T> {
    /// Apply a bounded side collection from a different fixed or sliding window.
    ///
    /// Beam maps each main window's last millisecond into a side window. For
    /// sliding side windows, it chooses the earliest interval whose end follows
    /// that millisecond. With gaps, the interval may start later. Both
    /// collections need default triggers, zero lateness, and millisecond
    /// aligned windows. The side values are materialized once per mapped
    /// window; an empty window supplies an empty slice.
    ///
    /// # Errors
    ///
    /// Returns an error for unrelated pipelines, unsupported window policies,
    /// an unbounded side collection, or a graph registration failure.
    pub fn map_with_mapped_window_side_named<S: Element, U: Element>(
        &self,
        key: &str,
        side: &PCollection<S>,
        f: impl Fn(T, &[S]) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        self.map_with_mapped_window_materialized_side_named(
            key,
            side,
            Ok,
            move |value, values: &Vec<S>| Ok(f(value, values)),
        )
    }

    /// Materialize a bounded side view in the window mapped from each main window.
    ///
    /// # Errors
    ///
    /// Returns an error for unrelated pipelines, unsupported window policies,
    /// an unbounded side collection, or a graph registration failure.
    pub fn map_with_mapped_window_materialized_side_named<
        S: Element,
        M: Send + 'static,
        U: Element,
    >(
        &self,
        key: &str,
        side: &PCollection<S>,
        materialize: impl Fn(Vec<S>) -> Result<M, Error> + Send + Sync + 'static,
        f: impl Fn(T, &M) -> Result<U, Error> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        if !Arc::ptr_eq(&self.pipeline.inner, &side.pipeline.inner) {
            return Err(Error::graph(key, "side input belongs to a different pipeline"));
        }
        let main = self.parent()?;
        let other = side.parent()?;
        if other.boundedness != Boundedness::Bounded {
            return Err(Error::graph(key, "mapped side input must be bounded"));
        }
        validate_mapped_side_windows(key, &main.time, &other.time)?;
        let side_window = other.time.window.clone();
        let key_owned = key.to_owned();
        let materialize = Arc::new(materialize);
        let f = Arc::new(f);
        let side_key = key_owned.clone();
        let main_key = key_owned.clone();
        let registration = SideInputRegistration {
            materialize: Arc::new(move |values| {
                let mut typed = Vec::with_capacity(values.len());
                for value in values {
                    typed.push(*value.downcast::<S>().map_err(|_| {
                        Error::execution(&side_key, "direct", "side input type mismatch")
                    })?);
                }
                Ok(Box::new(materialize(typed)?) as Payload)
            }),
            apply: Arc::new(move |event, view| {
                let Envelope {
                    pane,
                    value,
                    timestamp_ns,
                    window,
                    assigned_window,
                } = event;
                let value = *value.downcast::<T>().map_err(|_| {
                    Error::execution(&main_key, "direct", "main input type mismatch")
                })?;
                let view = view.downcast_ref::<M>().ok_or_else(|| {
                    Error::execution(&main_key, "direct", "side input view type mismatch")
                })?;
                Ok(Envelope {
                    pane,
                    value: Box::new(f(value, view)?),
                    timestamp_ns,
                    window,
                    assigned_window,
                })
            }),
        };
        let direct_registration = registration.clone();
        let aggregate: Aggregate = Arc::new(move |inputs| {
            let [main_values, side_values]: [Vec<Envelope>; 2] = inputs.try_into().map_err(|_| {
                Error::execution(&key_owned, "direct", "mapped side input requires two inputs")
            })?;
            let mut side_buckets: HashMap<WindowInterval, Vec<Payload>> = HashMap::new();
            for event in side_values {
                let window = event.assigned_window.ok_or_else(|| {
                    Error::execution(&key_owned, "direct", "mapped side value has no window")
                })?;
                side_buckets.entry(window).or_default().push(event.value);
            }
            let mut views: HashMap<WindowInterval, Payload> = HashMap::new();
            let mut output = Vec::with_capacity(main_values.len());
            for event in main_values {
                let window = event.assigned_window.ok_or_else(|| {
                    Error::execution(&key_owned, "direct", "mapped main value has no window")
                })?;
                let target = mapped_side_window(window, &side_window)?;
                if let std::collections::hash_map::Entry::Vacant(slot) = views.entry(target) {
                    let values = side_buckets.remove(&target).unwrap_or_default();
                    slot.insert((direct_registration.materialize)(values)?);
                }
                output.push((direct_registration.apply)(event, views[&target].as_ref())?);
            }
            Ok(output)
        });
        let collection = self.next_aggregate(
            key,
            Operation::SideInputMap,
            vec![InputEdge {
                key: side.key.clone(),
                element: ElementDescriptor {
                    type_id: S::type_identity(),
                    coder_id: S::coder_identity(),
                    coder_payload: S::coder_payload(),
                    coder_version: S::CODER_VERSION,
                },
            }],
            main.boundedness,
            aggregate,
            None,
        )?;
        collection
            .pipeline
            .inner
            .lock()
            .map_err(|_| Error::graph(key, "builder lock poisoned"))?
            .registry
            .side_inputs
            .insert(key.to_owned(), registration);
        Ok(collection)
    }
}
