fn singleton_side_view<S>(key: &str, values: Vec<S>) -> Result<S, Error> {
    let mut values = values.into_iter();
    match (values.next(), values.next()) {
        (Some(value), None) => Ok(value),
        _ => Err(Error::execution(
            key,
            "side-input",
            "singleton side view requires exactly one value",
        )),
    }
}

fn map_side_view<K: Eq + Hash, V>(key: &str, values: Vec<(K, V)>) -> Result<HashMap<K, V>, Error> {
    let mut view = HashMap::with_capacity(values.len());
    for (row_key, row_value) in values {
        if view.insert(row_key, row_value).is_some() {
            return Err(Error::execution(
                key,
                "side-input",
                "map side view has a duplicate key",
            ));
        }
    }
    Ok(view)
}

fn multimap_side_view<K: Eq + Hash, V>(values: Vec<(K, V)>) -> HashMap<K, Vec<V>> {
    let mut view: HashMap<K, Vec<V>> = HashMap::new();
    for (row_key, row_value) in values {
        view.entry(row_key).or_default().push(row_value);
    }
    view
}

/// A typed collection reduced to a watermark-bearing windowed signal.
/// Its original values are discarded; its windows and source progress remain.
#[derive(Clone)]
pub struct WindowedWaitSignal {
    marker: PCollection<u8>,
}

impl<T: Element> PCollection<T> {
    /// Convert this collection into a windowed completion signal.
    /// A distinct key is required because the signal is a graph transform.
    pub fn windowed_wait_signal_named(&self, key: &str) -> Result<WindowedWaitSignal, Error> {
        Ok(WindowedWaitSignal {
            marker: self.map_named(key, |_| 0_u8)?,
        })
    }
    pub fn map_with_side_named<S: Element, U: Element>(
        &self,
        key: &str,
        side: &PCollection<S>,
        f: impl Fn(T, &[S]) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        self.map_with_materialized_side_named(key, side, Ok, move |value, values: &Vec<S>| {
            Ok(f(value, values))
        })
    }
    /// Materialize exactly one bounded side value for the whole collection.
    /// An empty side collection or more than one value fails execution.
    pub fn map_with_singleton_side_named<S: Element, U: Element>(
        &self,
        key: &str,
        side: &PCollection<S>,
        f: impl Fn(T, &S) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        let key_owned = key.to_owned();
        self.map_with_materialized_side_named(
            key,
            side,
            move |values| singleton_side_view(&key_owned, values),
            move |value, view| Ok(f(value, view)),
        )
    }
    /// Materialize a bounded keyed side collection as one lookup map.
    /// Duplicate keys fail execution because shuffle order is unspecified.
    pub fn map_with_map_side_named<K: Element + Eq + Hash, V: Element, U: Element>(
        &self,
        key: &str,
        side: &PCollection<(K, V)>,
        f: impl Fn(T, &HashMap<K, V>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        let key_owned = key.to_owned();
        self.map_with_materialized_side_named(
            key,
            side,
            move |values| map_side_view(&key_owned, values),
            move |value, view| Ok(f(value, view)),
        )
    }
    /// Materialize a bounded keyed side collection as a lookup multimap.
    /// Values for one key have no runner-independent order.
    pub fn map_with_multimap_side_named<K: Element + Eq + Hash, V: Element, U: Element>(
        &self,
        key: &str,
        side: &PCollection<(K, V)>,
        f: impl Fn(T, &HashMap<K, Vec<V>>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        self.map_with_materialized_side_named(
            key,
            side,
            move |values| Ok(multimap_side_view(values)),
            move |value, view| Ok(f(value, view)),
        )
    }
    /// Apply side values separately within each matching window.
    /// This composes a keyed co-group, so the runner may shuffle and buffer all
    /// values until both input watermarks close the window. An unbounded side
    /// collection therefore supplies one view at window closure, rather than
    /// updates to main elements already emitted from that window. Main element
    /// timestamps are restored after the group.
    /// Use `map_with_side_named` for one global side view across all windows.
    pub fn map_with_windowed_side_named<S: Element, U: Element>(
        &self,
        key: &str,
        side: &PCollection<S>,
        f: impl Fn(T, &[S]) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        self.map_with_windowed_materialized_side_named(
            key,
            side,
            Ok,
            move |value, values: &Vec<S>| Ok(f(value, values)),
        )
    }
    /// Release each main value after its matching window closes on both inputs.
    /// The signal may be unbounded; its values are ignored. This barrier uses
    /// a co-group and therefore waits for the main watermark as well as the
    /// signal watermark. It does not provide early per-window release.
    pub fn wait_on_windowed_named<S: Element>(
        &self,
        key: &str,
        signal: &PCollection<S>,
    ) -> Result<Self, Error> {
        self.map_with_windowed_side_named(key, signal, |value, _| value)
    }
    /// Release each main window after it and every matching signal window close.
    /// Signals may be unbounded and their values are ignored. One keyed
    /// co-group combines their watermarks, so all signal windows must close.
    /// Main values are buffered until closure.
    pub fn wait_on_all_windowed_named<S: Element>(
        &self,
        key: &str,
        signals: &[PCollection<S>],
    ) -> Result<Self, Error> {
        if signals.is_empty() {
            return Err(Error::graph(key, "windowed wait requires at least one signal"));
        }
        let main = self.parent()?;
        for signal in signals {
            if !Arc::ptr_eq(&self.pipeline.inner, &signal.pipeline.inner) {
                return Err(Error::graph(key, "wait inputs belong to different pipelines"));
            }
            if signal.parent()?.time != main.time {
                return Err(Error::graph(key, "signal time/window declaration mismatch"));
            }
        }
        if signals.len() == 1 {
            return self.wait_on_windowed_named(key, &signals[0]);
        }
        let main = self
            .reify_timestamps_named(&format!("{key}/main-time"))?
            .map_named(&format!("{key}/main-key"), |value| {
                (0_i32, Some(value))
            })?;
        let signals = signals
            .iter()
            .enumerate()
            .map(|(index, signal)| {
                signal.map_named(&format!("{key}/signal-{index}"), |_| {
                    (0_i32, None::<(i64, T)>)
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        main.co_group_homogeneous_named(&format!("{key}/group"), &signals)?
            .flat_map_named(&format!("{key}/restore"), |(_, buckets)| {
                buckets
                    .into_iter()
                    .next()
                    .into_iter()
                    .flatten()
                    .flatten()
                    .collect::<Vec<_>>()
            })?
            .timestamps_from_pairs_in_window_named(key)
    }
    /// Wait for matching windows from signals with different element types.
    /// Create each handle with `windowed_wait_signal_named`. Every source
    /// watermark must close the window before main values are released.
    pub fn wait_on_windowed_signals_named(
        &self,
        key: &str,
        signals: &[WindowedWaitSignal],
    ) -> Result<Self, Error> {
        let markers = signals
            .iter()
            .map(|signal| signal.marker.clone())
            .collect::<Vec<_>>();
        self.wait_on_all_windowed_named(key, &markers)
    }
    /// Materialize one side view for each closed window. The side collection
    /// may be unbounded if both source watermarks can close the window.
    pub fn map_with_windowed_materialized_side_named<S: Element, M: Send + 'static, U: Element>(
        &self,
        key: &str,
        side: &PCollection<S>,
        materialize: impl Fn(Vec<S>) -> Result<M, Error> + Send + Sync + 'static,
        f: impl Fn(T, &M) -> Result<U, Error> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        if !Arc::ptr_eq(&self.pipeline.inner, &side.pipeline.inner) {
            return Err(Error::graph(
                key,
                "side input belongs to a different pipeline",
            ));
        }
        let main = self.parent()?;
        let other = side.parent()?;
        if main.time != other.time {
            return Err(Error::graph(
                key,
                "side input time/window declaration mismatch",
            ));
        }
        let main = self
            .reify_timestamps_named(&format!("{key}/main-time"))?
            .map_named(&format!("{key}/main-key"), |value| (0_i32, value))?;
        let side = side.map_named(&format!("{key}/side-key"), |value| (0_i32, value))?;
        crate::cogroup_by_key_2_named(&format!("{key}/group"), &main, &side)?
            .flat_map_fallible_named(&format!("{key}/apply"), move |(_, (main, side))| {
                let view = materialize(side)?;
                main.into_iter()
                    .map(|(timestamp_ns, value)| Ok((timestamp_ns, f(value, &view)?)))
                    .collect::<Result<Vec<_>, Error>>()
            })?
            .timestamps_from_pairs_in_window_named(key)
    }
    /// Apply exactly one side value in each matching window.
    /// A main window with no side value or several side values fails execution.
    pub fn map_with_windowed_singleton_side_named<S: Element, U: Element>(
        &self,
        key: &str,
        side: &PCollection<S>,
        f: impl Fn(T, &S) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        let key_owned = key.to_owned();
        self.map_with_windowed_materialized_side_named(
            key,
            side,
            move |values| singleton_side_view(&key_owned, values),
            move |value, view| Ok(f(value, view)),
        )
    }
    /// Apply a keyed lookup side view separately within each matching window.
    /// Duplicate side keys in one window fail execution.
    pub fn map_with_windowed_map_side_named<K: Element + Eq + Hash, V: Element, U: Element>(
        &self,
        key: &str,
        side: &PCollection<(K, V)>,
        f: impl Fn(T, &HashMap<K, V>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        let key_owned = key.to_owned();
        self.map_with_windowed_materialized_side_named(
            key,
            side,
            move |values| map_side_view(&key_owned, values),
            move |value, view| Ok(f(value, view)),
        )
    }
    /// Apply a keyed multimap side view separately within each matching window.
    /// The value order within a key is unspecified after a runner shuffle.
    pub fn map_with_windowed_multimap_side_named<K: Element + Eq + Hash, V: Element, U: Element>(
        &self,
        key: &str,
        side: &PCollection<(K, V)>,
        f: impl Fn(T, &HashMap<K, Vec<V>>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        self.map_with_windowed_materialized_side_named(
            key,
            side,
            move |values| Ok(multimap_side_view(values)),
            move |value, view| Ok(f(value, view)),
        )
    }
    /// Build a bounded side view once per execution, then apply it to each main value.
    /// Materialization failures are returned as typed execution errors. The
    /// view must be transferable because portable workers can resume a bundle
    /// on another executor thread.
    pub fn map_with_materialized_side_named<S: Element, M: Send + 'static, U: Element>(
        &self,
        key: &str,
        side: &PCollection<S>,
        materialize: impl Fn(Vec<S>) -> Result<M, Error> + Send + Sync + 'static,
        f: impl Fn(T, &M) -> Result<U, Error> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        if !Arc::ptr_eq(&self.pipeline.inner, &side.pipeline.inner) {
            return Err(Error::graph(
                key,
                "side input belongs to a different pipeline",
            ));
        }
        let main = self.parent()?;
        let other = side.parent()?;
        if other.boundedness != Boundedness::Bounded {
            return Err(Error::graph(key, "side input must be bounded"));
        }
        if main.time != other.time {
            return Err(Error::graph(
                key,
                "side input time/window declaration mismatch",
            ));
        }
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
            let [main_values, side_values]: [Vec<Envelope>; 2] =
                inputs.try_into().map_err(|_| {
                    Error::execution(&key_owned, "direct", "side input requires two inputs")
                })?;
            let side_view = (direct_registration.materialize)(
                side_values.into_iter().map(|event| event.value).collect(),
            )?;
            let mut output = Vec::with_capacity(main_values.len());
            for event in main_values {
                output.push((direct_registration.apply)(event, side_view.as_ref())?);
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
