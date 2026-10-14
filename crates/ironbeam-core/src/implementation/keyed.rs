type KeyedBatchRow<K, V> = (K, V, i64, Window, Option<WindowInterval>, PaneInfo);

impl<K: Element + Eq + Hash + Clone, V: Element> PCollection<(K, V)> {
    /// Group keyed events into merging event-time sessions.
    /// Intervals merge when they overlap or touch; the output
    /// timestamp is the earliest event timestamp in each merged session.
    pub fn group_by_session_named(
        &self,
        key: &str,
        gap_ns: i64,
    ) -> Result<PCollection<(K, Vec<V>)>, Error> {
        let windowed = self.session_windows_named(&format!("{key}/session"), gap_ns)?;
        windowed.group_by_key_named(key)
    }

    pub fn combine_values_lifted_named<C, A, O>(
        &self,
        key: &str,
        combiner: C,
    ) -> Result<PCollection<(K, O)>, Error>
    where
        C: CombineFn<V, A, O>,
        A: Send + 'static,
        O: Element,
    {
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded && parent.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global aggregation requires a finite window",
            ));
        }
        let node_key = key.to_owned();
        let combiner = Arc::new(MergeableAdapter(combiner));
        let factory: FoldFactory = Arc::new(move || {
            Box::new(KeyedFoldState::<K, V, A, O, MergeableAdapter<C>> {
                key: node_key.clone(),
                combiner: Arc::clone(&combiner),
                indices: HashMap::new(),
                rows: Vec::new(),
                marker: PhantomData,
            })
        });
        self.next_fold(
            key,
            Operation::FoldPerKey,
            factory,
            Some(GroupCoderRegistration::for_elements::<K, V>()),
        )
    }
    pub fn fold_values_ordered_named<C, A, O>(
        &self,
        key: &str,
        fold: C,
    ) -> Result<PCollection<(K, O)>, Error>
    where
        C: OrderedFoldFn<V, A, O>,
        A: Send + 'static,
        O: Element,
    {
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded && parent.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global aggregation requires a finite window",
            ));
        }
        let node_key = key.to_owned();
        let fold = Arc::new(OrderedAdapter(fold));
        let factory: FoldFactory = Arc::new(move || {
            Box::new(KeyedFoldState::<K, V, A, O, OrderedAdapter<C>> {
                key: node_key.clone(),
                combiner: Arc::clone(&fold),
                indices: HashMap::new(),
                rows: Vec::new(),
                marker: PhantomData,
            })
        });
        self.next_fold(key, Operation::FoldOrderedPerKey, factory, None)
    }
    /// Fold each key/window in an explicit order on Direct and portable runners.
    /// The order key must be unique within its key/window. The result timestamp
    /// is the timestamp of the first value in that order.
    pub fn fold_values_ordered_by_named<Q, C, A, O>(
        &self,
        key: &str,
        order_key: impl Fn(&V) -> Q + Send + Sync + 'static,
        fold: C,
    ) -> Result<PCollection<(K, O)>, Error>
    where
        Q: Element + Ord,
        C: OrderedFoldFn<V, A, O>,
        A: Send + 'static,
        O: Element,
    {
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded && parent.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global aggregation requires a finite window",
            ));
        }
        let reify_key = format!("{key}/order");
        let order_transform = Transform::Single(Arc::new(move |event| {
            let Envelope {
                pane,
                value,
                timestamp_ns,
                window,
                assigned_window,
            } = event;
            let (group_key, value) = *value.downcast::<(K, V)>().map_err(|_| {
                Error::execution(&reify_key, "runner", "ordered fold input type mismatch")
            })?;
            let order = order_key(&value);
            Ok(Some(Envelope {
                pane,
                value: Box::new((group_key, (order, (timestamp_ns, value)))),
                timestamp_ns,
                window,
                assigned_window,
            }))
        }));
        let keyed = self.next::<(K, (Q, (i64, V)))>(
            &format!("{key}/order"),
            Operation::Map,
            Some(order_transform),
            None,
        )?;
        let grouped = keyed.group_by_key_named(&format!("{key}/group"))?;
        let node_key = key.to_owned();
        let select = Transform::Single(Arc::new(move |event| {
            let Envelope {
                pane,
                value,
                window,
                assigned_window,
                ..
            } = event;
            let (group_key, values) =
                *value.downcast::<(K, Vec<(Q, (i64, V))>)>().map_err(|_| {
                    Error::execution(&node_key, "runner", "ordered fold group type mismatch")
                })?;
            let mut ordered = BTreeMap::new();
            for (order, row) in values {
                if ordered.insert(order, row).is_some() {
                    return Err(Error::execution(
                        &node_key,
                        "runner",
                        "duplicate ordered fold key in key/window",
                    ));
                }
            }
            let timestamp_ns = ordered
                .first_key_value()
                .ok_or_else(|| {
                    Error::execution(&node_key, "runner", "ordered fold group is empty")
                })?
                .1
                .0;
            let mut accumulator = fold.create();
            for (_, (_, value)) in ordered {
                fold.add_input(&mut accumulator, value);
            }
            Ok(Some(Envelope {
                pane,
                value: Box::new((group_key, fold.finish(accumulator))),
                timestamp_ns,
                window,
                assigned_window,
            }))
        }));
        grouped.next(key, Operation::Map, Some(select), None)
    }
    /// Map equal-length batches of keyed values while retaining each row's
    /// key and event-time metadata. A size of one runs on unbounded input;
    /// larger batches require bounded materialization.
    pub fn map_values_batches_named<O: Element>(
        &self,
        key: &str,
        batch_size: usize,
        f: impl Fn(&[V]) -> Vec<O> + Send + Sync + 'static,
    ) -> Result<PCollection<(K, O)>, Error> {
        let limit = batch_size.max(1);
        if limit == 1 {
            let node_key = key.to_owned();
            let transform = Transform::Single(Arc::new(move |event| {
                let Envelope {
                    pane,
                    value,
                    timestamp_ns,
                    window,
                    assigned_window,
                } = event;
                let (row_key, row_value) = *value.downcast::<(K, V)>().map_err(|_| {
                    Error::execution(&node_key, "direct", "batch keyed input type mismatch")
                })?;
                let mapped = f(std::slice::from_ref(&row_value));
                let [mapped]: [O; 1] = mapped.try_into().map_err(|_| {
                    Error::execution(&node_key, "direct", "batch value map changed length")
                })?;
                Ok(Some(Envelope {
                    pane,
                    value: Box::new((row_key, mapped)),
                    timestamp_ns,
                    window,
                    assigned_window,
                }))
            }));
            return self.next(key, Operation::Map, Some(transform), None);
        }
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded {
            return Err(Error::graph(key, "batch mapping requires a bounded input"));
        }
        let node_key = key.to_owned();
        let aggregate: Aggregate = Arc::new(move |mut inputs| {
            let events = inputs
                .pop()
                .ok_or_else(|| Error::execution(&node_key, "direct", "batch input missing"))?;
            let mut output = Vec::new();
            let mut pending: Vec<KeyedBatchRow<K, V>> = Vec::new();
            let mut flush = |pending: &mut Vec<KeyedBatchRow<K, V>>| -> Result<(), Error> {
                if pending.is_empty() { return Ok(()); }
                let rows = std::mem::take(pending);
                let mut keys = Vec::with_capacity(rows.len());
                let mut values = Vec::with_capacity(rows.len());
                let mut metadata = Vec::with_capacity(rows.len());
                for (key, value, timestamp_ns, window, assigned_window, pane) in rows {
                    keys.push(key);
                    values.push(value);
                    metadata.push((timestamp_ns, window, assigned_window, pane));
                }
                let results = f(&values);
                if results.len() != values.len() {
                    return Err(Error::execution(&node_key, "direct", "batch value map changed length"));
                }
                for ((key, value), (timestamp_ns, window, assigned_window, pane)) in
                    keys.into_iter().zip(results).zip(metadata) {
                    output.push(Envelope {
                        pane,
                        value: Box::new((key, value)), timestamp_ns, window, assigned_window
                    });
                }
                Ok(())
            };
            for event in events {
                let Envelope {
                    pane,
                    value,
                    timestamp_ns,
                    window,
                    assigned_window,
                } = event;
                if pending.len() == limit
                    || pending.first().is_some_and(|row| row.4 != assigned_window)
                {
                    flush(&mut pending)?;
                }
                let (row_key, row_value) = *value.downcast::<(K, V)>().map_err(|_| {
                    Error::execution(&node_key, "direct", "batch keyed input type mismatch")
                })?;
                pending.push((row_key, row_value, timestamp_ns, window, assigned_window, pane));
            }
            flush(&mut pending)?;
            Ok(output)
        });
        self.next_aggregate(
            key,
            Operation::BatchMapValues { batch_size: limit },
            Vec::new(),
            parent.boundedness,
            aggregate,
            None,
        )
    }
    /// Map fixed-size value batches in a unique order within each key/window.
    /// The returned batch must contain one value per input. Each output keeps
    /// the corresponding input event time. Portable runners may shuffle before
    /// grouping, so order keys must be unique within a key/window.
    pub fn map_values_batches_ordered_by_named<Q, O>(
        &self,
        key: &str,
        batch_size: usize,
        order_key: impl Fn(&V) -> Q + Send + Sync + 'static,
        f: impl Fn(&[V]) -> Vec<O> + Send + Sync + 'static,
    ) -> Result<PCollection<(K, O)>, Error>
    where
        Q: Element + Ord,
        O: Element,
    {
        if batch_size == 0 {
            return Err(Error::graph(key, "ordered batch size must be positive"));
        }
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded && parent.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded ordered batches need a finite window",
            ));
        }
        let order_node = format!("{key}/order");
        let order_error_node = order_node.clone();
        let order_transform = Transform::Single(Arc::new(move |event| {
            let Envelope {
                pane,
                value,
                timestamp_ns,
                window,
                assigned_window,
            } = event;
            let (row_key, row_value) = *value.downcast::<(K, V)>().map_err(|_| {
                Error::execution(
                    &order_error_node,
                    "runner",
                    "ordered batch input type mismatch",
                )
            })?;
            let order = order_key(&row_value);
            Ok(Some(Envelope {
                pane,
                value: Box::new((row_key, (order, (timestamp_ns, row_value)))),
                timestamp_ns,
                window,
                assigned_window,
            }))
        }));
        let ordered = self.next::<(K, (Q, (i64, V)))>(
            &order_node,
            Operation::Map,
            Some(order_transform),
            None,
        )?;
        let grouped = ordered.group_by_key_named(&format!("{key}/group"))?;
        let node_key = key.to_owned();
        let transform = Transform::Multi(Arc::new(move |event| {
            let Envelope {
                pane,
                value,
                window,
                assigned_window,
                ..
            } = event;
            let (group_key, values) =
                *value.downcast::<(K, Vec<(Q, (i64, V))>)>().map_err(|_| {
                    Error::execution(&node_key, "runner", "ordered batch group type mismatch")
                })?;
            let mut ordered = BTreeMap::new();
            for (order, row) in values {
                if ordered.insert(order, row).is_some() {
                    return Err(Error::execution(
                        &node_key,
                        "runner",
                        "duplicate ordered batch key in key/window",
                    ));
                }
            }
            let mut output = Vec::new();
            let mut pending_values = Vec::with_capacity(batch_size);
            let mut pending_timestamps = Vec::with_capacity(batch_size);
            let mut flush = |values: &mut Vec<V>, timestamps: &mut Vec<i64>| -> Result<(), Error> {
                if values.is_empty() {
                    return Ok(());
                }
                let mapped = f(values.as_slice());
                if mapped.len() != values.len() {
                    return Err(Error::execution(
                        &node_key,
                        "runner",
                        "ordered batch value map changed length",
                    ));
                }
                for (timestamp_ns, mapped) in timestamps.drain(..).zip(mapped) {
                    output.push(Envelope {
                        pane,
                        value: Box::new((group_key.clone(), mapped)),
                        timestamp_ns,
                        window: window.clone(),
                        assigned_window,
                    });
                }
                values.clear();
                Ok(())
            };
            for (_, (timestamp_ns, value)) in ordered {
                pending_timestamps.push(timestamp_ns);
                pending_values.push(value);
                if pending_values.len() == batch_size {
                    flush(&mut pending_values, &mut pending_timestamps)?;
                }
            }
            flush(&mut pending_values, &mut pending_timestamps)?;
            Ok(output)
        }));
        let semantic_id = format!("ironbeam/map-values-ordered-batches/{batch_size}");
        grouped.next_with_semantic(
            key,
            Operation::FlatMap,
            Some(transform),
            None,
            Some((&semantic_id, BUILTIN_OPERATION_VERSION)),
        )
    }
    pub fn group_into_batches_named(
        &self,
        key: &str,
        batch_size: usize,
    ) -> Result<PCollection<(K, Vec<V>)>, Error> {
        let limit = batch_size.max(1);
        let batch_semantic_id = format!("ironbeam/group-into-batches/{limit}");
        self.group_by_key_named(&format!("{key}/group"))?
            .flat_map_registered(
                key,
                Some((&batch_semantic_id, BUILTIN_OPERATION_VERSION)),
                move |(group_key, values)| {
                    let mut batches = Vec::new();
                    let mut current = Vec::with_capacity(limit);
                    for value in values {
                        current.push(value);
                        if current.len() == limit {
                            batches.push((
                                group_key.clone(),
                                std::mem::replace(&mut current, Vec::with_capacity(limit)),
                            ));
                        }
                    }
                    if !current.is_empty() {
                        batches.push((group_key, current));
                    }
                    Ok(batches)
                },
            )
    }
    pub fn combine_per_key_and_window_named<U: Element>(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
        f: impl Fn(Vec<V>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<KeyedWindowed<K, U>>, Error> {
        let grouped = self
            .fixed_windows_with_offset(&format!("{key}/window"), size_ns, offset_ns)?
            .group_by_key_named(&format!("{key}/group"))?;
        grouped
            .map_named(&format!("{key}/combine"), move |(key, values)| {
                (key, f(values))
            })?
            .with_window_named(&format!("{key}/tag"))?
            .map_named(key, |(window, (key, value))| ((key, window), value))
    }
    pub fn latest_per_key_named(&self, key: &str) -> Result<Self, Error> {
        let parent = self.parent()?;
        if parent.time.output_time != WindowOutputTime::EarliestInPane {
            return Err(Error::graph(key, "latest_per_key does not yet support a custom output time policy"));
        }
        if parent.time.allowed_lateness_ns > 0 {
            return Err(Error::graph(key, "latest_per_key does not yet support allowed lateness"));
        }
        if parent.boundedness == Boundedness::Unbounded && parent.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global aggregation requires a finite window",
            ));
        }
        let group_key = format!("{key}/group");
        let grouped = self
            .reify_keyed_timestamp_named(&format!("{key}/timestamp"))?
            .group_by_key_named(&group_key)?;
        let fold_key = group_key.clone();
        let fold_factory: FoldFactory = Arc::new(move || {
            Box::new(LatestPerKeyFoldState::<K, V> {
                key: fold_key.clone(),
                indices: HashMap::new(),
                groups: Vec::new(),
            })
        });
        let aggregate_factory = Arc::clone(&fold_factory);
        let aggregate_key = group_key.clone();
        let aggregate: Aggregate = Arc::new(move |inputs| {
            let [events]: [Vec<Envelope>; 1] = inputs.try_into().map_err(|_| {
                Error::execution(&aggregate_key, "direct", "latest group requires one input")
            })?;
            let mut fold = aggregate_factory();
            for event in events {
                fold.add(event)?;
            }
            fold.finish()
        });
        {
            let mut builder = grouped
                .pipeline
                .inner
                .lock()
                .map_err(|_| Error::graph(&group_key, "builder lock poisoned"))?;
            builder
                .registry
                .aggregates
                .insert(group_key.clone(), aggregate);
            builder.registry.folds.insert(group_key, fold_factory);
        }
        let select_key = key.to_owned();
        let select = Transform::Single(Arc::new(move |event| {
            let Envelope {
                pane,
                value,
                window,
                assigned_window,
                ..
            } = event;
            let (group_key, values) = *value.downcast::<(K, Vec<(i64, V)>)>().map_err(|_| {
                Error::execution(&select_key, "direct", "latest group type mismatch")
            })?;
            let mut values = values.into_iter();
            let (mut latest_timestamp, mut latest_value) = values
                .next()
                .ok_or_else(|| Error::execution(&select_key, "direct", "latest group is empty"))?;
            for (timestamp, value) in values {
                if timestamp > latest_timestamp {
                    latest_timestamp = timestamp;
                    latest_value = value;
                }
            }
            Ok(Some(Envelope {
                pane,
                value: Box::new((group_key, latest_value)),
                timestamp_ns: latest_timestamp,
                window,
                assigned_window,
            }))
        }));
        grouped.next(key, Operation::Map, Some(select), None)
    }
    /// Select the latest event per key/window with a deterministic tie key.
    /// Equal event times choose the greatest tie key. A repeated event time
    /// and tie key in one key/window is an error on every runner.
    pub fn latest_per_key_by_named<Q: Element + Ord>(
        &self,
        key: &str,
        tie_key: impl Fn(&V) -> Q + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded && parent.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global aggregation requires a finite window",
            ));
        }
        let reify_key = format!("{key}/tie");
        let transform = Transform::Single(Arc::new(move |event| {
            let Envelope {
                pane,
                value,
                timestamp_ns,
                window,
                assigned_window,
            } = event;
            let (group_key, value) = *value.downcast::<(K, V)>().map_err(|_| {
                Error::execution(&reify_key, "runner", "latest tie input type mismatch")
            })?;
            let tie = tie_key(&value);
            Ok(Some(Envelope {
                pane,
                value: Box::new((group_key, (timestamp_ns, (tie, value)))),
                timestamp_ns,
                window,
                assigned_window,
            }))
        }));
        let reified = self.next::<(K, (i64, (Q, V)))>(
            &format!("{key}/tie"),
            Operation::Map,
            Some(transform),
            None,
        )?;
        let grouped = reified.group_by_key_named(&format!("{key}/group"))?;
        let select_key = key.to_owned();
        let select = Transform::Single(Arc::new(move |event| {
            let Envelope {
                pane,
                value,
                window,
                assigned_window,
                ..
            } = event;
            let (group_key, values) =
                *value.downcast::<(K, Vec<(i64, (Q, V))>)>().map_err(|_| {
                    Error::execution(&select_key, "runner", "latest tie group type mismatch")
                })?;
            let mut seen = BTreeSet::new();
            let mut latest = None;
            for (timestamp, (tie, value)) in values {
                let order = (timestamp, tie);
                let is_later = seen.last().is_none_or(|current| order.cmp(current).is_gt());
                if !seen.insert(order) {
                    return Err(Error::execution(
                        &select_key,
                        "runner",
                        "duplicate latest tie key in key/window/event time",
                    ));
                }
                if is_later {
                    latest = Some((timestamp, value));
                }
            }
            let (timestamp_ns, latest) = latest.ok_or_else(|| {
                Error::execution(&select_key, "runner", "latest tie group is empty")
            })?;
            Ok(Some(Envelope {
                pane,
                value: Box::new((group_key, latest)),
                timestamp_ns,
                window,
                assigned_window,
            }))
        }));
        grouped.next(key, Operation::Map, Some(select), None)
    }
    fn reify_keyed_timestamp_named(
        &self,
        key: &str,
    ) -> Result<PCollection<ReifiedKeyedValue<K, V>>, Error> {
        let reify_key = key.to_owned();
        let transform = Transform::Single(Arc::new(move |event| {
            let Envelope {
                pane,
                value,
                timestamp_ns,
                window,
                assigned_window,
            } = event;
            let (group_key, value) = *value.downcast::<(K, V)>().map_err(|_| {
                Error::execution(&reify_key, "direct", "latest input type mismatch")
            })?;
            Ok(Some(Envelope {
                pane,
                value: Box::new((group_key, (timestamp_ns, value))),
                timestamp_ns,
                window,
                assigned_window,
            }))
        }));
        let output = self.next(key, Operation::Map, Some(transform), None)?;
        self.pipeline
            .inner
            .lock()
            .map_err(|_| Error::graph(key, "builder lock poisoned"))?
            .registry
            .keyed_timestamp_reifications
            .insert(key.to_owned());
        Ok(output)
    }
    pub fn group_by_key_named(&self, key: &str) -> Result<PCollection<(K, Vec<V>)>, Error> {
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded && parent.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global aggregation requires a finite window",
            ));
        }
        let key_owned = key.to_owned();
        let fold_key = key_owned.clone();
        let trigger = parent.time.trigger;
        let output_time = parent.time.output_time;
        let allowed_lateness_ns = parent.time.allowed_lateness_ns;
        let fold_factory: FoldFactory = Arc::new(move || {
            Box::new(GroupByKeyFoldState::<K, V> {
                key: fold_key.clone(),
                trigger,
                output_time,
                allowed_lateness_ns,
                global_indices: HashMap::new(),
                window_indices: HashMap::new(),
                session_indices: HashMap::new(),
                fired_panes: HashMap::new(),
                nonspeculative_base: HashMap::new(),
                fired_value_counts: HashMap::new(),
                latest_timestamps: HashMap::new(),
                groups: Vec::new(),
            })
        });
        let aggregate: Aggregate = Arc::new(move |inputs| {
            let [events]: [Vec<Envelope>; 1] = inputs.try_into().map_err(|_| {
                Error::execution(&key_owned, "direct", "group requires exactly one input")
            })?;
            let mut fold: Box<dyn FoldSession> = Box::new(GroupByKeyFoldState::<K, V> {
                key: key_owned.clone(),
                trigger,
                output_time,
                allowed_lateness_ns,
                global_indices: HashMap::new(),
                window_indices: HashMap::new(),
                session_indices: HashMap::new(),
                fired_panes: HashMap::new(),
                nonspeculative_base: HashMap::new(),
                fired_value_counts: HashMap::new(),
                latest_timestamps: HashMap::new(),
                groups: Vec::new(),
            });
            for event in events {
                fold.add(event)?;
            }
            if let Some(count) = trigger.early_count() {
                let (mut early, _) = fold.drain_ready_count(count)?;
                early.extend(fold.finish()?);
                Ok(early)
            } else {
                fold.finish()
            }
        });
        let result = self.next_aggregate(
            key,
            Operation::GroupByKey,
            Vec::new(),
            parent.boundedness,
            aggregate,
            Some(GroupCoderRegistration::for_elements::<K, V>()),
        )?;
        self.pipeline
            .inner
            .lock()
            .map_err(|_| Error::graph(key, "builder lock poisoned"))?
            .registry
            .folds
            .insert(key.to_owned(), fold_factory);
        Ok(result)
    }

    pub fn join_inner_named<W: Element + Clone>(
        &self,
        key: &str,
        right: &PCollection<(K, W)>,
    ) -> Result<PCollection<Joined<K, V, W>>, Error>
    where
        V: Clone,
    {
        self.join_named(key, right, Operation::JoinInner, |left, right| {
            left.zip(right)
        })
    }

    pub fn join_left_named<W: Element + Clone>(
        &self,
        key: &str,
        right: &PCollection<(K, W)>,
    ) -> Result<PCollection<LeftJoined<K, V, W>>, Error>
    where
        V: Clone,
    {
        self.join_named(key, right, Operation::JoinLeft, |left, right| {
            left.map(|value| (value, right))
        })
    }

    pub fn join_right_named<W: Element + Clone>(
        &self,
        key: &str,
        right: &PCollection<(K, W)>,
    ) -> Result<PCollection<RightJoined<K, V, W>>, Error>
    where
        V: Clone,
    {
        self.join_named(key, right, Operation::JoinRight, |left, right| {
            right.map(|value| (left, value))
        })
    }

    pub fn join_full_named<W: Element + Clone>(
        &self,
        key: &str,
        right: &PCollection<(K, W)>,
    ) -> Result<PCollection<FullJoined<K, V, W>>, Error>
    where
        V: Clone,
    {
        self.join_named(key, right, Operation::JoinFull, |left, right| {
            Some((left, right))
        })
    }

    fn join_named<W: Element + Clone, O: Element>(
        &self,
        key: &str,
        right: &PCollection<(K, W)>,
        operation: Operation,
        emit: impl Fn(Option<V>, Option<W>) -> Option<O> + Send + Sync + 'static,
    ) -> Result<PCollection<(K, O)>, Error>
    where
        V: Clone,
    {
        if !Arc::ptr_eq(&self.pipeline.inner, &right.pipeline.inner) {
            return Err(Error::graph(
                key,
                "join inputs belong to different pipelines",
            ));
        }
        let left = self.parent()?;
        let other = right.parent()?;
        if left.time != other.time {
            return Err(Error::graph(
                key,
                "join inputs must have equal time/window declarations",
            ));
        }
        let unbounded = left.boundedness == Boundedness::Unbounded
            || other.boundedness == Boundedness::Unbounded;
        let session_join = matches!(left.time.window, Window::Session { .. });
        if unbounded && left.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global join requires a finite window",
            ));
        }
        let inner_join = matches!(operation, Operation::JoinInner);
        let key_owned = key.to_owned();
        let emit = Arc::new(emit);
        let typed_emit = Arc::clone(&emit);
        let aggregate: Aggregate = Arc::new(move |inputs| {
            let [mut left, mut right]: [Vec<Envelope>; 2] = inputs
                .try_into()
                .map_err(|_| Error::execution(&key_owned, "direct", "join requires two inputs"))?;
            if session_join {
                merge_bounded_join_sessions::<K, V, W>(&mut left, &mut right, &key_owned)?;
            }
            if inner_join && left.len() <= right.len() {
                return inner_join_reusing_right::<K, V, W, O>(
                    left,
                    right,
                    &key_owned,
                    emit.as_ref(),
                );
            }
            if inner_join && right.len() < left.len() {
                return inner_join_indexed_right::<K, V, W, O>(
                    left,
                    right,
                    &key_owned,
                    emit.as_ref(),
                );
            }
            // Join output order is unspecified; indexing the smaller side
            // avoids retaining one position per row of a highly skewed input.
            if right.len() < left.len() {
                let mut index: HashMap<(K, Option<WindowInterval>), JoinPositions> = HashMap::new();
                let mut right_rows = Vec::with_capacity(right.len());
                for event in right {
                    let (row_key, value) = *event.value.downcast::<(K, W)>().map_err(|_| {
                        Error::execution(&key_owned, "direct", "right join type mismatch")
                    })?;
                    let position = right_rows.len();
                    match index.entry((row_key.clone(), event.assigned_window)) {
                        std::collections::hash_map::Entry::Occupied(mut entry) => {
                            entry.get_mut().push(position);
                        }
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            entry.insert(JoinPositions::One(position));
                        }
                    }
                    right_rows.push((
                        row_key,
                        value,
                        event.timestamp_ns,
                        event.window,
                        event.assigned_window,
                        false,
                    ));
                }
                let mut output = Vec::new();
                for event in left {
                    let Envelope {
                        pane: _,
                        value,
                        timestamp_ns,
                        window,
                        assigned_window,
                    } = event;
                    let (row_key, value) = *value.downcast::<(K, V)>().map_err(|_| {
                        Error::execution(&key_owned, "direct", "left join type mismatch")
                    })?;
                    if let Some(matches) = index.get(&(row_key.clone(), assigned_window)) {
                        for &position in matches.as_slice() {
                            let (_, right_value, right_time, right_window, right_assigned, matched) =
                                &mut right_rows[position];
                            *matched = true;
                            if let Some(pair) = emit(Some(value.clone()), Some(right_value.clone()))
                            {
                                output.push(Envelope {
                                    pane: PaneInfo::FIRST_LAST_ON_TIME,
                                    value: Box::new((row_key.clone(), pair)),
                                    timestamp_ns: timestamp_ns.min(*right_time),
                                    window: right_window.clone(),
                                    assigned_window: *right_assigned,
                                });
                            }
                        }
                    } else if let Some(pair) = emit(Some(value), None) {
                        output.push(Envelope {
                            pane: PaneInfo::FIRST_LAST_ON_TIME,
                            value: Box::new((row_key, pair)),
                            timestamp_ns,
                            window,
                            assigned_window,
                        });
                    }
                }
                for (row_key, value, timestamp_ns, window, assigned_window, matched) in right_rows {
                    if !matched && let Some(pair) = emit(None, Some(value)) {
                        output.push(Envelope {
                            pane: PaneInfo::FIRST_LAST_ON_TIME,
                            value: Box::new((row_key, pair)),
                            timestamp_ns,
                            window,
                            assigned_window,
                        });
                    }
                }
                return Ok(output);
            }
            let mut index: HashMap<(K, Option<WindowInterval>), JoinPositions> = HashMap::new();
            let mut left_rows = Vec::with_capacity(left.len());
            for event in left {
                let (row_key, value) = *event.value.downcast::<(K, V)>().map_err(|_| {
                    Error::execution(&key_owned, "direct", "left join type mismatch")
                })?;
                let position = left_rows.len();
                match index.entry((row_key.clone(), event.assigned_window)) {
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        entry.get_mut().push(position);
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(JoinPositions::One(position));
                    }
                }
                left_rows.push((
                    row_key,
                    value,
                    event.timestamp_ns,
                    event.window,
                    event.assigned_window,
                    false,
                ));
            }
            let mut output = Vec::new();
            for event in right {
                let Envelope {
                    pane: _,
                    value,
                    timestamp_ns,
                    window,
                    assigned_window,
                } = event;
                let (row_key, value) = *value.downcast::<(K, W)>().map_err(|_| {
                    Error::execution(&key_owned, "direct", "right join type mismatch")
                })?;
                if let Some(matches) = index.get(&(row_key.clone(), assigned_window)) {
                    for &position in matches.as_slice() {
                        let (_, left_value, left_time, _, _, matched) = &mut left_rows[position];
                        *matched = true;
                        if let Some(pair) = emit(Some(left_value.clone()), Some(value.clone())) {
                            output.push(Envelope {
                                pane: PaneInfo::FIRST_LAST_ON_TIME,
                                value: Box::new((row_key.clone(), pair)),
                                timestamp_ns: timestamp_ns.min(*left_time),
                                window: window.clone(),
                                assigned_window,
                            });
                        }
                    }
                } else if let Some(pair) = emit(None, Some(value)) {
                    output.push(Envelope {
                        pane: PaneInfo::FIRST_LAST_ON_TIME,
                        value: Box::new((row_key, pair)),
                        timestamp_ns,
                        window,
                        assigned_window,
                    });
                }
            }
            for (row_key, value, timestamp_ns, window, assigned_window, matched) in left_rows {
                if !matched && let Some(pair) = emit(Some(value), None) {
                    output.push(Envelope {
                        pane: PaneInfo::FIRST_LAST_ON_TIME,
                        value: Box::new((row_key, pair)),
                        timestamp_ns,
                        window,
                        assigned_window,
                    });
                }
            }
            Ok(output)
        });
        let collection = self.next_aggregate(
            key,
            operation,
            vec![InputEdge {
                key: right.key.clone(),
                element: ElementDescriptor {
                    type_id: <(K, W)>::type_identity(),
                    coder_id: <(K, W)>::coder_identity(),
                    coder_version: <(K, W)>::CODER_VERSION,
                },
            }],
            if unbounded {
                Boundedness::Unbounded
            } else {
                Boundedness::Bounded
            },
            aggregate,
            None,
        )?;
        let registration = JoinCoderRegistration {
            split_inputs: [
                |bytes| {
                    let (row_key, value) = <(K, V)>::decode(bytes)?;
                    Ok((row_key.encode()?, value.encode()?))
                },
                |bytes| {
                    let (row_key, value) = <(K, W)>::decode(bytes)?;
                    Ok((row_key.encode()?, value.encode()?))
                },
            ],
            assemble_inputs: [
                |encoded_key, encoded_value| {
                    Ok(Box::new((
                        K::decode(encoded_key)?,
                        V::decode(encoded_value)?,
                    )))
                },
                |encoded_key, encoded_value| {
                    Ok(Box::new((
                        K::decode(encoded_key)?,
                        W::decode(encoded_value)?,
                    )))
                },
            ],
        };
        let mut inner = collection
            .pipeline
            .inner
            .lock()
            .map_err(|_| Error::graph(key, "builder lock poisoned"))?;
        inner
            .registry
            .join_coders
            .insert(key.to_owned(), registration);
        if inner_join && left.time.window == Window::Global && left.time.initial_timestamp_ns == 0 {
            let left_source = inner.registry.sources.get(&self.key).and_then(|source| {
                Some((
                    source.len_hint?,
                    source
                        .vec_batch_factory
                        .as_ref()?
                        .downcast_ref::<TypedVecBatchFactory<(K, V)>>()?
                        .clone(),
                ))
            });
            let right_source = inner.registry.sources.get(&right.key).and_then(|source| {
                Some((
                    source.len_hint?,
                    source
                        .vec_batch_factory
                        .as_ref()?
                        .downcast_ref::<TypedVecBatchFactory<(K, W)>>()?
                        .clone(),
                ))
            });
            if let (Some((left_len, left_factory)), Some((right_len, right_factory))) =
                (left_source, right_source)
            {
                let left_key = self.key.clone();
                let right_key = right.key.clone();
                let typed_join: TypedJoinFactory<(K, O)> = Arc::new(move |budget, check| {
                    check()?;
                    for (source_key, len) in [(&left_key, left_len), (&right_key, right_len)] {
                        if len.saturating_mul(std::mem::size_of::<Envelope>()) > budget {
                            return Err(Error::resource(
                                source_key,
                                "materialized source exceeds job memory budget",
                            ));
                        }
                    }
                    let left = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        left_factory(0, left_len)
                    }))
                    .unwrap_or_else(|_| {
                        Err(Error::execution(
                            &left_key,
                            "direct",
                            "source value clone panicked",
                        ))
                    })?;
                    check()?;
                    let right = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        right_factory(0, right_len)
                    }))
                    .unwrap_or_else(|_| {
                        Err(Error::execution(
                            &right_key,
                            "direct",
                            "source value clone panicked",
                        ))
                    })?;
                    typed_inner_join::<K, V, W, O>(left, right, budget, check, typed_emit.as_ref())
                });
                inner
                    .registry
                    .typed_joins
                    .insert(key.to_owned(), Arc::new(typed_join));
            }
        }
        drop(inner);
        Ok(collection)
    }
}
