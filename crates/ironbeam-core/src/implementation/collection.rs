impl<T: Element> PCollection<T> {
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }
    fn parent(&self) -> Result<Node, Error> {
        let inner = self
            .pipeline
            .inner
            .lock()
            .map_err(|_| Error::graph(&self.key, "builder lock poisoned"))?;
        inner
            .graph
            .node(&self.key)
            .cloned()
            .ok_or_else(|| Error::graph(&self.key, "collection missing"))
    }
    fn next<U: Element>(
        &self,
        key: &str,
        operation: Operation,
        transform: Option<Transform>,
        window: Option<Window>,
    ) -> Result<PCollection<U>, Error> {
        self.next_with_semantic(key, operation, transform, window, None)
    }
    fn next_with_semantic<U: Element>(
        &self,
        key: &str,
        operation: Operation,
        transform: Option<Transform>,
        window: Option<Window>,
        semantic: Option<(&str, Version)>,
    ) -> Result<PCollection<U>, Error> {
        let parent = self.parent()?;
        let (semantic_id, semantic_version) = semantic.map_or_else(
            || Pipeline::semantic_identity(&operation),
            |(id, version)| (id.to_owned(), version),
        );
        let node = Node {
            key: key.into(),
            input: Some(self.key.clone()),
            input_element: Some(ElementDescriptor {
                type_id: T::type_identity(),
                coder_id: T::coder_identity(),
                coder_version: T::CODER_VERSION,
            }),
            additional_inputs: Vec::new(),
            operation,
            semantic_id,
            semantic_version,
            type_id: U::type_identity(),
            coder_id: U::coder_identity(),
            coder_version: U::CODER_VERSION,
            boundedness: parent.boundedness,
            time: Time {
                initial_timestamp_ns: parent.time.initial_timestamp_ns,
                window: window.unwrap_or(parent.time.window),
            },
            local_id: 0,
        };
        self.pipeline
            .add::<U>(node, None, transform, None, None, None)?;
        Ok(PCollection {
            pipeline: self.pipeline.clone(),
            key: key.into(),
            marker: PhantomData,
        })
    }
    fn next_aggregate<U: Element>(
        &self,
        key: &str,
        operation: Operation,
        additional_inputs: Vec<InputEdge>,
        boundedness: Boundedness,
        aggregate: Aggregate,
        group_coder: Option<GroupCoderRegistration>,
    ) -> Result<PCollection<U>, Error> {
        let parent = self.parent()?;
        let (semantic_id, semantic_version) = Pipeline::semantic_identity(&operation);
        let node = Node {
            key: key.into(),
            input: Some(self.key.clone()),
            input_element: Some(ElementDescriptor {
                type_id: T::type_identity(),
                coder_id: T::coder_identity(),
                coder_version: T::CODER_VERSION,
            }),
            additional_inputs,
            operation,
            semantic_id,
            semantic_version,
            type_id: U::type_identity(),
            coder_id: U::coder_identity(),
            coder_version: U::CODER_VERSION,
            boundedness,
            time: parent.time,
            local_id: 0,
        };
        self.pipeline
            .add::<U>(node, None, None, Some(aggregate), None, group_coder)?;
        Ok(PCollection {
            pipeline: self.pipeline.clone(),
            key: key.into(),
            marker: PhantomData,
        })
    }
    fn next_fold<U: Element>(
        &self,
        key: &str,
        operation: Operation,
        factory: FoldFactory,
        group_coder: Option<GroupCoderRegistration>,
    ) -> Result<PCollection<U>, Error> {
        let parent = self.parent()?;
        let (semantic_id, semantic_version) = Pipeline::semantic_identity(&operation);
        let node = Node {
            key: key.into(),
            input: Some(self.key.clone()),
            input_element: Some(ElementDescriptor {
                type_id: T::type_identity(),
                coder_id: T::coder_identity(),
                coder_version: T::CODER_VERSION,
            }),
            additional_inputs: Vec::new(),
            operation,
            semantic_id,
            semantic_version,
            type_id: U::type_identity(),
            coder_id: U::coder_identity(),
            coder_version: U::CODER_VERSION,
            boundedness: parent.boundedness,
            time: parent.time,
            local_id: 0,
        };
        self.pipeline.add_fold::<U>(node, factory, group_coder)?;
        Ok(PCollection {
            pipeline: self.pipeline.clone(),
            key: key.into(),
            marker: PhantomData,
        })
    }
    pub fn map_named<U: Element>(
        &self,
        key: &str,
        f: impl Fn(T) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        self.map_registered(key, None, f)
    }
    /// Register a versioned unary map implementation under an application
    /// semantic ID. The portable worker must construct the same handler, and
    /// callers must change the version when its behavior changes: closure
    /// bytes cannot be fingerprinted by the graph descriptor.
    pub fn map_with_semantic_named<U: Element>(
        &self,
        key: &str,
        semantic_id: &str,
        version: Version,
        f: impl Fn(T) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        Self::validate_semantic_handler(key, semantic_id, version)?;
        self.map_registered(key, Some((semantic_id, version)), f)
    }
    fn validate_semantic_handler(
        key: &str,
        semantic_id: &str,
        version: Version,
    ) -> Result<(), Error> {
        if !semantic_id.contains('/') || semantic_id.starts_with("ironbeam/") || version.major == 0
        {
            return Err(Error::graph(
                key,
                "semantic handler requires an application namespace and major version",
            ));
        }
        Ok(())
    }
    fn map_registered<U: Element>(
        &self,
        key: &str,
        semantic: Option<(&str, Version)>,
        f: impl Fn(T) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        let key_owned = key.to_owned();
        let f = Arc::new(f);
        let envelope_f = Arc::clone(&f);
        let typed_f: TypedMap<T, U> = f;
        let transform = Transform::Single(Arc::new(move |mut e| {
            let value = e
                .value
                .downcast::<T>()
                .map_err(|_| Error::execution(&key_owned, "direct", "input type mismatch"))?;
            e.value = Box::new(envelope_f(*value));
            Ok(Some(e))
        }));
        let output =
            self.next_with_semantic(key, Operation::Map, Some(transform), None, semantic)?;
        self.pipeline
            .add_typed_transform(key, TypedTransformRegistration::Map(Arc::new(typed_f)))?;
        Ok(output)
    }
    pub fn filter_named(
        &self,
        key: &str,
        f: impl Fn(&T) -> bool + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        self.filter_registered(key, None, f)
    }
    /// Register a versioned predicate under an application semantic ID.
    /// The portable worker must construct the same predicate and version.
    pub fn filter_with_semantic_named(
        &self,
        key: &str,
        semantic_id: &str,
        version: Version,
        f: impl Fn(&T) -> bool + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        Self::validate_semantic_handler(key, semantic_id, version)?;
        self.filter_registered(key, Some((semantic_id, version)), f)
    }
    fn filter_registered(
        &self,
        key: &str,
        semantic: Option<(&str, Version)>,
        f: impl Fn(&T) -> bool + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        let key_owned = key.to_owned();
        let f = Arc::new(f);
        let envelope_f = Arc::clone(&f);
        let typed_f: TypedFilter<T> = f;
        let transform = Transform::Single(Arc::new(move |e| {
            let value = e
                .value
                .downcast_ref::<T>()
                .ok_or_else(|| Error::execution(&key_owned, "direct", "input type mismatch"))?;
            Ok(if envelope_f(value) { Some(e) } else { None })
        }));
        let output =
            self.next_with_semantic(key, Operation::Filter, Some(transform), None, semantic)?;
        self.pipeline
            .add_typed_transform(key, TypedTransformRegistration::Filter(Arc::new(typed_f)))?;
        Ok(output)
    }
    pub fn flat_map_named<U: Element>(
        &self,
        key: &str,
        f: impl Fn(T) -> Vec<U> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        self.flat_map_registered(key, None, f)
    }
    /// Register a versioned zero-or-many-output handler under an application
    /// semantic ID. The portable worker must construct the same handler.
    pub fn flat_map_with_semantic_named<U: Element>(
        &self,
        key: &str,
        semantic_id: &str,
        version: Version,
        f: impl Fn(T) -> Vec<U> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        Self::validate_semantic_handler(key, semantic_id, version)?;
        self.flat_map_registered(key, Some((semantic_id, version)), f)
    }
    fn flat_map_registered<U: Element>(
        &self,
        key: &str,
        semantic: Option<(&str, Version)>,
        f: impl Fn(T) -> Vec<U> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        let key_owned = key.to_owned();
        let transform = Transform::Multi(Arc::new(move |e| {
            let Envelope {
                value,
                timestamp_ns,
                window,
                assigned_window,
            } = e;
            let value = value
                .downcast::<T>()
                .map_err(|_| Error::execution(&key_owned, "direct", "input type mismatch"))?;
            Ok(f(*value)
                .into_iter()
                .map(|value| Envelope {
                    value: Box::new(value),
                    timestamp_ns,
                    window: window.clone(),
                    assigned_window,
                })
                .collect())
        }));
        self.next_with_semantic(key, Operation::FlatMap, Some(transform), None, semantic)
    }
    pub fn flatten_named(&self, key: &str, others: &[Self]) -> Result<Self, Error> {
        if others.is_empty() {
            return Err(Error::graph(key, "flatten requires at least two inputs"));
        }
        let parent = self.parent()?;
        let mut additional_inputs = Vec::with_capacity(others.len());
        let mut unbounded = parent.boundedness == Boundedness::Unbounded;
        for other in others {
            if !Arc::ptr_eq(&self.pipeline.inner, &other.pipeline.inner) {
                return Err(Error::graph(
                    key,
                    "flatten inputs belong to different pipelines",
                ));
            }
            let upstream = other.parent()?;
            if upstream.time != parent.time {
                return Err(Error::graph(
                    key,
                    "flatten inputs must have equal time/window declarations",
                ));
            }
            unbounded |= upstream.boundedness == Boundedness::Unbounded;
            additional_inputs.push(InputEdge {
                key: other.key.clone(),
                element: ElementDescriptor {
                    type_id: T::type_identity(),
                    coder_id: T::coder_identity(),
                    coder_version: T::CODER_VERSION,
                },
            });
        }
        let operation = Operation::Flatten;
        let (semantic_id, semantic_version) = Pipeline::semantic_identity(&operation);
        let node = Node {
            key: key.into(),
            input: Some(self.key.clone()),
            input_element: Some(ElementDescriptor {
                type_id: T::type_identity(),
                coder_id: T::coder_identity(),
                coder_version: T::CODER_VERSION,
            }),
            additional_inputs,
            operation,
            semantic_id,
            semantic_version,
            type_id: T::type_identity(),
            coder_id: T::coder_identity(),
            coder_version: T::CODER_VERSION,
            boundedness: if unbounded {
                Boundedness::Unbounded
            } else {
                Boundedness::Bounded
            },
            time: parent.time,
            local_id: 0,
        };
        self.pipeline.add::<T>(node, None, None, None, None, None)?;
        Ok(Self {
            pipeline: self.pipeline.clone(),
            key: key.into(),
            marker: PhantomData,
        })
    }
    pub fn reshuffle_named(&self, key: &str) -> Result<Self, Error> {
        self.next(key, Operation::Reshuffle, None, None)
    }
    pub fn wait_on_named<S: Element>(
        &self,
        key: &str,
        signal: &PCollection<S>,
    ) -> Result<Self, Error> {
        if !Arc::ptr_eq(&self.pipeline.inner, &signal.pipeline.inner) {
            return Err(Error::graph(
                key,
                "wait inputs belong to different pipelines",
            ));
        }
        let parent = self.parent()?;
        if signal.parent()?.boundedness == Boundedness::Unbounded {
            return Err(Error::graph(key, "wait signal must be bounded"));
        }
        let operation = Operation::WaitOn;
        let (semantic_id, semantic_version) = Pipeline::semantic_identity(&operation);
        let node = Node {
            key: key.into(),
            input: Some(self.key.clone()),
            input_element: Some(ElementDescriptor {
                type_id: T::type_identity(),
                coder_id: T::coder_identity(),
                coder_version: T::CODER_VERSION,
            }),
            additional_inputs: vec![InputEdge {
                key: signal.key.clone(),
                element: ElementDescriptor {
                    type_id: S::type_identity(),
                    coder_id: S::coder_identity(),
                    coder_version: S::CODER_VERSION,
                },
            }],
            operation,
            semantic_id,
            semantic_version,
            type_id: T::type_identity(),
            coder_id: T::coder_identity(),
            coder_version: T::CODER_VERSION,
            boundedness: parent.boundedness,
            time: parent.time,
            local_id: 0,
        };
        self.pipeline.add::<T>(node, None, None, None, None, None)?;
        Ok(Self {
            pipeline: self.pipeline.clone(),
            key: key.into(),
            marker: PhantomData,
        })
    }
    pub fn fixed_windows(&self, key: &str, size_ns: i64) -> Result<Self, Error> {
        if size_ns <= 0 {
            return Err(Error::graph(key, "fixed window size must be positive"));
        }
        let window = Window::Fixed { size_ns };
        let runtime_window = window.clone();
        let transform = Transform::Single(Arc::new(move |mut e| {
            e.assigned_window = runtime_window.assign(e.timestamp_ns)?;
            e.window = runtime_window.clone();
            Ok(Some(e))
        }));
        self.next(key, Operation::FixedWindow, Some(transform), Some(window))
    }
    pub fn fixed_windows_with_offset(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<Self, Error> {
        if size_ns <= 0 {
            return Err(Error::graph(key, "fixed window size must be positive"));
        }
        let window = Window::FixedOffset { size_ns, offset_ns };
        let runtime_window = window.clone();
        let transform = Transform::Single(Arc::new(move |mut event| {
            event.assigned_window = runtime_window.assign(event.timestamp_ns)?;
            event.window = runtime_window.clone();
            Ok(Some(event))
        }));
        self.next(key, Operation::FixedWindow, Some(transform), Some(window))
    }
    pub fn with_window_named(&self, key: &str) -> Result<PCollection<(WindowInterval, T)>, Error> {
        if self.parent()?.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "window metadata requires a finite window",
            ));
        }
        let node_key = key.to_owned();
        let transform = Transform::Single(Arc::new(move |event| {
            let Envelope {
                value,
                timestamp_ns,
                window,
                assigned_window,
            } = event;
            let interval = assigned_window
                .ok_or_else(|| Error::execution(&node_key, "direct", "window interval missing"))?;
            let value = *value
                .downcast::<T>()
                .map_err(|_| Error::execution(&node_key, "direct", "window input type mismatch"))?;
            Ok(Some(Envelope {
                value: Box::new((interval, value)),
                timestamp_ns,
                window,
                assigned_window,
            }))
        }));
        self.next(key, Operation::Map, Some(transform), None)
    }
    pub fn assign_timestamps_named(
        &self,
        key: &str,
        f: impl Fn(&T) -> i64 + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        if self.parent()?.time.window != Window::Global {
            return Err(Error::graph(key, "assign timestamps before windowing"));
        }
        let key_owned = key.to_owned();
        let timestamp: TypedTimestamp<T> = Arc::new(f);
        let envelope_timestamp = Arc::clone(&timestamp);
        let transform = Transform::Single(Arc::new(move |mut event| {
            let value = event.value.downcast_ref::<T>().ok_or_else(|| {
                Error::execution(&key_owned, "direct", "timestamp input type mismatch")
            })?;
            event.timestamp_ns = envelope_timestamp(value);
            Ok(Some(event))
        }));
        let output = self.next(key, Operation::AssignTimestamp, Some(transform), None)?;
        self.pipeline.add_typed_transform(
            key,
            TypedTransformRegistration::Timestamp(Arc::new(timestamp)),
        )?;
        Ok(output)
    }
    /// Expose each element's event timestamp as `(nanoseconds, value)` while
    /// retaining the timestamp and window in the output envelope.
    pub fn reify_timestamps_named(&self, key: &str) -> Result<PCollection<(i64, T)>, Error> {
        let key_owned = key.to_owned();
        let transform = Transform::Single(Arc::new(move |event| {
            let Envelope {
                value,
                timestamp_ns,
                window,
                assigned_window,
            } = event;
            let value = *value.downcast::<T>().map_err(|_| {
                Error::execution(&key_owned, "direct", "timestamp input type mismatch")
            })?;
            Ok(Some(Envelope {
                value: Box::new((timestamp_ns, value)),
                timestamp_ns,
                window,
                assigned_window,
            }))
        }));
        self.next(key, Operation::Map, Some(transform), None)
    }
    pub fn output(&self, key: &str) -> Result<Output<T>, Error> {
        self.next::<T>(key, Operation::Output, None, None)?;
        Ok(Output {
            pipeline: self.pipeline.clone(),
            key: key.into(),
            marker: PhantomData,
        })
    }
    /// Create an output terminal on an isolated graph snapshot.
    /// Repeated local collections can use the same stable terminal key without
    /// registering duplicate nodes on the source pipeline.
    pub fn detached_output(&self, key: &str) -> Result<Output<T>, Error> {
        let (graph, registry) = self.pipeline.snapshot()?;
        let detached = Self {
            pipeline: Pipeline {
                inner: Arc::new(Mutex::new(Builder { graph, registry })),
            },
            key: self.key.clone(),
            marker: PhantomData,
        };
        detached.output(key)
    }
    pub fn append_sink(&self, key: &str, replay: Replay) -> Result<(), Error> {
        self.next::<T>(
            key,
            Operation::Sink {
                spec: SinkSpec {
                    mode: SinkMode::Append,
                    replay,
                    commit: true,
                },
            },
            None,
            None,
        )?;
        Ok(())
    }
    pub fn sink_named(
        &self,
        key: &str,
        spec: SinkSpec,
        open: impl Fn() -> Result<Box<dyn TypedSink<T>>, Error> + Send + Sync + 'static,
    ) -> Result<Sink<T>, Error> {
        self.sink_named_inner(key, spec, None, false, open)
    }

    /// Register a sink with a stable digest of its destination and options.
    /// Portable runners require this digest to reject a differently configured
    /// worker before opening either the source or the sink.
    pub fn sink_named_with_config(
        &self,
        key: &str,
        spec: SinkSpec,
        config_digest: String,
        open: impl Fn() -> Result<Box<dyn TypedSink<T>>, Error> + Send + Sync + 'static,
    ) -> Result<Sink<T>, Error> {
        if config_digest.is_empty() {
            return Err(Error::graph(key, "sink configuration digest is required"));
        }
        self.sink_named_inner(key, spec, Some(config_digest), false, open)
    }

    /// Register a sink whose bundle commit must be confirmed by the runner.
    /// The session returned by `open` must also declare finalization support.
    pub fn sink_named_with_config_and_finalization(
        &self,
        key: &str,
        spec: SinkSpec,
        config_digest: String,
        open: impl Fn() -> Result<Box<dyn TypedSink<T>>, Error> + Send + Sync + 'static,
    ) -> Result<Sink<T>, Error> {
        if config_digest.is_empty() || !spec.commit {
            return Err(Error::graph(
                key,
                "finalized sink needs a digest and commit",
            ));
        }
        self.sink_named_inner(key, spec, Some(config_digest), true, open)
    }

    fn sink_named_inner(
        &self,
        key: &str,
        spec: SinkSpec,
        config_digest: Option<String>,
        bundle_finalization: bool,
        open: impl Fn() -> Result<Box<dyn TypedSink<T>>, Error> + Send + Sync + 'static,
    ) -> Result<Sink<T>, Error> {
        let parent = self.parent()?;
        let operation = Operation::Sink { spec };
        let (semantic_id, semantic_version) = Pipeline::semantic_identity(&operation);
        let node = Node {
            key: key.into(),
            input: Some(self.key.clone()),
            input_element: Some(ElementDescriptor {
                type_id: T::type_identity(),
                coder_id: T::coder_identity(),
                coder_version: T::CODER_VERSION,
            }),
            additional_inputs: Vec::new(),
            operation,
            semantic_id,
            semantic_version,
            type_id: T::type_identity(),
            coder_id: T::coder_identity(),
            coder_version: T::CODER_VERSION,
            boundedness: parent.boundedness,
            time: parent.time,
            local_id: 0,
        };
        let key_owned = key.to_owned();
        let factory: SinkFactory = Arc::new(move || {
            Ok(Box::new(SinkAdapter {
                key: key_owned.clone(),
                inner: open()?,
            }) as Box<dyn SinkSession>)
        });
        self.pipeline.add::<T>(
            node,
            None,
            None,
            None,
            Some(SinkRegistration {
                factory,
                config_digest,
                bundle_finalization,
            }),
            None,
        )?;
        Ok(Sink {
            pipeline: self.pipeline.clone(),
            key: key.into(),
            marker: PhantomData,
        })
    }
    pub fn custom_named(
        &self,
        key: &str,
        semantic_id: &str,
        version: Version,
    ) -> Result<Self, Error> {
        if !semantic_id.contains('/') {
            return Err(Error::graph(key, "custom operation ID must be namespaced"));
        }
        self.next(
            key,
            Operation::Custom {
                semantic_id: semantic_id.into(),
                version,
            },
            None,
            None,
        )
    }
    pub fn global_combine_marker(&self, key: &str) -> Result<Self, Error> {
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded && parent.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global aggregation requires a finite window",
            ));
        }
        self.next(key, Operation::GlobalCombine, None, None)
    }
    pub fn combine_globally_named<U: Element>(
        &self,
        key: &str,
        f: impl Fn(Vec<T>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded && parent.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global aggregation requires a finite window",
            ));
        }
        let key_owned = key.to_owned();
        let output_window = parent.time.window.clone();
        let aggregate: Aggregate = Arc::new(move |inputs| {
            let [values]: [Vec<Envelope>; 1] = inputs.try_into().map_err(|_| {
                Error::execution(&key_owned, "direct", "combine requires exactly one input")
            })?;
            let mut groups: BTreeMap<Option<i64>, (Vec<T>, i64, Option<WindowInterval>)> =
                BTreeMap::new();
            for event in values {
                let typed = *event.value.downcast::<T>().map_err(|_| {
                    Error::execution(&key_owned, "direct", "combine input type mismatch")
                })?;
                let scope = event.assigned_window.map(|interval| interval.start_ns);
                groups
                    .entry(scope)
                    .or_insert_with(|| (Vec::new(), event.timestamp_ns, event.assigned_window))
                    .0
                    .push(typed);
            }
            if groups.is_empty() && output_window == Window::Global {
                groups.insert(None, (Vec::new(), 0, None));
            }
            Ok(groups
                .into_values()
                .map(|(values, timestamp_ns, assigned_window)| Envelope {
                    value: Box::new(f(values)),
                    timestamp_ns,
                    window: output_window.clone(),
                    assigned_window,
                })
                .collect())
        });
        self.next_aggregate(
            key,
            Operation::GlobalCombine,
            Vec::new(),
            parent.boundedness,
            aggregate,
            Some(GroupCoderRegistration::for_global::<T>()),
        )
    }
    pub fn combine_globally_lifted_named<C, A, O>(
        &self,
        key: &str,
        combiner: C,
    ) -> Result<PCollection<O>, Error>
    where
        C: CombineFn<T, A, O>,
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
        let window = parent.time.window;
        let node_key = key.to_owned();
        let combiner = Arc::new(MergeableAdapter(combiner));
        let factory: FoldFactory = Arc::new(move || {
            Box::new(GlobalFoldState::<T, A, O, MergeableAdapter<C>> {
                key: node_key.clone(),
                combiner: Arc::clone(&combiner),
                window: window.clone(),
                entries: BTreeMap::new(),
                marker: PhantomData,
            })
        });
        self.next_fold(
            key,
            Operation::FoldGlobal,
            factory,
            Some(GroupCoderRegistration::for_global::<T>()),
        )
    }
    pub fn fold_globally_ordered_named<C, A, O>(
        &self,
        key: &str,
        fold: C,
    ) -> Result<PCollection<O>, Error>
    where
        C: OrderedFoldFn<T, A, O>,
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
        let window = parent.time.window;
        let node_key = key.to_owned();
        let fold = Arc::new(OrderedAdapter(fold));
        let factory: FoldFactory = Arc::new(move || {
            Box::new(GlobalFoldState::<T, A, O, OrderedAdapter<C>> {
                key: node_key.clone(),
                combiner: Arc::clone(&fold),
                window: window.clone(),
                entries: BTreeMap::new(),
                marker: PhantomData,
            })
        });
        self.next_fold(key, Operation::FoldOrderedGlobal, factory, None)
    }
    /// Fold each window in an explicit order on Direct and portable runners.
    /// Order keys must be unique within a window. An empty global input emits
    /// `fold.finish(fold.create())` at timestamp zero.
    pub fn fold_globally_ordered_by_named<Q, C, A, O>(
        &self,
        key: &str,
        order_key: impl Fn(&T) -> Q + Send + Sync + 'static,
        fold: C,
    ) -> Result<PCollection<O>, Error>
    where
        Q: Ord + Send + 'static,
        C: OrderedFoldFn<T, A, O>,
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
        let output_window = parent.time.window.clone();
        let aggregate: Aggregate = Arc::new(move |inputs| {
            let [events]: [Vec<Envelope>; 1] = inputs.try_into().map_err(|_| {
                Error::execution(
                    &node_key,
                    "runner",
                    "ordered global fold requires one input",
                )
            })?;
            let mut groups: OrderedGlobalGroups<Q, T> = BTreeMap::new();
            for event in events {
                let value = *event.value.downcast::<T>().map_err(|_| {
                    Error::execution(
                        &node_key,
                        "runner",
                        "ordered global fold input type mismatch",
                    )
                })?;
                let scope = event.assigned_window.map(|interval| interval.start_ns);
                let entry = groups
                    .entry(scope)
                    .or_insert_with(|| (BTreeMap::new(), event.assigned_window));
                if entry
                    .0
                    .insert(order_key(&value), (value, event.timestamp_ns))
                    .is_some()
                {
                    return Err(Error::execution(
                        &node_key,
                        "runner",
                        "duplicate ordered fold key in key/window",
                    ));
                }
            }
            if groups.is_empty() && output_window == Window::Global {
                groups.insert(None, (BTreeMap::new(), None));
            }
            let mut result = Vec::with_capacity(groups.len());
            for (ordered, assigned_window) in groups.into_values() {
                let timestamp_ns = ordered
                    .first_key_value()
                    .map_or(0, |(_, (_, timestamp))| *timestamp);
                let mut accumulator = fold.create();
                for (_, (value, _)) in ordered {
                    fold.add_input(&mut accumulator, value);
                }
                result.push(Envelope {
                    value: Box::new(fold.finish(accumulator)),
                    timestamp_ns,
                    window: output_window.clone(),
                    assigned_window,
                });
            }
            Ok(result)
        });
        self.next_aggregate(
            key,
            Operation::GlobalCombine,
            Vec::new(),
            parent.boundedness,
            aggregate,
            Some(GroupCoderRegistration::for_global::<T>()),
        )
    }
    pub fn latest_globally_named(&self, key: &str) -> Result<Self, Error> {
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded && parent.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global aggregation requires a finite window",
            ));
        }
        let node_key = key.to_owned();
        let aggregate: Aggregate = Arc::new(move |mut inputs| {
            let values = inputs
                .pop()
                .ok_or_else(|| Error::execution(&node_key, "direct", "latest input missing"))?;
            let mut latest: BTreeMap<Option<i64>, Envelope> = BTreeMap::new();
            for event in values {
                let scope = event.assigned_window.map(|interval| interval.start_ns);
                match latest.entry(scope) {
                    std::collections::btree_map::Entry::Occupied(mut slot) => {
                        if event.timestamp_ns > slot.get().timestamp_ns {
                            slot.insert(event);
                        }
                    }
                    std::collections::btree_map::Entry::Vacant(slot) => {
                        slot.insert(event);
                    }
                }
            }
            if latest.is_empty() {
                return Err(Error::execution(
                    &node_key,
                    "direct",
                    "latest on empty input",
                ));
            }
            Ok(latest.into_values().collect())
        });
        self.next_aggregate(
            key,
            Operation::GlobalCombine,
            Vec::new(),
            parent.boundedness,
            aggregate,
            None,
        )
    }
    /// Select the latest event in each window with a deterministic tie key.
    /// Equal event times choose the greatest tie key; duplicate pairs fail.
    pub fn latest_globally_by_named<Q: Ord + Send + 'static>(
        &self,
        key: &str,
        tie_key: impl Fn(&T) -> Q + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded && parent.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global aggregation requires a finite window",
            ));
        }
        let node_key = key.to_owned();
        let aggregate: Aggregate = Arc::new(move |mut inputs| {
            let events = inputs
                .pop()
                .ok_or_else(|| Error::execution(&node_key, "runner", "latest tie input missing"))?;
            let mut groups: LatestTieGlobalGroups<Q> = BTreeMap::new();
            for event in events {
                let value = event.value.downcast_ref::<T>().ok_or_else(|| {
                    Error::execution(&node_key, "runner", "latest tie input type mismatch")
                })?;
                let scope = event.assigned_window.map(|interval| interval.start_ns);
                let order = (event.timestamp_ns, tie_key(value));
                match groups.entry(scope) {
                    std::collections::btree_map::Entry::Vacant(slot) => {
                        slot.insert((BTreeSet::from([order]), event));
                    }
                    std::collections::btree_map::Entry::Occupied(mut slot) => {
                        let (seen, latest) = slot.get_mut();
                        let is_later = seen.last().is_none_or(|current| order.cmp(current).is_gt());
                        if !seen.insert(order) {
                            return Err(Error::execution(
                                &node_key,
                                "runner",
                                "duplicate latest tie key in key/window/event time",
                            ));
                        }
                        if is_later {
                            *latest = event;
                        }
                    }
                }
            }
            if groups.is_empty() {
                return Err(Error::execution(
                    &node_key,
                    "runner",
                    "latest on empty input",
                ));
            }
            Ok(groups.into_values().map(|(_, latest)| latest).collect())
        });
        self.next_aggregate(
            key,
            Operation::GlobalCombine,
            Vec::new(),
            parent.boundedness,
            aggregate,
            Some(GroupCoderRegistration::for_global::<T>()),
        )
    }
    pub fn combine_per_window_named<U: Element>(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
        f: impl Fn(Vec<T>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<(WindowInterval, U)>, Error> {
        self.fixed_windows_with_offset(&format!("{key}/window"), size_ns, offset_ns)?
            .combine_globally_named(&format!("{key}/combine"), f)?
            .with_window_named(key)
    }
    pub fn batch_elements_named(
        &self,
        key: &str,
        batch_size: usize,
    ) -> Result<PCollection<Vec<T>>, Error> {
        let limit = batch_size.max(1);
        if limit == 1 {
            return self.map_named(key, |value| vec![value]);
        }
        let batch_semantic_id = format!("ironbeam/batch-elements/{limit}");
        self.combine_globally_named(&format!("{key}/collect"), |values| values)?
            .flat_map_registered(
                key,
                Some((&batch_semantic_id, BUILTIN_OPERATION_VERSION)),
                move |values| {
                    let mut batches = Vec::new();
                    let mut current = Vec::with_capacity(limit);
                    for value in values {
                        current.push(value);
                        if current.len() == limit {
                            batches
                                .push(std::mem::replace(&mut current, Vec::with_capacity(limit)));
                        }
                    }
                    if !current.is_empty() {
                        batches.push(current);
                    }
                    batches
                },
            )
    }
    pub fn map_batches_named<U: Element>(
        &self,
        key: &str,
        batch_size: usize,
        f: impl Fn(&[T]) -> Vec<U> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        self.batch_elements_named(&format!("{key}/batches"), batch_size)?
            .flat_map_named(key, move |batch| f(&batch))
    }
    /// Apply a versioned custom handler to explicitly sized batches.
    /// Batch boundaries follow `batch_elements_named`; input encounter order
    /// within a batch is not portable across runners after a shuffle.
    pub fn map_batches_with_semantic_named<U: Element>(
        &self,
        key: &str,
        batch_size: usize,
        semantic_id: &str,
        version: Version,
        f: impl Fn(&[T]) -> Vec<U> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        self.apply_transform_with_semantic_named(
            key,
            batch_size,
            semantic_id,
            version,
            move |batch| f(&batch),
        )
    }
    /// Apply a versioned partition transform to an owned, explicitly sized
    /// batch. This is the typed v5 replacement for the executable part of
    /// v4 `apply_transform`; the runner chooses no implicit partition size.
    /// A portable shuffle can change encounter order within each batch.
    /// A size of one runs incrementally on unbounded input.
    pub fn apply_transform_with_semantic_named<U: Element>(
        &self,
        key: &str,
        batch_size: usize,
        semantic_id: &str,
        version: Version,
        f: impl Fn(Vec<T>) -> Vec<U> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        if batch_size == 0 {
            return Err(Error::graph(key, "semantic batch size must be positive"));
        }
        Self::validate_semantic_handler(key, semantic_id, version)?;
        if batch_size == 1 {
            self.flat_map_with_semantic_named(key, semantic_id, version, move |value| {
                f(vec![value])
            })
        } else {
            self.batch_elements_named(&format!("{key}/batches"), batch_size)?
                .flat_map_with_semantic_named(key, semantic_id, version, f)
        }
    }
    /// Register a reusable v4-style partition operator with a portable
    /// application identity. The same operator must be registered by the
    /// worker. Batch size is explicit because physical partitions differ
    /// between runners. A size of one runs as a stateless operator and also
    /// supports unbounded input without waiting for a window to close.
    pub fn apply_dyn_transform_with_semantic_named<U: Element + Sync>(
        &self,
        key: &str,
        batch_size: usize,
        semantic_id: &str,
        version: Version,
        op: Arc<dyn DynOp>,
    ) -> Result<PCollection<U>, Error>
    where
        T: Sync,
    {
        if batch_size == 0 {
            return Err(Error::graph(key, "semantic batch size must be positive"));
        }
        Self::validate_semantic_handler(key, semantic_id, version)?;
        let key_owned = key.to_owned();
        let single = batch_size == 1;
        let transform = Transform::Multi(Arc::new(move |event| {
            let Envelope {
                value,
                timestamp_ns,
                window,
                assigned_window,
            } = event;
            let input = if single {
                vec![*value.downcast::<T>().map_err(|_| {
                    Error::execution(&key_owned, "direct", "partition input type mismatch")
                })?]
            } else {
                *value.downcast::<Vec<T>>().map_err(|_| {
                    Error::execution(&key_owned, "direct", "partition input type mismatch")
                })?
            };
            let output = op
                .apply(Box::new(input))
                .downcast::<Vec<U>>()
                .map_err(|_| {
                    Error::execution(&key_owned, "direct", "partition output type mismatch")
                })?;
            Ok((*output)
                .into_iter()
                .map(|value| Envelope {
                    value: Box::new(value) as Payload,
                    timestamp_ns,
                    window: window.clone(),
                    assigned_window,
                })
                .collect())
        }));
        if single {
            self.next_with_semantic(
                key,
                Operation::FlatMap,
                Some(transform),
                None,
                Some((semantic_id, version)),
            )
        } else {
            self.batch_elements_named(&format!("{key}/batches"), batch_size)?
                .next_with_semantic(
                    key,
                    Operation::FlatMap,
                    Some(transform),
                    None,
                    Some((semantic_id, version)),
                )
        }
    }
    pub fn batch_by_size_named(
        &self,
        key: &str,
        max_bytes: usize,
        size_fn: impl Fn(&T) -> usize + Send + Sync + 'static,
    ) -> Result<PCollection<Vec<T>>, Error> {
        let limit = max_bytes.max(1);
        let batch_semantic_id = format!("ironbeam/batch-by-size/{limit}");
        self.combine_globally_named(&format!("{key}/collect"), |values| values)?
            .flat_map_registered(
                key,
                Some((&batch_semantic_id, BUILTIN_OPERATION_VERSION)),
                move |values| {
                    let mut batches = Vec::new();
                    let mut current = Vec::new();
                    let mut bytes = 0_usize;
                    for value in values {
                        let size = size_fn(&value);
                        if !current.is_empty() && bytes.saturating_add(size) > limit {
                            batches.push(std::mem::take(&mut current));
                            bytes = 0;
                        }
                        bytes = bytes.saturating_add(size);
                        current.push(value);
                    }
                    if !current.is_empty() {
                        batches.push(current);
                    }
                    batches
                },
            )
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
    /// Apply a bounded side collection separately within each matching window.
    /// This composes a keyed co-group, so the runner may shuffle and buffer all
    /// values in a window. Main element timestamps are restored after the group.
    /// Use `map_with_side_named` for one global side view across all windows.
    pub fn map_with_windowed_side_named<S: Element, U: Element>(
        &self,
        key: &str,
        side: &PCollection<S>,
        f: impl Fn(T, &[S]) -> U + Send + Sync + 'static,
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
        let main = self
            .reify_timestamps_named(&format!("{key}/main-time"))?
            .map_named(&format!("{key}/main-key"), |value| (0_i32, value))?;
        let side = side.map_named(&format!("{key}/side-key"), |value| (0_i32, value))?;
        crate::cogroup_by_key_2_named(&format!("{key}/group"), &main, &side)?
            .flat_map_named(&format!("{key}/apply"), move |(_, (main, side))| {
                main.into_iter()
                    .map(|(timestamp_ns, value)| (timestamp_ns, f(value, &side)))
                    .collect::<Vec<_>>()
            })?
            .timestamps_from_pairs_in_window_named(key)
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

impl<T: Element> PCollection<(i64, T)> {
    /// Move timestamps from tuple values into event-time metadata. Timestamp
    /// assignment must happen before windowing; the timestamp field is removed
    /// from the value without cloning the payload.
    pub fn timestamps_from_pairs_named(&self, key: &str) -> Result<PCollection<T>, Error> {
        let assigned = self.assign_timestamps_named(&format!("{key}/assign"), |pair| pair.0)?;
        assigned.map_named(key, |(_, value)| value)
    }

    /// Restore a reified timestamp after grouping, provided it still belongs
    /// to the element's current window. This cannot move a row between windows.
    pub fn timestamps_from_pairs_in_window_named(
        &self,
        key: &str,
    ) -> Result<PCollection<T>, Error> {
        let window = self.parent()?.time.window;
        if window == Window::Global {
            return self.timestamps_from_pairs_named(key);
        }
        let assign_key = format!("{key}/assign");
        let error_key = assign_key.clone();
        let transform = Transform::Single(Arc::new(move |mut event| {
            let pair = event.value.downcast_ref::<(i64, T)>().ok_or_else(|| {
                Error::execution(&error_key, "runner", "timestamp pair type mismatch")
            })?;
            if window.assign(pair.0)? != event.assigned_window {
                return Err(Error::execution(
                    &error_key,
                    "runner",
                    "restored timestamp falls outside its window",
                ));
            }
            event.timestamp_ns = pair.0;
            Ok(Some(event))
        }));
        self.next::<(i64, T)>(
            &assign_key,
            Operation::AssignTimestamp,
            Some(transform),
            None,
        )?
        .map_named(key, |(_, value)| value)
    }
}
