type VectorCombineEntry<T> = (Vec<T>, i64, i64, Option<WindowInterval>);

#[derive(Clone, Copy, Default)]
struct TimePolicyOverride {
    trigger: Option<WindowTrigger>,
    output_time: Option<WindowOutputTime>,
    allowed_lateness_ns: Option<i64>,
}

#[derive(Clone, Copy)]
enum CompositeTriggerMode {
    Any,
    All,
    Each,
}

impl<T: Element> PCollection<T> {
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }
    /// Whether this collection can have more elements after its source's
    /// current output. A transform may retain unboundedness after windowing.
    pub fn boundedness(&self) -> Result<Boundedness, Error> {
        Ok(self.parent()?.boundedness)
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
        self.next_with_time_policy(
            key,
            operation,
            transform,
            window,
            semantic,
            TimePolicyOverride::default(),
        )
    }
    fn next_with_time_policy<U: Element>(
        &self,
        key: &str,
        operation: Operation,
        transform: Option<Transform>,
        window: Option<Window>,
        semantic: Option<(&str, Version)>,
        policy: TimePolicyOverride,
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
                coder_payload: T::coder_payload(),
                coder_version: T::CODER_VERSION,
            }),
            additional_inputs: Vec::new(),
            operation,
            semantic_id,
            semantic_version,
            type_id: U::type_identity(),
            coder_id: U::coder_identity(),
            coder_payload: U::coder_payload(),
            coder_version: U::CODER_VERSION,
            boundedness: parent.boundedness,
            time: Time {
                initial_timestamp_ns: parent.time.initial_timestamp_ns,
                window: window.unwrap_or(parent.time.window),
                trigger: policy.trigger.unwrap_or(parent.time.trigger),
                output_time: policy.output_time.unwrap_or(parent.time.output_time),
                allowed_lateness_ns: policy
                    .allowed_lateness_ns
                    .unwrap_or(parent.time.allowed_lateness_ns),
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
                coder_payload: T::coder_payload(),
                coder_version: T::CODER_VERSION,
            }),
            additional_inputs,
            operation,
            semantic_id,
            semantic_version,
            type_id: U::type_identity(),
            coder_id: U::coder_identity(),
            coder_payload: U::coder_payload(),
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
                coder_payload: T::coder_payload(),
                coder_version: T::CODER_VERSION,
            }),
            additional_inputs: Vec::new(),
            operation,
            semantic_id,
            semantic_version,
            type_id: U::type_identity(),
            coder_id: U::coder_identity(),
            coder_payload: U::coder_payload(),
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
        self.flat_map_registered(key, None, move |value| Ok(f(value)))
    }
    /// Run a zero-or-many-output handler that can return a typed execution error.
    pub fn flat_map_fallible_named<U: Element>(
        &self,
        key: &str,
        f: impl Fn(T) -> Result<Vec<U>, Error> + Send + Sync + 'static,
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
        self.flat_map_registered(key, Some((semantic_id, version)), move |value| Ok(f(value)))
    }
    /// Register a versioned fallible zero-or-many-output handler.
    pub fn flat_map_fallible_with_semantic_named<U: Element>(
        &self,
        key: &str,
        semantic_id: &str,
        version: Version,
        f: impl Fn(T) -> Result<Vec<U>, Error> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        Self::validate_semantic_handler(key, semantic_id, version)?;
        self.flat_map_registered(key, Some((semantic_id, version)), f)
    }
    fn flat_map_registered<U: Element>(
        &self,
        key: &str,
        semantic: Option<(&str, Version)>,
        f: impl Fn(T) -> Result<Vec<U>, Error> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        let key_owned = key.to_owned();
        let transform = Transform::Multi(Arc::new(move |e| {
            let Envelope {
                pane,
                value,
                timestamp_ns,
                window,
                assigned_window,
            } = e;
            let value = value
                .downcast::<T>()
                .map_err(|_| Error::execution(&key_owned, "direct", "input type mismatch"))?;
            Ok(f(*value)?
                .into_iter()
                .map(|value| Envelope {
                    pane,
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
                    coder_payload: T::coder_payload(),
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
                coder_payload: T::coder_payload(),
                coder_version: T::CODER_VERSION,
            }),
            additional_inputs,
            operation,
            semantic_id,
            semantic_version,
            type_id: T::type_identity(),
            coder_id: T::coder_identity(),
            coder_payload: T::coder_payload(),
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
    pub(crate) fn reshuffle_stream_shard_boundary_named(&self, key: &str) -> Result<Self, Error> {
        self.next_with_semantic(
            key,
            Operation::Reshuffle,
            None,
            None,
            Some(("ironbeam-builtin/stream-shard-boundary", BUILTIN_OPERATION_VERSION)),
        )
    }
    /// Erase a signal's element type for a heterogeneous completion barrier.
    #[must_use]
    pub fn wait_signal(&self) -> WaitSignal {
        WaitSignal {
            pipeline: self.pipeline.clone(),
            key: self.key.clone(),
            element: ElementDescriptor {
                type_id: T::type_identity(),
                coder_id: T::coder_identity(),
                coder_payload: T::coder_payload(),
                coder_version: T::CODER_VERSION,
            },
        }
    }
    pub fn wait_on_named<S: Element>(
        &self,
        key: &str,
        signal: &PCollection<S>,
    ) -> Result<Self, Error> {
        self.wait_on_signal_named(key, &signal.wait_signal())
    }
    fn wait_on_signal_named(&self, key: &str, signal: &WaitSignal) -> Result<Self, Error> {
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
                coder_payload: T::coder_payload(),
                coder_version: T::CODER_VERSION,
            }),
            additional_inputs: vec![InputEdge {
                key: signal.key.clone(),
                element: signal.element.clone(),
            }],
            operation,
            semantic_id,
            semantic_version,
            type_id: T::type_identity(),
            coder_id: T::coder_identity(),
            coder_payload: T::coder_payload(),
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
    /// Wait for every bounded signal collection to finish before emitting main values.
    /// Signal element values are ignored. Each signal has its own wait node so
    /// failures in any producer prevent downstream output from committing.
    pub fn wait_on_all_named<S: Element>(
        &self,
        key: &str,
        signals: &[PCollection<S>],
    ) -> Result<Self, Error> {
        let signals = signals.iter().map(PCollection::wait_signal).collect::<Vec<_>>();
        self.wait_on_signals_named(key, &signals)
    }
    /// Wait for bounded signals of different element types to finish.
    /// Convert each signal with `PCollection::wait_signal`; values are ignored.
    pub fn wait_on_signals_named(&self, key: &str, signals: &[WaitSignal]) -> Result<Self, Error> {
        if signals.is_empty() {
            return Err(Error::graph(key, "wait requires at least one signal"));
        }
        for signal in signals {
            if !Arc::ptr_eq(&self.pipeline.inner, &signal.pipeline.inner) {
                return Err(Error::graph(
                    key,
                    "wait inputs belong to different pipelines",
                ));
            }
            if signal.parent()?.boundedness == Boundedness::Unbounded {
                return Err(Error::graph(key, "wait signal must be bounded"));
            }
        }
        let mut current = self.clone();
        for (index, signal) in signals.iter().enumerate() {
            let node_key = if index + 1 == signals.len() {
                key.to_owned()
            } else {
                format!("{key}/signal-{index}")
            };
            current = current.wait_on_signal_named(&node_key, signal)?;
        }
        Ok(current)
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
    /// Fire a global, fixed, or sliding window early when its element count reaches `count`.
    /// Beam may combine several threshold crossings into one pane within a
    /// bundle. Direct supports this policy for single-input keyed grouping.
    pub fn early_count_trigger_named(&self, key: &str, count: u64) -> Result<Self, Error> {
        self.count_trigger_named(key, count, false)
    }

    /// Fire repeated early panes that each contain all values seen in the window.
    /// This requires a registered value coder when Direct runs the keyed fold.
    pub fn accumulating_count_trigger_named(&self, key: &str, count: u64) -> Result<Self, Error> {
        self.count_trigger_named(key, count, true)
    }

    /// Fire repeated early panes after new input has waited the given
    /// processing-time duration. The duration is a positive whole millisecond.
    pub fn early_processing_time_trigger_named(
        &self,
        key: &str,
        delay_ns: i64,
    ) -> Result<Self, Error> {
        self.processing_time_trigger_named(key, delay_ns, false)
    }

    /// Retain previous values in repeated processing-time early panes.
    pub fn accumulating_processing_time_trigger_named(
        &self,
        key: &str,
        delay_ns: i64,
    ) -> Result<Self, Error> {
        self.processing_time_trigger_named(key, delay_ns, true)
    }

    fn processing_time_trigger_named(
        &self,
        key: &str,
        delay_ns: i64,
        accumulating: bool,
    ) -> Result<Self, Error> {
        if delay_ns <= 0 || delay_ns % 1_000_000 != 0 {
            return Err(Error::graph(
                key,
                "processing-time trigger requires a positive whole-millisecond delay",
            ));
        }
        let window = self.parent()?.time.window;
        let operation = match window {
            Window::Fixed { .. } | Window::FixedOffset { .. } => Operation::FixedWindow,
            Window::Sliding { .. } | Window::Global => Operation::Map,
            Window::Session { .. } => {
                return Err(Error::graph(
                    key,
                    "processing-time trigger requires global, fixed, or sliding windows",
                ));
            }
        };
        self.next_with_time_policy(
            key,
            operation,
            Some(Transform::Single(Arc::new(|event| Ok(Some(event))))),
            Some(window),
            None,
            TimePolicyOverride {
                trigger: Some(if accumulating {
                    WindowTrigger::EarlyProcessingTimeAccumulating { delay_ns }
                } else {
                    WindowTrigger::EarlyProcessingTime { delay_ns }
                }),
                output_time: None,
                allowed_lateness_ns: None,
            },
        )
    }

    /// Repeatedly fire a discarding pane when either `count` new elements
    /// arrive or `delay_ns` elapses after the first pending element.
    pub fn count_or_processing_time_trigger_named(
        &self,
        key: &str,
        count: u64,
        delay_ns: i64,
    ) -> Result<Self, Error> {
        self.count_processing_time_trigger_with_mode_named(key, count, delay_ns, false, CompositeTriggerMode::Any)
    }

    /// Repeated count-or-processing-time firings that retain values from
    /// previous panes in the same key and window.
    pub fn accumulating_count_or_processing_time_trigger_named(
        &self,
        key: &str,
        count: u64,
        delay_ns: i64,
    ) -> Result<Self, Error> {
        self.count_processing_time_trigger_with_mode_named(key, count, delay_ns, true, CompositeTriggerMode::Any)
    }

    /// Fire early panes on a count or processing-time delay, then fire late
    /// panes after `late_count` new values. Requires positive allowed lateness.
    pub fn count_or_processing_time_and_late_count_trigger_named(
        &self,
        key: &str,
        count: u64,
        delay_ns: i64,
        late_count: u64,
    ) -> Result<Self, Error> {
        self.count_or_time_and_late_count_named(key, count, delay_ns, late_count, false)
    }

    /// Retain values across early and late count-or-time panes.
    pub fn accumulating_count_or_processing_time_and_late_count_trigger_named(
        &self,
        key: &str,
        count: u64,
        delay_ns: i64,
        late_count: u64,
    ) -> Result<Self, Error> {
        self.count_or_time_and_late_count_named(key, count, delay_ns, late_count, true)
    }

    fn count_or_time_and_late_count_named(
        &self,
        key: &str,
        count: u64,
        delay_ns: i64,
        late_count: u64,
        accumulating: bool,
    ) -> Result<Self, Error> {
        if count == 0 || count > i32::MAX as u64 || late_count == 0 || late_count > i32::MAX as u64 {
            return Err(Error::graph(key, "early and late counts must be positive 32-bit values"));
        }
        if delay_ns <= 0 || delay_ns % 1_000_000 != 0 {
            return Err(Error::graph(key, "processing-time trigger requires a positive whole-millisecond delay"));
        }
        let time = self.parent()?.time;
        if time.allowed_lateness_ns == 0 || !time.window.is_millisecond_aligned_fixed_or_sliding() {
            return Err(Error::graph(key, "combined count-or-time and late count trigger requires allowed lateness and millisecond-aligned fixed or sliding windows"));
        }
        let operation = match time.window {
            Window::Fixed { .. } | Window::FixedOffset { .. } => Operation::FixedWindow,
            Window::Sliding { .. } => Operation::Map,
            _ => unreachable!("validated finite window"),
        };
        self.next_with_time_policy(
            key,
            operation,
            Some(Transform::Single(Arc::new(|event| Ok(Some(event))))),
            Some(time.window),
            None,
            TimePolicyOverride {
                trigger: Some(if accumulating {
                    WindowTrigger::EarlyCountOrProcessingTimeAndLateCountAccumulating { count, delay_ns, late_count }
                } else {
                    WindowTrigger::EarlyCountOrProcessingTimeAndLateCount { count, delay_ns, late_count }
                }),
                output_time: None,
                allowed_lateness_ns: None,
            },
        )
    }

    /// Repeatedly fire a discarding pane once both `count` elements have
    /// arrived and `delay_ns` has elapsed since the first pending element.
    pub fn count_and_processing_time_trigger_named(
        &self,
        key: &str,
        count: u64,
        delay_ns: i64,
    ) -> Result<Self, Error> {
        self.count_processing_time_trigger_with_mode_named(key, count, delay_ns, false, CompositeTriggerMode::All)
    }

    /// Repeated count-and-processing-time firings that retain earlier values.
    pub fn accumulating_count_and_processing_time_trigger_named(
        &self,
        key: &str,
        count: u64,
        delay_ns: i64,
    ) -> Result<Self, Error> {
        self.count_processing_time_trigger_with_mode_named(key, count, delay_ns, true, CompositeTriggerMode::All)
    }

    /// Repeatedly fire a count pane, then a processing-time pane for later
    /// input. Each phase starts when the preceding phase has fired.
    pub fn count_then_processing_time_trigger_named(
        &self,
        key: &str,
        count: u64,
        delay_ns: i64,
    ) -> Result<Self, Error> {
        self.count_processing_time_trigger_with_mode_named(key, count, delay_ns, false, CompositeTriggerMode::Each)
    }

    /// Sequential count and processing-time panes that retain earlier values.
    pub fn accumulating_count_then_processing_time_trigger_named(
        &self,
        key: &str,
        count: u64,
        delay_ns: i64,
    ) -> Result<Self, Error> {
        self.count_processing_time_trigger_with_mode_named(key, count, delay_ns, true, CompositeTriggerMode::Each)
    }

    fn count_processing_time_trigger_with_mode_named(
        &self,
        key: &str,
        count: u64,
        delay_ns: i64,
        accumulating: bool,
        mode: CompositeTriggerMode,
    ) -> Result<Self, Error> {
        if count == 0 || count > i32::MAX as u64 {
            return Err(Error::graph(key, "early count trigger requires a positive 32-bit count"));
        }
        if delay_ns <= 0 || delay_ns % 1_000_000 != 0 {
            return Err(Error::graph(key, "processing-time trigger requires a positive whole-millisecond delay"));
        }
        let window = self.parent()?.time.window;
        let operation = match window {
            Window::Fixed { .. } | Window::FixedOffset { .. } => Operation::FixedWindow,
            Window::Sliding { .. } | Window::Global => Operation::Map,
            Window::Session { .. } => {
                return Err(Error::graph(key, "count/processing-time trigger requires global, fixed, or sliding windows"));
            }
        };
        self.next_with_time_policy(
            key,
            operation,
            Some(Transform::Single(Arc::new(|event| Ok(Some(event))))),
            Some(window),
            None,
            TimePolicyOverride {
                trigger: Some(match (mode, accumulating) {
                    (CompositeTriggerMode::Any, false) => WindowTrigger::EarlyCountOrProcessingTime { count, delay_ns },
                    (CompositeTriggerMode::Any, true) => WindowTrigger::EarlyCountOrProcessingTimeAccumulating { count, delay_ns },
                    (CompositeTriggerMode::All, false) => WindowTrigger::EarlyCountAndProcessingTime { count, delay_ns },
                    (CompositeTriggerMode::All, true) => WindowTrigger::EarlyCountAndProcessingTimeAccumulating { count, delay_ns },
                    (CompositeTriggerMode::Each, false) => WindowTrigger::EarlyCountThenProcessingTime { count, delay_ns },
                    (CompositeTriggerMode::Each, true) => WindowTrigger::EarlyCountThenProcessingTimeAccumulating { count, delay_ns },
                }),
                output_time: None,
                allowed_lateness_ns: None,
            },
        )
    }

    fn count_trigger_named(&self, key: &str, count: u64, accumulating: bool) -> Result<Self, Error> {
        if count == 0 || count > i32::MAX as u64 {
            return Err(Error::graph(key, "early count trigger requires a positive 32-bit count"));
        }
        let window = self.parent()?.time.window;
        let operation = match window {
            Window::Fixed { .. } | Window::FixedOffset { .. } => Operation::FixedWindow,
            // A second sliding WindowInto would assign every existing window
            // again, multiplying rows. Carry the new strategy on an identity
            // ParDo instead.
            Window::Sliding { .. } | Window::Global => Operation::Map,
            Window::Session { .. } => return Err(Error::graph(key, "early count trigger requires global, fixed, or sliding windows")),
        };
        self.next_with_time_policy(
            key,
            operation,
            Some(Transform::Single(Arc::new(|event| Ok(Some(event))))),
            Some(window),
            None,
            TimePolicyOverride {
                trigger: Some(if accumulating {
                    WindowTrigger::EarlyElementCountAccumulating { count }
                } else {
                    WindowTrigger::EarlyElementCount { count }
                }),
                output_time: None,
                allowed_lateness_ns: None,
            },
        )
    }

    /// Timestamp grouped output at the final millisecond in each window.
    /// Finite window dimensions must be whole milliseconds.
    pub fn end_of_window_output_time_named(&self, key: &str) -> Result<Self, Error> {
        self.set_output_time_named(key, WindowOutputTime::EndOfWindow)
    }

    /// Timestamp each grouped pane at the latest event time it contains.
    pub fn latest_in_pane_output_time_named(&self, key: &str) -> Result<Self, Error> {
        self.set_output_time_named(key, WindowOutputTime::LatestInPane)
    }

    fn set_output_time_named(&self, key: &str, policy: WindowOutputTime) -> Result<Self, Error> {
        let window = self.parent()?.time.window;
        if !window.supports_output_time_policy() {
            return Err(Error::graph(key, "output time policy requires a global or millisecond-aligned finite window"));
        }
        self.next_with_time_policy(
            key,
            Operation::Map,
            Some(Transform::Single(Arc::new(|event| Ok(Some(event))))),
            Some(window),
            None,
            TimePolicyOverride {
                trigger: None,
                output_time: Some(policy),
                allowed_lateness_ns: None,
            },
        )
    }

    /// Retain fixed or sliding windows after their end for late elements.
    /// The duration must be a nonnegative whole number of milliseconds.
    /// Direct and portable runners emit default-trigger late panes for
    /// single-input keyed grouping. Direct rejects unsupported stateful fan-in.
    pub fn allowed_lateness_named(&self, key: &str, duration_ns: i64) -> Result<Self, Error> {
        if duration_ns < 0 || duration_ns % 1_000_000 != 0 {
            return Err(Error::graph(key, "allowed lateness must be nonnegative whole milliseconds"));
        }
        let window = self.parent()?.time.window;
        if !window.is_millisecond_aligned_fixed_or_sliding() {
            return Err(Error::graph(key, "allowed lateness requires millisecond-aligned fixed or sliding windows"));
        }
        self.next_with_time_policy(
            key,
            Operation::Map,
            Some(Transform::Single(Arc::new(|event| Ok(Some(event))))),
            Some(window),
            None,
            TimePolicyOverride {
                trigger: None,
                output_time: None,
                allowed_lateness_ns: Some(duration_ns),
            },
        )
    }

    /// Fire a late pane after `count` new elements reach a key and window.
    /// Set a positive allowed lateness before this policy. Incomplete late
    /// panes are emitted when the window expires.
    pub fn late_count_trigger_named(&self, key: &str, count: u64) -> Result<Self, Error> {
        self.set_late_count_trigger_named(key, count, false)
    }

    /// Fire repeated late panes that retain every value previously observed
    /// for each key and window. Direct requires a registered value coder.
    pub fn accumulating_late_count_trigger_named(&self, key: &str, count: u64) -> Result<Self, Error> {
        self.set_late_count_trigger_named(key, count, true)
    }

    fn set_late_count_trigger_named(&self, key: &str, count: u64, accumulating: bool) -> Result<Self, Error> {
        if count == 0 || count > i32::MAX as u64 {
            return Err(Error::graph(key, "late count trigger requires a positive 32-bit count"));
        }
        let time = self.parent()?.time;
        if time.allowed_lateness_ns == 0 || !time.window.is_millisecond_aligned_fixed_or_sliding() {
            return Err(Error::graph(key, "late count trigger requires allowed lateness and millisecond-aligned fixed or sliding windows"));
        }
        self.next_with_time_policy(
            key,
            Operation::Map,
            Some(Transform::Single(Arc::new(|event| Ok(Some(event))))),
            Some(time.window),
            None,
            TimePolicyOverride {
                trigger: Some(if accumulating {
                    WindowTrigger::LateElementCountAccumulating { count }
                } else {
                    WindowTrigger::LateElementCount { count }
                }),
                output_time: None,
                allowed_lateness_ns: None,
            },
        )
    }

    /// Fire repeated early and late count panes for each key and window.
    /// Positive allowed lateness must be set first. Both thresholds use
    /// discarding panes; the watermark produces an on-time pane if nonempty.
    pub fn early_and_late_count_trigger_named(
        &self,
        key: &str,
        early_count: u64,
        late_count: u64,
    ) -> Result<Self, Error> {
        self.set_early_and_late_count_trigger_named(key, early_count, late_count, false)
    }

    /// Fire repeated early and late count panes that retain prior values.
    /// Positive allowed lateness must be set first. Direct requires a
    /// registered value coder for checkpointed unbounded grouping.
    pub fn accumulating_early_and_late_count_trigger_named(
        &self,
        key: &str,
        early_count: u64,
        late_count: u64,
    ) -> Result<Self, Error> {
        self.set_early_and_late_count_trigger_named(key, early_count, late_count, true)
    }

    fn set_early_and_late_count_trigger_named(
        &self,
        key: &str,
        early_count: u64,
        late_count: u64,
        accumulating: bool,
    ) -> Result<Self, Error> {
        if early_count == 0 || early_count > i32::MAX as u64
            || late_count == 0 || late_count > i32::MAX as u64
        {
            return Err(Error::graph(key, "early and late counts must be positive 32-bit values"));
        }
        let time = self.parent()?.time;
        if time.allowed_lateness_ns == 0 || !time.window.is_millisecond_aligned_fixed_or_sliding() {
            return Err(Error::graph(key, "combined count trigger requires allowed lateness and millisecond-aligned fixed or sliding windows"));
        }
        self.next_with_time_policy(
            key,
            Operation::Map,
            Some(Transform::Single(Arc::new(|event| Ok(Some(event))))),
            Some(time.window),
            None,
            TimePolicyOverride {
                trigger: Some(if accumulating {
                    WindowTrigger::EarlyAndLateElementCountAccumulating { early_count, late_count }
                } else {
                    WindowTrigger::EarlyAndLateElementCount { early_count, late_count }
                }),
                output_time: None,
                allowed_lateness_ns: None,
            },
        )
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
    /// Assign merging session intervals without grouping the collection.
    /// A later keyed grouping can merge overlapping intervals for each key.
    pub fn session_windows_named(&self, key: &str, gap_ns: i64) -> Result<Self, Error> {
        if self.parent()?.time.window != Window::Global {
            return Err(Error::graph(key, "assign timestamps before session windowing"));
        }
        if gap_ns <= 0 {
            return Err(Error::graph(key, "session gap must be positive"));
        }
        let window = Window::Session { gap_ns };
        let runtime_window = window.clone();
        let transform = Transform::Single(Arc::new(move |mut event| {
            event.assigned_window = runtime_window.assign(event.timestamp_ns)?;
            event.window = runtime_window.clone();
            Ok(Some(event))
        }));
        self.next(key, Operation::SessionWindow, Some(transform), Some(window))
    }
    /// Assign an element to each overlapping sliding interval.
    pub fn sliding_windows(
        &self,
        key: &str,
        size_ns: i64,
        period_ns: i64,
        offset_ns: i64,
    ) -> Result<Self, Error>
    where
        T: Clone,
    {
        if size_ns <= 0 || period_ns <= 0 {
            return Err(Error::graph(key, "sliding size and period must be positive"));
        }
        let window = Window::Sliding { size_ns, period_ns, offset_ns };
        let runtime_window = window.clone();
        let key_owned = key.to_owned();
        let transform = Transform::Multi(Arc::new(move |event| {
            let value = event.value.downcast::<T>().map_err(|_| {
                Error::execution(&key_owned, "direct", "sliding window input type mismatch")
            })?;
            Ok(runtime_window
                .assign_all(event.timestamp_ns)?
                .into_iter()
                .map(|assigned_window| Envelope {
                    pane: event.pane,
                    value: Box::new((*value).clone()),
                    timestamp_ns: event.timestamp_ns,
                    window: runtime_window.clone(),
                    assigned_window,
                })
                .collect())
        }));
        self.next(key, Operation::SlidingWindow, Some(transform), Some(window))
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
                pane,
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
                pane,
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
                pane,
                value,
                timestamp_ns,
                window,
                assigned_window,
            } = event;
            let value = *value.downcast::<T>().map_err(|_| {
                Error::execution(&key_owned, "direct", "timestamp input type mismatch")
            })?;
            Ok(Some(Envelope {
                pane,
                value: Box::new((timestamp_ns, value)),
                timestamp_ns,
                window,
                assigned_window,
            }))
        }));
        self.next(key, Operation::Map, Some(transform), None)
    }
    /// Expose the runner's pane for each element while retaining that pane in
    /// the output envelope. Sources have `NO_FIRING`; a completed default
    /// window firing has `FIRST_LAST_ON_TIME`.
    pub fn reify_panes_named(&self, key: &str) -> Result<PCollection<(PaneInfo, T)>, Error> {
        let key_owned = key.to_owned();
        let transform = Transform::Single(Arc::new(move |event| {
            let Envelope {
                pane,
                value,
                timestamp_ns,
                window,
                assigned_window,
            } = event;
            let value = *value.downcast::<T>().map_err(|_| {
                Error::execution(&key_owned, "direct", "pane input type mismatch")
            })?;
            Ok(Some(Envelope {
                pane,
                value: Box::new((pane, value)),
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
                coder_payload: T::coder_payload(),
                coder_version: T::CODER_VERSION,
            }),
            additional_inputs: Vec::new(),
            operation,
            semantic_id,
            semantic_version,
            type_id: T::type_identity(),
            coder_id: T::coder_identity(),
            coder_payload: T::coder_payload(),
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
        let output_time = parent.time.output_time;
        let aggregate: Aggregate = Arc::new(move |inputs| {
            let [values]: [Vec<Envelope>; 1] = inputs.try_into().map_err(|_| {
                Error::execution(&key_owned, "direct", "combine requires exactly one input")
            })?;
            let mut groups: BTreeMap<Option<i64>, VectorCombineEntry<T>> =
                BTreeMap::new();
            for event in values {
                let typed = *event.value.downcast::<T>().map_err(|_| {
                    Error::execution(&key_owned, "direct", "combine input type mismatch")
                })?;
                let scope = event.assigned_window.map(|interval| interval.start_ns);
                let entry = groups
                    .entry(scope)
                    .or_insert_with(|| (Vec::new(), event.timestamp_ns, event.timestamp_ns, event.assigned_window));
                entry.0.push(typed);
                entry.1 = entry.1.min(event.timestamp_ns);
                entry.2 = entry.2.max(event.timestamp_ns);
            }
            if groups.is_empty() && output_window == Window::Global {
                groups.insert(None, (Vec::new(), 0, 0, None));
            }
            Ok(groups
                .into_values()
                .map(|(values, earliest_ns, latest_ns, assigned_window)| Envelope {
                    pane: PaneInfo::FIRST_LAST_ON_TIME,
                    value: Box::new(f(values)),
                    timestamp_ns: grouped_output_timestamp(output_time, earliest_ns, Some(latest_ns), assigned_window),
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
        let output_time = parent.time.output_time;
        let node_key = key.to_owned();
        let combiner = Arc::new(MergeableAdapter(combiner));
        let factory: FoldFactory = Arc::new(move || {
            Box::new(GlobalFoldState::<T, A, O, MergeableAdapter<C>> {
                key: node_key.clone(),
                combiner: Arc::clone(&combiner),
                window: window.clone(),
                output_time,
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
        let output_time = parent.time.output_time;
        let node_key = key.to_owned();
        let fold = Arc::new(OrderedAdapter(fold));
        let factory: FoldFactory = Arc::new(move || {
            Box::new(GlobalFoldState::<T, A, O, OrderedAdapter<C>> {
                key: node_key.clone(),
                combiner: Arc::clone(&fold),
                window: window.clone(),
                output_time,
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
                    pane: PaneInfo::FIRST_LAST_ON_TIME,
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
                    Ok(batches)
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
                pane,
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
                    pane,
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
                    Ok(batches)
                },
            )
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
            if !window.assign_all(pair.0)?.contains(&event.assigned_window) {
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
