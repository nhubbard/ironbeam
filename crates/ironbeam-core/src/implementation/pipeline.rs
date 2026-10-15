#[derive(Default)]
struct Builder {
    graph: Graph,
    registry: Registry,
}
impl Default for Graph {
    fn default() -> Self {
        Self {
            version: GRAPH_VERSION,
            checkpoint_version: CHECKPOINT_VERSION,
            nodes: Vec::new(),
        }
    }
}
#[derive(Clone, Default)]
pub struct Pipeline {
    inner: Arc<Mutex<Builder>>,
}

pub struct PCollection<T> {
    pipeline: Pipeline,
    key: String,
    marker: PhantomData<T>,
}

/// A typed collection's completion dependency without its element type.
/// The signal's values are ignored by wait transforms.
#[derive(Clone)]
pub struct WaitSignal {
    pipeline: Pipeline,
    key: String,
    element: ElementDescriptor,
}

impl WaitSignal {
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
}
impl<T> Clone for PCollection<T> {
    fn clone(&self) -> Self {
        Self {
            pipeline: self.pipeline.clone(),
            key: self.key.clone(),
            marker: PhantomData,
        }
    }
}

impl Pipeline {
    fn semantic_identity(operation: &Operation) -> (String, Version) {
        match operation {
            Operation::Source { .. } => ("ironbeam/source".into(), BUILTIN_OPERATION_VERSION),
            Operation::Map => ("ironbeam/map".into(), BUILTIN_OPERATION_VERSION),
            Operation::Filter => ("ironbeam/filter".into(), BUILTIN_OPERATION_VERSION),
            Operation::FlatMap => ("ironbeam/flat-map".into(), BUILTIN_OPERATION_VERSION),
            Operation::AssignTimestamp => (
                "ironbeam/assign-timestamp".into(),
                BUILTIN_OPERATION_VERSION,
            ),
            Operation::Flatten => ("ironbeam/flatten".into(), BUILTIN_OPERATION_VERSION),
            Operation::Reshuffle => ("ironbeam/reshuffle".into(), BUILTIN_OPERATION_VERSION),
            Operation::WaitOn => ("ironbeam/wait-on".into(), BUILTIN_OPERATION_VERSION),
            Operation::GroupByKey => ("ironbeam/group-by-key".into(), BUILTIN_OPERATION_VERSION),
            Operation::CoGroupByKey => {
                ("ironbeam/co-group-by-key".into(), BUILTIN_OPERATION_VERSION)
            }
            Operation::JoinInner => ("ironbeam/join-inner".into(), BUILTIN_OPERATION_VERSION),
            Operation::JoinLeft => ("ironbeam/join-left".into(), BUILTIN_OPERATION_VERSION),
            Operation::JoinRight => ("ironbeam/join-right".into(), BUILTIN_OPERATION_VERSION),
            Operation::JoinFull => ("ironbeam/join-full".into(), BUILTIN_OPERATION_VERSION),
            Operation::SideInputMap => {
                ("ironbeam/side-input-map".into(), BUILTIN_OPERATION_VERSION)
            }
            Operation::FixedWindow => ("ironbeam/fixed-window".into(), BUILTIN_OPERATION_VERSION),
            Operation::SlidingWindow => ("ironbeam/sliding-window".into(), BUILTIN_OPERATION_VERSION),
            Operation::SessionWindow => ("ironbeam/session-window".into(), BUILTIN_OPERATION_VERSION),
            Operation::GlobalCombine => {
                ("ironbeam/global-combine".into(), BUILTIN_OPERATION_VERSION)
            }
            Operation::FoldGlobal => ("ironbeam/fold-global".into(), BUILTIN_OPERATION_VERSION),
            Operation::FoldPerKey => ("ironbeam/fold-per-key".into(), BUILTIN_OPERATION_VERSION),
            Operation::FoldOrderedGlobal => (
                "ironbeam/fold-ordered-global".into(),
                BUILTIN_OPERATION_VERSION,
            ),
            Operation::FoldOrderedPerKey => (
                "ironbeam/fold-ordered-per-key".into(),
                BUILTIN_OPERATION_VERSION,
            ),
            Operation::BatchMapValues { .. } => (
                "ironbeam/batch-map-values".into(),
                BUILTIN_OPERATION_VERSION,
            ),
            Operation::Custom {
                semantic_id,
                version,
            } => (semantic_id.clone(), *version),
            Operation::Output => ("ironbeam/output".into(), BUILTIN_OPERATION_VERSION),
            Operation::Sink { .. } => ("ironbeam/sink".into(), BUILTIN_OPERATION_VERSION),
        }
    }
    fn add<T: Element>(
        &self,
        mut node: Node,
        source: Option<SourceRegistration>,
        transform: Option<Transform>,
        aggregate: Option<Aggregate>,
        sink: Option<SinkRegistration>,
        group_coder: Option<GroupCoderRegistration>,
    ) -> Result<(), Error> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| Error::graph(&node.key, "builder lock poisoned"))?;
        if inner.graph.node(&node.key).is_some() {
            return Err(Error::graph(&node.key, "duplicate stable node key"));
        }
        if node.type_id != T::type_identity()
            || node.coder_id != T::coder_identity()
            || node.coder_version != T::CODER_VERSION
        {
            return Err(Error::graph(
                &node.key,
                "node element descriptor differs from coder",
            ));
        }
        node.local_id = inner.graph.nodes.len();
        if T::supports_coder() {
            inner
                .registry
                .coders
                .insert(node.key.clone(), CoderRegistration::for_element::<T>());
        }
        if let Some(source) = source {
            inner.registry.sources.insert(node.key.clone(), source);
        }
        if let Some(transform) = transform {
            inner
                .registry
                .transforms
                .insert(node.key.clone(), transform);
        }
        if let Some(aggregate) = aggregate {
            inner
                .registry
                .aggregates
                .insert(node.key.clone(), aggregate);
        }
        if let Some(sink) = sink {
            inner.registry.sinks.insert(node.key.clone(), sink);
        }
        if let Some(group_coder) = group_coder {
            inner
                .registry
                .group_coders
                .insert(node.key.clone(), group_coder);
        }
        inner.graph.nodes.push(node);
        drop(inner);
        Ok(())
    }
    fn add_fold<T: Element>(
        &self,
        mut node: Node,
        factory: FoldFactory,
        group_coder: Option<GroupCoderRegistration>,
    ) -> Result<(), Error> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| Error::graph(&node.key, "builder lock poisoned"))?;
        if inner.graph.node(&node.key).is_some() {
            return Err(Error::graph(&node.key, "duplicate stable node key"));
        }
        if node.type_id != T::type_identity()
            || node.coder_id != T::coder_identity()
            || node.coder_version != T::CODER_VERSION
        {
            return Err(Error::graph(
                &node.key,
                "node element descriptor differs from coder",
            ));
        }
        node.local_id = inner.graph.nodes.len();
        if T::supports_coder() {
            inner
                .registry
                .coders
                .insert(node.key.clone(), CoderRegistration::for_element::<T>());
        }
        inner.registry.folds.insert(node.key.clone(), factory);
        if let Some(group_coder) = group_coder {
            inner
                .registry
                .group_coders
                .insert(node.key.clone(), group_coder);
        }
        inner.graph.nodes.push(node);
        drop(inner);
        Ok(())
    }
    fn add_typed_transform(
        &self,
        key: &str,
        transform: TypedTransformRegistration,
    ) -> Result<(), Error> {
        self.inner
            .lock()
            .map_err(|_| Error::graph(key, "builder lock poisoned"))?
            .registry
            .typed_transforms
            .insert(key.to_owned(), transform);
        Ok(())
    }
    /// Build a bounded collection from an owned vector.
    ///
    /// # Panics
    /// Panics only if an internal built-in coder violates its infallible contract
    /// while computing the deferred source identity.
    pub fn from_vec<T: Element + Clone + Sync>(
        &self,
        key: &str,
        values: Vec<T>,
    ) -> Result<PCollection<T>, Error> {
        let values = Arc::new(values);
        let config_digest = if !T::supports_coder() {
            None
        } else if infallible_builtin_coder::<T>() {
            let digest_values = Arc::clone(&values);
            Some(SourceConfigDigest::Deferred(Arc::new(
                DeferredSourceDigest {
                    value: OnceLock::new(),
                    compute: Box::new(move || {
                        vector_source_digest(&digest_values)
                            .expect("built-in vector coder is infallible")
                    }),
                },
            )))
        } else {
            Some(SourceConfigDigest::Ready(vector_source_digest(&values)?))
        };
        let len_hint = values.len();
        let batch_values = Arc::clone(&values);
        let batch_key = key.to_owned();
        let vec_batch_factory: TypedVecBatchFactory<T> = Arc::new(move |start, len| {
            let end = start
                .checked_add(len)
                .ok_or_else(|| Error::graph(&batch_key, "vector source batch range overflow"))?;
            let slice = batch_values.get(start..end).ok_or_else(|| {
                Error::graph(&batch_key, "vector source batch range exceeds length")
            })?;
            Ok(slice.to_vec())
        });
        let erased_batch = Arc::clone(&vec_batch_factory);
        let erased_vec_batch_factory: ErasedVecBatchFactory = Arc::new(move |start, len| {
            erased_batch(start, len).map(|values| Box::new(values) as Payload)
        });
        let envelope_batch = Arc::clone(&vec_batch_factory);
        let vec_envelope_batch_factory: VecEnvelopeBatchFactory = Arc::new(move |start, len| {
            envelope_batch(start, len).map(|values| {
                values
                    .into_iter()
                    .map(|value| Envelope {
                        pane: PaneInfo::NO_FIRING,
                        value: Box::new(value),
                        timestamp_ns: 0,
                        window: Window::Global,
                        assigned_window: None,
                    })
                    .collect()
            })
        });
        let typed_factory: TypedSourceFactory<T> = Arc::new(move || {
            let values = Arc::clone(&values);
            Ok(Box::new(
                (0..values.len()).map(move |i| Ok(values[i].clone())),
            ))
        });
        let envelope_factory = Arc::clone(&typed_factory);
        let factory: SourceFactory = Arc::new(move || {
            let source = envelope_factory()?;
            Ok(Box::new(source.map(|result| {
                result.map(|value| Envelope {
                    pane: PaneInfo::NO_FIRING,
                    value: Box::new(value),
                    timestamp_ns: 0,
                    window: Window::Global,
                    assigned_window: None,
                })
            })) as SourceIterator)
        });
        self.source_node::<T>(
            key,
            "ironbeam/source",
            Boundedness::Bounded,
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            Some(SourceRegistration {
                factory,
                unbounded_factory: None,
                typed_factory: Some(Arc::new(typed_factory)),
                vec_batch_factory: Some(Arc::new(vec_batch_factory)),
                erased_vec_batch_factory: Some((TypeId::of::<Vec<T>>(), erased_vec_batch_factory)),
                vec_envelope_batch_factory: Some(vec_envelope_batch_factory),
                len_hint: Some(len_hint),
                config_digest,
                test_stream: None,
            }),
            0,
        )
    }
    /// Create a bounded in-memory source from an iterator.
    pub fn from_iter<T: Element + Clone + Sync>(
        &self,
        key: &str,
        values: impl IntoIterator<Item = T>,
    ) -> Result<PCollection<T>, Error> {
        self.from_vec(key, values.into_iter().collect())
    }
    pub fn bounded_source<T: Element>(
        &self,
        key: &str,
        progress: SourceProgress,
        config_digest: String,
        len_hint: Option<usize>,
        open: impl Fn() -> Result<TypedSourceIterator<T>, Error> + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error> {
        self.bounded_source_with_semantic(
            key,
            "ironbeam/source",
            progress,
            config_digest,
            len_hint,
            open,
        )
    }
    pub fn bounded_source_with_semantic<T: Element>(
        &self,
        key: &str,
        semantic_id: &str,
        progress: SourceProgress,
        config_digest: String,
        len_hint: Option<usize>,
        open: impl Fn() -> Result<TypedSourceIterator<T>, Error> + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error> {
        if config_digest.is_empty() {
            return Err(Error::graph(
                key,
                "bounded source configuration digest is required",
            ));
        }
        let typed_factory: TypedSourceFactory<T> = Arc::new(open);
        let envelope_factory = Arc::clone(&typed_factory);
        let factory: SourceFactory = Arc::new(move || {
            let source = envelope_factory()?;
            Ok(Box::new(source.map(|result| {
                result.map(|value| Envelope {
                    pane: PaneInfo::NO_FIRING,
                    value: Box::new(value),
                    timestamp_ns: 0,
                    window: Window::Global,
                    assigned_window: None,
                })
            })) as SourceIterator)
        });
        self.source_node(
            key,
            semantic_id,
            Boundedness::Bounded,
            progress,
            Some(SourceRegistration {
                factory,
                unbounded_factory: None,
                typed_factory: Some(Arc::new(typed_factory)),
                vec_batch_factory: None,
                erased_vec_batch_factory: None,
                vec_envelope_batch_factory: None,
                len_hint,
                config_digest: Some(SourceConfigDigest::Ready(config_digest)),
                test_stream: None,
            }),
            0,
        )
    }
    pub fn unbounded_source<T: Element>(
        &self,
        key: &str,
        progress: SourceProgress,
        initial_timestamp_ns: i64,
    ) -> Result<PCollection<T>, Error> {
        self.source_node::<T>(
            key,
            "ironbeam/source",
            Boundedness::Unbounded,
            progress,
            None,
            initial_timestamp_ns,
        )
    }
    /// Register a resumable, nonblocking unbounded source implementation.
    /// `open` receives the last committed checkpoint position, if any.
    pub fn unbounded_source_with_factory<T: Element>(
        &self,
        key: &str,
        progress: SourceProgress,
        config_digest: String,
        initial_timestamp_ns: i64,
        open: impl Fn(Option<Vec<u8>>) -> Result<TypedUnboundedSourceReader<T>, Error>
        + Send
        + Sync
        + 'static,
    ) -> Result<PCollection<T>, Error> {
        if config_digest.is_empty() {
            return Err(Error::graph(
                key,
                "unbounded source configuration digest is required",
            ));
        }
        let typed_factory = Arc::new(open);
        let unbounded_factory: UnboundedSourceFactory = Arc::new(move |position| {
            Ok(Box::new(TypedUnboundedAdapter {
                inner: typed_factory(position)?,
            }))
        });
        self.source_node::<T>(
            key,
            "ironbeam/source",
            Boundedness::Unbounded,
            progress,
            Some(SourceRegistration {
                factory: Arc::new(|| Err(Error::incompatible("unbounded source requires polling"))),
                unbounded_factory: Some(unbounded_factory),
                typed_factory: None,
                vec_batch_factory: None,
                erased_vec_batch_factory: None,
                vec_envelope_batch_factory: None,
                len_hint: None,
                config_digest: Some(SourceConfigDigest::Ready(config_digest)),
                test_stream: None,
            }),
            initial_timestamp_ns,
        )
    }
    /// Register a deterministic unbounded fixture with event-time and
    /// processing-time controls for runner conformance tests. Direct advances
    /// processing time as a progress step; it does not simulate timers.
    pub fn test_stream_source<T: Element>(
        &self,
        key: &str,
        events: Vec<TestStreamEvent<T>>,
    ) -> Result<PCollection<T>, Error> {
        if !T::supports_coder() || events.is_empty() {
            return Err(Error::graph(key, "test stream needs a coder and events"));
        }
        let mut encoded = Vec::with_capacity(events.len());
        let mut watermark = i64::MIN;
        let mut processing_time_ns = 0_i64;
        for event in events {
            encoded.push(match event {
                TestStreamEvent::Elements(values) => EncodedTestStreamEvent::Elements(
                    values
                        .into_iter()
                        .map(|(value, timestamp)| value.encode().map(|bytes| (bytes, timestamp)))
                        .collect::<Result<Vec<_>, _>>()?,
                ),
                TestStreamEvent::AdvanceWatermark(next) => {
                    if next <= watermark {
                        return Err(Error::graph(key, "test stream watermark must advance"));
                    }
                    watermark = next;
                    EncodedTestStreamEvent::AdvanceWatermark(next)
                }
                TestStreamEvent::AdvanceProcessingTime(duration) => {
                    if duration <= 0 {
                        return Err(Error::graph(
                            key,
                            "test stream processing time must advance",
                        ));
                    }
                    processing_time_ns = processing_time_ns.checked_add(duration).ok_or_else(|| {
                        Error::graph(key, "test stream processing time overflows i64 nanoseconds")
                    })?;
                    EncodedTestStreamEvent::AdvanceProcessingTime(duration)
                }
            });
        }
        let bytes =
            serde_json::to_vec(&encoded).map_err(|error| Error::incompatible(error.to_string()))?;
        let mut digest = String::from("sha256-v1:");
        for byte in Sha256::digest(bytes) {
            write!(digest, "{byte:02x}").map_err(|error| Error::incompatible(error.to_string()))?;
        }
        let direct_events = Arc::new(encoded.clone());
        let direct_key = key.to_owned();
        self.source_node::<T>(
            key,
            "ironbeam/test-stream",
            Boundedness::Unbounded,
            SourceProgress {
                stable_split_id: true,
                replay: Replay::Idempotent,
                reports_event_time: true,
            },
            Some(SourceRegistration {
                factory: Arc::new(|| {
                    Err(Error::incompatible(
                        "test stream requires a portable runner",
                    ))
                }),
                unbounded_factory: Some(Arc::new(move |position| {
                    Ok(Box::new(TypedUnboundedAdapter {
                        inner: Box::new(DirectTestStream::<T>::open(
                            direct_key.clone(),
                            Arc::clone(&direct_events),
                            position,
                        )?),
                    }))
                })),
                typed_factory: None,
                vec_batch_factory: None,
                erased_vec_batch_factory: None,
                vec_envelope_batch_factory: None,
                len_hint: None,
                config_digest: Some(SourceConfigDigest::Ready(digest)),
                test_stream: Some(encoded),
            }),
            0,
        )
    }
    fn source_node<T: Element>(
        &self,
        key: &str,
        semantic_id: &str,
        boundedness: Boundedness,
        progress: SourceProgress,
        factory: Option<SourceRegistration>,
        initial_timestamp_ns: i64,
    ) -> Result<PCollection<T>, Error> {
        let operation = Operation::Source { progress };
        if !semantic_id.contains('/') {
            return Err(Error::graph(key, "source semantic ID must be namespaced"));
        }
        let (_, semantic_version) = Self::semantic_identity(&operation);
        self.add::<T>(
            Node {
                key: key.into(),
                input: None,
                input_element: None,
                additional_inputs: Vec::new(),
                operation,
                semantic_id: semantic_id.into(),
                semantic_version,
                type_id: T::type_identity(),
                coder_id: T::coder_identity(),
                coder_version: T::CODER_VERSION,
                boundedness,
                time: Time {
                    initial_timestamp_ns,
                    window: Window::Global,
                    trigger: WindowTrigger::Default,
                    output_time: WindowOutputTime::EarliestInPane,
                    allowed_lateness_ns: 0,
                },
                local_id: 0,
            },
            factory,
            None,
            None,
            None,
            None,
        )?;
        Ok(PCollection {
            pipeline: self.clone(),
            key: key.into(),
            marker: PhantomData,
        })
    }
    pub fn snapshot(&self) -> Result<(Graph, Registry), Error> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| Error::graph("pipeline", "builder lock poisoned"))?;
        inner.graph.validate()?;
        Ok((inner.graph.clone(), inner.registry.clone()))
    }
}
