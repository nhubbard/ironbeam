pub type Payload = Box<dyn Any + Send>;
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ElementMetadata {
    pub timestamp_ns: i64,
    pub window: Window,
    pub assigned_window: Option<WindowInterval>,
    pub pane: PaneInfo,
}
pub struct Envelope {
    pub value: Payload,
    pub timestamp_ns: i64,
    pub window: Window,
    pub assigned_window: Option<WindowInterval>,
    pub pane: PaneInfo,
}
pub type SourceIterator = Box<dyn Iterator<Item = Result<Envelope, Error>> + Send>;
pub type TypedSourceIterator<T> = Box<dyn Iterator<Item = Result<T, Error>> + Send>;
/// One nonblocking poll result from an unbounded source split.
#[derive(Clone, Debug)]
pub enum UnboundedPoll<T> {
    Pending,
    Ready(UnboundedBatch<T>),
}

/// One timestamped value emitted by a source poll.
#[derive(Clone, Debug)]
pub struct Timestamped<T> {
    pub value: T,
    pub timestamp_ns: i64,
}

/// Data and the durable position after those elements from one source split.
#[derive(Clone, Debug)]
pub struct UnboundedBatch<T> {
    pub elements: Vec<T>,
    pub progress: ProgressSignal,
}

/// A resumable, nonblocking reader. `Pending` lets a runner check cancellation
/// and schedule another poll without trapping a worker thread in the source.
pub trait UnboundedSource<T>: Send {
    fn poll(&mut self, max_elements: usize) -> Result<UnboundedPoll<T>, Error>;

    /// Cumulative processing time advanced by a scripted source, if known.
    /// Runners may use this clock to evaluate processing-time triggers.
    fn processing_time_ns(&self) -> Option<i64> {
        None
    }
}

pub type UnboundedSourceReader = Box<dyn UnboundedSource<Envelope>>;
pub type TypedUnboundedSourceReader<T> = Box<dyn UnboundedSource<Timestamped<T>>>;
type UnboundedSourceFactory =
    Arc<dyn Fn(Option<Vec<u8>>) -> Result<UnboundedSourceReader, Error> + Send + Sync>;
type SourceFactory = Arc<dyn Fn() -> Result<SourceIterator, Error> + Send + Sync>;
type TypedSourceFactory<T> = Arc<dyn Fn() -> Result<TypedSourceIterator<T>, Error> + Send + Sync>;
type TypedVecBatchFactory<T> = Arc<dyn Fn(usize, usize) -> Result<Vec<T>, Error> + Send + Sync>;
/// Local typed join callback used by a runner with an in-memory vector source.
pub type TypedJoin<T> =
    dyn Fn(usize, &dyn Fn() -> Result<(), Error>) -> Result<Vec<T>, Error> + Send + Sync;
type TypedJoinFactory<T> = Arc<TypedJoin<T>>;
type ErasedVecBatchFactory = Arc<dyn Fn(usize, usize) -> Result<Payload, Error> + Send + Sync>;
type VecEnvelopeBatchFactory =
    Arc<dyn Fn(usize, usize) -> Result<Vec<Envelope>, Error> + Send + Sync>;
/// A typed bounded-slice reader retained by an in-memory vector source.
pub type VecBatchReader<'a, T> = &'a (dyn Fn(usize, usize) -> Result<Vec<T>, Error> + Send + Sync);
/// A bounded vector slice whose type is checked by a fold before execution.
pub type ErasedVecBatchReader<'a> =
    &'a (dyn Fn(usize, usize) -> Result<Payload, Error> + Send + Sync);
/// Read an in-memory vector slice as envelopes for a checkpointed transform path.
pub type VecEnvelopeBatchReader<'a> =
    &'a (dyn Fn(usize, usize) -> Result<Vec<Envelope>, Error> + Send + Sync);
type SingleTransform = Arc<dyn Fn(Envelope) -> Result<Option<Envelope>, Error> + Send + Sync>;
type MultiTransform = Arc<dyn Fn(Envelope) -> Result<Vec<Envelope>, Error> + Send + Sync>;
type TypedMap<T, U> = Arc<dyn Fn(T) -> U + Send + Sync>;
type TypedFilter<T> = Arc<dyn Fn(&T) -> bool + Send + Sync>;
type TypedTimestamp<T> = Arc<dyn Fn(&T) -> i64 + Send + Sync>;
#[derive(Clone)]
enum TypedTransformRegistration {
    Map(Arc<dyn Any + Send + Sync>),
    Filter(Arc<dyn Any + Send + Sync>),
    Timestamp(Arc<dyn Any + Send + Sync>),
}

/// A same-type value operation for a Direct route with no envelope metadata changes.
pub enum TypedValueTransform<'a, T> {
    Map(&'a (dyn Fn(T) -> T + Send + Sync)),
    Filter(&'a (dyn Fn(&T) -> bool + Send + Sync)),
}
impl<T> TypedValueTransform<'_, T> {
    pub fn apply(&self, value: T) -> Option<T> {
        match self {
            Self::Map(map) => Some(map(value)),
            Self::Filter(predicate) => predicate(&value).then_some(value),
        }
    }
}
#[derive(Clone)]
enum Transform {
    Single(SingleTransform),
    Multi(MultiTransform),
    StatefulValue(StatefulValueTransform),
    StatefulTimer(Box<StatefulTimerTransform>),
}
type StatefulValueCallback = Arc<
    dyn Fn(Payload, Option<&[u8]>) -> Result<(Option<Vec<u8>>, Payload), Error> + Send + Sync,
>;
type StatefulValueCells = HashMap<(Option<WindowInterval>, Vec<u8>), Vec<u8>>;
struct StatefulValueTransform {
    key_bytes: fn(&(dyn Any + Send)) -> Result<Vec<u8>, Error>,
    validate_key: fn(&[u8]) -> Result<(), Error>,
    validate_state: fn(&[u8]) -> Result<(), Error>,
    global_window: bool,
    allowed_lateness_ns: i64,
    callback: StatefulValueCallback,
    cells: Mutex<StatefulValueCells>,
}
impl Clone for StatefulValueTransform {
    fn clone(&self) -> Self {
        Self {
            key_bytes: self.key_bytes,
            validate_key: self.validate_key,
            validate_state: self.validate_state,
            global_window: self.global_window,
            allowed_lateness_ns: self.allowed_lateness_ns,
            callback: Arc::clone(&self.callback),
            // Pipeline snapshots are separate executions. Never carry a
            // previous Direct run's user state into the next one.
            cells: Mutex::new(HashMap::new()),
        }
    }
}
impl StatefulValueTransform {
    fn expire(&self, watermark: Option<i64>) -> Result<(), Error> {
        let Some(watermark) = watermark.filter(|_| !self.global_window) else {
            return Ok(());
        };
        self.cells.lock().map_err(|_| Error::recovery("stateful-value", "state lock poisoned"))?
            .retain(|(window, _), _| window.is_some_and(|interval| {
                interval.end_ns.saturating_add(self.allowed_lateness_ns) > watermark
            }));
        Ok(())
    }

    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        let cells = self.cells.lock().map_err(|_| Error::recovery("stateful-value", "state lock poisoned"))?;
        let mut entries = cells.iter()
            .map(|((window, key), value)| (*window, key.clone(), value.clone()))
            .collect::<Vec<_>>();
        drop(cells);
        entries.sort_by(|left, right| {
            (left.0.map(|window| (window.start_ns, window.end_ns)), &left.1)
                .cmp(&(right.0.map(|window| (window.start_ns, window.end_ns)), &right.1))
        });
        serde_json::to_vec(&StatefulValueSnapshot { version: 1, entries })
            .map_err(|error| Error::recovery("stateful-value", error.to_string()))
    }

    fn restore(&self, bytes: &[u8]) -> Result<(), Error> {
        let snapshot: StatefulValueSnapshot = serde_json::from_slice(bytes)
            .map_err(|error| Error::recovery("stateful-value", error.to_string()))?;
        if snapshot.version != 1 {
            return Err(Error::incompatible("stateful value checkpoint version mismatch"));
        }
        let mut restored = HashMap::new();
        for (window, key, value) in snapshot.entries {
            if window.is_some_and(|window| window.end_ns <= window.start_ns)
                || (self.global_window && window.is_some())
                || (!self.global_window && window.is_none())
            {
                return Err(Error::recovery("stateful-value", "invalid state window"));
            }
            (self.validate_key)(&key)?;
            (self.validate_state)(&value)?;
            if restored.insert((window, key), value).is_some() {
                return Err(Error::recovery("stateful-value", "duplicate keyed state cell"));
            }
        }
        *self.cells.lock().map_err(|_| Error::recovery("stateful-value", "state lock poisoned"))? = restored;
        Ok(())
    }
    fn apply(
        &self,
        input: Envelope,
        current: Option<&[u8]>,
    ) -> Result<(Option<Vec<u8>>, Envelope), Error> {
        let (next, value) = (self.callback)(input.value, current)?;
        Ok((next, Envelope { value, ..input }))
    }
    fn apply_direct(&self, input: Envelope) -> Result<Envelope, Error> {
        if !self.global_window && input.assigned_window.is_none() {
            return Err(Error::execution("stateful-value", "direct", "windowed state has no assigned window"));
        }
        let key = (self.key_bytes)(input.value.as_ref())?;
        let cell = (input.assigned_window, key);
        let mut cells = self.cells.lock().map_err(|_| Error::execution("stateful-value", "direct", "state lock poisoned"))?;
        let (next, output) = self.apply(input, cells.get(&cell).map(Vec::as_slice))?;
        match next {
            Some(bytes) => { cells.insert(cell, bytes); }
            None => { cells.remove(&cell); }
        }
        drop(cells);
        Ok(output)
    }
}
#[derive(Serialize, Deserialize)]
struct StatefulValueSnapshot {
    version: u8,
    entries: Vec<(Option<WindowInterval>, Vec<u8>, Vec<u8>)>,
}
type Aggregate = Arc<dyn Fn(Vec<Vec<Envelope>>) -> Result<Vec<Envelope>, Error> + Send + Sync>;
type OrderedGlobalGroups<Q, T> =
    BTreeMap<Option<i64>, (BTreeMap<Q, (T, i64)>, Option<WindowInterval>)>;
type LatestTieGlobalGroups<Q> = BTreeMap<Option<i64>, (BTreeSet<(i64, Q)>, Envelope)>;
pub trait CombineFn<V, A, O>: Send + Sync + 'static {
    fn create(&self) -> A;
    fn add_input(&self, accumulator: &mut A, value: V);
    fn merge(&self, accumulator: &mut A, other: A);
    fn finish(&self, accumulator: A) -> O;
}
pub trait OrderedFoldFn<V, A, O>: Send + Sync + 'static {
    fn create(&self) -> A;
    fn add_input(&self, accumulator: &mut A, value: V);
    fn finish(&self, accumulator: A) -> O;
}
trait FoldLogic<V, A, O>: Send + Sync + 'static {
    fn create(&self) -> A;
    fn add_input(&self, accumulator: &mut A, value: V);
    fn finish(&self, accumulator: A) -> O;
}
struct MergeableAdapter<C>(C);
impl<V, A, O, C: CombineFn<V, A, O>> FoldLogic<V, A, O> for MergeableAdapter<C> {
    fn create(&self) -> A {
        self.0.create()
    }
    fn add_input(&self, accumulator: &mut A, value: V) {
        self.0.add_input(accumulator, value);
    }
    fn finish(&self, accumulator: A) -> O {
        self.0.finish(accumulator)
    }
}
struct OrderedAdapter<C>(C);
impl<V, A, O, C: OrderedFoldFn<V, A, O>> FoldLogic<V, A, O> for OrderedAdapter<C> {
    fn create(&self) -> A {
        self.0.create()
    }
    fn add_input(&self, accumulator: &mut A, value: V) {
        self.0.add_input(accumulator, value);
    }
    fn finish(&self, accumulator: A) -> O {
        self.0.finish(accumulator)
    }
}
pub trait FoldSession: Send {
    fn add(&mut self, event: Envelope) -> Result<(), Error>;
    /// Resolve an incoming window against active keyed state before a runner
    /// checks expiry. Merging folds may extend the assigned interval.
    fn resolved_interval(&self, event: &Envelope) -> Result<Option<WindowInterval>, Error> {
        Ok(event.assigned_window)
    }
    /// Fire ready keyed groups for a repeated early element-count trigger.
    /// Returns emitted panes and the number of buffered inputs released.
    fn drain_ready_count(&mut self, _count: u64) -> Result<(Vec<Envelope>, usize), Error> {
        Err(Error::unsupported("fold", "runner", "early-count-trigger"))
    }
    /// Start processing-time timers for groups with new, unfired input.
    fn arm_processing_time(&mut self, _now_ns: i64, _delay_ns: i64) -> Result<(), Error> {
        Err(Error::unsupported("fold", "runner", "processing-time-trigger"))
    }
    /// Earliest armed processing-time deadline in this fold.
    fn next_processing_time_deadline(&self) -> Option<i64> {
        None
    }
    /// Fire groups whose processing-time deadline has elapsed.
    fn drain_ready_processing_time(&mut self, _now_ns: i64) -> Result<(Vec<Envelope>, usize), Error> {
        Err(Error::unsupported("fold", "runner", "processing-time-trigger"))
    }
    /// Remove groups whose windows end at or before the watermark. Returns
    /// their output and the number of buffered source elements released.
    fn drain_closed(&mut self, _watermark_ns: i64) -> Result<(Vec<Envelope>, usize), Error> {
        Err(Error::unsupported("fold", "runner", "drain-closed-windows"))
    }
    /// Fire buffered late input without expiring the window. The runner calls
    /// this after a source poll whose prior watermark passed the window end.
    fn drain_late(&mut self, _final_firing: bool) -> Result<(Vec<Envelope>, usize), Error> {
        Err(Error::unsupported("fold", "runner", "late-window-firing"))
    }
    /// Return a versioned snapshot when this fold supports in-progress recovery.
    fn checkpoint_state(&self) -> Result<Option<Vec<u8>>, Error> {
        Ok(None)
    }
    /// Restore a snapshot into a newly opened fold before feeding more input.
    fn restore_checkpoint_state(&mut self, _state: &[u8]) -> Result<(), Error> {
        Err(Error::unsupported(
            "fold",
            "runner",
            "fold-checkpoint-state",
        ))
    }
    /// Whether a built-in vector source can feed a whole globally windowed batch.
    fn accepts_vec_batch(&self, _batch_type: TypeId) -> bool {
        false
    }
    /// Feed a checked built-in vector batch without per-element envelopes.
    fn add_vec_batch(&mut self, _values: Payload) -> Result<(), Error> {
        Err(Error::unsupported("fold", "direct", "typed-vector-batch"))
    }
    /// Whether this fold can consume a typed vector batch after timestamp
    /// assignment and private keyed timestamp capture.
    fn accepts_timed_vec_batch(&self, _batch_type: TypeId) -> bool {
        false
    }
    fn add_timed_vec_batch(
        &mut self,
        _values: Payload,
        _timestamp_fn: &(dyn Any + Send + Sync),
        _timestamp_key: &str,
    ) -> Result<(), Error> {
        Err(Error::unsupported(
            "fold",
            "direct",
            "typed-timestamp-batch",
        ))
    }
    fn finish(self: Box<Self>) -> Result<Vec<Envelope>, Error>;
    /// Drain completed output without requiring a second whole-result vector.
    fn finish_into(
        self: Box<Self>,
        emit: &mut dyn FnMut(Envelope) -> Result<(), Error>,
    ) -> Result<(), Error> {
        for event in self.finish()? {
            emit(event)?;
        }
        Ok(())
    }
}
type FoldFactory = Arc<dyn Fn() -> Box<dyn FoldSession> + Send + Sync>;
type FoldEntry<A> = (A, i64, i64, Window, Option<WindowInterval>);

struct GlobalFoldState<T, A, O, C> {
    key: String,
    combiner: Arc<C>,
    window: Window,
    output_time: WindowOutputTime,
    entries: BTreeMap<Option<i64>, FoldEntry<A>>,
    marker: PhantomData<(T, O)>,
}
impl<T: Element, A: Send + 'static, O: Element, C: FoldLogic<T, A, O>> FoldSession
    for GlobalFoldState<T, A, O, C>
{
    fn add(&mut self, event: Envelope) -> Result<(), Error> {
        let Envelope {
            pane: _,
            value,
            timestamp_ns,
            window,
            assigned_window,
        } = event;
        let value = *value
            .downcast::<T>()
            .map_err(|_| Error::execution(&self.key, "direct", "fold input type mismatch"))?;
        let scope = assigned_window.map(|interval| interval.start_ns);
        let entry = self.entries.entry(scope).or_insert_with(|| {
            (
                self.combiner.create(),
                timestamp_ns,
                timestamp_ns,
                window,
                assigned_window,
            )
        });
        entry.1 = entry.1.min(timestamp_ns);
        entry.2 = entry.2.max(timestamp_ns);
        self.combiner.add_input(&mut entry.0, value);
        Ok(())
    }
    fn finish(mut self: Box<Self>) -> Result<Vec<Envelope>, Error> {
        if self.entries.is_empty() && self.window == Window::Global {
            self.entries
                .insert(None, (self.combiner.create(), 0, 0, Window::Global, None));
        }
        Ok(std::mem::take(&mut self.entries)
            .into_values()
            .map(
                |(accumulator, earliest_ns, latest_ns, window, assigned_window)| Envelope {
                    pane: PaneInfo::FIRST_LAST_ON_TIME,
                    value: Box::new(self.combiner.finish(accumulator)),
                    timestamp_ns: grouped_output_timestamp(self.output_time, earliest_ns, Some(latest_ns), assigned_window),
                    window,
                    assigned_window,
                },
            )
            .collect())
    }
    fn finish_into(
        mut self: Box<Self>,
        emit: &mut dyn FnMut(Envelope) -> Result<(), Error>,
    ) -> Result<(), Error> {
        if self.entries.is_empty() && self.window == Window::Global {
            self.entries
                .insert(None, (self.combiner.create(), 0, 0, Window::Global, None));
        }
        for (accumulator, earliest_ns, latest_ns, window, assigned_window) in
            std::mem::take(&mut self.entries).into_values()
        {
            emit(Envelope {
                pane: PaneInfo::FIRST_LAST_ON_TIME,
                value: Box::new(self.combiner.finish(accumulator)),
                timestamp_ns: grouped_output_timestamp(self.output_time, earliest_ns, Some(latest_ns), assigned_window),
                window,
                assigned_window,
            })?;
        }
        Ok(())
    }
}

struct KeyedFoldState<K, V, A, O, C> {
    key: String,
    combiner: Arc<C>,
    output_time: WindowOutputTime,
    indices: HashMap<(K, Option<WindowInterval>), usize>,
    rows: Vec<(K, FoldEntry<A>)>,
    marker: PhantomData<(V, O)>,
}
impl<
    K: Element + Eq + Hash + Clone,
    V: Element,
    A: Send + 'static,
    O: Element,
    C: FoldLogic<V, A, O>,
> FoldSession for KeyedFoldState<K, V, A, O, C>
{
    fn add(&mut self, event: Envelope) -> Result<(), Error> {
        let Envelope {
            pane: _,
            value,
            timestamp_ns,
            window,
            assigned_window,
        } = event;
        let (row_key, value) = *value
            .downcast::<(K, V)>()
            .map_err(|_| Error::execution(&self.key, "direct", "keyed fold input type mismatch"))?;
        let scope = (row_key.clone(), assigned_window);
        let position = if let Some(&existing) = self.indices.get(&scope) {
            existing
        } else {
            let next = self.rows.len();
            self.indices.insert(scope, next);
            self.rows.push((
                row_key,
                (
                    self.combiner.create(),
                    timestamp_ns,
                    timestamp_ns,
                    window,
                    assigned_window,
                ),
            ));
            next
        };
        self.rows[position].1.1 = self.rows[position].1.1.min(timestamp_ns);
        self.rows[position].1.2 = self.rows[position].1.2.max(timestamp_ns);
        self.combiner.add_input(&mut self.rows[position].1.0, value);
        Ok(())
    }
    fn finish(mut self: Box<Self>) -> Result<Vec<Envelope>, Error> {
        Ok(std::mem::take(&mut self.rows)
            .into_iter()
            .map(
                |(key, (accumulator, earliest_ns, latest_ns, window, assigned_window))| Envelope {
                    pane: PaneInfo::FIRST_LAST_ON_TIME,
                    value: Box::new((key, self.combiner.finish(accumulator))),
                    timestamp_ns: grouped_output_timestamp(self.output_time, earliest_ns, Some(latest_ns), assigned_window),
                    window,
                    assigned_window,
                },
            )
            .collect())
    }
    fn finish_into(
        mut self: Box<Self>,
        emit: &mut dyn FnMut(Envelope) -> Result<(), Error>,
    ) -> Result<(), Error> {
        for (key, (accumulator, earliest_ns, latest_ns, window, assigned_window)) in
            std::mem::take(&mut self.rows)
        {
            emit(Envelope {
                pane: PaneInfo::FIRST_LAST_ON_TIME,
                value: Box::new((key, self.combiner.finish(accumulator))),
                timestamp_ns: grouped_output_timestamp(self.output_time, earliest_ns, Some(latest_ns), assigned_window),
                window,
                assigned_window,
            })?;
        }
        Ok(())
    }
}
pub trait SinkSession: Send {
    /// Bind a portable runner's stable bundle-attempt ID before the first write.
    /// Direct streaming binds this to the source cursor; bounded Direct jobs
    /// do not call this method.
    fn begin_bundle(&mut self, _instruction_id: &str) -> Result<(), Error> {
        Ok(())
    }
    fn write(&mut self, event: Envelope) -> Result<(), Error>;
    fn commit(&mut self) -> Result<(), Error>;
    fn abort(&mut self) -> Result<(), Error>;
    /// Whether this committed bundle needs a runner callback before its side
    /// effect is considered accepted.
    fn requires_bundle_finalization(&self) -> bool {
        false
    }
    /// Complete a previously committed bundle after the runner accepts it.
    /// Implementations must make retries idempotent.
    fn finalize_bundle(&mut self) -> Result<(), Error> {
        Err(Error::unsupported("sink", "runner", "bundle-finalization"))
    }
    /// Recover a committed attempt after worker state was lost. Implementations
    /// must verify durable state for this exact ID and fail if it is missing.
    fn recover_bundle_finalization(&mut self, _instruction_id: &str) -> Result<(), Error> {
        Err(Error::unsupported(
            "sink",
            "runner",
            "bundle-finalization-recovery",
        ))
    }
}
pub trait TypedSink<T: Element>: Send {
    fn begin_bundle(&mut self, _instruction_id: &str) -> Result<(), Error> {
        Ok(())
    }
    fn write(&mut self, value: T) -> Result<(), Error>;
    /// Write a value with its event-time and window context. Existing sinks
    /// can continue implementing `write`; metadata-aware sinks override this.
    fn write_with_metadata(&mut self, value: T, _metadata: ElementMetadata) -> Result<(), Error> {
        self.write(value)
    }
    fn commit(&mut self) -> Result<(), Error>;
    fn abort(&mut self) -> Result<(), Error>;
    fn requires_bundle_finalization(&self) -> bool {
        false
    }
    fn finalize_bundle(&mut self) -> Result<(), Error> {
        Err(Error::unsupported("sink", "runner", "bundle-finalization"))
    }
    fn recover_bundle_finalization(&mut self, _instruction_id: &str) -> Result<(), Error> {
        Err(Error::unsupported(
            "sink",
            "runner",
            "bundle-finalization-recovery",
        ))
    }
}
type SinkFactory = Arc<dyn Fn() -> Result<Box<dyn SinkSession>, Error> + Send + Sync>;
#[derive(Clone)]
struct SinkRegistration {
    factory: SinkFactory,
    config_digest: Option<String>,
    bundle_finalization: bool,
}
struct SinkAdapter<T: Element> {
    key: String,
    inner: Box<dyn TypedSink<T>>,
}
impl<T: Element> SinkSession for SinkAdapter<T> {
    fn begin_bundle(&mut self, instruction_id: &str) -> Result<(), Error> {
        self.inner.begin_bundle(instruction_id)
    }
    fn write(&mut self, event: Envelope) -> Result<(), Error> {
        let value = event
            .value
            .downcast::<T>()
            .map_err(|_| Error::execution(&self.key, "direct", "sink input type mismatch"))?;
        self.inner.write_with_metadata(
            *value,
            ElementMetadata {
                timestamp_ns: event.timestamp_ns,
                window: event.window,
                assigned_window: event.assigned_window,
                pane: event.pane,
            },
        )
    }
    fn commit(&mut self) -> Result<(), Error> {
        self.inner.commit()
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.inner.abort()
    }
    fn requires_bundle_finalization(&self) -> bool {
        self.inner.requires_bundle_finalization()
    }
    fn finalize_bundle(&mut self) -> Result<(), Error> {
        self.inner.finalize_bundle()
    }
    fn recover_bundle_finalization(&mut self, instruction_id: &str) -> Result<(), Error> {
        self.inner.recover_bundle_finalization(instruction_id)
    }
}

#[derive(Clone)]
struct SourceRegistration {
    factory: SourceFactory,
    unbounded_factory: Option<UnboundedSourceFactory>,
    typed_factory: Option<Arc<dyn Any + Send + Sync>>,
    vec_batch_factory: Option<Arc<dyn Any + Send + Sync>>,
    erased_vec_batch_factory: Option<(TypeId, ErasedVecBatchFactory)>,
    vec_envelope_batch_factory: Option<VecEnvelopeBatchFactory>,
    len_hint: Option<usize>,
    config_digest: Option<SourceConfigDigest>,
    test_stream: Option<Vec<EncodedTestStreamEvent>>,
}

struct TypedUnboundedAdapter<T> {
    inner: TypedUnboundedSourceReader<T>,
}

struct DirectTestStream<T> {
    key: String,
    events: Arc<Vec<EncodedTestStreamEvent>>,
    index: usize,
    offset: usize,
    watermark: Option<i64>,
    processing_time_ns: i64,
    finished: bool,
    marker: PhantomData<T>,
}

impl<T: Element> DirectTestStream<T> {
    fn open(
        key: String,
        events: Arc<Vec<EncodedTestStreamEvent>>,
        position: Option<Vec<u8>>,
    ) -> Result<Self, Error> {
        let (index, offset) = if let Some(position) = position {
            let bytes: [u8; 16] = position
                .try_into()
                .map_err(|_| Error::recovery(&key, "invalid test stream cursor"))?;
            let index = usize::try_from(u64::from_le_bytes(bytes[..8].try_into().unwrap()))
                .map_err(|_| Error::recovery(&key, "invalid test stream event position"))?;
            let offset = usize::try_from(u64::from_le_bytes(bytes[8..].try_into().unwrap()))
                .map_err(|_| Error::recovery(&key, "invalid test stream element position"))?;
            (index, offset)
        } else {
            (0, 0)
        };
        if index > events.len()
            || (index == events.len() && offset != 0)
            || (index < events.len()
                && match &events[index] {
                    EncodedTestStreamEvent::Elements(values) => offset > values.len(),
                    _ => offset != 0,
                })
        {
            return Err(Error::recovery(&key, "test stream cursor is out of range"));
        }
        let watermark = events[..index]
            .iter()
            .filter_map(|event| {
                if let EncodedTestStreamEvent::AdvanceWatermark(value) = event {
                    Some(*value)
                } else {
                    None
                }
            })
            .next_back();
        let processing_time_ns = events[..index]
            .iter()
            .filter_map(|event| {
                if let EncodedTestStreamEvent::AdvanceProcessingTime(duration) = event {
                    Some(*duration)
                } else {
                    None
                }
            })
            .try_fold(0_i64, i64::checked_add)
            .ok_or_else(|| Error::recovery(&key, "test stream processing time overflow"))?;
        Ok(Self {
            key,
            events,
            index,
            offset,
            watermark,
            processing_time_ns,
            finished: false,
            marker: PhantomData,
        })
    }

    fn progress(&self, completed: bool) -> ProgressSignal {
        let mut position = Vec::with_capacity(16);
        position.extend_from_slice(&u64::try_from(self.index).unwrap_or(u64::MAX).to_le_bytes());
        position.extend_from_slice(&u64::try_from(self.offset).unwrap_or(u64::MAX).to_le_bytes());
        ProgressSignal {
            source_key: self.key.clone(),
            split_id: "test-stream".into(),
            checkpoint_position: Some(position),
            event_time_ns: self.watermark,
            completed,
        }
    }
}

impl<T: Element> UnboundedSource<Timestamped<T>> for DirectTestStream<T> {
    fn poll(&mut self, max_elements: usize) -> Result<UnboundedPoll<Timestamped<T>>, Error> {
        if self.finished {
            return Ok(UnboundedPoll::Pending);
        }
        let Some(event) = self.events.get(self.index) else {
            self.finished = true;
            return Ok(UnboundedPoll::Ready(UnboundedBatch {
                elements: Vec::new(),
                progress: self.progress(true),
            }));
        };
        let elements = match event {
            EncodedTestStreamEvent::Elements(values) => {
                let end = self.offset.saturating_add(max_elements).min(values.len());
                let elements = values[self.offset..end]
                    .iter()
                    .map(|(bytes, timestamp_ns)| {
                        T::decode(bytes).map(|value| Timestamped {
                            value,
                            timestamp_ns: *timestamp_ns,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                self.offset = end;
                if self.offset == values.len() {
                    self.index += 1;
                    self.offset = 0;
                }
                elements
            }
            EncodedTestStreamEvent::AdvanceWatermark(next) => {
                self.watermark = Some(*next);
                self.index += 1;
                Vec::new()
            }
            EncodedTestStreamEvent::AdvanceProcessingTime(duration) => {
                self.processing_time_ns = self
                    .processing_time_ns
                    .checked_add(*duration)
                    .ok_or_else(|| Error::recovery(&self.key, "test stream processing time overflow"))?;
                self.index += 1;
                Vec::new()
            }
        };
        Ok(UnboundedPoll::Ready(UnboundedBatch {
            elements,
            progress: self.progress(false),
        }))
    }

    fn processing_time_ns(&self) -> Option<i64> {
        Some(self.processing_time_ns)
    }
}

impl<T: Element> UnboundedSource<Envelope> for TypedUnboundedAdapter<T> {
    fn poll(&mut self, max_elements: usize) -> Result<UnboundedPoll<Envelope>, Error> {
        match self.inner.poll(max_elements)? {
            UnboundedPoll::Pending => Ok(UnboundedPoll::Pending),
            UnboundedPoll::Ready(batch) => Ok(UnboundedPoll::Ready(UnboundedBatch {
                elements: batch
                    .elements
                    .into_iter()
                    .map(|item| Envelope {
                        pane: PaneInfo::NO_FIRING,
                        value: Box::new(item.value),
                        timestamp_ns: item.timestamp_ns,
                        window: Window::Global,
                        assigned_window: None,
                    })
                    .collect(),
                progress: batch.progress,
            })),
        }
    }

    fn processing_time_ns(&self) -> Option<i64> {
        self.inner.processing_time_ns()
    }
}

struct ValidatingUnboundedSource {
    inner: UnboundedSourceReader,
    key: String,
    split_id: Option<String>,
    last_watermark: Option<i64>,
    completed: bool,
}

impl UnboundedSource<Envelope> for ValidatingUnboundedSource {
    fn poll(&mut self, max_elements: usize) -> Result<UnboundedPoll<Envelope>, Error> {
        if max_elements == 0 || self.completed {
            return Err(Error::execution(
                &self.key,
                "source",
                "invalid poll after completion or with zero capacity",
            ));
        }
        let result = self.inner.poll(max_elements)?;
        if let UnboundedPoll::Ready(batch) = &result {
            let signal = &batch.progress;
            if batch.elements.len() > max_elements {
                return Err(Error::execution(
                    &self.key,
                    "source",
                    "source exceeded poll capacity",
                ));
            }
            if signal.source_key != self.key || signal.split_id.is_empty() {
                return Err(Error::execution(
                    &self.key,
                    "source",
                    "source progress identity differs",
                ));
            }
            if self
                .split_id
                .as_ref()
                .is_some_and(|id| id != &signal.split_id)
            {
                return Err(Error::execution(
                    &self.key,
                    "source",
                    "source split changed during a session",
                ));
            }
            if !batch.elements.is_empty() && signal.checkpoint_position.is_none() {
                return Err(Error::execution(
                    &self.key,
                    "source",
                    "data batch has no checkpoint position",
                ));
            }
            if batch.elements.is_empty()
                && signal.checkpoint_position.is_none()
                && signal.event_time_ns.is_none()
                && !signal.completed
            {
                return Err(Error::execution(
                    &self.key,
                    "source",
                    "empty poll made no progress",
                ));
            }
            if signal
                .checkpoint_position
                .as_ref()
                .is_some_and(|position| position.len() > 1024 * 1024)
            {
                return Err(Error::execution(
                    &self.key,
                    "source",
                    "source checkpoint position exceeds 1 MiB",
                ));
            }
            if let Some(watermark) = signal.event_time_ns {
                if self.last_watermark.is_some_and(|last| watermark < last) {
                    return Err(Error::execution(
                        &self.key,
                        "source",
                        "source watermark regressed",
                    ));
                }
                self.last_watermark = Some(watermark);
            }
            self.split_id.get_or_insert_with(|| signal.split_id.clone());
            self.completed = signal.completed;
        }
        Ok(result)
    }

    fn processing_time_ns(&self) -> Option<i64> {
        self.inner.processing_time_ns()
    }
}

#[derive(Clone)]
enum SourceConfigDigest {
    Ready(String),
    Deferred(Arc<DeferredSourceDigest>),
}

struct DeferredSourceDigest {
    value: OnceLock<String>,
    compute: Box<dyn Fn() -> String + Send + Sync>,
}

impl SourceConfigDigest {
    fn as_str(&self) -> &str {
        match self {
            Self::Ready(value) => value,
            Self::Deferred(deferred) => deferred.value.get_or_init(|| (deferred.compute)()),
        }
    }
}

fn infallible_builtin_coder<T: Element>() -> bool {
    let type_id = TypeId::of::<T>();
    [
        TypeId::of::<i32>(),
        TypeId::of::<i64>(),
        TypeId::of::<u32>(),
        TypeId::of::<u64>(),
        TypeId::of::<f32>(),
        TypeId::of::<f64>(),
        TypeId::of::<bool>(),
        TypeId::of::<String>(),
        TypeId::of::<(u32, u32)>(),
    ]
    .contains(&type_id)
}

fn vector_source_digest<T: Element>(values: &[T]) -> Result<String, Error> {
    let mut hash = Sha256::new();
    hash.update(T::coder_identity().as_bytes());
    hash.update(T::CODER_VERSION.major.to_le_bytes());
    hash.update(T::CODER_VERSION.minor.to_le_bytes());
    let mut encoded = Vec::new();
    for value in values {
        value.encode_reusing(&mut encoded)?;
        // Supported Rust targets have at most 64-bit `usize`.
        #[allow(clippy::cast_possible_truncation)]
        let length = encoded.len() as u64;
        hash.update(length.to_le_bytes());
        hash.update(&encoded);
    }
    let digest = hash.finalize();
    let mut result = String::from("sha256-v1:");
    for byte in digest {
        write!(result, "{byte:02x}").map_err(|e| Error::incompatible(e.to_string()))?;
    }
    Ok(result)
}

#[derive(Clone, Default)]
pub struct Registry {
    sources: BTreeMap<String, SourceRegistration>,
    transforms: BTreeMap<String, Transform>,
    combining_mergers: BTreeMap<String, CodedAccumulatorMerger>,
    typed_transforms: BTreeMap<String, TypedTransformRegistration>,
    keyed_timestamp_reifications: BTreeSet<String>,
    aggregates: BTreeMap<String, Aggregate>,
    typed_joins: BTreeMap<String, Arc<dyn Any + Send + Sync>>,
    folds: BTreeMap<String, FoldFactory>,
    sinks: BTreeMap<String, SinkRegistration>,
    coders: BTreeMap<String, CoderRegistration>,
    group_coders: BTreeMap<String, GroupCoderRegistration>,
    cogroup_coders: BTreeMap<String, CoGroupCoderRegistration>,
    join_coders: BTreeMap<String, JoinCoderRegistration>,
    side_inputs: BTreeMap<String, SideInputRegistration>,
}
type CodedAccumulatorMerger =
    Arc<dyn Fn(&[Vec<u8>]) -> Result<Vec<u8>, Error> + Send + Sync>;
#[derive(Clone, Copy)]
struct CoderRegistration {
    encode: fn(&(dyn Any + Send)) -> Result<Vec<u8>, Error>,
    decode: CoderDecoder,
}
type CoderDecoder = fn(&[u8]) -> Result<Box<dyn Any + Send>, Error>;
#[derive(Clone, Copy)]
struct GroupCoderRegistration {
    split_input: GroupInputSplitter,
    assemble_input: GroupInputAssembler,
    assemble_output: GroupOutputAssembler,
}
type GroupInputSplitter = fn(&[u8]) -> Result<(Vec<u8>, Vec<u8>), Error>;
type GroupInputAssembler = fn(&[u8], &[u8]) -> Result<Vec<u8>, Error>;
type GroupOutputAssembler = fn(&[u8], &[Vec<u8>]) -> Result<Vec<u8>, Error>;
#[derive(Clone)]
struct CoGroupCoderRegistration {
    split_inputs: Vec<GroupInputSplitter>,
    assemble_output: GroupOutputAssembler,
}
#[derive(Clone, Copy)]
struct JoinCoderRegistration {
    split_inputs: [GroupInputSplitter; 2],
    assemble_inputs: [JoinInputAssembler; 2],
}
type JoinInputAssembler = fn(&[u8], &[u8]) -> Result<Payload, Error>;
#[derive(Clone)]
struct SideInputRegistration {
    materialize: SideMaterializer,
    apply: SideMapper,
}
type SideMaterializer = Arc<dyn Fn(Vec<Payload>) -> Result<Payload, Error> + Send + Sync>;
type SideMapper = Arc<dyn Fn(Envelope, &dyn Any) -> Result<Envelope, Error> + Send + Sync>;
impl GroupCoderRegistration {
    fn for_global<T: Element>() -> Self {
        Self {
            split_input: |bytes| {
                T::decode(bytes)?;
                Ok((Vec::new(), bytes.to_vec()))
            },
            assemble_input: |key, value| {
                if !key.is_empty() {
                    return Err(Error::incompatible("global fold has a nonempty runner key"));
                }
                T::decode(value)?;
                Ok(value.to_vec())
            },
            assemble_output: |_, _| {
                Err(Error::incompatible(
                    "global fold has no grouped output coder",
                ))
            },
        }
    }
    fn for_elements<K: Element, V: Element>() -> Self {
        Self {
            split_input: |bytes| {
                let (key, value) = <(K, V)>::decode(bytes)?;
                Ok((key.encode()?, value.encode()?))
            },
            assemble_input: |key, value| (K::decode(key)?, V::decode(value)?).encode(),
            assemble_output: |key, values| {
                let key = K::decode(key)?;
                let values = values
                    .iter()
                    .map(|value| V::decode(value))
                    .collect::<Result<Vec<_>, _>>()?;
                (key, values).encode()
            },
        }
    }
}
impl CoderRegistration {
    fn for_element<T: Element>() -> Self {
        Self {
            encode: |value| {
                value
                    .downcast_ref::<T>()
                    .ok_or_else(|| {
                        Error::incompatible("element type differs from registered coder")
                    })?
                    .encode()
            },
            decode: |bytes| T::decode(bytes).map(|value| Box::new(value) as Box<dyn Any + Send>),
        }
    }
}
