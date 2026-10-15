impl Registry {
    #[must_use]
    pub fn has_side_input(&self, key: &str) -> bool {
        self.side_inputs.contains_key(key)
    }

    /// Materialize a decoded bounded side input once per execution scope.
    pub fn materialize_side_input(
        &self,
        key: &str,
        values: Vec<Payload>,
    ) -> Result<Payload, Error> {
        let side = self.side_inputs.get(key).ok_or_else(|| {
            Error::incompatible(format!("no side input registered for node {key}"))
        })?;
        (side.materialize)(values)
    }

    /// Apply a main element using a view made by `materialize_side_input`.
    pub fn apply_side_input(
        &self,
        key: &str,
        event: Envelope,
        view: &dyn Any,
    ) -> Result<Envelope, Error> {
        let side = self.side_inputs.get(key).ok_or_else(|| {
            Error::incompatible(format!("no side input registered for node {key}"))
        })?;
        (side.apply)(event, view)
    }

    #[must_use]
    pub fn has_join_coder(&self, key: &str) -> bool {
        self.join_coders.contains_key(key)
    }

    /// Preserve each input event timestamp inside the tagged shuffle value.
    pub fn split_join_input(
        &self,
        key: &str,
        input_index: usize,
        bytes: &[u8],
        timestamp_ns: i64,
    ) -> Result<(Vec<u8>, Vec<u8>), Error> {
        let coder = self.join_coders.get(key).ok_or_else(|| {
            Error::incompatible(format!("no join coder registered for node {key}"))
        })?;
        let splitter = coder.split_inputs.get(input_index).ok_or_else(|| {
            Error::incompatible(format!("join input index {input_index} is out of range"))
        })?;
        let (encoded_key, encoded_value) = splitter(bytes)?;
        let mut tagged = Vec::with_capacity(9 + encoded_value.len());
        tagged.push(
            u8::try_from(input_index)
                .map_err(|_| Error::incompatible("join input tag exceeds one byte"))?,
        );
        tagged.extend_from_slice(&timestamp_ns.to_be_bytes());
        tagged.extend_from_slice(&encoded_value);
        Ok((encoded_key, tagged))
    }

    /// Reuse the Direct join aggregate for each Beam grouped key.
    pub fn execute_join_group(
        &self,
        key: &str,
        encoded_key: &[u8],
        tagged_values: &[Vec<u8>],
        window: &Window,
        assigned_window: Option<WindowInterval>,
    ) -> Result<Vec<(Vec<u8>, i64)>, Error> {
        let coder = self.join_coders.get(key).ok_or_else(|| {
            Error::incompatible(format!("no join coder registered for node {key}"))
        })?;
        let mut inputs = [Vec::new(), Vec::new()];
        for tagged in tagged_values {
            let (&tag, payload) = tagged
                .split_first()
                .ok_or_else(|| Error::incompatible("join tagged value is empty"))?;
            let assembler = coder
                .assemble_inputs
                .get(usize::from(tag))
                .ok_or_else(|| Error::incompatible("join input tag is out of range"))?;
            let timestamp_bytes = payload
                .get(..8)
                .ok_or_else(|| Error::incompatible("join tagged value lacks timestamp"))?;
            let mut timestamp_array = [0_u8; 8];
            timestamp_array.copy_from_slice(timestamp_bytes);
            let timestamp_ns = i64::from_be_bytes(timestamp_array);
            inputs[usize::from(tag)].push(Envelope {
                pane: PaneInfo::NO_FIRING,
                value: assembler(encoded_key, &payload[8..])?,
                timestamp_ns,
                window: window.clone(),
                assigned_window,
            });
        }
        self.apply_aggregate(key, inputs.into_iter().collect())?
            .into_iter()
            .map(|event| {
                Ok((
                    self.encode_element(key, event.value.as_ref())?,
                    event.timestamp_ns,
                ))
            })
            .collect()
    }

    /// Whether a multi-input co-group has typed tagged-union coders.
    #[must_use]
    pub fn has_cogroup_coder(&self, key: &str) -> bool {
        self.cogroup_coders.contains_key(key)
    }

    /// Encode an input pair as a common key and a tagged value for Beam GBK.
    pub fn split_cogroup_input(
        &self,
        key: &str,
        input_index: usize,
        bytes: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>), Error> {
        let coder = self.cogroup_coders.get(key).ok_or_else(|| {
            Error::incompatible(format!("no co-group coder registered for node {key}"))
        })?;
        let splitter = coder.split_inputs.get(input_index).ok_or_else(|| {
            Error::incompatible(format!(
                "co-group input index {input_index} is out of range"
            ))
        })?;
        splitter(bytes)
    }

    /// Reassemble a Beam GBK of tagged values into the typed co-group result.
    pub fn assemble_cogroup_output(
        &self,
        key: &str,
        encoded_key: &[u8],
        encoded_values: &[Vec<u8>],
    ) -> Result<Vec<u8>, Error> {
        let coder = self.cogroup_coders.get(key).ok_or_else(|| {
            Error::incompatible(format!("no co-group coder registered for node {key}"))
        })?;
        (coder.assemble_output)(encoded_key, encoded_values)
    }

    /// Whether this node has a registered typed key/value split coder for a
    /// runner grouping boundary. Global folds use the empty key.
    #[must_use]
    pub fn has_group_coder(&self, key: &str) -> bool {
        self.group_coders.contains_key(key)
    }
    /// Split a registered grouping input into independently encoded key and value.
    pub fn split_group_input(&self, key: &str, bytes: &[u8]) -> Result<(Vec<u8>, Vec<u8>), Error> {
        let coder = self.group_coders.get(key).ok_or_else(|| {
            Error::incompatible(format!("no group coder registered for node {key}"))
        })?;
        (coder.split_input)(bytes)
    }
    /// Reassemble a runner KV record into the registered input tuple coder.
    pub fn assemble_group_input(
        &self,
        key: &str,
        encoded_key: &[u8],
        encoded_value: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let coder = self.group_coders.get(key).ok_or_else(|| {
            Error::incompatible(format!("no group coder registered for node {key}"))
        })?;
        (coder.assemble_input)(encoded_key, encoded_value)
    }
    /// Reassemble runner grouped key/value bytes into the v5 output element coder.
    pub fn assemble_group_output(
        &self,
        key: &str,
        encoded_key: &[u8],
        encoded_values: &[Vec<u8>],
    ) -> Result<Vec<u8>, Error> {
        let coder = self.group_coders.get(key).ok_or_else(|| {
            Error::incompatible(format!("no group coder registered for node {key}"))
        })?;
        (coder.assemble_output)(encoded_key, encoded_values)
    }
    #[must_use]
    pub fn has_coder(&self, key: &str) -> bool {
        self.coders.contains_key(key)
    }
    pub fn encode_element(&self, key: &str, value: &(dyn Any + Send)) -> Result<Vec<u8>, Error> {
        let coder = self
            .coders
            .get(key)
            .ok_or_else(|| Error::incompatible(format!("no coder registered for node {key}")))?;
        (coder.encode)(value)
    }
    pub fn decode_element(&self, key: &str, bytes: &[u8]) -> Result<Box<dyn Any + Send>, Error> {
        let coder = self
            .coders
            .get(key)
            .ok_or_else(|| Error::incompatible(format!("no coder registered for node {key}")))?;
        (coder.decode)(bytes)
    }
    pub fn open_fold(&self, key: &str) -> Result<Box<dyn FoldSession>, Error> {
        self.folds
            .get(key)
            .map(|factory| factory())
            .ok_or_else(|| Error::execution(key, "direct", "fold factory missing"))
    }
    #[must_use]
    pub fn has_fold(&self, key: &str) -> bool {
        self.folds.contains_key(key)
    }
    pub fn open_source(&self, key: &str) -> Result<SourceIterator, Error> {
        self.sources
            .get(key)
            .ok_or_else(|| Error::execution(key, "direct", "source factory missing"))
            .and_then(|source| (source.factory)())
    }
    #[must_use]
    pub fn has_source(&self, key: &str) -> bool {
        self.sources.contains_key(key)
    }
    /// Resume a registered unbounded split from its last committed progress.
    /// The returned reader validates source identity, poll limits, and watermark
    /// order before a runner can consume its events.
    #[allow(clippy::needless_pass_by_value)] // Keep the public resume contract owned.
    pub fn open_unbounded_source(
        &self,
        key: &str,
        resume: Option<ProgressSignal>,
    ) -> Result<UnboundedSourceReader, Error> {
        if let Some(signal) = &resume
            && (signal.source_key != key
                || signal.split_id.is_empty()
                || signal.completed
                || signal.checkpoint_position.is_none()
                || signal
                    .checkpoint_position
                    .as_ref()
                    .is_some_and(|position| position.len() > 1024 * 1024))
        {
            return Err(Error::execution(
                key,
                "source",
                "invalid unbounded resume progress",
            ));
        }
        let factory = self
            .sources
            .get(key)
            .and_then(|source| source.unbounded_factory.as_ref())
            .ok_or_else(|| Error::execution(key, "source", "unbounded source factory missing"))?;
        let inner = factory(
            resume
                .as_ref()
                .and_then(|signal| signal.checkpoint_position.clone()),
        )?;
        Ok(Box::new(ValidatingUnboundedSource {
            inner,
            key: key.to_owned(),
            split_id: resume.as_ref().map(|signal| signal.split_id.clone()),
            last_watermark: resume.as_ref().and_then(|signal| signal.event_time_ns),
            completed: false,
        }))
    }
    #[must_use]
    pub fn has_unbounded_source(&self, key: &str) -> bool {
        self.sources
            .get(key)
            .is_some_and(|source| source.unbounded_factory.is_some())
    }
    /// Open a source directly as `T` when its registration retains a typed factory.
    ///
    /// `None` means that the backend should use the regular envelope source.
    pub fn open_typed_source<T: Element>(
        &self,
        key: &str,
    ) -> Result<Option<TypedSourceIterator<T>>, Error> {
        let source = self
            .sources
            .get(key)
            .ok_or_else(|| Error::execution(key, "direct", "source factory missing"))?;
        source
            .typed_factory
            .as_ref()
            .and_then(|factory| factory.downcast_ref::<TypedSourceFactory<T>>())
            .map(|factory| factory())
            .transpose()
    }
    /// Read bounded slices directly from an immutable in-memory vector source.
    /// Other source kinds retain their ordinary factory contract.
    #[must_use]
    pub fn vec_source_batches<T: Element>(
        &self,
        key: &str,
    ) -> Option<(usize, VecBatchReader<'_, T>)> {
        let source = self.sources.get(key)?;
        let batch = source
            .vec_batch_factory
            .as_ref()?
            .downcast_ref::<TypedVecBatchFactory<T>>()?;
        Some((source.len_hint?, batch.as_ref()))
    }
    /// Return type-erased vector batches for folds that accept the exact source type.
    #[must_use]
    pub fn erased_vec_source_batches(
        &self,
        key: &str,
    ) -> Option<(usize, TypeId, ErasedVecBatchReader<'_>)> {
        let source = self.sources.get(key)?;
        let (batch_type, batch) = source.erased_vec_batch_factory.as_ref()?;
        Some((source.len_hint?, *batch_type, batch.as_ref()))
    }
    /// Return envelopes for a bounded vector slice without cloning earlier rows.
    #[must_use]
    pub fn vec_source_envelope_batches(
        &self,
        key: &str,
    ) -> Option<(usize, VecEnvelopeBatchReader<'_>)> {
        let source = self.sources.get(key)?;
        Some((
            source.len_hint?,
            source.vec_envelope_batch_factory.as_ref()?.as_ref(),
        ))
    }
    #[must_use]
    pub fn source_len_hint(&self, key: &str) -> Option<usize> {
        self.sources.get(key).and_then(|source| source.len_hint)
    }
    #[must_use]
    pub fn source_config_digest(&self, key: &str) -> Option<&str> {
        self.sources.get(key).and_then(|source| {
            source
                .config_digest
                .as_ref()
                .map(SourceConfigDigest::as_str)
        })
    }
    /// Return a portable test stream script without opening a source.
    #[must_use]
    pub fn test_stream(&self, key: &str) -> Option<&[EncodedTestStreamEvent]> {
        self.sources.get(key)?.test_stream.as_deref()
    }
    pub fn apply(&self, key: &str, input: Envelope) -> Result<Option<Envelope>, Error> {
        match self
            .transforms
            .get(key)
            .ok_or_else(|| Error::execution(key, "direct", "transform missing"))?
        {
            Transform::Single(transform) => transform(input),
            Transform::Multi(transform) => {
                let mut values = transform(input)?;
                if values.len() > 1 {
                    return Err(Error::execution(
                        key,
                        "direct",
                        "operation emits multiple elements",
                    ));
                }
                Ok(values.pop())
            }
            Transform::StatefulValue(transform) => Ok(Some(transform.apply_direct(input)?)),
            Transform::StatefulTimer(transform) => {
                let mut values = transform.apply_element(input)?;
                if values.len() > 1 {
                    return Err(Error::execution(key, "direct", "operation emits multiple elements"));
                }
                Ok(values.pop())
            }
        }
    }
    pub fn apply_many(&self, key: &str, input: Envelope) -> Result<Vec<Envelope>, Error> {
        match self
            .transforms
            .get(key)
            .ok_or_else(|| Error::execution(key, "direct", "transform missing"))?
        {
            Transform::Single(transform) => Ok(transform(input)?.into_iter().collect()),
            Transform::Multi(transform) => transform(input),
            Transform::StatefulValue(transform) => Ok(vec![transform.apply_direct(input)?]),
            Transform::StatefulTimer(transform) => transform.apply_element(input),
        }
    }
    #[must_use]
    pub fn has_single_transform(&self, key: &str) -> bool {
        matches!(self.transforms.get(key), Some(Transform::Single(_)))
    }
    #[must_use]
    pub fn has_transform(&self, key: &str) -> bool {
        self.transforms.contains_key(key)
    }
    #[must_use]
    pub fn single_transform(
        &self,
        key: &str,
    ) -> Option<&(dyn Fn(Envelope) -> Result<Option<Envelope>, Error> + Send + Sync)> {
        match self.transforms.get(key)? {
            Transform::Single(transform) => Some(transform.as_ref()),
            Transform::Multi(_) | Transform::StatefulValue(_) | Transform::StatefulTimer(_) => None,
        }
    }
    /// Invoke a registered value-state callback with runner-owned cell bytes.
    /// The returned state replaces the cell; `None` clears it.
    pub fn apply_stateful_value(
        &self,
        key: &str,
        input: Envelope,
        current: Option<&[u8]>,
    ) -> Result<(Option<Vec<u8>>, Envelope), Error> {
        match self.transforms.get(key) {
            Some(Transform::StatefulValue(transform)) => transform.apply(input, current),
            _ => Err(Error::execution(key, "runner", "stateful value handler missing")),
        }
    }
    #[must_use]
    pub fn has_stateful_value(&self, key: &str) -> bool {
        matches!(self.transforms.get(key), Some(Transform::StatefulValue(_) | Transform::StatefulTimer(_)))
    }
    /// Fire due user timers after a source clock or watermark advances.
    pub fn drain_stateful_timers(
        &self,
        key: &str,
        event_watermark_ns: Option<i64>,
        processing_time_ns: Option<i64>,
    ) -> Result<Vec<Envelope>, Error> {
        match self.transforms.get(key) {
            Some(Transform::StatefulTimer(transform)) => {
                transform.take_ready(event_watermark_ns, processing_time_ns)
            }
            _ => Ok(Vec::new()),
        }
    }
    /// Earliest pending processing-time user timer on one stateful node.
    pub fn next_stateful_timer_deadline(&self, key: &str) -> Result<Option<i64>, Error> {
        match self.transforms.get(key) {
            Some(Transform::StatefulTimer(transform)) => transform.next_processing_deadline(),
            _ => Ok(None),
        }
    }
    /// Merge the coded accumulator elements returned by one Beam combining-state bag.
    pub fn merge_combining_accumulators(
        &self,
        key: &str,
        encoded: &[Vec<u8>],
    ) -> Result<Vec<u8>, Error> {
        let merge = self.combining_mergers.get(key)
            .ok_or_else(|| Error::incompatible("combining-state merge function is not registered"))?;
        merge(encoded)
    }
    /// Remove value cells whose window has expired at the source watermark.
    pub fn expire_stateful_value(&self, key: &str, watermark: Option<i64>) -> Result<(), Error> {
        match self.transforms.get(key) {
            Some(Transform::StatefulValue(transform)) => transform.expire(watermark),
            Some(Transform::StatefulTimer(transform)) => transform.expire(watermark),
            _ => Err(Error::recovery(key, "stateful value handler missing")),
        }
    }
    /// Encode one keyed value-state transform for a Direct stream checkpoint.
    pub fn checkpoint_stateful_value(&self, key: &str) -> Result<Vec<u8>, Error> {
        match self.transforms.get(key) {
            Some(Transform::StatefulValue(transform)) => transform.checkpoint(),
            Some(Transform::StatefulTimer(transform)) => transform.checkpoint(),
            _ => Err(Error::recovery(key, "stateful value handler missing")),
        }
    }
    /// Restore one keyed value-state transform before opening a stream source.
    pub fn restore_stateful_value(&self, key: &str, bytes: &[u8]) -> Result<(), Error> {
        match self.transforms.get(key) {
            Some(Transform::StatefulValue(transform)) => transform.restore(bytes),
            Some(Transform::StatefulTimer(transform)) => transform.restore(bytes),
            _ => Err(Error::recovery(key, "stateful value handler missing")),
        }
    }
    /// Return a typed same-type map or filter when a route can avoid envelopes.
    #[must_use]
    pub fn typed_value_transform<T: Element>(
        &self,
        key: &str,
    ) -> Option<TypedValueTransform<'_, T>> {
        match self.typed_transforms.get(key)? {
            TypedTransformRegistration::Map(map) => map
                .downcast_ref::<TypedMap<T, T>>()
                .map(|map| TypedValueTransform::Map(map.as_ref())),
            TypedTransformRegistration::Filter(predicate) => predicate
                .downcast_ref::<TypedFilter<T>>()
                .map(|predicate| TypedValueTransform::Filter(predicate.as_ref())),
            TypedTransformRegistration::Timestamp(_) => None,
        }
    }
    #[must_use]
    pub fn typed_timestamp_transform(&self, key: &str) -> Option<&(dyn Any + Send + Sync)> {
        match self.typed_transforms.get(key)? {
            TypedTransformRegistration::Timestamp(timestamp) => Some(timestamp.as_ref()),
            _ => None,
        }
    }
    #[must_use]
    pub fn is_keyed_timestamp_reification(&self, key: &str) -> bool {
        self.keyed_timestamp_reifications.contains(key)
    }
    pub fn apply_aggregate(
        &self,
        key: &str,
        inputs: Vec<Vec<Envelope>>,
    ) -> Result<Vec<Envelope>, Error> {
        self.aggregates
            .get(key)
            .ok_or_else(|| Error::execution(key, "direct", "aggregate missing"))?(inputs)
    }
    /// Return a typed in-memory inner join registered for two vector sources.
    #[must_use]
    pub fn typed_join<T: Element>(&self, key: &str) -> Option<&TypedJoin<T>> {
        self.typed_joins
            .get(key)?
            .downcast_ref::<TypedJoinFactory<T>>()
            .map(Arc::as_ref)
    }
    #[must_use]
    pub fn has_aggregate(&self, key: &str) -> bool {
        self.aggregates.contains_key(key)
    }
    #[must_use]
    pub fn has_sink(&self, key: &str) -> bool {
        self.sinks.contains_key(key)
    }
    #[must_use]
    pub fn sink_config_digest(&self, key: &str) -> Option<&str> {
        self.sinks
            .get(key)
            .and_then(|sink| sink.config_digest.as_deref())
    }
    #[must_use]
    pub fn sink_requires_bundle_finalization(&self, key: &str) -> bool {
        self.sinks
            .get(key)
            .is_some_and(|sink| sink.bundle_finalization)
    }
    pub fn open_sink(&self, key: &str) -> Result<Box<dyn SinkSession>, Error> {
        let sink = self
            .sinks
            .get(key)
            .ok_or_else(|| Error::execution(key, "direct", "sink factory missing"))?;
        (sink.factory)()
    }
}
