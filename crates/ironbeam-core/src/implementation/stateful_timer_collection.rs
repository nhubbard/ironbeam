/// Register a keyed value-state callback with declared event-time or
/// processing-time timer families. The callback may emit zero or more values
/// and set or clear tagged timers for its current key and window.
impl<K, V> PCollection<(K, V)>
where
    K: Element + Eq + Hash + Clone,
    V: Element,
{
    pub fn stateful_timer_map_named<S: Element, O: Element>(
        &self,
        key: &str,
        semantic_id: &str,
        version: Version,
        families: BTreeMap<String, UserTimerDomain>,
        on_element: impl Fn(&K, V, Option<S>, ElementMetadata) -> Result<TimedStateResult<S, O>, Error>
            + Send + Sync + 'static,
        on_timer: impl Fn(&K, Option<S>, UserTimerEvent) -> Result<TimedStateResult<S, O>, Error>
            + Send + Sync + 'static,
    ) -> Result<PCollection<(K, O)>, Error> {
        self.register_stateful_timer_map(
            key,
            semantic_id,
            version,
            families,
            None,
            move |key, value, state, metadata, _| on_element(key, value, state, metadata),
            on_timer,
        )
    }

    /// Like `stateful_timer_map_named`, but also supply the runner's current
    /// processing clock to each element callback. `None` means that the
    /// execution path has no processing clock. The clock is in nanoseconds;
    /// processing-time timer deadlines must use this clock, not event time.
    pub fn stateful_timer_map_with_clock_named<S: Element, O: Element>(
        &self,
        key: &str,
        semantic_id: &str,
        version: Version,
        families: BTreeMap<String, UserTimerDomain>,
        on_element: impl Fn(&K, V, Option<S>, ElementMetadata, Option<i64>) -> Result<TimedStateResult<S, O>, Error>
            + Send + Sync + 'static,
        on_timer: impl Fn(&K, Option<S>, UserTimerEvent) -> Result<TimedStateResult<S, O>, Error>
            + Send + Sync + 'static,
    ) -> Result<PCollection<(K, O)>, Error> {
        self.register_stateful_timer_map(
            key,
            semantic_id,
            version,
            families,
            None,
            on_element,
            on_timer,
        )
    }

    /// Run the timer callback once for each key when its global or fixed window expires.
    /// `expiration_family_id` must name a declared event-time family. The
    /// callback receives that family ID and a terminal event-time timestamp.
    /// Global expiration runs only when a finite source completes.
    /// Pinned local Prism can omit a callback when one key spans two windows
    /// in one source batch; see the Beam semantics guide for the tested scope.
    #[allow(clippy::too_many_arguments)]
    pub fn stateful_timer_map_with_window_expiration_named<S: Element, O: Element>(
        &self,
        key: &str,
        semantic_id: &str,
        version: Version,
        families: BTreeMap<String, UserTimerDomain>,
        expiration_family_id: &str,
        on_element: impl Fn(&K, V, Option<S>, ElementMetadata) -> Result<TimedStateResult<S, O>, Error>
            + Send + Sync + 'static,
        on_timer: impl Fn(&K, Option<S>, UserTimerEvent) -> Result<TimedStateResult<S, O>, Error>
            + Send + Sync + 'static,
    ) -> Result<PCollection<(K, O)>, Error> {
        self.register_stateful_timer_map(
            key,
            semantic_id,
            version,
            families,
            Some(expiration_family_id.to_owned()),
            move |key, value, state, metadata, _| on_element(key, value, state, metadata),
            on_timer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn register_stateful_timer_map<S: Element, O: Element>(
        &self,
        key: &str,
        semantic_id: &str,
        version: Version,
        families: BTreeMap<String, UserTimerDomain>,
        expiration_family_id: Option<String>,
        on_element: impl Fn(&K, V, Option<S>, ElementMetadata, Option<i64>) -> Result<TimedStateResult<S, O>, Error>
            + Send + Sync + 'static,
        on_timer: impl Fn(&K, Option<S>, UserTimerEvent) -> Result<TimedStateResult<S, O>, Error>
            + Send + Sync + 'static,
    ) -> Result<PCollection<(K, O)>, Error> {
        Self::validate_semantic_handler(key, semantic_id, version)?;
        let parent = self.parent()?;
        if parent.boundedness != Boundedness::Unbounded
            || matches!(parent.time.window, Window::Session { .. })
            || parent.time.trigger != WindowTrigger::Default
            || parent.time.allowed_lateness_ns != 0
        {
            return Err(Error::graph(key, "timer state requires unbounded input and default global, fixed, or sliding windows"));
        }
        if expiration_family_id.is_some() {
            let aligned = match &parent.time.window {
                Window::Fixed { size_ns } => *size_ns >= 1_000_000 && size_ns % 1_000_000 == 0,
                Window::FixedOffset { size_ns, offset_ns } => {
                    *size_ns >= 1_000_000
                        && size_ns % 1_000_000 == 0
                        && offset_ns % 1_000_000 == 0
                }
                Window::Global => true,
                Window::Sliding { .. } | Window::Session { .. } => false,
            };
            if !aligned {
                return Err(Error::graph(key, "window expiration requires global or millisecond-aligned fixed windows"));
            }
        }
        if families.is_empty() || !K::supports_coder() || !V::supports_coder()
            || !S::supports_coder() || !O::supports_coder()
            || !<(K, O)>::supports_coder()
        {
            return Err(Error::graph(key, "timer state requires timer families and registered element coders"));
        }
        let mut timers = BTreeMap::new();
        for (family_id, domain) in families {
            let mut hash = Sha256::new();
            hash.update(key.as_bytes());
            hash.update([0]);
            hash.update(family_id.as_bytes());
            let digest = hash.finalize();
            let coder_id = digest[..16].iter().fold(String::from("v5_timer_"), |mut id, byte| {
                use std::fmt::Write;
                write!(id, "{byte:02x}").expect("write to String");
                id
            });
            timers.insert(family_id, UserTimerFamily { domain, coder_id });
        }
        let contract = StatefulParDoContract {
            semantic_id: semantic_id.into(),
            version,
            key_coder_id: "v5_lp_bytes".into(),
            window_coder_id: if parent.time.window == Window::Global { "c_gw" } else { "c_iw" }.into(),
            states: BTreeMap::from([("value".into(), UserStateKind::Value { coder_id: "v5_lp_bytes".into() })]),
            timers,
            on_window_expiration_timer: expiration_family_id,
        };
        contract.validate()?;
        let on_element: TimedElementCallback = Arc::new(move |payload, prior, metadata, processing_time_ns| {
            let pair = payload.downcast::<(K, V)>()
                .map_err(|_| Error::incompatible("timed state input type differs from registration"))?;
            let state = prior.map(S::decode).transpose()?;
            let result = on_element(&pair.0, pair.1, state, metadata, processing_time_ns)?;
            Ok(TimedCallbackOutput {
                state: result.state.map(|state| state.encode()).transpose()?,
                outputs: result.outputs.into_iter()
                    .map(|output| Box::new((pair.0.clone(), output)) as Payload)
                    .collect(),
                timers: result.timers,
            })
        });
        let on_timer: TimedFiringCallback = Arc::new(move |firing, prior| {
            let key = decode_timer_key::<K>(&firing.identity.encoded_key)?;
            let state = prior.map(S::decode).transpose()?;
            let result = on_timer(&key, state, UserTimerEvent {
                family_id: firing.identity.family_id.clone(),
                dynamic_tag: firing.identity.dynamic_tag.clone(),
                domain: firing.domain,
                fire_timestamp_ns: firing.schedule.fire_timestamp_ns,
                output_timestamp_ns: firing.schedule.hold_timestamp_ns,
                window: firing.identity.window,
                pane: firing.schedule.pane,
            })?;
            Ok(TimedCallbackOutput {
                state: result.state.map(|state| state.encode()).transpose()?,
                outputs: result.outputs.into_iter()
                    .map(|output| Box::new((key.clone(), output)) as Payload)
                    .collect(),
                timers: result.timers,
            })
        });
        let transform = StatefulTimerTransform {
            key_bytes: |payload| {
                let pair = payload.downcast_ref::<(K, V)>()
                    .ok_or_else(|| Error::incompatible("timed state key type differs from registration"))?;
                encode_timer_key(&pair.0)
            },
            validate_key: |bytes| decode_timer_key::<K>(bytes).map(|_| ()),
            validate_state: |bytes| S::decode(bytes).map(|_| ()),
            window: parent.time.window.clone(),
            allowed_lateness_ns: parent.time.allowed_lateness_ns,
            contract: contract.clone(),
            on_element,
            on_timer,
            cells: Mutex::new(HashMap::new()),
            timers: Mutex::new(TimerQueue::new(contract.clone(), parent.time.window == Window::Global)?),
        };
        let result = self.next::<(K, O)>(
            key,
            Operation::StatefulParDo {
                contract: Box::new(contract),
                state_element: ElementDescriptor {
                    type_id: S::type_identity(),
                    coder_id: S::coder_identity(),
                    coder_payload: S::coder_payload(),
                    coder_version: S::CODER_VERSION,
                },
            },
            Some(Transform::StatefulTimer(Box::new(transform))),
            None,
        )?;
        self.pipeline.inner.lock()
            .map_err(|_| Error::graph(key, "builder lock poisoned"))?
            .registry.group_coders.insert(key.into(), GroupCoderRegistration::for_elements::<K, V>());
        Ok(result)
    }
}
