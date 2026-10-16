type TimedElementCallback = Arc<
    dyn Fn(Payload, Option<&[u8]>, ElementMetadata, Option<i64>) -> Result<TimedCallbackOutput, Error>
        + Send
        + Sync,
>;

fn encode_timer_key<K: Element>(key: &K) -> Result<Vec<u8>, Error> {
    let bytes = key.encode()?;
    let mut length = u64::try_from(bytes.len())
        .map_err(|_| Error::resource("timer-key", "encoded key length overflow"))?;
    let mut encoded = Vec::new();
    while length >= 0x80 {
        encoded.push(u8::try_from(length & 0x7f).expect("seven-bit value") | 0x80);
        length >>= 7;
    }
    encoded.push(u8::try_from(length).expect("final seven-bit value"));
    encoded.extend_from_slice(&bytes);
    Ok(encoded)
}

fn decode_timer_key<K: Element>(encoded: &[u8]) -> Result<K, Error> {
    let mut length = 0_u64;
    for (index, byte) in encoded.iter().copied().enumerate().take(10) {
        if index == 9 && byte > 1 {
            return Err(Error::incompatible("timer key length exceeds 64 bits"));
        }
        length |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            let prefix = index + 1;
            let length = usize::try_from(length)
                .map_err(|_| Error::incompatible("timer key length exceeds usize"))?;
            if encoded.len().checked_sub(prefix) != Some(length) {
                return Err(Error::incompatible("timer key coder length differs"));
            }
            return K::decode(&encoded[prefix..]);
        }
    }
    Err(Error::incompatible("timer key length prefix is incomplete"))
}
type TimedFiringCallback =
    Arc<dyn Fn(&TimerFiring, Option<&[u8]>) -> Result<TimedCallbackOutput, Error> + Send + Sync>;

struct TimedCallbackOutput {
    state: Option<Vec<u8>>,
    outputs: Vec<Payload>,
    timers: Vec<UserTimerRequest>,
}

/// The effects of one registered timed callback.
///
/// The runner applies the encoded state, output elements, and timer mutations
/// under one bundle instruction. Invocation does not mutate Direct's cells.
pub struct TimedCallbackChange {
    pub state: Option<Vec<u8>>,
    pub outputs: Vec<Envelope>,
    pub timers: Vec<TimerMutation>,
}

struct StatefulTimerTransform {
    key_bytes: fn(&(dyn Any + Send)) -> Result<Vec<u8>, Error>,
    validate_key: fn(&[u8]) -> Result<(), Error>,
    validate_state: fn(&[u8]) -> Result<(), Error>,
    window: Window,
    allowed_lateness_ns: i64,
    contract: StatefulParDoContract,
    on_element: TimedElementCallback,
    on_timer: TimedFiringCallback,
    cells: Mutex<StatefulValueCells>,
    timers: Mutex<TimerQueue>,
}

impl Clone for StatefulTimerTransform {
    fn clone(&self) -> Self {
        Self {
            key_bytes: self.key_bytes,
            validate_key: self.validate_key,
            validate_state: self.validate_state,
            window: self.window.clone(),
            allowed_lateness_ns: self.allowed_lateness_ns,
            contract: self.contract.clone(),
            on_element: Arc::clone(&self.on_element),
            on_timer: Arc::clone(&self.on_timer),
            cells: Mutex::new(HashMap::new()),
            timers: Mutex::new(
                TimerQueue::new(self.contract.clone(), self.window == Window::Global)
                    .expect("validated timed transform contract"),
            ),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct TimedStateSnapshot {
    version: u8,
    cells: Vec<(Option<WindowInterval>, Vec<u8>, Vec<u8>)>,
    timers: Vec<u8>,
}

impl StatefulTimerTransform {
    fn invoke_element(
        &self,
        input: Envelope,
        prior: Option<&[u8]>,
        processing_time_ns: Option<i64>,
    ) -> Result<TimedCallbackChange, Error> {
        if self.window != Window::Global && input.assigned_window.is_none() {
            return Err(Error::execution(
                &self.contract.semantic_id,
                "runner",
                "timed state has no assigned window",
            ));
        }
        let key = (self.key_bytes)(input.value.as_ref())?;
        let metadata = ElementMetadata {
            timestamp_ns: input.timestamp_ns,
            window: input.window.clone(),
            assigned_window: input.assigned_window,
            pane: input.pane,
        };
        let result = (self.on_element)(input.value, prior, metadata.clone(), processing_time_ns)?;
        if let Some(bytes) = &result.state {
            (self.validate_state)(bytes)?;
        }
        let timers = self.bind_requests(
            &key,
            metadata.assigned_window,
            metadata.pane,
            metadata.timestamp_ns,
            result.timers,
        )?;
        self.validate_mutations(&timers)?;
        Ok(TimedCallbackChange {
            state: result.state,
            outputs: result
                .outputs
                .into_iter()
                .map(|value| Envelope {
                    value,
                    timestamp_ns: metadata.timestamp_ns,
                    window: metadata.window.clone(),
                    assigned_window: metadata.assigned_window,
                    pane: metadata.pane,
                })
                .collect(),
            timers,
        })
    }

    fn invoke_timer(
        &self,
        firing: &TimerFiring,
        prior: Option<&[u8]>,
    ) -> Result<TimedCallbackChange, Error> {
        (self.validate_key)(&firing.identity.encoded_key)?;
        let family = self.contract.timers.get(&firing.identity.family_id).ok_or_else(|| {
            Error::execution(&self.contract.semantic_id, "runner", "undeclared timer family")
        })?;
        if family.domain != firing.domain {
            return Err(Error::execution(
                &self.contract.semantic_id,
                "runner",
                "timer domain differs from declaration",
            ));
        }
        TimerQueue::new(self.contract.clone(), self.window == Window::Global)?.set(
            firing.identity.clone(),
            firing.schedule,
        )?;
        let result = (self.on_timer)(firing, prior)?;
        if let Some(bytes) = &result.state {
            (self.validate_state)(bytes)?;
        }
        let timers = self.bind_requests(
            &firing.identity.encoded_key,
            firing.identity.window,
            firing.schedule.pane,
            firing.schedule.hold_timestamp_ns,
            result.timers,
        )?;
        self.validate_mutations(&timers)?;
        Ok(TimedCallbackChange {
            state: result.state,
            outputs: result
                .outputs
                .into_iter()
                .map(|value| Envelope {
                    value,
                    timestamp_ns: firing.schedule.hold_timestamp_ns,
                    window: self.window.clone(),
                    assigned_window: firing.identity.window,
                    pane: firing.schedule.pane,
                })
                .collect(),
            timers,
        })
    }

    fn validate_mutations(&self, mutations: &[TimerMutation]) -> Result<(), Error> {
        let mut queue = TimerQueue::new(self.contract.clone(), self.window == Window::Global)?;
        queue.apply_mutations(mutations.to_vec())
    }

    fn bind_requests(
        &self,
        key: &[u8],
        window: Option<WindowInterval>,
        pane: PaneInfo,
        input_timestamp_ns: i64,
        requests: Vec<UserTimerRequest>,
    ) -> Result<Vec<TimerMutation>, Error> {
        requests.into_iter().map(|request| {
            let (family_id, dynamic_tag, set) = match request {
                UserTimerRequest::Set { family_id, dynamic_tag, fire_timestamp_ns, output_timestamp_ns } => {
                    (family_id, dynamic_tag, Some((fire_timestamp_ns, output_timestamp_ns)))
                }
                UserTimerRequest::Clear { family_id, dynamic_tag } => (family_id, dynamic_tag, None),
            };
            let family = self.contract.timers.get(&family_id)
                .ok_or_else(|| Error::execution(&self.contract.semantic_id, "timer", "undeclared timer family"))?;
            if self.contract.on_window_expiration_timer.as_deref() == Some(family_id.as_str()) {
                return Err(Error::execution(&self.contract.semantic_id, "timer", "window-expiration family is runner owned"));
            }
            let identity = TimerIdentity {
                family_id,
                dynamic_tag,
                encoded_key: key.to_vec(),
                window,
            };
            Ok(match set {
                Some((fire_timestamp_ns, output_timestamp_ns)) => {
                    let hold_timestamp_ns = output_timestamp_ns.unwrap_or(match family.domain {
                        UserTimerDomain::EventTime => fire_timestamp_ns,
                        UserTimerDomain::ProcessingTime => input_timestamp_ns,
                    });
                    TimerMutation::Set(identity, TimerSchedule {
                        fire_timestamp_ns,
                        hold_timestamp_ns,
                        pane,
                    })
                }
                None => TimerMutation::Clear(identity),
            })
        }).collect()
    }

    fn apply_element(
        &self,
        input: Envelope,
        processing_time_ns: Option<i64>,
    ) -> Result<Vec<Envelope>, Error> {
        let key = (self.key_bytes)(input.value.as_ref())?;
        let cell = (input.assigned_window, key.clone());
        let mut cells = self.cells.lock()
            .map_err(|_| Error::execution(&self.contract.semantic_id, "direct", "timed state lock poisoned"))?;
        let change = self.invoke_element(input, cells.get(&cell).map(Vec::as_slice), processing_time_ns)?;
        let mut mutations = change.timers;
        if let Some(family_id) = &self.contract.on_window_expiration_timer {
            let window = cell.0.ok_or_else(|| Error::incompatible("expiration timer has no window"))?;
            let fire_timestamp_ns = window.end_ns.checked_sub(1_000_000)
                .ok_or_else(|| Error::incompatible("expiration timestamp underflows"))?;
            mutations.push(TimerMutation::Set(
                TimerIdentity {
                    family_id: family_id.clone(),
                    dynamic_tag: String::new(),
                    encoded_key: key,
                    window: Some(window),
                },
                TimerSchedule {
                    fire_timestamp_ns,
                    hold_timestamp_ns: fire_timestamp_ns,
                    pane: PaneInfo::NO_FIRING,
                },
            ));
        }
        self.timers.lock()
            .map_err(|_| Error::execution(&self.contract.semantic_id, "direct", "timer lock poisoned"))?
            .apply_mutations(mutations)?;
        match change.state {
            Some(bytes) => { cells.insert(cell, bytes); }
            None => { cells.remove(&cell); }
        }
        drop(cells);
        Ok(change.outputs)
    }

    fn apply_timer(&self, firing: &TimerFiring) -> Result<Vec<Envelope>, Error> {
        let cell = (firing.identity.window, firing.identity.encoded_key.clone());
        let mut cells = self.cells.lock()
            .map_err(|_| Error::execution(&self.contract.semantic_id, "direct", "timed state lock poisoned"))?;
        let change = self.invoke_timer(firing, cells.get(&cell).map(Vec::as_slice))?;
        self.timers.lock()
            .map_err(|_| Error::execution(&self.contract.semantic_id, "direct", "timer lock poisoned"))?
            .apply_mutations(change.timers)?;
        match change.state {
            Some(bytes) => { cells.insert(cell, bytes); }
            None => { cells.remove(&cell); }
        }
        drop(cells);
        Ok(change.outputs)
    }

    fn take_ready(&self, event_watermark_ns: Option<i64>, processing_time_ns: Option<i64>) -> Result<Vec<Envelope>, Error> {
        let firings = self.timers.lock()
            .map_err(|_| Error::execution(&self.contract.semantic_id, "direct", "timer lock poisoned"))?
            .take_ready(event_watermark_ns, processing_time_ns);
        firings.into_iter().try_fold(Vec::new(), |mut outputs, firing| {
            outputs.extend(self.apply_timer(&firing)?);
            Ok(outputs)
        })
    }

    fn next_processing_deadline(&self) -> Result<Option<i64>, Error> {
        Ok(self.timers.lock()
            .map_err(|_| Error::execution(&self.contract.semantic_id, "direct", "timer lock poisoned"))?
            .next_deadline(UserTimerDomain::ProcessingTime))
    }

    fn expire(&self, watermark: Option<i64>) -> Result<(), Error> {
        let Some(watermark) = watermark.filter(|_| self.window != Window::Global) else {
            return Ok(());
        };
        self.cells.lock()
            .map_err(|_| Error::recovery(&self.contract.semantic_id, "timed state lock poisoned"))?
            .retain(|(window, _), _| window.is_some_and(|interval| {
                interval.end_ns.saturating_add(self.allowed_lateness_ns) > watermark
            }));
        Ok(())
    }

    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        let cells = self.cells.lock()
            .map_err(|_| Error::recovery(&self.contract.semantic_id, "timed state lock poisoned"))?;
        let mut entries = cells.iter()
            .map(|((window, key), value)| (*window, key.clone(), value.clone()))
            .collect::<Vec<_>>();
        drop(cells);
        entries.sort_by(|left, right| {
            (left.0.map(|window| (window.start_ns, window.end_ns)), &left.1)
                .cmp(&(right.0.map(|window| (window.start_ns, window.end_ns)), &right.1))
        });
        let timers = self.timers.lock()
            .map_err(|_| Error::recovery(&self.contract.semantic_id, "timer lock poisoned"))?
            .checkpoint()?;
        serde_json::to_vec(&TimedStateSnapshot { version: 1, cells: entries, timers })
            .map_err(|error| Error::recovery(&self.contract.semantic_id, error.to_string()))
    }

    fn restore(&self, bytes: &[u8]) -> Result<(), Error> {
        let snapshot: TimedStateSnapshot = serde_json::from_slice(bytes)
            .map_err(|error| Error::recovery(&self.contract.semantic_id, error.to_string()))?;
        if snapshot.version != 1 {
            return Err(Error::incompatible("timed state checkpoint version differs"));
        }
        let mut cells = HashMap::new();
        for (window, key, value) in snapshot.cells {
            if window.is_some() == (self.window == Window::Global)
                || window.is_some_and(|interval| interval.end_ns <= interval.start_ns)
            {
                return Err(Error::recovery(&self.contract.semantic_id, "invalid state window"));
            }
            (self.validate_key)(&key)?;
            (self.validate_state)(&value)?;
            if cells.insert((window, key), value).is_some() {
                return Err(Error::recovery(&self.contract.semantic_id, "duplicate state cell"));
            }
        }
        let mut timers = TimerQueue::new(self.contract.clone(), self.window == Window::Global)?;
        timers.restore(&snapshot.timers, self.validate_key)?;
        let mut cell_guard = self.cells.lock()
            .map_err(|_| Error::recovery(&self.contract.semantic_id, "timed state lock poisoned"))?;
        let mut timer_guard = self.timers.lock()
            .map_err(|_| Error::recovery(&self.contract.semantic_id, "timer lock poisoned"))?;
        *cell_guard = cells;
        *timer_guard = timers;
        drop(timer_guard);
        drop(cell_guard);
        Ok(())
    }
}
