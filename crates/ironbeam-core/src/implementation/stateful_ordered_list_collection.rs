/// Change to a keyed, windowed ordered-list state cell. Sort keys are
/// milliseconds, matching Beam's ordered-list Fn State range protocol.
#[derive(Clone, Debug)]
pub enum OrderedListStateMutation<S> {
    Keep,
    Clear,
    Append(i64, S),
    ClearRange { start_ms: i64, end_ms: i64 },
    Replace(Vec<(i64, S)>),
}

fn decode_ordered_list_items<S: Element>(bytes: &[u8]) -> Result<Vec<(i64, S)>, Error> {
    let mut encoded: Vec<(i64, Vec<u8>)> = serde_json::from_slice(bytes)
        .map_err(|error| Error::incompatible(format!("invalid ordered-list state: {error}")))?;
    encoded.sort();
    encoded
        .into_iter()
        .map(|(sort_ms, value)| {
            if sort_ms == i64::MAX {
                return Err(Error::incompatible("ordered-list sort key cannot be i64::MAX"));
            }
            Ok((sort_ms, S::decode(&value)?))
        })
        .collect()
}

fn encode_ordered_list_items<S: Element>(items: Vec<(i64, S)>) -> Result<Option<Vec<u8>>, Error> {
    if items.is_empty() {
        return Ok(None);
    }
    let mut encoded = items
        .into_iter()
        .map(|(sort_ms, value)| {
            if sort_ms == i64::MAX {
                return Err(Error::incompatible("ordered-list sort key cannot be i64::MAX"));
            }
            Ok((sort_ms, value.encode()?))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    encoded.sort();
    Ok(Some(serde_json::to_vec(&encoded).map_err(|error| Error::incompatible(error.to_string()))?))
}

/// Register a Beam ordered-list callback for one coded key and window.
/// Equal sort keys are ordered by encoded value bytes for deterministic reads.
impl<K, V> PCollection<(K, V)>
where
    K: Element + Eq + Hash + Clone,
    V: Element,
{
    pub fn stateful_ordered_list_map_named<S: Element, O: Element>(
        &self,
        key: &str,
        semantic_id: &str,
        version: Version,
        f: impl Fn(&K, V, &[(i64, S)]) -> Result<(OrderedListStateMutation<S>, O), Error>
            + Send + Sync + 'static,
    ) -> Result<PCollection<(K, O)>, Error> {
        Self::validate_semantic_handler(key, semantic_id, version)?;
        let parent = self.parent()?;
        if matches!(parent.time.window, Window::Session { .. })
            || parent.time.trigger != WindowTrigger::Default
            || parent.time.allowed_lateness_ns != 0
        {
            return Err(Error::graph(key, "ordered-list transform requires default global, fixed, or sliding window policy"));
        }
        if !K::supports_coder() || !V::supports_coder() || !S::supports_coder()
            || !O::supports_coder() || !<(K, O)>::supports_coder()
        {
            return Err(Error::graph(key, "ordered-list transform requires registered element coders"));
        }
        let contract = StatefulParDoContract {
            semantic_id: semantic_id.into(),
            version,
            key_coder_id: "v5_lp_bytes".into(),
            window_coder_id: if parent.time.window == Window::Global { "c_gw" } else { "c_iw" }.into(),
            states: BTreeMap::from([("ordered_list".into(), UserStateKind::OrderedList { element_coder_id: "v5_lp_bytes".into() })]),
            timers: BTreeMap::new(),
            on_window_expiration_timer: None,
        };
        contract.validate()?;
        let callback: StatefulValueCallback = Arc::new(move |payload, prior| {
            let pair = payload.downcast::<(K, V)>()
                .map_err(|_| Error::incompatible("ordered-list input type differs from registration"))?;
            let mut values = prior.map(decode_ordered_list_items::<S>).transpose()?.unwrap_or_default();
            let (mutation, output) = f(&pair.0, pair.1, &values)?;
            match mutation {
                OrderedListStateMutation::Keep => {}
                OrderedListStateMutation::Clear => values.clear(),
                OrderedListStateMutation::Append(sort_ms, value) => values.push((sort_ms, value)),
                OrderedListStateMutation::ClearRange { start_ms, end_ms } => {
                    if start_ms >= end_ms {
                        return Err(Error::incompatible("ordered-list clear range must be nonempty"));
                    }
                    values.retain(|(sort_ms, _)| *sort_ms < start_ms || *sort_ms >= end_ms);
                }
                OrderedListStateMutation::Replace(items) => values = items,
            }
            let next = encode_ordered_list_items(values)?;
            Ok((next, Box::new((pair.0, output)) as Payload))
        });
        let transform = StatefulValueTransform {
            key_bytes: |payload| payload.downcast_ref::<(K, V)>()
                .ok_or_else(|| Error::incompatible("ordered-list key type differs from registration"))?
                .0.encode(),
            validate_key: |bytes| K::decode(bytes).map(|_| ()),
            validate_state: |bytes| decode_ordered_list_items::<S>(bytes).map(|_| ()),
            global_window: parent.time.window == Window::Global,
            allowed_lateness_ns: parent.time.allowed_lateness_ns,
            callback,
            cells: Mutex::new(HashMap::new()),
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
            Some(Transform::StatefulValue(transform)),
            None,
        )?;
        self.pipeline.inner.lock()
            .map_err(|_| Error::graph(key, "builder lock poisoned"))?
            .registry.group_coders.insert(key.into(), GroupCoderRegistration::for_elements::<K, V>());
        Ok(result)
    }
}
