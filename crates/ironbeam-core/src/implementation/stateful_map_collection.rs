/// Change to a keyed, windowed `MapState` cell. Map iteration order is unspecified.
#[derive(Clone, Debug)]
pub enum MapStateMutation<MK, MV> {
    Keep,
    Clear,
    Put(MK, MV),
    Remove(MK),
    Replace(HashMap<MK, MV>),
}

fn decode_map_items<MK: Element + Eq + Hash, MV: Element>(bytes: &[u8]) -> Result<HashMap<MK, MV>, Error> {
    let encoded: Vec<(Vec<u8>, Vec<u8>)> = serde_json::from_slice(bytes)
        .map_err(|error| Error::incompatible(format!("invalid map state: {error}")))?;
    let mut items = HashMap::with_capacity(encoded.len());
    for (key, value) in encoded {
        if items.insert(MK::decode(&key)?, MV::decode(&value)?).is_some() {
            return Err(Error::incompatible("map state contains a duplicate key"));
        }
    }
    Ok(items)
}

fn encode_map_items<MK: Element + Eq + Hash, MV: Element>(items: HashMap<MK, MV>) -> Result<Option<Vec<u8>>, Error> {
    if items.is_empty() {
        return Ok(None);
    }
    let mut encoded = items.into_iter()
        .map(|(key, value)| Ok((key.encode()?, value.encode()?)))
        .collect::<Result<Vec<_>, Error>>()?;
    encoded.sort_by(|left, right| left.0.cmp(&right.0));
    if encoded.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(Error::incompatible("distinct map keys have identical coder bytes"));
    }
    Ok(Some(serde_json::to_vec(&encoded).map_err(|error| Error::incompatible(error.to_string()))?))
}

/// Register a Beam `MapState` callback for coded map keys and values.
/// The callback sees current entries and returns one output.
impl<K, V> PCollection<(K, V)>
where
    K: Element + Eq + Hash + Clone,
    V: Element,
{
    pub fn stateful_map_map_named<MK: Element + Eq + Hash, MV: Element, O: Element>(
        &self,
        key: &str,
        semantic_id: &str,
        version: Version,
        f: impl Fn(&K, V, &HashMap<MK, MV>) -> Result<(MapStateMutation<MK, MV>, O), Error> + Send + Sync + 'static,
    ) -> Result<PCollection<(K, O)>, Error> {
        Self::validate_semantic_handler(key, semantic_id, version)?;
        let parent = self.parent()?;
        if matches!(parent.time.window, Window::Session { .. })
            || parent.time.trigger != WindowTrigger::Default
            || parent.time.allowed_lateness_ns != 0
        {
            return Err(Error::graph(key, "map-state transform requires default global, fixed, or sliding window policy"));
        }
        if !K::supports_coder() || !V::supports_coder() || !MK::supports_coder()
            || !MV::supports_coder() || !O::supports_coder() || !<(K, O)>::supports_coder()
        {
            return Err(Error::graph(key, "map-state transform requires registered element coders"));
        }
        let contract = StatefulParDoContract {
            semantic_id: semantic_id.into(),
            version,
            key_coder_id: "v5_lp_bytes".into(),
            window_coder_id: if parent.time.window == Window::Global { "c_gw" } else { "c_iw" }.into(),
            states: BTreeMap::from([("map".into(), UserStateKind::Map { key_coder_id: "v5_lp_bytes".into(), value_coder_id: "v5_lp_bytes".into() })]),
            timers: BTreeMap::new(),
            on_window_expiration_timer: None,
        };
        contract.validate()?;
        let callback: StatefulValueCallback = Arc::new(move |payload, prior| {
            let pair = payload.downcast::<(K, V)>()
                .map_err(|_| Error::incompatible("stateful map input type differs from registration"))?;
            let mut values = prior.map(decode_map_items::<MK, MV>).transpose()?.unwrap_or_default();
            let (mutation, output) = f(&pair.0, pair.1, &values)?;
            match mutation {
                MapStateMutation::Keep => {}
                MapStateMutation::Clear => values.clear(),
                MapStateMutation::Put(key, value) => { values.insert(key, value); }
                MapStateMutation::Remove(key) => { values.remove(&key); }
                MapStateMutation::Replace(items) => values = items,
            }
            let next = encode_map_items(values)?;
            Ok((next, Box::new((pair.0, output)) as Payload))
        });
        let transform = StatefulValueTransform {
            key_bytes: |payload| payload.downcast_ref::<(K, V)>()
                .ok_or_else(|| Error::incompatible("stateful map key type differs from registration"))?
                .0.encode(),
            validate_key: |bytes| K::decode(bytes).map(|_| ()),
            validate_state: |bytes| decode_map_items::<MK, MV>(bytes).map(|_| ()),
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
                    type_id: MV::type_identity(),
                    coder_id: MV::coder_identity(),
                    coder_payload: MV::coder_payload(),
                    coder_version: MV::CODER_VERSION,
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
