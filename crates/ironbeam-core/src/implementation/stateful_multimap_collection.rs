/// Change to a keyed, windowed `MultimapState` cell. Duplicate values are retained.
#[derive(Clone, Debug)]
pub enum MultimapStateMutation<MK, MV> {
    Keep,
    Clear,
    Append(MK, MV),
    Remove(MK),
    Replace(HashMap<MK, Vec<MV>>),
}

fn decode_multimap_items<MK: Element + Eq + Hash, MV: Element>(
    bytes: &[u8],
) -> Result<HashMap<MK, Vec<MV>>, Error> {
    let encoded: Vec<(Vec<u8>, Vec<Vec<u8>>)> = serde_json::from_slice(bytes)
        .map_err(|error| Error::incompatible(format!("invalid multimap state: {error}")))?;
    let mut items = HashMap::with_capacity(encoded.len());
    for (key, values) in encoded {
        let key = MK::decode(&key)?;
        let values = values.iter().map(|value| MV::decode(value)).collect::<Result<Vec<_>, _>>()?;
        if items.insert(key, values).is_some() {
            return Err(Error::incompatible("multimap state contains a duplicate key"));
        }
    }
    Ok(items)
}

fn encode_multimap_items<MK: Element + Eq + Hash, MV: Element>(
    items: HashMap<MK, Vec<MV>>,
) -> Result<Option<Vec<u8>>, Error> {
    if items.is_empty() {
        return Ok(None);
    }
    let mut encoded = items.into_iter()
        .filter(|(_, values)| !values.is_empty())
        .map(|(key, values)| {
            let mut values = values.iter().map(Element::encode).collect::<Result<Vec<_>, _>>()?;
            values.sort();
            Ok((key.encode()?, values))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    encoded.sort_by(|left, right| left.0.cmp(&right.0));
    if encoded.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(Error::incompatible("distinct multimap keys have identical coder bytes"));
    }
    if encoded.is_empty() {
        return Ok(None);
    }
    Ok(Some(serde_json::to_vec(&encoded).map_err(|error| Error::incompatible(error.to_string()))?))
}

/// Register a Beam `MultimapState` callback for coded map keys and values.
/// The callback sees all current values per map key and returns one output.
impl<K, V> PCollection<(K, V)>
where
    K: Element + Eq + Hash + Clone,
    V: Element,
{
    pub fn stateful_multimap_map_named<MK: Element + Eq + Hash, MV: Element, O: Element>(
        &self,
        key: &str,
        semantic_id: &str,
        version: Version,
        f: impl Fn(&K, V, &HashMap<MK, Vec<MV>>) -> Result<(MultimapStateMutation<MK, MV>, O), Error> + Send + Sync + 'static,
    ) -> Result<PCollection<(K, O)>, Error> {
        Self::validate_semantic_handler(key, semantic_id, version)?;
        let parent = self.parent()?;
        if matches!(parent.time.window, Window::Session { .. })
            || parent.time.trigger != WindowTrigger::Default
            || parent.time.allowed_lateness_ns != 0
        {
            return Err(Error::graph(key, "multimap-state transform requires default global, fixed, or sliding window policy"));
        }
        if !K::supports_coder() || !V::supports_coder() || !MK::supports_coder()
            || !MV::supports_coder() || !O::supports_coder() || !<(K, O)>::supports_coder()
        {
            return Err(Error::graph(key, "multimap-state transform requires registered element coders"));
        }
        let contract = StatefulParDoContract {
            semantic_id: semantic_id.into(),
            version,
            key_coder_id: "v5_lp_bytes".into(),
            window_coder_id: if parent.time.window == Window::Global { "c_gw" } else { "c_iw" }.into(),
            states: BTreeMap::from([("multimap".into(), UserStateKind::Multimap { key_coder_id: "v5_lp_bytes".into(), value_coder_id: "v5_lp_bytes".into() })]),
            timers: BTreeMap::new(),
            on_window_expiration_timer: None,
        };
        contract.validate()?;
        let callback: StatefulValueCallback = Arc::new(move |payload, prior| {
            let pair = payload.downcast::<(K, V)>()
                .map_err(|_| Error::incompatible("stateful multimap input type differs from registration"))?;
            let mut values = prior.map(decode_multimap_items::<MK, MV>).transpose()?.unwrap_or_default();
            let (mutation, output) = f(&pair.0, pair.1, &values)?;
            match mutation {
                MultimapStateMutation::Keep => {}
                MultimapStateMutation::Clear => values.clear(),
                MultimapStateMutation::Append(key, value) => values.entry(key).or_default().push(value),
                MultimapStateMutation::Remove(key) => { values.remove(&key); }
                MultimapStateMutation::Replace(items) => values = items,
            }
            let next = encode_multimap_items(values)?;
            Ok((next, Box::new((pair.0, output)) as Payload))
        });
        let transform = StatefulValueTransform {
            key_bytes: |payload| payload.downcast_ref::<(K, V)>()
                .ok_or_else(|| Error::incompatible("stateful multimap key type differs from registration"))?
                .0.encode(),
            validate_key: |bytes| K::decode(bytes).map(|_| ()),
            validate_state: |bytes| decode_multimap_items::<MK, MV>(bytes).map(|_| ()),
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
