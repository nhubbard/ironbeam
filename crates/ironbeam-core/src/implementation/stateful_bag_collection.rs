/// Change to a keyed, windowed `BagState` cell. Bag iteration order is unspecified.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BagStateMutation<S> {
    Keep,
    Clear,
    Append(Vec<S>),
    Replace(Vec<S>),
}

fn encode_bag_items<S: Element>(items: Vec<S>) -> Result<Vec<Vec<u8>>, Error> {
    items.into_iter().map(|item| item.encode()).collect()
}

fn decode_bag_items<S: Element>(bytes: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
    let encoded: Vec<Vec<u8>> = serde_json::from_slice(bytes)
        .map_err(|error| Error::incompatible(format!("invalid bag state: {error}")))?;
    for item in &encoded {
        S::decode(item)?;
    }
    Ok(encoded)
}

/// Register a Beam `BagState` callback for coded keys and values.
/// Each callback sees the current bag contents and returns one output.
/// The runner may present bag entries in any order.
impl<K, V> PCollection<(K, V)>
where
    K: Element + Eq + Hash + Clone,
    V: Element,
{
    pub fn stateful_bag_map_named<S: Element, O: Element>(
        &self,
        key: &str,
        semantic_id: &str,
        version: Version,
        f: impl Fn(&K, V, Vec<S>) -> Result<(BagStateMutation<S>, O), Error> + Send + Sync + 'static,
    ) -> Result<PCollection<(K, O)>, Error> {
        Self::validate_semantic_handler(key, semantic_id, version)?;
        let parent = self.parent()?;
        if matches!(parent.time.window, Window::Session { .. })
            || parent.time.trigger != WindowTrigger::Default
            || parent.time.allowed_lateness_ns != 0
        {
            return Err(Error::graph(key, "bag-state transform requires default global, fixed, or sliding window policy"));
        }
        if !K::supports_coder() || !V::supports_coder() || !S::supports_coder()
            || !O::supports_coder() || !<(K, O)>::supports_coder()
        {
            return Err(Error::graph(key, "bag-state transform requires registered element coders"));
        }
        let contract = StatefulParDoContract {
            semantic_id: semantic_id.into(),
            version,
            key_coder_id: "v5_lp_bytes".into(),
            window_coder_id: if parent.time.window == Window::Global { "c_gw" } else { "c_iw" }.into(),
            states: BTreeMap::from([("bag".into(), UserStateKind::Bag { element_coder_id: "v5_lp_bytes".into() })]),
            timers: BTreeMap::new(),
            on_window_expiration_timer: None,
        };
        contract.validate()?;
        let callback: StatefulValueCallback = Arc::new(move |payload, prior| {
            let pair = payload.downcast::<(K, V)>()
                .map_err(|_| Error::incompatible("stateful bag input type differs from registration"))?;
            let mut encoded = prior.map(decode_bag_items::<S>).transpose()?.unwrap_or_default();
            let values = encoded.iter().map(|item| S::decode(item)).collect::<Result<Vec<_>, _>>()?;
            let (mutation, output) = f(&pair.0, pair.1, values)?;
            match mutation {
                BagStateMutation::Keep => {}
                BagStateMutation::Clear => encoded.clear(),
                BagStateMutation::Append(items) => encoded.extend(encode_bag_items(items)?),
                BagStateMutation::Replace(items) => encoded = encode_bag_items(items)?,
            }
            let next = if encoded.is_empty() { None } else {
                Some(serde_json::to_vec(&encoded).map_err(|error| Error::incompatible(error.to_string()))?)
            };
            Ok((next, Box::new((pair.0, output)) as Payload))
        });
        let transform = StatefulValueTransform {
            key_bytes: |payload| payload.downcast_ref::<(K, V)>()
                .ok_or_else(|| Error::incompatible("stateful bag key type differs from registration"))?
                .0.encode(),
            validate_key: |bytes| K::decode(bytes).map(|_| ()),
            validate_state: |bytes| decode_bag_items::<S>(bytes).map(|_| ()),
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
