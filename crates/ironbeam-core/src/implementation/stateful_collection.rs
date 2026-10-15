/// A keyed value-state transform. The callback receives the current
/// cell for this key and window and returns its replacement plus one output.
/// It must be registered with the same semantic identity in every worker.
/// Unbounded input currently uses the global window; Direct checkpoints its
/// cells with the source cursor on a single linear stream route.
impl<K, V> PCollection<(K, V)>
where
    K: Element + Eq + Hash + Clone,
    V: Element,
{
    pub fn stateful_value_map_named<S: Element, O: Element>(
        &self,
        key: &str,
        semantic_id: &str,
        version: Version,
        f: impl Fn(&K, V, Option<S>) -> Result<(Option<S>, O), Error> + Send + Sync + 'static,
    ) -> Result<PCollection<(K, O)>, Error> {
        Self::validate_semantic_handler(key, semantic_id, version)?;
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded && parent.time.window != Window::Global {
            return Err(Error::graph(key, "unbounded value state currently requires the global window"));
        }
        if matches!(parent.time.window, Window::Session { .. })
            || parent.time.trigger != WindowTrigger::Default
            || parent.time.allowed_lateness_ns != 0
        {
            return Err(Error::graph(key, "value-state transform requires default global, fixed, or sliding window policy"));
        }
        if !K::supports_coder() || !V::supports_coder() || !S::supports_coder()
            || !O::supports_coder() || !<(K, O)>::supports_coder()
        {
            return Err(Error::graph(key, "value-state transform requires registered element coders"));
        }
        let contract = StatefulParDoContract {
            semantic_id: semantic_id.into(),
            version,
            key_coder_id: "v5_lp_bytes".into(),
            window_coder_id: if parent.time.window == Window::Global { "c_gw" } else { "c_iw" }.into(),
            states: BTreeMap::from([("value".into(), UserStateKind::Value { coder_id: "v5_lp_bytes".into() })]),
            timers: BTreeMap::new(),
            on_window_expiration_timer: None,
        };
        contract.validate()?;
        let callback: StatefulValueCallback = Arc::new(move |payload, prior| {
            let pair = payload.downcast::<(K, V)>()
                .map_err(|_| Error::incompatible("stateful value input type differs from registration"))?;
            let state = prior.map(S::decode).transpose()?;
            let (next, output) = f(&pair.0, pair.1, state)?;
            Ok((next.map(|value| value.encode()).transpose()?, Box::new((pair.0, output)) as Payload))
        });
        let transform = StatefulValueTransform {
            key_bytes: |payload| payload.downcast_ref::<(K, V)>()
                .ok_or_else(|| Error::incompatible("stateful value key type differs from registration"))?
                .0.encode(),
            validate_key: |bytes| K::decode(bytes).map(|_| ()),
            validate_state: |bytes| S::decode(bytes).map(|_| ()),
            global_window: parent.time.window == Window::Global,
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
