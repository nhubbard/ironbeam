/// Mutation of one keyed, windowed Beam combining-state accumulator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CombiningStateMutation<A> {
    Keep,
    Clear,
    Replace(A),
    Add(A),
}

/// Stable identity of the accumulator merge function.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CombiningFunction<'a> {
    pub semantic_id: &'a str,
    pub version: Version,
}

/// A combining-state transform with an explicit versioned merge function.
/// The element callback sees the accumulator before its requested mutation.
impl<K, V> PCollection<(K, V)>
where
    K: Element + Eq + Hash + Clone,
    V: Element,
{
    pub fn stateful_combining_map_named<A: Element, O: Element>(
        &self,
        key: &str,
        semantic_id: &str,
        version: Version,
        combine: CombiningFunction<'_>,
        merge: impl Fn(A, A) -> Result<A, Error> + Send + Sync + 'static,
        f: impl Fn(&K, V, Option<&A>) -> Result<(CombiningStateMutation<A>, O), Error>
            + Send + Sync + 'static,
    ) -> Result<PCollection<(K, O)>, Error> {
        Self::validate_semantic_handler(key, semantic_id, version)?;
        if !combine.semantic_id.contains('/') || combine.version.major == 0 {
            return Err(Error::graph(key, "combining state requires a versioned merge function"));
        }
        let parent = self.parent()?;
        if matches!(parent.time.window, Window::Session { .. })
            || parent.time.trigger != WindowTrigger::Default
            || parent.time.allowed_lateness_ns != 0
        {
            return Err(Error::graph(key, "combining-state transform requires default global, fixed, or sliding window policy"));
        }
        if !K::supports_coder() || !V::supports_coder() || !A::supports_coder()
            || !O::supports_coder() || !<(K, O)>::supports_coder()
        {
            return Err(Error::graph(key, "combining-state transform requires registered element coders"));
        }
        let contract = StatefulParDoContract {
            semantic_id: semantic_id.into(),
            version,
            key_coder_id: "v5_lp_bytes".into(),
            window_coder_id: if parent.time.window == Window::Global { "c_gw" } else { "c_iw" }.into(),
            states: BTreeMap::from([("combining".into(), UserStateKind::Combining {
                accumulator_coder_id: "v5_lp_bytes".into(),
                combine_semantic_id: combine.semantic_id.into(),
                combine_version: combine.version,
            })]),
            timers: BTreeMap::new(),
            on_window_expiration_timer: None,
        };
        contract.validate()?;
        let merge = Arc::new(merge);
        let coded_merge: CodedAccumulatorMerger = {
            let merge = Arc::clone(&merge);
            Arc::new(move |encoded| {
                let mut parts = encoded.iter();
                let first = parts.next().ok_or_else(|| Error::incompatible("combining accumulator bag is empty"))?;
                let mut accumulator = A::decode(first)?;
                for part in parts {
                    accumulator = merge(accumulator, A::decode(part)?)?;
                }
                accumulator.encode()
            })
        };
        let callback: StatefulValueCallback = Arc::new(move |payload, prior| {
            let pair = payload.downcast::<(K, V)>()
                .map_err(|_| Error::incompatible("combining-state input type differs from registration"))?;
            let state = prior.map(A::decode).transpose()?;
            let (mutation, output) = f(&pair.0, pair.1, state.as_ref())?;
            let next = match mutation {
                CombiningStateMutation::Keep => state,
                CombiningStateMutation::Clear => None,
                CombiningStateMutation::Replace(value) => Some(value),
                CombiningStateMutation::Add(value) => {
                    Some(match state { Some(prior) => merge(prior, value)?, None => value })
                }
            };
            Ok((next.map(|value| value.encode()).transpose()?, Box::new((pair.0, output)) as Payload))
        });
        let transform = StatefulValueTransform {
            key_bytes: |payload| payload.downcast_ref::<(K, V)>()
                .ok_or_else(|| Error::incompatible("combining-state key type differs from registration"))?
                .0.encode(),
            validate_key: |bytes| K::decode(bytes).map(|_| ()),
            validate_state: |bytes| A::decode(bytes).map(|_| ()),
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
                    type_id: A::type_identity(),
                    coder_id: A::coder_identity(),
                    coder_payload: A::coder_payload(),
                    coder_version: A::CODER_VERSION,
                },
            },
            Some(Transform::StatefulValue(transform)),
            None,
        )?;
        let mut builder = self.pipeline.inner.lock()
            .map_err(|_| Error::graph(key, "builder lock poisoned"))?;
        builder.registry.group_coders.insert(key.into(), GroupCoderRegistration::for_elements::<K, V>());
        builder.registry.combining_mergers.insert(key.into(), coded_merge);
        drop(builder);
        Ok(result)
    }
}
