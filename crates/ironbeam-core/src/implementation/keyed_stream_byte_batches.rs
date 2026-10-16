/// Incremental byte-limited batches for replayable unbounded keyed input.
fn stream_byte_limited_batches<K, V>(
    input: &PCollection<(K, V)>,
    key: &str,
    max_count: usize,
    max_encoded_bytes: usize,
) -> Result<PCollection<(K, Vec<V>)>, Error>
where
    K: Element + Eq + Hash + Clone,
    V: Element,
{
    let parent = input.parent()?;
    let aligned = match parent.time.window {
        Window::Global => true,
        Window::Fixed { size_ns } => size_ns >= 1_000_000 && size_ns % 1_000_000 == 0,
        Window::FixedOffset { size_ns, offset_ns } => {
            size_ns >= 1_000_000
                && size_ns % 1_000_000 == 0
                && offset_ns % 1_000_000 == 0
        }
        Window::Sliding {
            size_ns,
            period_ns,
            offset_ns,
        } => {
            size_ns >= 1_000_000
                && size_ns % 1_000_000 == 0
                && period_ns >= 1_000_000
                && period_ns % 1_000_000 == 0
                && offset_ns % 1_000_000 == 0
        }
        Window::Session { .. } => false,
    };
    if !aligned {
        return Err(Error::graph(
            key,
            "stream byte batches require millisecond-aligned global, fixed, or sliding windows",
        ));
    }
    let byte_limit = u64::try_from(max_encoded_bytes)
        .map_err(|_| Error::graph(key, "encoded byte limit exceeds u64"))?;
    let semantic_id =
        format!("ironbeam-builtin/group-into-batches/limits/{max_count}/{max_encoded_bytes}");
    let node_key = key.to_owned();
    input
        .stateful_timer_map_named::<(u64, Vec<(i64, V)>), (i64, Vec<V>)>(
            &format!("{key}/byte-state"),
            &semantic_id,
            BUILTIN_OPERATION_VERSION,
            BTreeMap::from([("window-end".into(), UserTimerDomain::EventTime)]),
            move |_, value, prior, metadata| {
                let encoded_len = u64::try_from(value.encoded_len()?)
                    .map_err(|_| Error::resource(&node_key, "encoded value size exceeds u64"))?;
                let (mut bytes, mut pending) = prior.unwrap_or((0, Vec::new()));
                let mut outputs = Vec::new();
                if !pending.is_empty()
                    && (pending.len() >= max_count
                        || bytes.saturating_add(encoded_len) > byte_limit)
                {
                    outputs.push(take_byte_batch(&mut pending));
                    bytes = 0;
                }
                bytes = bytes.saturating_add(encoded_len);
                pending.push((metadata.timestamp_ns, value));
                if pending.len() >= max_count || bytes >= byte_limit {
                    outputs.push(take_byte_batch(&mut pending));
                    bytes = 0;
                }
                let timers = if pending.is_empty() {
                    vec![UserTimerRequest::Clear {
                        family_id: "window-end".into(),
                        dynamic_tag: "flush".into(),
                    }]
                } else {
                    let earliest = pending
                        .iter()
                        .map(|(timestamp, _)| *timestamp)
                        .min()
                        .expect("nonempty pending byte batch has a timestamp");
                    let deadline = metadata
                        .assigned_window
                        .map_or(i64::MAX - i64::MAX.rem_euclid(1_000_000), |window| {
                            window.end_ns
                        });
                    vec![UserTimerRequest::Set {
                        family_id: "window-end".into(),
                        dynamic_tag: "flush".into(),
                        fire_timestamp_ns: deadline,
                        output_timestamp_ns: Some(earliest),
                    }]
                };
                Ok(TimedStateResult {
                    state: if pending.is_empty() { None } else { Some((bytes, pending)) },
                    outputs,
                    timers,
                })
            },
            |_, prior, event| {
                if event.family_id != "window-end" || event.dynamic_tag != "flush" {
                    return Err(Error::incompatible("unexpected byte-batch timer"));
                }
                let mut outputs = Vec::new();
                if let Some((_, mut pending)) = prior
                    && !pending.is_empty()
                {
                    outputs.push(take_byte_batch(&mut pending));
                }
                Ok(TimedStateResult {
                    state: None,
                    outputs,
                    timers: Vec::new(),
                })
            },
        )?
        .map_named(&format!("{key}/timestamp"), |(group_key, (timestamp, values))| {
            (timestamp, (group_key, values))
        })?
        .timestamps_from_pairs_in_window_named(key)
}

fn take_byte_batch<V>(pending: &mut Vec<(i64, V)>) -> (i64, Vec<V>) {
    let earliest = pending
        .iter()
        .map(|(timestamp, _)| *timestamp)
        .min()
        .expect("nonempty byte batch has a timestamp");
    let values = std::mem::take(pending)
        .into_iter()
        .map(|(_, value)| value)
        .collect();
    (earliest, values)
}
