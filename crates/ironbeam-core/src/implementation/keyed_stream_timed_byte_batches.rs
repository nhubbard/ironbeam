/// Incremental count, byte, and processing-time batches for replayable
/// unbounded keyed input.
fn stream_timed_byte_limited_batches<K, V>(
    input: &PCollection<(K, V)>,
    key: &str,
    max_count: usize,
    max_encoded_bytes: usize,
    max_buffering_duration_ns: i64,
) -> Result<PCollection<(K, Vec<V>)>, Error>
where
    K: Element + Eq + Hash + Clone,
    V: Element,
{
    let parent = input.parent()?;
    if !byte_batch_windows_are_millisecond_aligned(&parent.time.window) {
        return Err(Error::graph(
            key,
            "timed byte batches require millisecond-aligned global, fixed, or sliding windows",
        ));
    }
    let byte_limit = u64::try_from(max_encoded_bytes)
        .map_err(|_| Error::graph(key, "encoded byte limit exceeds u64"))?;
    let semantic_id = format!(
        "ironbeam-builtin/group-into-batches/timed-limits/{max_count}/{max_encoded_bytes}/{max_buffering_duration_ns}"
    );
    let node_key = key.to_owned();
    input
        .stateful_timer_map_with_clock_named::<(u64, Vec<(i64, V)>, i64), (i64, Vec<V>)>(
            &format!("{key}/timed-byte-state"),
            &semantic_id,
            BUILTIN_OPERATION_VERSION,
            BTreeMap::from([
                ("window-end".into(), UserTimerDomain::EventTime),
                ("buffering".into(), UserTimerDomain::ProcessingTime),
            ]),
            move |_, value, prior, metadata, clock| {
                let now = clock.ok_or_else(|| {
                    Error::execution(&node_key, "runner", "processing clock unavailable")
                })?;
                let encoded_len = u64::try_from(value.encoded_len()?)
                    .map_err(|_| Error::resource(&node_key, "encoded value size exceeds u64"))?;
                let (mut bytes, mut pending, mut deadline) =
                    prior.unwrap_or((0, Vec::new(), 0));
                let mut outputs = Vec::new();
                if !pending.is_empty()
                    && (now >= deadline
                        || pending.len() >= max_count
                        || bytes.saturating_add(encoded_len) > byte_limit)
                {
                    outputs.push(take_byte_batch(&mut pending));
                    bytes = 0;
                    deadline = 0;
                }
                bytes = bytes.saturating_add(encoded_len);
                pending.push((metadata.timestamp_ns, value));
                if pending.len() >= max_count || bytes >= byte_limit {
                    outputs.push(take_byte_batch(&mut pending));
                    return Ok(TimedStateResult {
                        state: None,
                        outputs,
                        timers: vec![
                            UserTimerRequest::Clear {
                                family_id: "window-end".into(),
                                dynamic_tag: "flush".into(),
                            },
                            UserTimerRequest::Clear {
                                family_id: "buffering".into(),
                                dynamic_tag: "flush".into(),
                            },
                        ],
                    });
                }
                if deadline == 0 {
                    deadline = timed_byte_deadline(now, max_buffering_duration_ns, &node_key)?;
                }
                let earliest = pending
                    .iter()
                    .map(|(timestamp, _)| *timestamp)
                    .min()
                    .expect("nonempty pending timed byte batch has a timestamp");
                let mut timers = vec![UserTimerRequest::Set {
                    family_id: "buffering".into(),
                    dynamic_tag: "flush".into(),
                    fire_timestamp_ns: deadline,
                    output_timestamp_ns: Some(earliest),
                }];
                if let Some(window) = metadata.assigned_window {
                    timers.push(UserTimerRequest::Set {
                        family_id: "window-end".into(),
                        dynamic_tag: "flush".into(),
                        fire_timestamp_ns: window.end_ns,
                        output_timestamp_ns: Some(earliest),
                    });
                }
                Ok(TimedStateResult {
                    state: Some((bytes, pending, deadline)),
                    outputs,
                    timers,
                })
            },
            |_, prior, event| {
                if event.dynamic_tag != "flush"
                    || !matches!(event.family_id.as_str(), "buffering" | "window-end")
                {
                    return Err(Error::incompatible("unexpected timed byte-batch timer"));
                }
                let Some((_, mut pending, _)) = prior else {
                    return Ok(TimedStateResult {
                        state: None,
                        outputs: Vec::new(),
                        timers: Vec::new(),
                    });
                };
                let outputs = if pending.is_empty() {
                    Vec::new()
                } else {
                    vec![take_byte_batch(&mut pending)]
                };
                let opposite = if event.family_id == "buffering" {
                    "window-end"
                } else {
                    "buffering"
                };
                Ok(TimedStateResult {
                    state: None,
                    outputs,
                    timers: vec![UserTimerRequest::Clear {
                        family_id: opposite.into(),
                        dynamic_tag: "flush".into(),
                    }],
                })
            },
        )?
        .map_named(&format!("{key}/timestamp"), |(group_key, (timestamp, values))| {
            (timestamp, (group_key, values))
        })?
        .timestamps_from_pairs_in_window_named(key)
}

fn timed_byte_deadline(now: i64, delay: i64, key: &str) -> Result<i64, Error> {
    let deadline = now
        .checked_add(delay)
        .ok_or_else(|| Error::resource(key, "processing timer deadline overflow"))?;
    let remainder = deadline.rem_euclid(1_000_000);
    if remainder == 0 {
        return Ok(deadline);
    }
    deadline
        .checked_add(1_000_000 - remainder)
        .ok_or_else(|| Error::resource(key, "processing timer deadline overflow"))
}
