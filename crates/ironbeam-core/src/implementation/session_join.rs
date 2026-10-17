type SessionKeyExtractor<K> = fn(&Payload) -> Result<K, Error>;

fn merge_bounded_join_sessions<K, V, W>(
    left: &mut [Envelope],
    right: &mut [Envelope],
    key: &str,
) -> Result<(), Error>
where
    K: Element + Eq + Hash + Clone,
    V: Element,
    W: Element,
{
    let mut by_key: HashMap<K, Vec<WindowInterval>> = HashMap::new();
    let mut left_rows = Vec::with_capacity(left.len());
    for event in left.iter() {
        let (row_key, _) = event.value.downcast_ref::<(K, V)>().ok_or_else(|| {
            Error::execution(key, "direct", "left session join input type mismatch")
        })?;
        let interval = event.assigned_window.ok_or_else(|| {
            Error::execution(key, "direct", "left session join interval missing")
        })?;
        by_key.entry(row_key.clone()).or_default().push(interval);
        left_rows.push((row_key.clone(), interval));
    }
    let mut right_rows = Vec::with_capacity(right.len());
    for event in right.iter() {
        let (row_key, _) = event.value.downcast_ref::<(K, W)>().ok_or_else(|| {
            Error::execution(key, "direct", "right session join input type mismatch")
        })?;
        let interval = event.assigned_window.ok_or_else(|| {
            Error::execution(key, "direct", "right session join interval missing")
        })?;
        by_key.entry(row_key.clone()).or_default().push(interval);
        right_rows.push((row_key.clone(), interval));
    }
    merge_session_intervals(&mut by_key);
    for (event, (row_key, original)) in left.iter_mut().zip(left_rows) {
        event.assigned_window = Some(merged_join_interval(&by_key[&row_key], original));
    }
    for (event, (row_key, original)) in right.iter_mut().zip(right_rows) {
        event.assigned_window = Some(merged_join_interval(&by_key[&row_key], original));
    }
    Ok(())
}

fn merge_bounded_cogroup_sessions<K>(
    inputs: &mut [Vec<Envelope>],
    extractors: &[SessionKeyExtractor<K>],
    key: &str,
) -> Result<(), Error>
where
    K: Element + Eq + Hash + Clone,
{
    let mut by_key: HashMap<K, Vec<WindowInterval>> = HashMap::new();
    let mut original_rows = Vec::with_capacity(inputs.len());
    for (input, extractor) in inputs.iter().zip(extractors) {
        let mut rows = Vec::with_capacity(input.len());
        for event in input {
            let row_key = extractor(&event.value).map_err(|_| {
                Error::execution(key, "direct", "session co-group input type mismatch")
            })?;
            let interval = event.assigned_window.ok_or_else(|| {
                Error::execution(key, "direct", "session co-group interval missing")
            })?;
            by_key.entry(row_key.clone()).or_default().push(interval);
            rows.push((row_key, interval));
        }
        original_rows.push(rows);
    }
    merge_session_intervals(&mut by_key);
    for (input, rows) in inputs.iter_mut().zip(original_rows) {
        for (event, (row_key, original)) in input.iter_mut().zip(rows) {
            event.assigned_window = Some(merged_join_interval(&by_key[&row_key], original));
        }
    }
    Ok(())
}

fn merge_session_intervals<K: Eq + Hash>(by_key: &mut HashMap<K, Vec<WindowInterval>>) {
    for intervals in by_key.values_mut() {
        intervals.sort_unstable_by_key(|interval| (interval.start_ns, interval.end_ns));
        let mut merged: Vec<WindowInterval> = Vec::with_capacity(intervals.len());
        for interval in intervals.drain(..) {
            if let Some(last) = merged.last_mut()
                // A half-open session that only touches the prior one is separate.
                && interval.start_ns < last.end_ns
            {
                last.end_ns = last.end_ns.max(interval.end_ns);
                continue;
            }
            merged.push(interval);
        }
        *intervals = merged;
    }
}

fn merged_join_interval(
    intervals: &[WindowInterval],
    original: WindowInterval,
) -> WindowInterval {
    let position = intervals.partition_point(|interval| interval.start_ns <= original.start_ns);
    // Every original interval was indexed before merging, so one merged
    // interval must contain it. This lookup is over those same intervals.
    let merged = intervals[position - 1];
    debug_assert!(merged.end_ns >= original.end_ns);
    merged
}

