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
    for intervals in by_key.values_mut() {
        intervals.sort_unstable_by_key(|interval| (interval.start_ns, interval.end_ns));
        let mut merged: Vec<WindowInterval> = Vec::with_capacity(intervals.len());
        for interval in intervals.drain(..) {
            if let Some(last) = merged.last_mut()
                && interval.start_ns <= last.end_ns
            {
                last.end_ns = last.end_ns.max(interval.end_ns);
                continue;
            }
            merged.push(interval);
        }
        *intervals = merged;
    }
    for (event, (row_key, original)) in left.iter_mut().zip(left_rows) {
        event.assigned_window = Some(merged_join_interval(&by_key[&row_key], original));
    }
    for (event, (row_key, original)) in right.iter_mut().zip(right_rows) {
        event.assigned_window = Some(merged_join_interval(&by_key[&row_key], original));
    }
    Ok(())
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

