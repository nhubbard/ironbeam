//! Bounded sources feeding stateless sink branches through optional Flatten nodes.
use crate::{
    JobConfig, JobHandle, cancelled, ensure_bounded_sink_session, open_source_safe,
    read_source_batch,
};
use ironbeam_core::{
    Boundedness, Envelope, Error, Graph, Operation, Registry, Replay, SinkMode, SinkSession,
};
use std::collections::{HashMap, HashSet};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::Ordering;

pub(crate) struct Plan {
    source_keys: Vec<String>,
    children: HashMap<String, Vec<String>>,
}

fn validate_sink(graph: &Graph, registry: &Registry, key: &str) -> Result<(), Error> {
    let node = graph
        .node(key)
        .ok_or_else(|| Error::graph(key, "sink is absent"))?;
    let Operation::Sink { spec } = &node.operation else {
        return Err(Error::graph(key, "requested terminal is not a sink"));
    };
    if !matches!(spec.mode, SinkMode::Append | SinkMode::FinalResult)
        || !spec.commit
        || spec.replay != Replay::Idempotent
    {
        return Err(Error::unsupported(
            key,
            "direct",
            "multi-sink-idempotent-commit",
        ));
    }
    if !registry.has_sink(key) {
        return Err(Error::unsupported(key, "direct", "sink-factory"));
    }
    if registry.sink_requires_bundle_finalization(key) {
        return Err(Error::unsupported(key, "direct", "bundle-finalization"));
    }
    Ok(())
}

pub(crate) fn prepare(graph: &Graph, registry: &Registry, keys: &[&str]) -> Result<Plan, Error> {
    if keys.len() < 2 {
        return Err(Error::graph(
            "job",
            "multi-sink job needs at least two sinks",
        ));
    }
    let mut unique = HashSet::new();
    let mut sources = HashSet::new();
    let mut visited = HashSet::new();
    let mut children: HashMap<String, Vec<String>> = HashMap::new();
    for &key in keys {
        if !unique.insert(key) {
            return Err(Error::graph(key, "sink branches must be distinct"));
        }
        validate_sink(graph, registry, key)?;
    }
    let mut pending = keys.iter().map(|key| (*key).to_owned()).collect::<Vec<_>>();
    while let Some(current) = pending.pop() {
        if !visited.insert(current.clone()) {
            continue;
        }
        let node = graph
            .node(&current)
            .ok_or_else(|| Error::graph(&current, "route node is absent"))?;
        if node.boundedness != Boundedness::Bounded {
            return Err(Error::unsupported(
                &current,
                "direct",
                "multi-sink-unbounded",
            ));
        }
        match &node.operation {
            Operation::Source { .. } => {
                sources.insert(current);
                continue;
            }
            Operation::Sink { .. } if unique.contains(current.as_str()) => {}
            Operation::Map
            | Operation::Filter
            | Operation::FlatMap
            | Operation::AssignTimestamp
            | Operation::FixedWindow
            | Operation::SlidingWindow
            | Operation::SessionWindow => {
                if !registry.has_transform(&current) {
                    return Err(Error::unsupported(&current, "direct", "transform-function"));
                }
            }
            Operation::Reshuffle | Operation::Flatten => {}
            _ => {
                return Err(Error::unsupported(
                    &current,
                    "direct",
                    "multi-sink-operation",
                ));
            }
        }
        if !matches!(node.operation, Operation::Flatten) && !node.additional_inputs.is_empty() {
            return Err(Error::unsupported(&current, "direct", "multi-sink-fan-in"));
        }
        let parent = node
            .input
            .as_deref()
            .ok_or_else(|| Error::graph(&current, "route input is absent"))?;
        children
            .entry(parent.to_owned())
            .or_default()
            .push(current.clone());
        pending.push(parent.to_owned());
        for edge in &node.additional_inputs {
            children
                .entry(edge.key.clone())
                .or_default()
                .push(current.clone());
            pending.push(edge.key.clone());
        }
    }
    let source_keys = graph
        .nodes
        .iter()
        .filter(|node| sources.contains(&node.key))
        .map(|node| node.key.clone())
        .collect::<Vec<_>>();
    if source_keys.is_empty() {
        return Err(Error::graph("job", "multi-sink job has no source"));
    }
    for (key, branches) in &children {
        if branches.len() > 1 && !registry.has_coder(key) {
            return Err(Error::unsupported(key, "direct", "multi-sink-split-coder"));
        }
    }
    Ok(Plan {
        source_keys,
        children,
    })
}

fn apply(
    registry: &Registry,
    key: &str,
    event: Envelope,
    config: &JobConfig,
) -> Result<Vec<Envelope>, Error> {
    if config.cancellation.load(Ordering::Acquire) {
        return Err(cancelled());
    }
    if !registry.has_transform(key) {
        return Ok(vec![event]);
    }
    catch_unwind(AssertUnwindSafe(|| registry.apply_many(key, event)))
        .unwrap_or_else(|_| Err(Error::execution(key, "direct", "user transform panicked")))
}

fn dispatch(
    key: &str,
    event: Envelope,
    plan: &Plan,
    registry: &Registry,
    config: &JobConfig,
    handle: &JobHandle,
    sessions: &mut HashMap<String, Box<dyn SinkSession>>,
) -> Result<u64, Error> {
    if let Some(session) = sessions.get_mut(key) {
        if config.cancellation.load(Ordering::Acquire) {
            return Err(cancelled());
        }
        catch_unwind(AssertUnwindSafe(|| session.write(event)))
            .unwrap_or_else(|_| Err(Error::execution(key, "direct", "sink writer panicked")))?;
        return Ok(1);
    }
    let produced = apply(registry, key, event, config)?;
    if produced.is_empty() {
        handle
            .metrics
            .filtered_elements
            .fetch_add(1, Ordering::Relaxed);
    }
    if produced.len() > config.buffer_budget_elements {
        return Err(Error::resource(
            key,
            "multi-sink transform output exceeds buffer budget",
        ));
    }
    let branches = plan
        .children
        .get(key)
        .ok_or_else(|| Error::graph(key, "route has no sink branch"))?;
    if branches.len() > config.buffer_budget_elements {
        return Err(Error::resource(
            key,
            "multi-sink fan-out exceeds buffer budget",
        ));
    }
    let mut written = 0_u64;
    for event in produced {
        let mut original = Some(event);
        let copies = if branches.len() > 1 {
            let event = original.as_ref().expect("one event");
            let bytes = catch_unwind(AssertUnwindSafe(|| {
                registry.encode_element(key, event.value.as_ref())
            }))
            .unwrap_or_else(|_| Err(Error::execution(key, "direct", "split coder panicked")))?;
            let mut copies = Vec::with_capacity(branches.len() - 1);
            for _ in 1..branches.len() {
                let value = catch_unwind(AssertUnwindSafe(|| registry.decode_element(key, &bytes)))
                    .unwrap_or_else(|_| {
                        Err(Error::execution(key, "direct", "split coder panicked"))
                    })?;
                copies.push(Envelope {
                    pane: event.pane,
                    value,
                    timestamp_ns: event.timestamp_ns,
                    window: event.window.clone(),
                    assigned_window: event.assigned_window,
                });
            }
            copies
        } else {
            Vec::new()
        };
        let mut copies = copies.into_iter();
        for (index, branch) in branches.iter().enumerate() {
            let value = if index == 0 {
                original.take().expect("first branch owns original")
            } else {
                // Every extra branch receives its own typed value and metadata.
                copies.next().expect("one copy per extra branch")
            };
            written = written.saturating_add(dispatch(
                branch, value, plan, registry, config, handle, sessions,
            )?);
        }
    }
    Ok(written)
}

pub(crate) fn execute(
    registry: &Registry,
    plan: &Plan,
    keys: &[&str],
    config: &JobConfig,
    handle: &JobHandle,
) -> Result<(), Error> {
    let mut sessions = HashMap::new();
    let result = (|| {
        for &key in keys {
            let mut session = catch_unwind(AssertUnwindSafe(|| registry.open_sink(key)))
                .unwrap_or_else(|_| {
                    Err(Error::execution(key, "direct", "sink factory panicked"))
                })?;
            if let Err(error) = ensure_bounded_sink_session(key, session.as_ref()) {
                let _ = catch_unwind(AssertUnwindSafe(|| session.abort()));
                return Err(error);
            }
            sessions.insert(key.to_owned(), session);
        }
        for source_key in &plan.source_keys {
            let mut source = open_source_safe(registry, source_key)?;
            loop {
                if config.cancellation.load(Ordering::Acquire) {
                    return Err(cancelled());
                }
                let batch =
                    read_source_batch(&mut source, config.buffer_budget_elements, source_key)?;
                if batch.is_empty() {
                    break;
                }
                let count = u64::try_from(batch.len()).unwrap_or(u64::MAX);
                handle
                    .metrics
                    .source_elements
                    .fetch_add(count, Ordering::Relaxed);
                handle
                    .metrics
                    .peak_batch_elements
                    .fetch_max(count, Ordering::Relaxed);
                handle.metrics.batches.fetch_add(1, Ordering::Relaxed);
                let mut written = 0_u64;
                for event in batch {
                    written = written.saturating_add(dispatch(
                        source_key,
                        event,
                        plan,
                        registry,
                        config,
                        handle,
                        &mut sessions,
                    )?);
                }
                let total = handle
                    .metrics
                    .output_elements
                    .fetch_add(written, Ordering::Relaxed)
                    .saturating_add(written);
                handle.processed.store(total, Ordering::Relaxed);
            }
        }
        for &key in keys {
            if config.cancellation.load(Ordering::Acquire) {
                return Err(cancelled());
            }
            let session = sessions.get_mut(key).expect("all sinks opened");
            catch_unwind(AssertUnwindSafe(|| session.commit()))
                .unwrap_or_else(|_| Err(Error::execution(key, "direct", "sink commit panicked")))
                .map_err(|mut error| {
                    error.message = format!(
                        "{}; earlier sinks may already be committed; retry idempotently",
                        error.message
                    );
                    error
                })?;
        }
        Ok(())
    })();
    if result.is_err() {
        for session in sessions.values_mut() {
            let _ = catch_unwind(AssertUnwindSafe(|| session.abort()));
        }
    }
    result
}
