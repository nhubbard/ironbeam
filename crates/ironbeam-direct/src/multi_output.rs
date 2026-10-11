//! One-job execution of two bounded, linear output branches.
use crate::metrics::MetricsState;
use crate::spill::OutputStore;
use crate::{JobConfig, JobHandle, cancelled, open_source_safe, read_source_batch};
use ironbeam_core::{Element, Envelope, Error, Graph, Operation, Registry, SinkSession};
use rayon::prelude::*;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::Ordering;

pub(crate) struct PairPlan {
    pub(crate) source_key: String,
    split_key: String,
    common_stages: Vec<String>,
    left_stages: Vec<String>,
    right_stages: Vec<String>,
}

fn linear_route(
    graph: &Graph,
    registry: &Registry,
    output_key: &str,
) -> Result<Vec<String>, Error> {
    let mut reversed = Vec::new();
    let mut key = output_key;
    loop {
        let node = graph
            .node(key)
            .ok_or_else(|| Error::graph(key, "output route node missing"))?;
        if !node.additional_inputs.is_empty() {
            return Err(Error::unsupported(key, "direct", "multi-output-fan-in"));
        }
        reversed.push(key.to_owned());
        match node.operation {
            Operation::Source { .. } => break,
            Operation::Output | Operation::Sink { .. } | Operation::Reshuffle => {}
            Operation::Map
            | Operation::Filter
            | Operation::FlatMap
            | Operation::AssignTimestamp
            | Operation::FixedWindow => {
                if !registry.has_transform(key) {
                    return Err(Error::unsupported(key, "direct", "transform-function"));
                }
            }
            _ => return Err(Error::unsupported(key, "direct", "multi-output-operation")),
        }
        key = node
            .input
            .as_deref()
            .ok_or_else(|| Error::graph(key, "output route input missing"))?;
    }
    reversed.reverse();
    Ok(reversed)
}

pub(crate) fn prepare(
    graph: &Graph,
    registry: &Registry,
    left_key: &str,
    right_key: &str,
) -> Result<PairPlan, Error> {
    for key in [left_key, right_key] {
        if !graph
            .node(key)
            .is_some_and(|node| matches!(node.operation, Operation::Output))
        {
            return Err(Error::graph(key, "requested terminal is not an output"));
        }
    }
    prepare_routes(graph, registry, left_key, right_key)
}

pub(crate) fn prepare_sinks(
    graph: &Graph,
    registry: &Registry,
    left_key: &str,
    right_key: &str,
) -> Result<PairPlan, Error> {
    for key in [left_key, right_key] {
        if !graph
            .node(key)
            .is_some_and(|node| matches!(node.operation, Operation::Sink { .. }))
        {
            return Err(Error::graph(key, "requested terminal is not a sink"));
        }
    }
    prepare_routes(graph, registry, left_key, right_key)
}

fn prepare_routes(
    graph: &Graph,
    registry: &Registry,
    left_key: &str,
    right_key: &str,
) -> Result<PairPlan, Error> {
    let left = linear_route(graph, registry, left_key)?;
    let right = linear_route(graph, registry, right_key)?;
    if left.first() != right.first() {
        return Err(Error::unsupported(
            right_key,
            "direct",
            "multi-output-source",
        ));
    }
    let common = left.iter().zip(&right).take_while(|(a, b)| a == b).count();
    let split_key = &left[common - 1];
    if !registry.has_coder(split_key) {
        return Err(Error::unsupported(
            split_key,
            "direct",
            "multi-output-split-coder",
        ));
    }
    Ok(PairPlan {
        source_key: left[0].clone(),
        split_key: split_key.clone(),
        common_stages: left[1..common].to_vec(),
        left_stages: left[common..].to_vec(),
        right_stages: right[common..].to_vec(),
    })
}

fn apply_stages(
    registry: &Registry,
    mut events: Vec<Envelope>,
    stages: &[String],
    config: &JobConfig,
    metrics: &MetricsState,
) -> Result<Vec<Envelope>, Error> {
    for key in stages {
        let mut next = Vec::new();
        for event in events {
            if config.cancellation.load(Ordering::Acquire) {
                return Err(cancelled());
            }
            if !registry.has_transform(key) {
                next.push(event);
                continue;
            }
            let produced = match catch_unwind(AssertUnwindSafe(|| registry.apply_many(key, event)))
            {
                Ok(result) => result?,
                Err(_) => return Err(Error::execution(key, "direct", "user transform panicked")),
            };
            if produced.is_empty() {
                metrics.filtered_elements.fetch_add(1, Ordering::Relaxed);
            }
            if next.len().saturating_add(produced.len()) > config.buffer_budget_elements {
                return Err(Error::resource(
                    key,
                    "pair transform output exceeds buffer budget",
                ));
            }
            next.extend(produced);
        }
        events = next;
    }
    Ok(events)
}

pub(crate) fn process_event(
    event: Envelope,
    plan: &PairPlan,
    registry: &Registry,
    config: &JobConfig,
    metrics: &MetricsState,
) -> Result<(Vec<Envelope>, Vec<Envelope>), Error> {
    if config.cancellation.load(Ordering::Acquire) {
        return Err(cancelled());
    }
    let common = apply_stages(registry, vec![event], &plan.common_stages, config, metrics)?;
    let mut left = Vec::new();
    let mut right = Vec::new();
    for event in common {
        let copy = catch_unwind(AssertUnwindSafe(|| {
            let bytes = registry.encode_element(&plan.split_key, event.value.as_ref())?;
            registry.decode_element(&plan.split_key, &bytes)
        }))
        .unwrap_or_else(|_| {
            Err(Error::execution(
                &plan.split_key,
                "direct",
                "split coder panicked",
            ))
        })?;
        let right_event = Envelope {
            value: copy,
            timestamp_ns: event.timestamp_ns,
            window: event.window.clone(),
            assigned_window: event.assigned_window,
        };
        let left_produced =
            apply_stages(registry, vec![event], &plan.left_stages, config, metrics)?;
        if left.len().saturating_add(left_produced.len()) > config.buffer_budget_elements {
            return Err(Error::resource(
                &plan.split_key,
                "left pair output exceeds buffer budget",
            ));
        }
        left.extend(left_produced);
        let right_produced = apply_stages(
            registry,
            vec![right_event],
            &plan.right_stages,
            config,
            metrics,
        )?;
        if right.len().saturating_add(right_produced.len()) > config.buffer_budget_elements {
            return Err(Error::resource(
                &plan.split_key,
                "right pair output exceeds buffer budget",
            ));
        }
        right.extend(right_produced);
    }
    Ok((left, right))
}

fn push_output<T: Element>(
    event: Envelope,
    key: &str,
    store: &mut OutputStore<T>,
) -> Result<(), Error> {
    let value = event
        .value
        .downcast::<T>()
        .map_err(|_| Error::execution(key, "direct", "output type mismatch"))?;
    catch_unwind(AssertUnwindSafe(|| store.push(*value)))
        .unwrap_or_else(|_| Err(Error::execution(key, "direct", "output coder panicked")))
}

fn enforce_memory<T: Element, U: Element>(
    left: &mut OutputStore<T>,
    right: &mut OutputStore<U>,
    config: &JobConfig,
) -> Result<(), Error> {
    if left.memory_usage().saturating_add(right.memory_usage()) <= config.memory_budget_bytes {
        return Ok(());
    }
    if config.spill.is_none() {
        return Err(Error::resource(
            "job",
            "multi-output results exceed job memory budget; enable spilling",
        ));
    }
    if left.memory_usage() >= right.memory_usage() {
        left.flush()
    } else {
        right.flush()
    }
}

fn write_sink_event(
    session: &mut dyn SinkSession,
    key: &str,
    event: Envelope,
    config: &JobConfig,
) -> Result<(), Error> {
    if config.cancellation.load(Ordering::Acquire) {
        return Err(cancelled());
    }
    catch_unwind(AssertUnwindSafe(|| session.write(event)))
        .unwrap_or_else(|_| Err(Error::execution(key, "direct", "sink writer panicked")))
}

fn write_sink_pair(
    events: (Vec<Envelope>, Vec<Envelope>),
    left: &mut dyn SinkSession,
    right: &mut dyn SinkSession,
    left_key: &str,
    right_key: &str,
    config: &JobConfig,
) -> Result<u64, Error> {
    let (left_events, right_events) = events;
    let mut written = 0_u64;
    for event in left_events {
        write_sink_event(left, left_key, event, config)?;
        written = written.saturating_add(1);
    }
    for event in right_events {
        write_sink_event(right, right_key, event, config)?;
        written = written.saturating_add(1);
    }
    Ok(written)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_sinks(
    pool: &rayon::ThreadPool,
    registry: &Registry,
    plan: &PairPlan,
    left_key: &str,
    right_key: &str,
    config: &JobConfig,
    handle: &JobHandle,
    left: &mut dyn SinkSession,
    right: &mut dyn SinkSession,
) -> Result<(), Error> {
    let mut source = open_source_safe(registry, &plan.source_key)?;
    loop {
        if config.cancellation.load(Ordering::Acquire) {
            return Err(cancelled());
        }
        let batch =
            read_source_batch(&mut source, config.buffer_budget_elements, &plan.source_key)?;
        if batch.is_empty() {
            break;
        }
        let source_count = u64::try_from(batch.len()).unwrap_or(u64::MAX);
        handle
            .metrics
            .source_elements
            .fetch_add(source_count, Ordering::Relaxed);
        handle
            .metrics
            .peak_batch_elements
            .fetch_max(source_count, Ordering::Relaxed);
        handle.metrics.batches.fetch_add(1, Ordering::Relaxed);
        let written = if config.parallelism == 1 {
            pool.install(|| {
                let mut written = 0_u64;
                for event in batch {
                    let events = process_event(event, plan, registry, config, &handle.metrics)?;
                    written = written.saturating_add(write_sink_pair(
                        events, left, right, left_key, right_key, config,
                    )?);
                }
                Ok::<u64, Error>(written)
            })?
        } else {
            let mut written = 0_u64;
            let mut pending = batch.into_iter();
            loop {
                let chunk: Vec<_> = pending.by_ref().take(config.parallelism).collect();
                if chunk.is_empty() {
                    break;
                }
                let results: Vec<_> = pool.install(|| {
                    chunk
                        .into_par_iter()
                        .map(|event| process_event(event, plan, registry, config, &handle.metrics))
                        .collect()
                });
                for result in results {
                    written = written.saturating_add(write_sink_pair(
                        result?, left, right, left_key, right_key, config,
                    )?);
                }
            }
            written
        };
        let total = handle
            .metrics
            .output_elements
            .fetch_add(written, Ordering::Relaxed)
            .saturating_add(written);
        handle.processed.store(total, Ordering::Relaxed);
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
pub(crate) fn execute<T: Element, U: Element>(
    pool: &rayon::ThreadPool,
    registry: &Registry,
    plan: &PairPlan,
    left_key: &str,
    right_key: &str,
    config: &JobConfig,
    handle: &JobHandle,
) -> Result<(Vec<T>, Vec<U>), Error> {
    let mut left = OutputStore::<T>::new(
        config.memory_budget_bytes,
        config.spill.as_ref(),
        handle.metrics.clone(),
    )?;
    let mut right = OutputStore::<U>::new(
        config.memory_budget_bytes,
        config.spill.as_ref(),
        handle.metrics.clone(),
    )?;
    let mut source = open_source_safe(registry, &plan.source_key)?;
    loop {
        if config.cancellation.load(Ordering::Acquire) {
            return Err(cancelled());
        }
        let batch =
            read_source_batch(&mut source, config.buffer_budget_elements, &plan.source_key)?;
        if batch.is_empty() {
            break;
        }
        let source_count = u64::try_from(batch.len()).unwrap_or(u64::MAX);
        handle
            .metrics
            .source_elements
            .fetch_add(source_count, Ordering::Relaxed);
        handle
            .metrics
            .peak_batch_elements
            .fetch_max(source_count, Ordering::Relaxed);
        handle.metrics.batches.fetch_add(1, Ordering::Relaxed);
        let mut output_count = 0_u64;
        let mut pending = batch.into_iter();
        loop {
            let chunk: Vec<_> = pending.by_ref().take(config.parallelism).collect();
            if chunk.is_empty() {
                break;
            }
            let results: Vec<_> = pool.install(|| {
                chunk
                    .into_par_iter()
                    .map(|event| process_event(event, plan, registry, config, &handle.metrics))
                    .collect()
            });
            for result in results {
                let (left_events, right_events) = result?;
                for event in left_events {
                    push_output(event, left_key, &mut left)?;
                    enforce_memory(&mut left, &mut right, config)?;
                    output_count = output_count.saturating_add(1);
                }
                for event in right_events {
                    push_output(event, right_key, &mut right)?;
                    enforce_memory(&mut left, &mut right, config)?;
                    output_count = output_count.saturating_add(1);
                }
            }
        }
        let total = handle
            .metrics
            .output_elements
            .fetch_add(output_count, Ordering::Relaxed)
            .saturating_add(output_count);
        handle.processed.store(total, Ordering::Relaxed);
    }
    let left = catch_unwind(AssertUnwindSafe(|| left.into_vec())).unwrap_or_else(|_| {
        Err(Error::execution(
            left_key,
            "direct",
            "output coder panicked",
        ))
    })?;
    let right = catch_unwind(AssertUnwindSafe(|| right.into_vec())).unwrap_or_else(|_| {
        Err(Error::execution(
            right_key,
            "direct",
            "output coder panicked",
        ))
    })?;
    Ok((left, right))
}

#[cfg(test)]
mod tests {
    use super::{prepare, prepare_sinks};
    use ironbeam_core::{ErrorKind, Pipeline};

    #[test]
    fn pair_planner_rejects_terminals_with_the_wrong_operation() {
        let pipeline = Pipeline::default();
        let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
        let left = source.output("left").unwrap();
        let right = source
            .map_named("right-map", |value| value + 1)
            .unwrap()
            .output("right")
            .unwrap();
        let (graph, registry) = left.snapshot().unwrap();
        assert_eq!(
            prepare(&graph, &registry, "source", right.key())
                .err()
                .unwrap()
                .kind,
            ErrorKind::Graph
        );
        assert_eq!(
            prepare_sinks(&graph, &registry, left.key(), right.key())
                .err()
                .unwrap()
                .kind,
            ErrorKind::Graph
        );
    }
}
