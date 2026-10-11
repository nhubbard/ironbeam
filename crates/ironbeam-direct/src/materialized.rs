//! Direct physical path for bounded grouping, joins, and global combines.
use crate::metrics::MetricsState;
use crate::spill::OutputStore;
use crate::{JobConfig, JobHandle, cancelled, open_source_safe, read_source_batch};
use ironbeam_core::{Element, Envelope, Error, FoldSession, Graph, Operation, Registry};
use rayon::prelude::*;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::Ordering;

pub(crate) fn execute<T: Element>(
    pool: &rayon::ThreadPool,
    graph: &Graph,
    registry: &Registry,
    output_key: &str,
    config: &JobConfig,
    handle: &JobHandle,
) -> Result<Vec<T>, Error> {
    let events = eval(pool, graph, registry, output_key, config, &handle.metrics)?;
    let mut store = OutputStore::<T>::new(
        config.memory_budget_bytes,
        config.spill.as_ref(),
        handle.metrics.clone(),
    )?;
    let output_count = u64::try_from(events.len()).unwrap_or(u64::MAX);
    for event in events {
        if config.cancellation.load(Ordering::Acquire) {
            return Err(cancelled());
        }
        let value = event
            .value
            .downcast::<T>()
            .map_err(|_| Error::execution(output_key, "direct", "output type mismatch"))?;
        store.push(*value)?;
    }
    let values = store.into_vec()?;
    let count = handle
        .metrics
        .output_elements
        .fetch_add(output_count, Ordering::Relaxed)
        .saturating_add(output_count);
    handle.processed.store(count, Ordering::Relaxed);
    Ok(values)
}

#[allow(clippy::too_many_lines)] // One bounded traversal keeps recursive evaluation and errors together.
fn eval(
    pool: &rayon::ThreadPool,
    graph: &Graph,
    registry: &Registry,
    key: &str,
    config: &JobConfig,
    metrics: &MetricsState,
) -> Result<Vec<Envelope>, Error> {
    if config.cancellation.load(Ordering::Acquire) {
        return Err(cancelled());
    }
    let node = graph
        .node(key)
        .ok_or_else(|| Error::graph(key, "physical node missing"))?;
    if matches!(node.operation, Operation::Source { .. }) {
        let mut source = open_source_safe(registry, key)?;
        let mut output = Vec::new();
        loop {
            if config.cancellation.load(Ordering::Acquire) {
                return Err(cancelled());
            }
            let batch = read_source_batch(&mut source, config.buffer_budget_elements, key)?;
            if batch.is_empty() {
                break;
            }
            let size = u64::try_from(batch.len()).unwrap_or(u64::MAX);
            metrics.source_elements.fetch_add(size, Ordering::Relaxed);
            metrics
                .peak_batch_elements
                .fetch_max(size, Ordering::Relaxed);
            metrics.batches.fetch_add(1, Ordering::Relaxed);
            output.extend(batch);
            if output.len().saturating_mul(std::mem::size_of::<Envelope>())
                > config.memory_budget_bytes
            {
                return Err(Error::resource(
                    key,
                    "materialized source exceeds job memory budget",
                ));
            }
        }
        return Ok(output);
    }
    let primary = node
        .input
        .as_deref()
        .ok_or_else(|| Error::graph(key, "physical input missing"))?;
    if matches!(
        node.operation,
        Operation::FoldGlobal
            | Operation::FoldPerKey
            | Operation::FoldOrderedGlobal
            | Operation::FoldOrderedPerKey
    ) {
        let mut fold = catch_unwind(AssertUnwindSafe(|| registry.open_fold(key)))
            .unwrap_or_else(|_| Err(Error::execution(key, "direct", "fold factory panicked")))?;
        if let Some((source_key, stages)) = linear_fold_route(graph, primary) {
            feed_linear_fold(
                registry,
                &source_key,
                &stages,
                config,
                metrics,
                key,
                fold.as_mut(),
            )?;
        } else {
            for event in eval(pool, graph, registry, primary, config, metrics)? {
                if config.cancellation.load(Ordering::Acquire) {
                    return Err(cancelled());
                }
                catch_unwind(AssertUnwindSafe(|| fold.add(event))).unwrap_or_else(|_| {
                    Err(Error::execution(key, "direct", "user fold panicked"))
                })?;
            }
        }
        return catch_unwind(AssertUnwindSafe(|| fold.finish()))
            .unwrap_or_else(|_| Err(Error::execution(key, "direct", "user fold panicked")));
    }
    if matches!(node.operation, Operation::WaitOn) {
        let signal = node
            .additional_inputs
            .first()
            .ok_or_else(|| Error::graph(key, "wait signal missing"))?;
        eval(pool, graph, registry, &signal.key, config, metrics)?;
        return eval(pool, graph, registry, primary, config, metrics);
    }
    let first = eval(pool, graph, registry, primary, config, metrics)?;
    match &node.operation {
        Operation::Output | Operation::Sink { .. } | Operation::Reshuffle => Ok(first),
        Operation::Map
        | Operation::Filter
        | Operation::FlatMap
        | Operation::AssignTimestamp
        | Operation::FixedWindow => {
            let results: Vec<_> = pool.install(|| {
                first
                    .into_par_iter()
                    .map(|event| {
                        if config.cancellation.load(Ordering::Acquire) {
                            return Err(cancelled());
                        }
                        match catch_unwind(AssertUnwindSafe(|| registry.apply_many(key, event))) {
                            Ok(Ok(values)) => {
                                if values.is_empty() {
                                    metrics.filtered_elements.fetch_add(1, Ordering::Relaxed);
                                }
                                Ok(values)
                            }
                            Ok(Err(error)) => Err(error),
                            Err(_) => {
                                Err(Error::execution(key, "direct", "user transform panicked"))
                            }
                        }
                    })
                    .collect()
            });
            let mut output = Vec::new();
            for result in results {
                output.extend(result?);
            }
            Ok(output)
        }
        Operation::Flatten => {
            let mut output = first;
            for edge in &node.additional_inputs {
                output.extend(eval(pool, graph, registry, &edge.key, config, metrics)?);
            }
            Ok(output)
        }
        Operation::GroupByKey
        | Operation::CoGroupByKey
        | Operation::JoinInner
        | Operation::JoinLeft
        | Operation::JoinRight
        | Operation::JoinFull
        | Operation::SideInputMap
        | Operation::GlobalCombine
        | Operation::BatchMapValues { .. } => {
            let mut inputs = vec![first];
            for edge in &node.additional_inputs {
                inputs.push(eval(pool, graph, registry, &edge.key, config, metrics)?);
            }
            catch_unwind(AssertUnwindSafe(|| registry.apply_aggregate(key, inputs)))
                .unwrap_or_else(|_| Err(Error::execution(key, "direct", "user aggregate panicked")))
        }
        _ => Err(Error::unsupported(key, "direct", "materialized-operation")),
    }
}

fn linear_fold_route(graph: &Graph, from: &str) -> Option<(String, Vec<String>)> {
    let mut key = from;
    let mut stages = Vec::new();
    loop {
        let node = graph.node(key)?;
        match node.operation {
            Operation::Source { .. } => {
                stages.reverse();
                return Some((key.to_owned(), stages));
            }
            Operation::Map
            | Operation::Filter
            | Operation::FlatMap
            | Operation::AssignTimestamp
            | Operation::FixedWindow => {
                stages.push(key.to_owned());
                key = node.input.as_deref()?;
            }
            _ => return None,
        }
    }
}

fn feed_linear_fold(
    registry: &Registry,
    source_key: &str,
    stages: &[String],
    config: &JobConfig,
    metrics: &MetricsState,
    fold_key: &str,
    fold: &mut dyn FoldSession,
) -> Result<(), Error> {
    let mut source = open_source_safe(registry, source_key)?;
    loop {
        if config.cancellation.load(Ordering::Acquire) {
            return Err(cancelled());
        }
        let batch = read_source_batch(&mut source, config.buffer_budget_elements, source_key)?;
        if batch.is_empty() {
            break;
        }
        let count = u64::try_from(batch.len()).unwrap_or(u64::MAX);
        metrics.source_elements.fetch_add(count, Ordering::Relaxed);
        metrics
            .peak_batch_elements
            .fetch_max(count, Ordering::Relaxed);
        metrics.batches.fetch_add(1, Ordering::Relaxed);
        for event in batch {
            let mut events = vec![event];
            for key in stages {
                let mut next = Vec::new();
                for event in events {
                    let produced =
                        catch_unwind(AssertUnwindSafe(|| registry.apply_many(key, event)))
                            .unwrap_or_else(|_| {
                                Err(Error::execution(key, "direct", "user transform panicked"))
                            })?;
                    if produced.is_empty() {
                        metrics.filtered_elements.fetch_add(1, Ordering::Relaxed);
                    }
                    next.extend(produced);
                }
                events = next;
            }
            for event in events {
                if config.cancellation.load(Ordering::Acquire) {
                    return Err(cancelled());
                }
                catch_unwind(AssertUnwindSafe(|| fold.add(event))).unwrap_or_else(|_| {
                    Err(Error::execution(fold_key, "direct", "user fold panicked"))
                })?;
            }
        }
    }
    Ok(())
}
