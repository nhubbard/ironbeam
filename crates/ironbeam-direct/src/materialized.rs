//! Direct physical path for bounded grouping, joins, and global combines.
use crate::checkpoint::{CheckpointManager, Identity};
use crate::metrics::MetricsState;
use crate::planner::Route;
use crate::spill::OutputStore;
use crate::{JobConfig, JobHandle, cancelled, open_source_safe, read_source_batch};
use ironbeam_core::{
    Boundedness, Element, Envelope, Error, FoldSession, Graph, Operation, Registry,
};
use rayon::prelude::*;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::Ordering;

pub(crate) fn execute<T: Element>(
    pool: &rayon::ThreadPool,
    graph: &Graph,
    registry: &Registry,
    output_key: &str,
    config: &JobConfig,
    completed_checkpoint: Option<(Identity, usize)>,
    handle: &JobHandle,
) -> Result<Vec<T>, Error> {
    let mut store = OutputStore::<T>::new(
        config.memory_budget_bytes,
        config.spill.as_ref(),
        handle.metrics.clone(),
    )?;
    let checkpoint = config
        .checkpoint
        .as_ref()
        .zip(completed_checkpoint)
        .map(|(checkpoint_config, (identity, source_len))| {
            CheckpointManager::new(checkpoint_config, identity, handle.metrics.clone())
                .map(|manager| (manager, source_len))
        })
        .transpose()?;
    let mut pending_state = checkpoint
        .as_ref()
        .map(|(manager, _)| manager.recover_fold_state(config.memory_budget_bytes))
        .transpose()?
        .flatten();
    if pending_state.is_none()
        && let Some((manager, source_len)) = &checkpoint
        && manager.recover_completed(*source_len, &mut store)?
    {
        let values = store.into_vec()?;
        handle.processed.store(
            u64::try_from(values.len()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        return Ok(values);
    }
    let events = if let Some((manager, source_len)) = &checkpoint
        && let Some((group_key, source_key, stages, downstream)) =
            checkpointed_group_route(graph, output_key)
    {
        let grouped = eval_checkpointed_group(
            registry,
            &group_key,
            &source_key,
            &stages,
            *source_len,
            pending_state.take(),
            manager,
            config,
            &handle.metrics,
        )?;
        apply_checkpointed_downstream(registry, grouped, &downstream, config, &handle.metrics)?
    } else {
        if pending_state.is_some() {
            return Err(Error::recovery(
                output_key,
                "fold state has no resumable Direct route",
            ));
        }
        eval(pool, graph, registry, output_key, config, &handle.metrics)?
    };
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
    if let Some((manager, source_len)) = &checkpoint {
        manager.save(*source_len, &store)?;
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

fn checkpointed_group_route(
    graph: &Graph,
    output_key: &str,
) -> Option<(String, String, Vec<String>, Vec<String>)> {
    let output = graph.node(output_key)?;
    if !matches!(output.operation, Operation::Output) {
        return None;
    }
    let mut key = output.input.as_deref()?;
    let mut downstream = Vec::new();
    let group = loop {
        let node = graph.node(key)?;
        if matches!(node.operation, Operation::GroupByKey) {
            break node;
        }
        if !matches!(
            node.operation,
            Operation::Map
                | Operation::Filter
                | Operation::FlatMap
                | Operation::AssignTimestamp
                | Operation::FixedWindow
        ) || !node.additional_inputs.is_empty()
        {
            return None;
        }
        downstream.push(key.to_owned());
        key = node.input.as_deref()?;
    };
    if !group.additional_inputs.is_empty() {
        return None;
    }
    let (source_key, stages) = linear_fold_route(graph, group.input.as_deref()?)?;
    downstream.reverse();
    Some((group.key.clone(), source_key, stages, downstream))
}

fn apply_checkpointed_downstream(
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
            let produced = catch_unwind(AssertUnwindSafe(|| registry.apply_many(key, event)))
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
    Ok(events)
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)] // Recovery validation and fold replay share one state machine.
fn eval_checkpointed_group(
    registry: &Registry,
    group_key: &str,
    source_key: &str,
    stages: &[String],
    source_len: usize,
    pending_state: Option<(usize, Vec<u8>)>,
    manager: &CheckpointManager,
    config: &JobConfig,
    metrics: &MetricsState,
) -> Result<Vec<Envelope>, Error> {
    let mut fold = catch_unwind(AssertUnwindSafe(|| registry.open_fold(group_key)))
        .unwrap_or_else(|_| {
            Err(Error::execution(
                group_key,
                "direct",
                "fold factory panicked",
            ))
        })?;
    let (registered_len, batch_type, read_batch) =
        registry
            .erased_vec_source_batches(source_key)
            .ok_or_else(|| Error::unsupported(source_key, "direct", "fold-checkpoint-source"))?;
    if registered_len != source_len || stages.is_empty() && !fold.accepts_vec_batch(batch_type) {
        return Err(Error::unsupported(
            group_key,
            "direct",
            "fold-checkpoint-batch",
        ));
    }
    let envelope_batches = if stages.is_empty() {
        None
    } else {
        let (envelope_len, reader) = registry
            .vec_source_envelope_batches(source_key)
            .ok_or_else(|| Error::unsupported(source_key, "direct", "fold-checkpoint-source"))?;
        if envelope_len != source_len {
            return Err(Error::unsupported(
                group_key,
                "direct",
                "fold-checkpoint-batch",
            ));
        }
        Some(reader)
    };
    let mut offset = if let Some((saved_offset, state)) = pending_state {
        if saved_offset > source_len {
            return Err(Error::recovery(
                group_key,
                "fold checkpoint exceeds source length",
            ));
        }
        catch_unwind(AssertUnwindSafe(|| fold.restore_checkpoint_state(&state))).unwrap_or_else(
            |_| {
                Err(Error::execution(
                    group_key,
                    "direct",
                    "fold recovery panicked",
                ))
            },
        )?;
        saved_offset
    } else {
        0_usize
    };
    let mut batches = 0_usize;
    while offset < source_len {
        if config.cancellation.load(Ordering::Acquire) {
            return Err(cancelled());
        }
        let batch_len = (source_len - offset).min(config.buffer_budget_elements);
        let typed_batch = if envelope_batches.is_none() {
            Some(
                catch_unwind(AssertUnwindSafe(|| read_batch(offset, batch_len))).unwrap_or_else(
                    |_| {
                        Err(Error::execution(
                            source_key,
                            "direct",
                            "source value clone panicked",
                        ))
                    },
                )?,
            )
        } else {
            None
        };
        let envelope_batch = if let Some(reader) = envelope_batches {
            Some(
                catch_unwind(AssertUnwindSafe(|| reader(offset, batch_len))).unwrap_or_else(
                    |_| {
                        Err(Error::execution(
                            source_key,
                            "direct",
                            "source value clone panicked",
                        ))
                    },
                )?,
            )
        } else {
            None
        };
        if config.cancellation.load(Ordering::Acquire) {
            return Err(cancelled());
        }
        if offset
            .saturating_add(batch_len)
            .saturating_mul(std::mem::size_of::<Envelope>())
            > config.memory_budget_bytes
        {
            return Err(Error::resource(
                source_key,
                "materialized source exceeds job memory budget",
            ));
        }
        let count = u64::try_from(batch_len).unwrap_or(u64::MAX);
        metrics.source_elements.fetch_add(count, Ordering::Relaxed);
        metrics
            .peak_batch_elements
            .fetch_max(count, Ordering::Relaxed);
        metrics.batches.fetch_add(1, Ordering::Relaxed);
        if let Some(batch) = typed_batch {
            catch_unwind(AssertUnwindSafe(|| fold.add_vec_batch(batch))).unwrap_or_else(|_| {
                Err(Error::execution(group_key, "direct", "user fold panicked"))
            })?;
        }
        if let Some(batch) = envelope_batch {
            feed_fold_events(
                registry,
                batch,
                stages,
                config,
                metrics,
                group_key,
                fold.as_mut(),
            )?;
        }
        offset += batch_len;
        batches += 1;
        if manager.should_save(batches) {
            let state = catch_unwind(AssertUnwindSafe(|| fold.checkpoint_state()))
                .unwrap_or_else(|_| {
                    Err(Error::execution(
                        group_key,
                        "direct",
                        "fold snapshot panicked",
                    ))
                })?
                .ok_or_else(|| Error::unsupported(group_key, "direct", "fold-checkpoint-state"))?;
            manager.save_fold_state(offset, &state, config.memory_budget_bytes)?;
        }
    }
    catch_unwind(AssertUnwindSafe(|| fold.finish()))
        .unwrap_or_else(|_| Err(Error::execution(group_key, "direct", "user fold panicked")))
}

#[allow(clippy::too_many_lines)] // One bounded traversal keeps recursive evaluation and errors together.
pub(crate) fn eval(
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
        let initial_capacity = registry
            .vec_source_envelope_batches(key)
            .map_or(0, |(len, _)| {
                len.min(config.memory_budget_bytes / std::mem::size_of::<Envelope>())
            });
        let mut output = Vec::with_capacity(initial_capacity);
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
    ) || matches!(node.operation, Operation::GroupByKey) && registry.has_fold(key)
    {
        let mut fold = catch_unwind(AssertUnwindSafe(|| registry.open_fold(key)))
            .unwrap_or_else(|_| Err(Error::execution(key, "direct", "fold factory panicked")))?;
        if let Some((source_key, stages)) = linear_fold_route(graph, primary) {
            feed_linear_fold(
                registry,
                &source_key,
                &stages,
                config,
                metrics,
                FoldFeedPolicy {
                    key,
                    enforce_source_budget: matches!(node.operation, Operation::GroupByKey),
                },
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

type DownstreamEmitter<'a> = dyn FnMut(Envelope, &[String]) -> Result<(), Error> + 'a;
/// Drain a linear fold and downstream stateless stages into a sink without
/// assembling all completed results into a second output vector. Other graph
/// shapes use the general materialized evaluator.
pub(crate) fn try_drain_fold_sink(
    graph: &Graph,
    registry: &Registry,
    sink_key: &str,
    config: &JobConfig,
    metrics: &MetricsState,
    emit: &mut DownstreamEmitter<'_>,
) -> Result<bool, Error> {
    let Some(mut fold_key) = graph.node(sink_key).and_then(|node| node.input.as_deref()) else {
        return Ok(false);
    };
    let mut downstream = Vec::new();
    loop {
        let Some(node) = graph.node(fold_key) else {
            return Ok(false);
        };
        if !matches!(
            node.operation,
            Operation::Map
                | Operation::Filter
                | Operation::FlatMap
                | Operation::AssignTimestamp
                | Operation::FixedWindow
        ) {
            break;
        }
        if config.parallelism != 1 || !node.additional_inputs.is_empty() {
            return Ok(false);
        }
        downstream.push(fold_key.to_owned());
        let Some(input) = node.input.as_deref() else {
            return Ok(false);
        };
        fold_key = input;
    }
    downstream.reverse();
    let Some(fold_node) = graph.node(fold_key) else {
        return Ok(false);
    };
    if !matches!(
        fold_node.operation,
        Operation::GroupByKey
            | Operation::FoldGlobal
            | Operation::FoldPerKey
            | Operation::FoldOrderedGlobal
            | Operation::FoldOrderedPerKey
    ) || !fold_node.additional_inputs.is_empty()
        || !registry.has_fold(fold_key)
    {
        return Ok(false);
    }
    let Some((source_key, stages)) = fold_node
        .input
        .as_deref()
        .and_then(|input| linear_fold_route(graph, input))
    else {
        return Ok(false);
    };
    let mut fold =
        catch_unwind(AssertUnwindSafe(|| registry.open_fold(fold_key))).unwrap_or_else(|_| {
            Err(Error::execution(
                fold_key,
                "direct",
                "fold factory panicked",
            ))
        })?;
    feed_linear_fold(
        registry,
        &source_key,
        &stages,
        config,
        metrics,
        FoldFeedPolicy {
            key: fold_key,
            enforce_source_budget: matches!(fold_node.operation, Operation::GroupByKey),
        },
        fold.as_mut(),
    )?;
    catch_unwind(AssertUnwindSafe(|| {
        fold.finish_into(&mut |event| emit(event, &downstream))
    }))
    .unwrap_or_else(|_| Err(Error::execution(fold_key, "direct", "user fold panicked")))?;
    Ok(true)
}

/// Complete a bounded wait signal, then return a linear main route that can
/// write directly to a sink in source-sized batches.
pub(crate) fn try_wait_sink_route(
    pool: &rayon::ThreadPool,
    graph: &Graph,
    registry: &Registry,
    sink_key: &str,
    config: &JobConfig,
    metrics: &MetricsState,
) -> Result<Option<Route>, Error> {
    let Some(mut key) = graph.node(sink_key).and_then(|node| node.input.as_deref()) else {
        return Ok(None);
    };
    let mut downstream = Vec::new();
    loop {
        let Some(node) = graph.node(key) else {
            return Ok(None);
        };
        if !matches!(
            node.operation,
            Operation::Map
                | Operation::Filter
                | Operation::FlatMap
                | Operation::AssignTimestamp
                | Operation::FixedWindow
        ) {
            break;
        }
        if !node.additional_inputs.is_empty() {
            return Ok(None);
        }
        downstream.push(key.to_owned());
        let Some(input) = node.input.as_deref() else {
            return Ok(None);
        };
        key = input;
    }
    let Some(wait) = graph.node(key) else {
        return Ok(None);
    };
    if !matches!(wait.operation, Operation::WaitOn) || wait.boundedness != Boundedness::Bounded {
        return Ok(None);
    }
    let Some((source_key, mut operations)) = wait
        .input
        .as_deref()
        .and_then(|input| linear_fold_route(graph, input))
    else {
        return Ok(None);
    };
    downstream.reverse();
    operations.extend(downstream);
    let signal = wait
        .additional_inputs
        .first()
        .ok_or_else(|| Error::graph(key, "wait signal missing"))?;
    if let Some((signal_source, signal_stages)) = linear_fold_route(graph, &signal.key) {
        drain_linear_signal(registry, &signal_source, &signal_stages, config, metrics)?;
    } else {
        eval(pool, graph, registry, &signal.key, config, metrics)?;
    }
    Ok(Some(Route {
        source_key,
        operations,
    }))
}

fn drain_linear_signal(
    registry: &Registry,
    source_key: &str,
    stages: &[String],
    config: &JobConfig,
    metrics: &MetricsState,
) -> Result<(), Error> {
    let mut source = open_source_safe(registry, source_key)?;
    loop {
        if config.cancellation.load(Ordering::Acquire) {
            return Err(cancelled());
        }
        let batch = read_source_batch(&mut source, config.buffer_budget_elements, source_key)?;
        if batch.is_empty() {
            return Ok(());
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
                    if config.cancellation.load(Ordering::Acquire) {
                        return Err(cancelled());
                    }
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
        }
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

#[derive(Clone, Copy)]
struct FoldFeedPolicy<'a> {
    key: &'a str,
    enforce_source_budget: bool,
}

#[allow(clippy::too_many_lines)] // One traversal preserves source and transform error attribution.
fn feed_linear_fold(
    registry: &Registry,
    source_key: &str,
    stages: &[String],
    config: &JobConfig,
    metrics: &MetricsState,
    policy: FoldFeedPolicy<'_>,
    fold: &mut dyn FoldSession,
) -> Result<(), Error> {
    let mut source_count = 0_usize;
    let FoldFeedPolicy {
        key: fold_key,
        enforce_source_budget,
    } = policy;
    if let [timestamp_key, reify_key] = stages
        && registry.is_keyed_timestamp_reification(reify_key)
        && let Some(timestamp_fn) = registry.typed_timestamp_transform(timestamp_key)
        && let Some((source_len, batch_type, read_batch)) =
            registry.erased_vec_source_batches(source_key)
        && fold.accepts_timed_vec_batch(batch_type)
    {
        let mut offset = 0_usize;
        while offset < source_len {
            if config.cancellation.load(Ordering::Acquire) {
                return Err(cancelled());
            }
            let batch_len = (source_len - offset).min(config.buffer_budget_elements);
            let batch = catch_unwind(AssertUnwindSafe(|| read_batch(offset, batch_len)))
                .unwrap_or_else(|_| {
                    Err(Error::execution(
                        source_key,
                        "direct",
                        "source value clone panicked",
                    ))
                })?;
            if enforce_source_budget {
                source_count = source_count.saturating_add(batch_len);
                if source_count.saturating_mul(std::mem::size_of::<Envelope>())
                    > config.memory_budget_bytes
                {
                    return Err(Error::resource(
                        source_key,
                        "materialized source exceeds job memory budget",
                    ));
                }
            }
            let count = u64::try_from(batch_len).unwrap_or(u64::MAX);
            metrics.source_elements.fetch_add(count, Ordering::Relaxed);
            metrics
                .peak_batch_elements
                .fetch_max(count, Ordering::Relaxed);
            metrics.batches.fetch_add(1, Ordering::Relaxed);
            catch_unwind(AssertUnwindSafe(|| {
                fold.add_timed_vec_batch(batch, timestamp_fn, timestamp_key)
            }))
            .unwrap_or_else(|_| Err(Error::execution(fold_key, "direct", "user fold panicked")))?;
            if config.cancellation.load(Ordering::Acquire) {
                return Err(cancelled());
            }
            offset += batch_len;
        }
        return Ok(());
    }
    if stages.is_empty()
        && let Some((source_len, batch_type, read_batch)) =
            registry.erased_vec_source_batches(source_key)
        && fold.accepts_vec_batch(batch_type)
    {
        let mut offset = 0_usize;
        while offset < source_len {
            if config.cancellation.load(Ordering::Acquire) {
                return Err(cancelled());
            }
            let batch_len = (source_len - offset).min(config.buffer_budget_elements);
            let batch = catch_unwind(AssertUnwindSafe(|| read_batch(offset, batch_len)))
                .unwrap_or_else(|_| {
                    Err(Error::execution(
                        source_key,
                        "direct",
                        "source value clone panicked",
                    ))
                })?;
            if enforce_source_budget {
                source_count = source_count.saturating_add(batch_len);
                if source_count.saturating_mul(std::mem::size_of::<Envelope>())
                    > config.memory_budget_bytes
                {
                    return Err(Error::resource(
                        source_key,
                        "materialized source exceeds job memory budget",
                    ));
                }
            }
            let count = u64::try_from(batch_len).unwrap_or(u64::MAX);
            metrics.source_elements.fetch_add(count, Ordering::Relaxed);
            metrics
                .peak_batch_elements
                .fetch_max(count, Ordering::Relaxed);
            metrics.batches.fetch_add(1, Ordering::Relaxed);
            catch_unwind(AssertUnwindSafe(|| fold.add_vec_batch(batch))).unwrap_or_else(|_| {
                Err(Error::execution(fold_key, "direct", "user fold panicked"))
            })?;
            offset += batch_len;
        }
        return Ok(());
    }
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
        if enforce_source_budget {
            source_count = source_count.saturating_add(batch.len());
            if source_count.saturating_mul(std::mem::size_of::<Envelope>())
                > config.memory_budget_bytes
            {
                return Err(Error::resource(
                    source_key,
                    "materialized source exceeds job memory budget",
                ));
            }
        }
        metrics.source_elements.fetch_add(count, Ordering::Relaxed);
        metrics
            .peak_batch_elements
            .fetch_max(count, Ordering::Relaxed);
        metrics.batches.fetch_add(1, Ordering::Relaxed);
        feed_fold_events(registry, batch, stages, config, metrics, fold_key, fold)?;
    }
    Ok(())
}

fn feed_fold_events(
    registry: &Registry,
    batch: Vec<Envelope>,
    stages: &[String],
    config: &JobConfig,
    metrics: &MetricsState,
    fold_key: &str,
    fold: &mut dyn FoldSession,
) -> Result<(), Error> {
    let single_stages = stages
        .iter()
        .map(|key| {
            registry
                .single_transform(key)
                .map(|transform| (key, transform))
        })
        .collect::<Option<Vec<_>>>();
    if let Some(single_stages) = single_stages {
        for event in batch {
            let mut current = Some(event);
            for (key, transform) in &single_stages {
                let Some(event) = current.take() else {
                    break;
                };
                current =
                    catch_unwind(AssertUnwindSafe(|| transform(event))).unwrap_or_else(|_| {
                        Err(Error::execution(*key, "direct", "user transform panicked"))
                    })?;
                if current.is_none() {
                    metrics.filtered_elements.fetch_add(1, Ordering::Relaxed);
                }
            }
            if let Some(event) = current {
                if config.cancellation.load(Ordering::Acquire) {
                    return Err(cancelled());
                }
                catch_unwind(AssertUnwindSafe(|| fold.add(event))).unwrap_or_else(|_| {
                    Err(Error::execution(fold_key, "direct", "user fold panicked"))
                })?;
            }
        }
        return Ok(());
    }
    for event in batch {
        let mut events = vec![event];
        for key in stages {
            let mut next = Vec::new();
            for event in events {
                let produced = catch_unwind(AssertUnwindSafe(|| registry.apply_many(key, event)))
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
    Ok(())
}

#[cfg(test)]
mod checkpoint_tests {
    use super::*;
    use crate::{CheckpointConfig, DirectRunner};
    use ironbeam_core::{ErrorKind, InputEdge, Output, Pipeline};
    use std::sync::Arc;

    fn manager<T: Element>(output: &Output<T>, config: &CheckpointConfig) -> CheckpointManager {
        let (graph, registry) = output.snapshot().unwrap();
        let plan = crate::planner::lower(&graph, &registry, output.key(), 1).unwrap();
        let identity = Identity::new::<T>(
            plan.summary.graph_fingerprint,
            registry
                .source_config_digest(&plan.summary.source_key)
                .unwrap()
                .to_owned(),
            plan.summary.source_key,
            output.key().to_owned(),
            config.binary_build_id.clone(),
        );
        CheckpointManager::new(config, identity, Arc::new(MetricsState::default())).unwrap()
    }

    #[test]
    fn grouped_fast_routes_decline_incomplete_or_non_linear_graphs() {
        fn declines(graph: &Graph, registry: &Registry, config: &JobConfig) -> bool {
            let metrics = MetricsState::default();
            let mut emit = |_: Envelope, _: &[String]| Ok(());
            !try_drain_fold_sink(graph, registry, "out", config, &metrics, &mut emit).unwrap()
        }

        let pipeline = Pipeline::default();
        let source = pipeline.from_vec("source", vec![(1_i32, 2_i32)]).unwrap();
        let grouped = source.group_by_key_named("group").unwrap();
        grouped
            .map_named("map", |row| row)
            .unwrap()
            .output("out")
            .unwrap();
        let (graph, registry) = pipeline.snapshot().unwrap();
        let config = JobConfig::default();
        assert_eq!(
            checkpointed_group_route(&graph, "out"),
            Some((
                "group".into(),
                "source".into(),
                Vec::new(),
                vec!["map".into()]
            ))
        );
        assert!(declines(
            &graph,
            &registry,
            &JobConfig {
                parallelism: 2,
                ..JobConfig::default()
            }
        ));
        assert!(declines(&graph, &Registry::default(), &config));

        let mut no_parent = graph.clone();
        no_parent
            .nodes
            .iter_mut()
            .find(|node| node.key == "out")
            .unwrap()
            .input = None;
        assert!(checkpointed_group_route(&no_parent, "out").is_none());
        assert!(declines(&no_parent, &registry, &config));

        let mut absent_parent = graph.clone();
        absent_parent
            .nodes
            .iter_mut()
            .find(|node| node.key == "out")
            .unwrap()
            .input = Some("missing".into());
        assert!(checkpointed_group_route(&absent_parent, "out").is_none());
        assert!(declines(&absent_parent, &registry, &config));

        let mut no_fold = graph.clone();
        no_fold
            .nodes
            .iter_mut()
            .find(|node| node.key == "out")
            .unwrap()
            .input = Some("source".into());
        assert!(checkpointed_group_route(&no_fold, "out").is_none());
        assert!(declines(&no_fold, &registry, &config));

        let mut detached_map = graph.clone();
        detached_map
            .nodes
            .iter_mut()
            .find(|node| node.key == "map")
            .unwrap()
            .input = None;
        assert!(checkpointed_group_route(&detached_map, "out").is_none());
        assert!(declines(&detached_map, &registry, &config));

        let mut multi_input_map = graph.clone();
        let map = multi_input_map
            .nodes
            .iter_mut()
            .find(|node| node.key == "map")
            .unwrap();
        map.additional_inputs.push(InputEdge {
            key: "source".into(),
            element: map.input_element.clone().unwrap(),
        });
        assert!(checkpointed_group_route(&multi_input_map, "out").is_none());
        assert!(declines(&multi_input_map, &registry, &config));

        let mut multi_input_group = graph;
        let group = multi_input_group
            .nodes
            .iter_mut()
            .find(|node| node.key == "group")
            .unwrap();
        group.additional_inputs.push(InputEdge {
            key: "source".into(),
            element: group.input_element.clone().unwrap(),
        });
        assert!(checkpointed_group_route(&multi_input_group, "out").is_none());
        assert!(declines(&multi_input_group, &registry, &config));
    }

    #[test]
    fn wait_fast_route_declines_incomplete_or_non_linear_graphs() {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap();
        let pipeline = Pipeline::default();
        let main = pipeline.from_vec("main", vec![1_i32]).unwrap();
        let signal = pipeline.from_vec("signal", vec![2_i32]).unwrap();
        main.wait_on_named("wait", &signal)
            .unwrap()
            .map_named("map", |value| value)
            .unwrap()
            .output("out")
            .unwrap();
        let (graph, registry) = pipeline.snapshot().unwrap();
        let config = JobConfig::default();
        let metrics = MetricsState::default();
        let declines = |graph: &Graph| {
            try_wait_sink_route(&pool, graph, &registry, "out", &config, &metrics)
                .unwrap()
                .is_none()
        };

        let mut no_parent = graph.clone();
        no_parent
            .nodes
            .iter_mut()
            .find(|node| node.key == "out")
            .unwrap()
            .input = None;
        assert!(declines(&no_parent));

        let mut absent_parent = graph.clone();
        absent_parent
            .nodes
            .iter_mut()
            .find(|node| node.key == "out")
            .unwrap()
            .input = Some("missing".into());
        assert!(declines(&absent_parent));

        let mut multi_input_map = graph.clone();
        let map = multi_input_map
            .nodes
            .iter_mut()
            .find(|node| node.key == "map")
            .unwrap();
        map.additional_inputs.push(InputEdge {
            key: "signal".into(),
            element: map.input_element.clone().unwrap(),
        });
        assert!(declines(&multi_input_map));

        let mut detached_map = graph.clone();
        detached_map
            .nodes
            .iter_mut()
            .find(|node| node.key == "map")
            .unwrap()
            .input = None;
        assert!(declines(&detached_map));

        let mut wrong_operation = graph.clone();
        wrong_operation
            .nodes
            .iter_mut()
            .find(|node| node.key == "wait")
            .unwrap()
            .operation = Operation::Reshuffle;
        assert!(declines(&wrong_operation));

        let mut unbounded_wait = graph.clone();
        unbounded_wait
            .nodes
            .iter_mut()
            .find(|node| node.key == "wait")
            .unwrap()
            .boundedness = Boundedness::Unbounded;
        assert!(declines(&unbounded_wait));

        let mut detached_wait = graph;
        detached_wait
            .nodes
            .iter_mut()
            .find(|node| node.key == "wait")
            .unwrap()
            .input = None;
        assert!(declines(&detached_wait));
    }

    #[test]
    fn fold_state_requires_the_same_direct_group_route_and_valid_source_offset() {
        let pipeline = Pipeline::default();
        let source = pipeline.from_vec("source", vec![(1_i32, 2_i32)]).unwrap();
        let direct = source
            .group_by_key_named("group")
            .unwrap()
            .output("out")
            .unwrap();
        let plain = source.output("plain").unwrap();
        let (graph, _) = direct.snapshot().unwrap();
        assert_eq!(
            checkpointed_group_route(&graph, "out"),
            Some(("group".into(), "source".into(), Vec::new(), Vec::new()))
        );
        assert!(checkpointed_group_route(&graph, "source").is_none());
        assert!(checkpointed_group_route(&graph, plain.key()).is_none());
        let dir = tempfile::tempdir().unwrap();
        let checkpoint = CheckpointConfig {
            directory: dir.path().to_path_buf(),
            binary_build_id: "route-test".into(),
            ..CheckpointConfig::default()
        };
        manager(&direct, &checkpoint)
            .save_fold_state(2, b"state", 1024)
            .unwrap();
        let config = JobConfig {
            checkpoint: Some(checkpoint),
            ..JobConfig::default()
        };
        assert_eq!(
            DirectRunner
                .run_collect(&direct, &config)
                .err()
                .unwrap()
                .kind,
            ErrorKind::Recovery
        );

        let pipeline = Pipeline::default();
        let source = pipeline.from_vec("source", vec![(1_i32, 2_i32)]).unwrap();
        let mapped = source.map_named("map", |pair| pair).unwrap();
        let output = mapped
            .group_by_key_named("group")
            .unwrap()
            .output("out")
            .unwrap();
        let (graph, _) = output.snapshot().unwrap();
        assert_eq!(
            checkpointed_group_route(&graph, "out"),
            Some((
                "group".into(),
                "source".into(),
                vec!["map".into()],
                Vec::new()
            ))
        );
        let pipeline = Pipeline::default();
        let source = pipeline.from_vec("source", vec![(1_i32, 2_i32)]).unwrap();
        let shuffled = source.reshuffle_named("shuffle").unwrap();
        let output = shuffled
            .group_by_key_named("group")
            .unwrap()
            .output("out")
            .unwrap();
        let (graph, _) = output.snapshot().unwrap();
        assert!(checkpointed_group_route(&graph, "out").is_none());
        let dir = tempfile::tempdir().unwrap();
        let checkpoint = CheckpointConfig {
            directory: dir.path().to_path_buf(),
            binary_build_id: "route-test".into(),
            ..CheckpointConfig::default()
        };
        manager(&output, &checkpoint)
            .save_fold_state(0, b"state", 1024)
            .unwrap();
        let config = JobConfig {
            checkpoint: Some(checkpoint),
            ..JobConfig::default()
        };
        assert_eq!(
            DirectRunner
                .run_collect(&output, &config)
                .err()
                .unwrap()
                .kind,
            ErrorKind::Recovery
        );
    }

    #[test]
    fn checkpointed_group_preserves_the_materialized_source_budget() {
        let pipeline = Pipeline::default();
        let output = pipeline
            .from_vec("source", vec![(1_i32, 2_i32)])
            .unwrap()
            .group_by_key_named("group")
            .unwrap()
            .output("out")
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let config = JobConfig {
            memory_budget_bytes: 1,
            checkpoint: Some(CheckpointConfig {
                directory: dir.path().to_path_buf(),
                binary_build_id: "budget-test".into(),
                ..CheckpointConfig::default()
            }),
            ..JobConfig::default()
        };
        assert_eq!(
            DirectRunner
                .run_collect(&output, &config)
                .err()
                .unwrap()
                .kind,
            ErrorKind::Resource
        );
    }

    #[test]
    fn checkpointed_group_rejects_state_larger_than_the_job_budget() {
        let pipeline = Pipeline::default();
        let output = pipeline
            .from_vec("source", vec![(1_i32, "x".repeat(5000))])
            .unwrap()
            .group_by_key_named("group")
            .unwrap()
            .output("out")
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let config = JobConfig {
            memory_budget_bytes: 1024,
            checkpoint: Some(CheckpointConfig {
                directory: directory.path().to_path_buf(),
                binary_build_id: "state-budget-test".into(),
                ..CheckpointConfig::default()
            }),
            ..JobConfig::default()
        };
        assert_eq!(
            DirectRunner
                .run_collect(&output, &config)
                .err()
                .unwrap()
                .kind,
            ErrorKind::Resource
        );
        assert!(!directory.path().join("latest.json").exists());
    }

    #[test]
    fn checkpointed_group_rejects_invalid_fold_payload() {
        let pipeline = Pipeline::default();
        let output = pipeline
            .from_vec("source", vec![(1_i32, 2_i32)])
            .unwrap()
            .group_by_key_named("group")
            .unwrap()
            .output("out")
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let checkpoint = CheckpointConfig {
            directory: dir.path().to_path_buf(),
            binary_build_id: "corrupt-state-test".into(),
            ..CheckpointConfig::default()
        };
        manager(&output, &checkpoint)
            .save_fold_state(0, b"invalid fold payload", 1024)
            .unwrap();
        let config = JobConfig {
            checkpoint: Some(checkpoint),
            ..JobConfig::default()
        };
        assert_eq!(
            DirectRunner
                .run_collect(&output, &config)
                .err()
                .unwrap()
                .kind,
            ErrorKind::Recovery
        );
    }

    #[test]
    fn staged_group_attributes_source_clone_panics_to_the_source() {
        struct PanicClone(i32);
        impl Clone for PanicClone {
            fn clone(&self) -> Self {
                panic!("source clone failed");
            }
        }
        impl Element for PanicClone {
            const TYPE_ID: &'static str = "test/staged-group-panic-clone";
            const CODER_ID: &'static str = "test/staged-group-panic-clone-i32";
            fn supports_coder() -> bool {
                true
            }
            fn encode(&self) -> Result<Vec<u8>, Error> {
                Ok(self.0.to_le_bytes().to_vec())
            }
            fn decode(bytes: &[u8]) -> Result<Self, Error> {
                Ok(Self(i32::decode(bytes)?))
            }
        }
        let pipeline = Pipeline::default();
        let output = pipeline
            .from_vec("source", vec![(1_i32, PanicClone(2))])
            .unwrap()
            .map_named("map", |pair| pair)
            .unwrap()
            .group_by_key_named("group")
            .unwrap()
            .output("out")
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let error = DirectRunner
            .run_collect(
                &output,
                &JobConfig {
                    checkpoint: Some(CheckpointConfig {
                        directory: directory.path().to_path_buf(),
                        binary_build_id: "clone-panic-test".into(),
                        ..CheckpointConfig::default()
                    }),
                    ..JobConfig::default()
                },
            )
            .err()
            .unwrap();
        assert!(matches!(error.kind, ErrorKind::Execution));
        assert_eq!(error.node.as_deref(), Some("source"));
    }

    #[test]
    fn staged_group_attributes_transform_panics_to_the_transform() {
        let pipeline = Pipeline::default();
        let output = pipeline
            .from_vec("source", vec![(1_i32, 2_i32)])
            .unwrap()
            .map_named("panic-map", |_: (i32, i32)| -> (i32, i32) {
                panic!("map failed");
            })
            .unwrap()
            .group_by_key_named("group")
            .unwrap()
            .output("out")
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let error = DirectRunner
            .run_collect(
                &output,
                &JobConfig {
                    checkpoint: Some(CheckpointConfig {
                        directory: directory.path().to_path_buf(),
                        binary_build_id: "map-panic-test".into(),
                        ..CheckpointConfig::default()
                    }),
                    ..JobConfig::default()
                },
            )
            .err()
            .unwrap();
        assert!(matches!(error.kind, ErrorKind::Execution));
        assert_eq!(error.node.as_deref(), Some("panic-map"));
    }
}
