//! Direct physical path for bounded grouping, joins, and global combines.
use crate::checkpoint::{CheckpointManager, Identity};
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
        && let Some((group_key, source_key, stages)) = checkpointed_group_route(graph, output_key)
    {
        eval_checkpointed_group(
            registry,
            &group_key,
            &source_key,
            &stages,
            *source_len,
            pending_state.take(),
            manager,
            config,
            &handle.metrics,
        )?
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
) -> Option<(String, String, Vec<String>)> {
    let output = graph.node(output_key)?;
    if !matches!(output.operation, Operation::Output) {
        return None;
    }
    let group = graph.node(output.input.as_deref()?)?;
    if !matches!(group.operation, Operation::GroupByKey) || !group.additional_inputs.is_empty() {
        return None;
    }
    let (source_key, stages) = linear_fold_route(graph, group.input.as_deref()?)?;
    Some((group.key.clone(), source_key, stages))
}

#[allow(clippy::too_many_arguments)]
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
    let mut offset = 0_usize;
    if let Some((saved_offset, state)) = pending_state {
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
        offset = saved_offset;
    }
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

/// Drain a directly connected GroupByKey fold into a sink without assembling
/// all completed groups into a second output vector. Other graph shapes use
/// the general materialized evaluator.
pub(crate) fn try_drain_group_sink(
    graph: &Graph,
    registry: &Registry,
    sink_key: &str,
    config: &JobConfig,
    metrics: &MetricsState,
    emit: &mut dyn FnMut(Envelope) -> Result<(), Error>,
) -> Result<bool, Error> {
    let Some(group_key) = graph.node(sink_key).and_then(|node| node.input.as_deref()) else {
        return Ok(false);
    };
    let Some(group) = graph.node(group_key) else {
        return Ok(false);
    };
    if !matches!(group.operation, Operation::GroupByKey)
        || !group.additional_inputs.is_empty()
        || !registry.has_fold(group_key)
    {
        return Ok(false);
    }
    let Some((source_key, stages)) = group
        .input
        .as_deref()
        .and_then(|input| linear_fold_route(graph, input))
    else {
        return Ok(false);
    };
    let mut fold = catch_unwind(AssertUnwindSafe(|| registry.open_fold(group_key)))
        .unwrap_or_else(|_| {
            Err(Error::execution(
                group_key,
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
        group_key,
        fold.as_mut(),
    )?;
    catch_unwind(AssertUnwindSafe(|| fold.finish_into(emit)))
        .unwrap_or_else(|_| Err(Error::execution(group_key, "direct", "user fold panicked")))?;
    Ok(true)
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
    let mut source_count = 0_usize;
    let enforce_source_budget = registry.has_group_coder(fold_key);
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
    use ironbeam_core::{ErrorKind, Output, Pipeline};
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
            Some(("group".into(), "source".into(), Vec::new()))
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
            Some(("group".into(), "source".into(), vec!["map".into()]))
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
