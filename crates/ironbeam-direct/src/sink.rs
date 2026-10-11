//! Direct sink execution without a whole-job output collection.

use crate::materialized;
use crate::planner::PhysicalPlan;
use crate::{JobConfig, JobHandle, cancelled, open_source_safe, read_source_batch};
use ironbeam_core::{Element, Envelope, Error, Graph, Registry, SinkSession, Window};
use rayon::ThreadPool;
use rayon::prelude::*;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::Ordering;

fn write_event(
    session: &mut dyn SinkSession,
    key: &str,
    event: Envelope,
    config: &JobConfig,
) -> Result<(), Error> {
    if config.cancellation.load(Ordering::Acquire) {
        return Err(cancelled());
    }
    catch_unwind(AssertUnwindSafe(|| session.write(event)))
        .unwrap_or_else(|_| Err(Error::execution(key, "direct", "sink writer panicked")))?;
    Ok(())
}

fn record_written(handle: &JobHandle, count: u64) {
    let total = handle
        .metrics
        .output_elements
        .fetch_add(count, Ordering::Relaxed)
        .saturating_add(count);
    handle.processed.store(total, Ordering::Relaxed);
}

fn apply_route(
    input: Envelope,
    operations: &[String],
    registry: &Registry,
    config: &JobConfig,
    handle: &JobHandle,
) -> Result<Vec<Envelope>, Error> {
    let mut events = vec![input];
    for key in operations {
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
                handle
                    .metrics
                    .filtered_elements
                    .fetch_add(1, Ordering::Relaxed);
            }
            next.extend(produced);
        }
        events = next;
    }
    Ok(events)
}

pub(crate) fn execute<T: Element>(
    pool: &ThreadPool,
    graph: &Graph,
    registry: &Registry,
    plan: &PhysicalPlan,
    config: &JobConfig,
    handle: &JobHandle,
    session: &mut dyn SinkSession,
) -> Result<(), Error> {
    let sink_key = &plan.summary.output_key;
    if plan.materialized {
        let mut written = 0_u64;
        if materialized::try_drain_group_sink(
            graph,
            registry,
            sink_key,
            config,
            &handle.metrics,
            &mut |event| {
                write_event(session, sink_key, event, config)?;
                written = written.saturating_add(1);
                Ok(())
            },
        )? {
            record_written(handle, written);
            return Ok(());
        }
        for event in materialized::eval(pool, graph, registry, sink_key, config, &handle.metrics)? {
            write_event(session, sink_key, event, config)?;
            written = written.saturating_add(1);
        }
        record_written(handle, written);
        return Ok(());
    }
    if let [route] = plan.routes.as_slice()
        && route.operations.is_empty()
        && let Some((source_len, read_batch)) = registry.vec_source_batches::<T>(&route.source_key)
    {
        for start in (0..source_len).step_by(plan.summary.batch_limit) {
            if config.cancellation.load(Ordering::Acquire) {
                return Err(cancelled());
            }
            let len = (source_len - start).min(plan.summary.batch_limit);
            let batch =
                catch_unwind(AssertUnwindSafe(|| read_batch(start, len))).unwrap_or_else(|_| {
                    Err(Error::execution(
                        &route.source_key,
                        "direct",
                        "source value clone panicked",
                    ))
                })?;
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
            for value in batch {
                write_event(
                    session,
                    sink_key,
                    Envelope {
                        value: Box::new(value),
                        timestamp_ns: 0,
                        window: Window::Global,
                        assigned_window: None,
                    },
                    config,
                )?;
            }
            record_written(handle, count);
        }
        return Ok(());
    }
    if config.parallelism == 1
        && let [route] = plan.routes.as_slice()
        && !route.operations.is_empty()
        && let Some((source_len, read_batch)) = registry.vec_source_batches::<T>(&route.source_key)
        && let Some(transforms) = route
            .operations
            .iter()
            .map(|key| registry.typed_value_transform::<T>(key))
            .collect::<Option<Vec<_>>>()
    {
        for start in (0..source_len).step_by(plan.summary.batch_limit) {
            if config.cancellation.load(Ordering::Acquire) {
                return Err(cancelled());
            }
            let len = (source_len - start).min(plan.summary.batch_limit);
            let batch =
                catch_unwind(AssertUnwindSafe(|| read_batch(start, len))).unwrap_or_else(|_| {
                    Err(Error::execution(
                        &route.source_key,
                        "direct",
                        "source value clone panicked",
                    ))
                })?;
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
            let mut filtered = 0_u64;
            let mut written = 0_u64;
            for value in batch {
                let mut current = Some(value);
                for (key, transform) in route.operations.iter().zip(&transforms) {
                    let Some(value) = current else { break };
                    if config.cancellation.load(Ordering::Acquire) {
                        return Err(cancelled());
                    }
                    current = catch_unwind(AssertUnwindSafe(|| transform.apply(value)))
                        .map_err(|_| Error::execution(key, "direct", "user transform panicked"))?;
                    if current.is_none() {
                        filtered = filtered.saturating_add(1);
                    }
                }
                if let Some(value) = current {
                    write_event(
                        session,
                        sink_key,
                        Envelope {
                            value: Box::new(value),
                            timestamp_ns: 0,
                            window: Window::Global,
                            assigned_window: None,
                        },
                        config,
                    )?;
                    written = written.saturating_add(1);
                }
            }
            handle
                .metrics
                .filtered_elements
                .fetch_add(filtered, Ordering::Relaxed);
            record_written(handle, written);
        }
        return Ok(());
    }
    for route in &plan.routes {
        let mut source = open_source_safe(registry, &route.source_key)?;
        loop {
            if config.cancellation.load(Ordering::Acquire) {
                return Err(cancelled());
            }
            let batch =
                read_source_batch(&mut source, plan.summary.batch_limit, &route.source_key)?;
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
            if config.parallelism == 1 || route.operations.is_empty() {
                for input in batch {
                    for event in apply_route(input, &route.operations, registry, config, handle)? {
                        write_event(session, sink_key, event, config)?;
                        written = written.saturating_add(1);
                    }
                }
            } else {
                let results: Vec<_> = pool.install(|| {
                    batch
                        .into_par_iter()
                        .map(|input| {
                            apply_route(input, &route.operations, registry, config, handle)
                        })
                        .collect()
                });
                for result in results {
                    for event in result? {
                        write_event(session, sink_key, event, config)?;
                        written = written.saturating_add(1);
                    }
                }
            }
            record_written(handle, written);
        }
    }
    Ok(())
}
