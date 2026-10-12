//! Multi-source Direct streaming with route-local progress and shared window checkpoints.

use super::*;

const MULTI_SOURCE_KEY: &str = "ironbeam:multi-source";

#[derive(Serialize, Deserialize)]
struct MultiSourceState {
    version: u8,
    sources: Vec<Option<ProgressSignal>>,
}

#[derive(Serialize, Deserialize)]
struct DualWindowState {
    version: u8,
    sources: [Option<ProgressSignal>; 2],
    main: Vec<u8>,
    side: Vec<u8>,
}

fn validate_dual_source(graph: &Graph, registry: &Registry, key: &str) -> Result<(), Error> {
    let node = graph
        .node(key)
        .ok_or_else(|| Error::graph(key, "stream source node missing"))?;
    let Operation::Source { progress } = &node.operation else {
        return Err(Error::graph(key, "stream route has no source"));
    };
    if node.boundedness != ironbeam_core::Boundedness::Unbounded
        || !progress.stable_split_id
        || !progress.reports_event_time
        || progress.replay == Replay::None
        || !registry.has_unbounded_source(key)
    {
        return Err(Error::unsupported(
            key,
            "direct",
            "resumable-watermarked-stream-source",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
pub(super) fn start_dual_side_stream<T: Element>(
    graph: &Graph,
    registry: Registry,
    plan: PhysicalPlan,
    sink_key: String,
    config: &JobConfig,
    route: BoundedSideStreamRoute,
) -> Result<StreamJob, Error> {
    if route.source_key == route.side_source_key {
        return Err(Error::unsupported(
            &route.aggregate_key,
            "direct",
            "shared-dual-stream-source",
        ));
    }
    for key in [&route.source_key, &route.side_source_key] {
        validate_dual_source(graph, &registry, key)?;
    }
    if config.checkpoint.is_some() {
        for key in [&route.main_input_key, &route.side_input_key] {
            if !registry.has_coder(key) {
                return Err(Error::unsupported(
                    key,
                    "direct",
                    "stream-window-state-coder",
                ));
            }
        }
    }
    let checkpoint = config
        .checkpoint
        .as_ref()
        .map(|checkpoint| {
            let mut digest = String::new();
            for key in [&route.source_key, &route.side_source_key] {
                let part = registry
                    .source_config_digest(key)
                    .ok_or_else(|| Error::unsupported(key, "direct", "source-config-digest"))?;
                digest.push_str(&format!("{}:{}{}:{}", key.len(), key, part.len(), part));
            }
            StreamCheckpoint::new::<T>(
                checkpoint,
                &plan.summary.graph_fingerprint,
                &digest,
                registry
                    .sink_config_digest(&sink_key)
                    .ok_or_else(|| Error::unsupported(&sink_key, "direct", "sink-config-digest"))?,
                MULTI_SOURCE_KEY,
                &sink_key,
            )
        })
        .transpose()?;
    let saved = checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.load_state(config.memory_budget_bytes))
        .transpose()?
        .flatten();
    let (sequence, progress_by_source, main_windows, side_windows, buffered) =
        if let Some((combined, bytes)) = &saved {
            let state: DualWindowState = serde_json::from_slice(bytes)
                .map_err(|error| Error::recovery("checkpoint", error.to_string()))?;
            if state.version != 1
                || state.sources[0]
                    .as_ref()
                    .is_some_and(|p| p.source_key != route.source_key || p.split_id.is_empty())
                || state.sources[1]
                    .as_ref()
                    .is_some_and(|p| p.source_key != route.side_source_key || p.split_id.is_empty())
            {
                return Err(Error::incompatible(
                    "dual stream checkpoint identity mismatch",
                ));
            }
            let sequence = u64::from_le_bytes(
                combined
                    .checkpoint_position
                    .as_deref()
                    .ok_or_else(|| Error::recovery("checkpoint", "missing dual stream sequence"))?
                    .try_into()
                    .map_err(|_| Error::recovery("checkpoint", "invalid dual stream sequence"))?,
            );
            if *combined != multi_progress(sequence, &state.sources) {
                return Err(Error::recovery(
                    "checkpoint",
                    "dual stream cursors do not match progress",
                ));
            }
            let (main, main_count) = restore_window_rows(
                &registry,
                &route.main_input_key,
                &state.main,
                config.buffer_budget_elements,
            )?;
            let (side, side_count) = restore_window_rows(
                &registry,
                &route.side_input_key,
                &state.side,
                config.buffer_budget_elements,
            )?;
            let buffered = main_count.checked_add(side_count).ok_or_else(|| {
                Error::resource("checkpoint", "dual stream state exceeds buffer budget")
            })?;
            if buffered > config.buffer_budget_elements || (combined.completed && buffered != 0) {
                return Err(Error::recovery(
                    "checkpoint",
                    "invalid dual stream open-window state",
                ));
            }
            (sequence, state.sources, main, side, buffered)
        } else {
            (0, [None, None], BTreeMap::new(), BTreeMap::new(), 0)
        };
    #[allow(unused_mut)]
    let mut handle = JobHandle::new(config.cancellation.clone(), JobStatus::Submitted);
    #[cfg(feature = "metrics")]
    {
        handle
            .metrics_collector
            .clone_from(&config.metrics_collector);
    }
    if config.cancellation.load(Ordering::Acquire) {
        return Err(cancelled());
    }
    let recovered = saved.map(|(progress, _)| progress);
    if recovered
        .as_ref()
        .is_some_and(|progress| progress.completed)
    {
        handle.set_status(JobStatus::Succeeded);
        return Ok(StreamJob {
            handle,
            progress: Arc::new(Mutex::new(recovered)),
            worker: None,
        });
    }
    let progress = Arc::new(Mutex::new(recovered));
    let worker_progress = Arc::clone(&progress);
    let worker_handle = handle.clone();
    let worker_config = config.clone();
    handle.set_status(JobStatus::Running);
    let worker = std::thread::Builder::new()
        .name(format!("ironbeam-stream-{}", handle.id()))
        .spawn(move || {
            let result = catch_unwind(AssertUnwindSafe(|| {
                run_dual_side_stream(
                    &registry,
                    &route,
                    &plan.summary.graph_fingerprint,
                    &sink_key,
                    &worker_config,
                    &worker_handle,
                    &worker_progress,
                    checkpoint.as_ref(),
                    sequence,
                    progress_by_source,
                    main_windows,
                    side_windows,
                    buffered,
                )
            }))
            .unwrap_or_else(|_| Err(Error::execution("job", "direct", "stream worker panicked")));
            worker_handle.set_status(match &result {
                Ok(()) => JobStatus::Succeeded,
                Err(error) if error.kind == ErrorKind::Cancelled => JobStatus::Cancelled,
                Err(_) => JobStatus::Failed,
            });
            result
        })
        .map_err(|error| Error::execution("job", "direct", error.to_string()))?;
    Ok(StreamJob {
        handle,
        progress,
        worker: Some(worker),
    })
}

fn multi_progress(sequence: u64, sources: &[Option<ProgressSignal>]) -> ProgressSignal {
    let active_watermarks = sources
        .iter()
        .filter(|signal| !signal.as_ref().is_some_and(|signal| signal.completed))
        .map(|signal| signal.as_ref().and_then(|signal| signal.event_time_ns))
        .collect::<Option<Vec<_>>>();
    ProgressSignal {
        source_key: MULTI_SOURCE_KEY.into(),
        split_id: "routes".into(),
        checkpoint_position: Some(sequence.to_le_bytes().to_vec()),
        event_time_ns: active_watermarks.and_then(|watermarks| watermarks.into_iter().min()),
        completed: sources
            .iter()
            .all(|signal| signal.as_ref().is_some_and(|signal| signal.completed)),
    }
}

#[allow(clippy::too_many_lines)]
pub(super) fn start_multi_stream_sink<T: Element>(
    graph: &Graph,
    registry: Registry,
    plan: PhysicalPlan,
    sink_key: String,
    config: &JobConfig,
) -> Result<StreamJob, Error> {
    let mut source_digest = String::new();
    for route in &plan.routes {
        let node = graph
            .node(&route.source_key)
            .ok_or_else(|| Error::graph(&route.source_key, "stream source node missing"))?;
        let Operation::Source { progress } = &node.operation else {
            return Err(Error::graph(
                &route.source_key,
                "stream route has no source",
            ));
        };
        if node.boundedness != ironbeam_core::Boundedness::Unbounded
            || !progress.stable_split_id
            || progress.replay == Replay::None
            || !registry.has_unbounded_source(&route.source_key)
        {
            return Err(Error::unsupported(
                &route.source_key,
                "direct",
                "resumable-stream-source",
            ));
        }
        if config.checkpoint.is_some() {
            let digest = registry
                .source_config_digest(&route.source_key)
                .ok_or_else(|| {
                    Error::unsupported(&route.source_key, "direct", "source-config-digest")
                })?;
            source_digest.push_str(&format!(
                "{}:{}{}:{}",
                route.source_key.len(),
                route.source_key,
                digest.len(),
                digest
            ));
        }
    }
    let checkpoint = config
        .checkpoint
        .as_ref()
        .map(|checkpoint| {
            StreamCheckpoint::new::<T>(
                checkpoint,
                &plan.summary.graph_fingerprint,
                &source_digest,
                registry
                    .sink_config_digest(&sink_key)
                    .ok_or_else(|| Error::unsupported(&sink_key, "direct", "sink-config-digest"))?,
                MULTI_SOURCE_KEY,
                &sink_key,
            )
        })
        .transpose()?;
    let saved = checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.load_state(config.memory_budget_bytes))
        .transpose()?
        .flatten();
    let (sequence, source_progress) = if let Some((progress, bytes)) = saved.as_ref() {
        let state: MultiSourceState = serde_json::from_slice(bytes)
            .map_err(|error| Error::recovery("checkpoint", error.to_string()))?;
        if state.version != 1
            || state.sources.len() != plan.routes.len()
            || state
                .sources
                .iter()
                .zip(&plan.routes)
                .any(|(signal, route)| {
                    signal.as_ref().is_some_and(|signal| {
                        signal.source_key != route.source_key || signal.split_id.is_empty()
                    })
                })
        {
            return Err(Error::incompatible(
                "multi-source stream checkpoint identity mismatch",
            ));
        }
        let cursor = progress
            .checkpoint_position
            .as_deref()
            .ok_or_else(|| Error::recovery("checkpoint", "missing multi-source sequence"))?;
        let sequence = u64::from_le_bytes(
            cursor
                .try_into()
                .map_err(|_| Error::recovery("checkpoint", "invalid multi-source sequence"))?,
        );
        if *progress != multi_progress(sequence, &state.sources) {
            return Err(Error::recovery(
                "checkpoint",
                "multi-source progress does not match cursors",
            ));
        }
        (sequence, state.sources)
    } else {
        (0, vec![None; plan.routes.len()])
    };
    #[allow(unused_mut)]
    let mut handle = JobHandle::new(config.cancellation.clone(), JobStatus::Submitted);
    #[cfg(feature = "metrics")]
    {
        handle
            .metrics_collector
            .clone_from(&config.metrics_collector);
    }
    if config.cancellation.load(Ordering::Acquire) {
        return Err(cancelled());
    }
    let recovered = saved.as_ref().map(|(progress, _)| progress.clone());
    if recovered
        .as_ref()
        .is_some_and(|progress| progress.completed)
    {
        handle.set_status(JobStatus::Succeeded);
        return Ok(StreamJob {
            handle,
            progress: Arc::new(Mutex::new(recovered)),
            worker: None,
        });
    }
    let progress = Arc::new(Mutex::new(recovered));
    let worker_progress = Arc::clone(&progress);
    let worker_handle = handle.clone();
    let worker_config = config.clone();
    handle.set_status(JobStatus::Running);
    let worker = std::thread::Builder::new()
        .name(format!("ironbeam-stream-{}", handle.id()))
        .spawn(move || {
            let result = catch_unwind(AssertUnwindSafe(|| {
                run_multi_stream(
                    &registry,
                    &plan,
                    &sink_key,
                    &worker_config,
                    &worker_handle,
                    &worker_progress,
                    checkpoint.as_ref(),
                    sequence,
                    source_progress,
                )
            }))
            .unwrap_or_else(|_| Err(Error::execution("job", "direct", "stream worker panicked")));
            worker_handle.set_status(match &result {
                Ok(()) => JobStatus::Succeeded,
                Err(error) if error.kind == ErrorKind::Cancelled => JobStatus::Cancelled,
                Err(_) => JobStatus::Failed,
            });
            result
        })
        .map_err(|error| Error::execution("job", "direct", error.to_string()))?;
    Ok(StreamJob {
        handle,
        progress,
        worker: Some(worker),
    })
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn run_multi_stream(
    registry: &Registry,
    plan: &PhysicalPlan,
    sink_key: &str,
    config: &JobConfig,
    handle: &JobHandle,
    shared_progress: &Arc<Mutex<Option<ProgressSignal>>>,
    checkpoint: Option<&StreamCheckpoint>,
    mut sequence: u64,
    mut source_progress: Vec<Option<ProgressSignal>>,
) -> Result<(), Error> {
    let mut readers = plan
        .routes
        .iter()
        .zip(&source_progress)
        .map(|(route, progress)| {
            if progress.as_ref().is_some_and(|progress| progress.completed) {
                Ok(None)
            } else {
                catch_unwind(AssertUnwindSafe(|| {
                    registry.open_unbounded_source(&route.source_key, progress.clone())
                }))
                .unwrap_or_else(|_| {
                    Err(Error::execution(
                        &route.source_key,
                        "direct",
                        "source factory panicked",
                    ))
                })
                .map(Some)
            }
        })
        .collect::<Result<Vec<_>, Error>>()?;
    loop {
        let mut ready = false;
        for (index, route) in plan.routes.iter().enumerate() {
            if config.cancellation.load(Ordering::Acquire) {
                return Err(cancelled());
            }
            let Some(reader) = readers[index].as_mut() else {
                continue;
            };
            let polled = catch_unwind(AssertUnwindSafe(|| reader.poll(plan.summary.batch_limit)))
                .unwrap_or_else(|_| {
                Err(Error::execution(
                    &route.source_key,
                    "direct",
                    "source poll panicked",
                ))
            })?;
            let UnboundedPoll::Ready(batch) = polled else {
                continue;
            };
            ready = true;
            if source_progress[index]
                .as_ref()
                .is_some_and(|previous| previous.split_id != batch.progress.split_id)
            {
                return Err(Error::execution(
                    &route.source_key,
                    "source",
                    "source split changed after checkpoint",
                ));
            }
            if !batch.elements.is_empty()
                && source_progress[index].as_ref().is_some_and(|previous| {
                    previous.checkpoint_position == batch.progress.checkpoint_position
                })
            {
                return Err(Error::execution(
                    &route.source_key,
                    "source",
                    "data batch did not advance its cursor",
                ));
            }
            let instruction_id = bundle_id(
                &format!("{}:{index}", plan.summary.graph_fingerprint),
                &[sink_key],
                &batch.progress,
            );
            let count = u64::try_from(batch.elements.len()).unwrap_or(u64::MAX);
            handle
                .metrics
                .source_elements
                .fetch_add(count, Ordering::Relaxed);
            handle
                .metrics
                .peak_batch_elements
                .fetch_max(count, Ordering::Relaxed);
            handle.metrics.batches.fetch_add(1, Ordering::Relaxed);
            let mut session = None;
            let mut written = 0_u64;
            let result = (|| {
                for input in batch.elements {
                    let events =
                        apply_grouped_stages(registry, &route.operations, input, config, handle)?;
                    for event in events {
                        if config.cancellation.load(Ordering::Acquire) {
                            return Err(cancelled());
                        }
                        if session.is_none() {
                            session = Some(begin_sink_bundle(registry, sink_key, &instruction_id)?);
                        }
                        catch_unwind(AssertUnwindSafe(|| {
                            session.as_mut().expect("opened sink").write(event)
                        }))
                        .unwrap_or_else(|_| {
                            Err(Error::execution(sink_key, "direct", "sink writer panicked"))
                        })?;
                        written = written.saturating_add(1);
                    }
                }
                if config.cancellation.load(Ordering::Acquire) {
                    return Err(cancelled());
                }
                if let Some(session) = &mut session {
                    catch_unwind(AssertUnwindSafe(|| session.commit())).unwrap_or_else(|_| {
                        Err(Error::execution(sink_key, "direct", "sink commit panicked"))
                    })?;
                }
                Ok::<(), Error>(())
            })();
            if let Err(error) = result {
                if let Some(session) = &mut session {
                    let _ = catch_unwind(AssertUnwindSafe(|| session.abort()));
                }
                return Err(error);
            }
            let next = merge_progress(source_progress[index].as_ref(), batch.progress);
            if next.completed {
                readers[index] = None;
            }
            if next.checkpoint_position.is_none() && !next.completed {
                continue;
            }
            source_progress[index] = Some(next);
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| Error::resource("checkpoint", "multi-source sequence exhausted"))?;
            let combined = multi_progress(sequence, &source_progress);
            if let Some(checkpoint) = checkpoint {
                let state = serde_json::to_vec(&MultiSourceState {
                    version: 1,
                    sources: source_progress.clone(),
                })
                .map_err(|error| Error::recovery("checkpoint", error.to_string()))?;
                checkpoint.save_state(&combined, &state, config.memory_budget_bytes)?;
            }
            let total = handle
                .metrics
                .output_elements
                .fetch_add(written, Ordering::Relaxed)
                .saturating_add(written);
            handle.processed.store(total, Ordering::Relaxed);
            *shared_progress
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(combined.clone());
            if combined.completed {
                return Ok(());
            }
        }
        if !ready {
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

fn dual_watermark(progress: &[Option<ProgressSignal>; 2]) -> Option<i64> {
    let mut watermark = i64::MAX;
    for source in progress {
        let source = source.as_ref()?;
        if !source.completed {
            watermark = watermark.min(source.event_time_ns?);
        }
    }
    Some(watermark)
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn run_dual_side_stream(
    registry: &Registry,
    route: &BoundedSideStreamRoute,
    graph_fingerprint: &str,
    sink_key: &str,
    config: &JobConfig,
    handle: &JobHandle,
    shared_progress: &Arc<Mutex<Option<ProgressSignal>>>,
    checkpoint: Option<&StreamCheckpoint>,
    mut sequence: u64,
    mut progress: [Option<ProgressSignal>; 2],
    mut main_windows: WindowRows,
    mut side_windows: WindowRows,
    mut buffered: usize,
) -> Result<(), Error> {
    let source_keys = [&route.source_key, &route.side_source_key];
    let stages = [&route.main_stages, &route.side_stages];
    let mut readers = source_keys
        .iter()
        .zip(&progress)
        .map(|(key, saved)| {
            if saved.as_ref().is_some_and(|saved| saved.completed) {
                Ok(None)
            } else {
                catch_unwind(AssertUnwindSafe(|| {
                    registry.open_unbounded_source(
                        key,
                        saved
                            .clone()
                            .filter(|signal| signal.checkpoint_position.is_some()),
                    )
                }))
                .unwrap_or_else(|_| {
                    Err(Error::execution(*key, "direct", "source factory panicked"))
                })
                .map(Some)
            }
        })
        .collect::<Result<Vec<_>, Error>>()?;
    loop {
        let mut ready = false;
        for index in 0..2 {
            if config.cancellation.load(Ordering::Acquire) {
                return Err(cancelled());
            }
            let Some(reader) = readers[index].as_mut() else {
                continue;
            };
            let polled = catch_unwind(AssertUnwindSafe(|| {
                reader.poll(config.buffer_budget_elements)
            }))
            .unwrap_or_else(|_| {
                Err(Error::execution(
                    source_keys[index],
                    "direct",
                    "source poll panicked",
                ))
            })?;
            let UnboundedPoll::Ready(batch) = polled else {
                continue;
            };
            ready = true;
            if progress[index]
                .as_ref()
                .is_some_and(|previous| previous.split_id != batch.progress.split_id)
            {
                return Err(Error::execution(
                    source_keys[index],
                    "source",
                    "source split changed after checkpoint",
                ));
            }
            if !batch.elements.is_empty()
                && progress[index].as_ref().is_some_and(|previous| {
                    previous.checkpoint_position == batch.progress.checkpoint_position
                })
            {
                return Err(Error::execution(
                    source_keys[index],
                    "source",
                    "data batch did not advance its cursor",
                ));
            }
            let count = u64::try_from(batch.elements.len()).unwrap_or(u64::MAX);
            handle
                .metrics
                .source_elements
                .fetch_add(count, Ordering::Relaxed);
            handle
                .metrics
                .peak_batch_elements
                .fetch_max(count, Ordering::Relaxed);
            handle.metrics.batches.fetch_add(1, Ordering::Relaxed);
            let old_watermark = dual_watermark(&progress);
            let windows = if index == 0 {
                &mut main_windows
            } else {
                &mut side_windows
            };
            for input in batch.elements {
                for event in apply_grouped_stages(registry, stages[index], input, config, handle)? {
                    let window = stream_window(&event, &route.aggregate_key)?;
                    if old_watermark.is_some_and(|watermark| window.1 <= watermark) {
                        continue;
                    }
                    if buffered >= config.buffer_budget_elements {
                        return Err(Error::resource(
                            &route.aggregate_key,
                            "stateful stream input exceeds buffer budget",
                        ));
                    }
                    windows.entry(window).or_default().push(event);
                    buffered += 1;
                }
            }
            let next = merge_progress(progress[index].as_ref(), batch.progress);
            if next.completed {
                readers[index] = None;
            }
            progress[index] = Some(next);
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| Error::resource("checkpoint", "dual stream sequence exhausted"))?;
            let combined = multi_progress(sequence, &progress);
            let closing = main_windows
                .keys()
                .chain(side_windows.keys())
                .copied()
                .filter(|(_, end)| {
                    dual_watermark(&progress).is_some_and(|watermark| *end <= watermark)
                })
                .collect::<BTreeSet<_>>();
            let instruction_id = bundle_id(graph_fingerprint, &[sink_key], &combined);
            let mut session = None;
            let mut written = 0_u64;
            let result = (|| {
                for window in closing {
                    let main = main_windows.remove(&window).unwrap_or_default();
                    let side = side_windows.remove(&window).unwrap_or_default();
                    buffered -= main.len() + side.len();
                    let grouped = catch_unwind(AssertUnwindSafe(|| {
                        registry.apply_aggregate(&route.aggregate_key, vec![main, side])
                    }))
                    .unwrap_or_else(|_| {
                        Err(Error::execution(
                            &route.aggregate_key,
                            "direct",
                            "user aggregate panicked",
                        ))
                    })?;
                    for event in grouped {
                        for output in apply_grouped_stages(
                            registry,
                            &route.after_aggregate,
                            event,
                            config,
                            handle,
                        )? {
                            if session.is_none() {
                                session =
                                    Some(begin_sink_bundle(registry, sink_key, &instruction_id)?);
                            }
                            catch_unwind(AssertUnwindSafe(|| {
                                session.as_mut().expect("opened sink").write(output)
                            }))
                            .unwrap_or_else(|_| {
                                Err(Error::execution(sink_key, "direct", "sink writer panicked"))
                            })?;
                            written = written.saturating_add(1);
                        }
                    }
                }
                if config.cancellation.load(Ordering::Acquire) {
                    return Err(cancelled());
                }
                if let Some(session) = &mut session {
                    catch_unwind(AssertUnwindSafe(|| session.commit())).unwrap_or_else(|_| {
                        Err(Error::execution(sink_key, "direct", "sink commit panicked"))
                    })?;
                }
                Ok::<(), Error>(())
            })();
            if let Err(error) = result {
                if let Some(session) = &mut session {
                    let _ = catch_unwind(AssertUnwindSafe(|| session.abort()));
                }
                return Err(error);
            }
            if let Some(checkpoint) = checkpoint {
                let state = serde_json::to_vec(&DualWindowState {
                    version: 1,
                    sources: progress.clone(),
                    main: encode_window_rows(
                        registry,
                        &route.main_input_key,
                        &main_windows,
                        config.memory_budget_bytes,
                    )?,
                    side: encode_window_rows(
                        registry,
                        &route.side_input_key,
                        &side_windows,
                        config.memory_budget_bytes,
                    )?,
                })
                .map_err(|error| Error::recovery("checkpoint", error.to_string()))?;
                checkpoint.save_state(&combined, &state, config.memory_budget_bytes)?;
            }
            let total = handle
                .metrics
                .output_elements
                .fetch_add(written, Ordering::Relaxed)
                .saturating_add(written);
            handle.processed.store(total, Ordering::Relaxed);
            *shared_progress
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(combined.clone());
            if combined.completed {
                return Ok(());
            }
        }
        if !ready {
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CheckpointConfig;
    use ironbeam_core::{
        Boundedness, Pipeline, SinkSpec, SourceProgress, TestStreamEvent, TypedSink,
    };

    struct NullSink;
    impl TypedSink<i32> for NullSink {
        fn write(&mut self, _: i32) -> Result<(), Error> {
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }

    fn checkpoint_config(path: &std::path::Path) -> JobConfig {
        JobConfig {
            checkpoint: Some(CheckpointConfig {
                directory: path.into(),
                interval_batches: 1,
                max_checkpoints: 1,
                recover: true,
                binary_build_id: "multi-recovery-test".into(),
            }),
            ..JobConfig::default()
        }
    }

    fn stateless_fixture() -> (Graph, Registry, PhysicalPlan, String) {
        let pipeline = Pipeline::default();
        let first = pipeline
            .test_stream_source("first", vec![TestStreamEvent::Elements(vec![(1_i32, 0)])])
            .unwrap();
        let second = pipeline
            .test_stream_source("second", vec![TestStreamEvent::Elements(vec![(2_i32, 0)])])
            .unwrap();
        let merged = first.flatten_named("merge", &[second]).unwrap();
        let sink = merged
            .sink_named_with_config(
                "sink",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                "sink-v1".into(),
                || Ok(Box::new(NullSink)),
            )
            .unwrap();
        let (graph, registry) = sink.snapshot().unwrap();
        let plan = planner::lower(&graph, &registry, sink.key(), 100).unwrap();
        (graph, registry, plan, sink.key().into())
    }

    fn save_stateless_state(
        registry: &Registry,
        plan: &PhysicalPlan,
        sink_key: &str,
        config: &JobConfig,
        state: &MultiSourceState,
        combined: &ProgressSignal,
    ) {
        let mut digest = String::new();
        for route in &plan.routes {
            let part = registry.source_config_digest(&route.source_key).unwrap();
            digest.push_str(&format!(
                "{}:{}{}:{}",
                route.source_key.len(),
                route.source_key,
                part.len(),
                part
            ));
        }
        let checkpoint = StreamCheckpoint::new::<i32>(
            config.checkpoint.as_ref().unwrap(),
            &plan.summary.graph_fingerprint,
            &digest,
            registry.sink_config_digest(sink_key).unwrap(),
            MULTI_SOURCE_KEY,
            sink_key,
        )
        .unwrap();
        let bytes = serde_json::to_vec(state).unwrap();
        checkpoint
            .save_state(combined, &bytes, config.memory_budget_bytes)
            .unwrap();
    }

    fn dual_fixture() -> (
        Graph,
        Registry,
        PhysicalPlan,
        BoundedSideStreamRoute,
        String,
    ) {
        let pipeline = Pipeline::default();
        let make = |key: &str, value| {
            pipeline
                .test_stream_source(key, vec![TestStreamEvent::Elements(vec![(value, 1_i64)])])
                .unwrap()
                .fixed_windows(&format!("{key}-window"), 10)
                .unwrap()
                .map_named(&format!("{key}-key"), |value| (1_i32, value))
                .unwrap()
        };
        let main = make("main", 1_i32);
        let side = make("side", 2_i32);
        let sink = main
            .join_full_named("join", &side)
            .unwrap()
            .map_named("sum", |(_, (main, side))| {
                main.unwrap_or(0) + side.unwrap_or(0)
            })
            .unwrap()
            .sink_named_with_config(
                "sink",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                "sink-v1".into(),
                || Ok(Box::new(NullSink)),
            )
            .unwrap();
        let (graph, registry) = sink.snapshot().unwrap();
        let plan = planner::lower(&graph, &registry, sink.key(), 100).unwrap();
        let route = side_stream_route(&graph, sink.key(), Boundedness::Unbounded).unwrap();
        (graph, registry, plan, route, sink.key().into())
    }

    fn save_dual_state(
        registry: &Registry,
        plan: &PhysicalPlan,
        route: &BoundedSideStreamRoute,
        sink_key: &str,
        config: &JobConfig,
        state: &DualWindowState,
        combined: &ProgressSignal,
    ) {
        let mut digest = String::new();
        for key in [&route.source_key, &route.side_source_key] {
            let part = registry.source_config_digest(key).unwrap();
            digest.push_str(&format!("{}:{}{}:{}", key.len(), key, part.len(), part));
        }
        let checkpoint = StreamCheckpoint::new::<i32>(
            config.checkpoint.as_ref().unwrap(),
            &plan.summary.graph_fingerprint,
            &digest,
            registry.sink_config_digest(sink_key).unwrap(),
            MULTI_SOURCE_KEY,
            sink_key,
        )
        .unwrap();
        checkpoint
            .save_state(
                combined,
                &serde_json::to_vec(state).unwrap(),
                config.memory_budget_bytes,
            )
            .unwrap();
    }

    #[test]
    fn dual_source_validation_rejects_each_missing_contract() {
        let pipeline = Pipeline::default();
        pipeline
            .test_stream_source("events", vec![TestStreamEvent::Elements(vec![(1_i32, 0)])])
            .unwrap();
        let (graph, registry) = pipeline.snapshot().unwrap();
        assert!(validate_dual_source(&graph, &registry, "events").is_ok());
        assert_eq!(
            validate_dual_source(&graph, &registry, "missing")
                .unwrap_err()
                .kind,
            ErrorKind::Graph
        );
        let mut invalid = graph.clone();
        invalid
            .nodes
            .iter_mut()
            .find(|node| node.key == "events")
            .unwrap()
            .operation = Operation::Map;
        assert_eq!(
            validate_dual_source(&invalid, &registry, "events")
                .unwrap_err()
                .kind,
            ErrorKind::Graph
        );
        let mut invalid = graph.clone();
        invalid
            .nodes
            .iter_mut()
            .find(|node| node.key == "events")
            .unwrap()
            .boundedness = Boundedness::Bounded;
        assert!(validate_dual_source(&invalid, &registry, "events").is_err());
        for progress in [
            SourceProgress {
                stable_split_id: false,
                replay: Replay::Idempotent,
                reports_event_time: true,
            },
            SourceProgress {
                stable_split_id: true,
                replay: Replay::Idempotent,
                reports_event_time: false,
            },
            SourceProgress {
                stable_split_id: true,
                replay: Replay::None,
                reports_event_time: true,
            },
        ] {
            let mut invalid = graph.clone();
            invalid
                .nodes
                .iter_mut()
                .find(|node| node.key == "events")
                .unwrap()
                .operation = Operation::Source { progress };
            assert!(validate_dual_source(&invalid, &registry, "events").is_err());
        }
        assert!(validate_dual_source(&graph, &Registry::default(), "events").is_err());
    }

    #[test]
    fn combined_progress_waits_for_every_active_source_watermark() {
        let ready = |key: &str, watermark, completed| ProgressSignal {
            source_key: key.into(),
            split_id: "split-0".into(),
            checkpoint_position: Some(vec![1]),
            event_time_ns: watermark,
            completed,
        };
        let mut sources = [None, None];
        assert_eq!(multi_progress(0, &sources).event_time_ns, None);
        assert_eq!(dual_watermark(&sources), None);
        sources[0] = Some(ready("main", Some(20), false));
        assert_eq!(dual_watermark(&sources), None);
        sources[1] = Some(ready("side", Some(10), false));
        assert_eq!(dual_watermark(&sources), Some(10));
        assert_eq!(multi_progress(3, &sources).event_time_ns, Some(10));
        sources[1] = Some(ready("side", None, false));
        assert_eq!(dual_watermark(&sources), None);
        sources[1] = Some(ready("side", None, true));
        assert_eq!(dual_watermark(&sources), Some(20));
        assert_eq!(multi_progress(4, &sources).event_time_ns, Some(20));
        sources[0] = Some(ready("main", None, true));
        assert_eq!(dual_watermark(&sources), Some(i64::MAX));
        assert!(multi_progress(5, &sources).completed);
    }

    #[test]
    fn stateless_checkpoint_rejects_mismatched_route_state() {
        for fault in [
            "version", "length", "identity", "split", "progress", "sequence",
        ] {
            let temporary = tempfile::tempdir().unwrap();
            let config = checkpoint_config(temporary.path());
            let (graph, registry, plan, sink_key) = stateless_fixture();
            let source = |key: &str| ProgressSignal {
                source_key: key.into(),
                split_id: "split-0".into(),
                checkpoint_position: Some(vec![1]),
                event_time_ns: None,
                completed: false,
            };
            let mut state = MultiSourceState {
                version: 1,
                sources: plan
                    .routes
                    .iter()
                    .map(|route| Some(source(&route.source_key)))
                    .collect(),
            };
            let mut combined = multi_progress(2, &state.sources);
            match fault {
                "version" => state.version = 2,
                "length" => {
                    state.sources.pop();
                }
                "identity" => state.sources[0].as_mut().unwrap().source_key = "other".into(),
                "split" => state.sources[0].as_mut().unwrap().split_id.clear(),
                "progress" => combined.event_time_ns = Some(99),
                "sequence" => combined.checkpoint_position = Some(vec![1]),
                _ => unreachable!(),
            }
            save_stateless_state(&registry, &plan, &sink_key, &config, &state, &combined);
            let error = start_multi_stream_sink::<i32>(&graph, registry, plan, sink_key, &config)
                .err()
                .unwrap();
            assert!(
                matches!(error.kind, ErrorKind::Recovery | ErrorKind::Incompatible),
                "{fault}: {error:?}"
            );
        }
    }

    #[test]
    fn dual_checkpoint_rejects_mismatched_cursors_and_open_windows() {
        for fault in [
            "version",
            "main-key",
            "side-key",
            "split",
            "progress",
            "sequence",
            "budget",
            "completed-open",
        ] {
            let temporary = tempfile::tempdir().unwrap();
            let mut config = checkpoint_config(temporary.path());
            let (graph, registry, plan, route, sink_key) = dual_fixture();
            let source = |key: &str| ProgressSignal {
                source_key: key.into(),
                split_id: "split-0".into(),
                checkpoint_position: Some(vec![1]),
                event_time_ns: Some(0),
                completed: false,
            };
            let empty = BTreeMap::new();
            let mut state = DualWindowState {
                version: 1,
                sources: [
                    Some(source(&route.source_key)),
                    Some(source(&route.side_source_key)),
                ],
                main: encode_window_rows(
                    &registry,
                    &route.main_input_key,
                    &empty,
                    config.memory_budget_bytes,
                )
                .unwrap(),
                side: encode_window_rows(
                    &registry,
                    &route.side_input_key,
                    &empty,
                    config.memory_budget_bytes,
                )
                .unwrap(),
            };
            let mut combined = multi_progress(2, &state.sources);
            if fault == "budget" || fault == "completed-open" {
                let event = Envelope {
                    value: Box::new((1_i32, 1_i32)),
                    timestamp_ns: 1,
                    window: Window::Fixed { size_ns: 10 },
                    assigned_window: Some(WindowInterval {
                        start_ns: 0,
                        end_ns: 10,
                    }),
                };
                let rows = BTreeMap::from([((0, 10), vec![event])]);
                state.main = encode_window_rows(
                    &registry,
                    &route.main_input_key,
                    &rows,
                    config.memory_budget_bytes,
                )
                .unwrap();
                if fault == "budget" {
                    state.side = encode_window_rows(
                        &registry,
                        &route.side_input_key,
                        &rows,
                        config.memory_budget_bytes,
                    )
                    .unwrap();
                    config.buffer_budget_elements = 1;
                } else {
                    state.sources[0].as_mut().unwrap().completed = true;
                    state.sources[1].as_mut().unwrap().completed = true;
                    combined = multi_progress(2, &state.sources);
                }
            }
            match fault {
                "version" => state.version = 2,
                "main-key" => state.sources[0].as_mut().unwrap().source_key = "other".into(),
                "side-key" => state.sources[1].as_mut().unwrap().source_key = "other".into(),
                "split" => state.sources[1].as_mut().unwrap().split_id.clear(),
                "progress" => combined.event_time_ns = Some(99),
                "sequence" => combined.checkpoint_position = Some(vec![1]),
                _ => {}
            }
            save_dual_state(
                &registry, &plan, &route, &sink_key, &config, &state, &combined,
            );
            let error =
                start_dual_side_stream::<i32>(&graph, registry, plan, sink_key, &config, route)
                    .err()
                    .unwrap();
            assert!(
                matches!(
                    error.kind,
                    ErrorKind::Recovery | ErrorKind::Incompatible | ErrorKind::Resource
                ),
                "{fault}: {error:?}"
            );
        }
    }

    #[test]
    fn dual_stream_rejects_shared_source_and_completes_without_checkpoint() {
        let (graph, registry, plan, mut route, sink_key) = dual_fixture();
        route.side_source_key.clone_from(&route.source_key);
        let error = start_dual_side_stream::<i32>(
            &graph,
            registry,
            plan,
            sink_key,
            &JobConfig::default(),
            route,
        )
        .err()
        .unwrap();
        assert!(matches!(
            error.kind,
            ErrorKind::UnsupportedCapability { .. }
        ));
        let (graph, registry, plan, route, sink_key) = dual_fixture();
        let job = start_dual_side_stream::<i32>(
            &graph,
            registry,
            plan,
            sink_key,
            &JobConfig::default(),
            route,
        )
        .unwrap();
        assert_eq!(job.wait().unwrap().status(), JobStatus::Succeeded);
    }

    #[test]
    fn stateless_start_validates_each_route_before_opening_sources() {
        for fault in ["operation", "bounded", "split", "replay", "factory"] {
            let (mut graph, mut registry, plan, sink_key) = stateless_fixture();
            let node = graph
                .nodes
                .iter_mut()
                .find(|node| node.key == "first")
                .unwrap();
            match fault {
                "operation" => node.operation = Operation::Map,
                "bounded" => node.boundedness = Boundedness::Bounded,
                "split" => {
                    if let Operation::Source { progress } = &mut node.operation {
                        progress.stable_split_id = false;
                    }
                }
                "replay" => {
                    if let Operation::Source { progress } = &mut node.operation {
                        progress.replay = Replay::None;
                    }
                }
                "factory" => registry = Registry::default(),
                _ => unreachable!(),
            }
            let error = start_multi_stream_sink::<i32>(
                &graph,
                registry,
                plan,
                sink_key,
                &JobConfig::default(),
            )
            .err()
            .unwrap();
            assert!(
                matches!(
                    error.kind,
                    ErrorKind::Graph | ErrorKind::UnsupportedCapability { .. }
                ),
                "{fault}: {error:?}"
            );
        }
        let (graph, registry, plan, sink_key) = stateless_fixture();
        let config = JobConfig::default();
        config.cancellation.store(true, Ordering::SeqCst);
        assert_eq!(
            start_multi_stream_sink::<i32>(&graph, registry, plan, sink_key, &config)
                .err()
                .unwrap()
                .kind,
            ErrorKind::Cancelled
        );
    }

    #[test]
    fn completed_multi_source_checkpoints_open_no_readers() {
        let temporary = tempfile::tempdir().unwrap();
        let config = checkpoint_config(temporary.path());
        let (graph, registry, plan, sink_key) = stateless_fixture();
        let state = MultiSourceState {
            version: 1,
            sources: plan
                .routes
                .iter()
                .map(|route| {
                    Some(ProgressSignal {
                        source_key: route.source_key.clone(),
                        split_id: "split-0".into(),
                        checkpoint_position: Some(vec![1]),
                        event_time_ns: None,
                        completed: true,
                    })
                })
                .collect(),
        };
        let combined = multi_progress(2, &state.sources);
        save_stateless_state(&registry, &plan, &sink_key, &config, &state, &combined);
        let job =
            start_multi_stream_sink::<i32>(&graph, registry, plan, sink_key, &config).unwrap();
        assert!(job.progress().unwrap().completed);
        assert_eq!(job.wait().unwrap().status(), JobStatus::Succeeded);
    }

    #[test]
    fn dual_start_rejects_missing_coder_and_cancellation() {
        let temporary = tempfile::tempdir().unwrap();
        let config = checkpoint_config(temporary.path());
        let (graph, registry, plan, mut route, sink_key) = dual_fixture();
        route.side_input_key = "missing-coder".into();
        assert!(matches!(
            start_dual_side_stream::<i32>(&graph, registry, plan, sink_key, &config, route)
                .err()
                .unwrap()
                .kind,
            ErrorKind::UnsupportedCapability { .. }
        ));
        let (graph, registry, plan, route, sink_key) = dual_fixture();
        let config = JobConfig::default();
        config.cancellation.store(true, Ordering::SeqCst);
        assert_eq!(
            start_dual_side_stream::<i32>(&graph, registry, plan, sink_key, &config, route)
                .err()
                .unwrap()
                .kind,
            ErrorKind::Cancelled
        );
    }

    #[test]
    fn completed_dual_checkpoint_returns_without_reopening() {
        let temporary = tempfile::tempdir().unwrap();
        let config = checkpoint_config(temporary.path());
        let (graph, registry, plan, route, sink_key) = dual_fixture();
        let sources = [&route.source_key, &route.side_source_key].map(|key| {
            Some(ProgressSignal {
                source_key: key.clone(),
                split_id: "split-0".into(),
                checkpoint_position: Some(vec![1]),
                event_time_ns: Some(10),
                completed: true,
            })
        });
        let empty = BTreeMap::new();
        let state = DualWindowState {
            version: 1,
            sources,
            main: encode_window_rows(
                &registry,
                &route.main_input_key,
                &empty,
                config.memory_budget_bytes,
            )
            .unwrap(),
            side: encode_window_rows(
                &registry,
                &route.side_input_key,
                &empty,
                config.memory_budget_bytes,
            )
            .unwrap(),
        };
        let combined = multi_progress(2, &state.sources);
        save_dual_state(
            &registry, &plan, &route, &sink_key, &config, &state, &combined,
        );
        let job = start_dual_side_stream::<i32>(&graph, registry, plan, sink_key, &config, route)
            .unwrap();
        assert!(job.progress().unwrap().completed);
        assert_eq!(job.wait().unwrap().status(), JobStatus::Succeeded);
    }

    #[test]
    fn stateless_stream_commits_data_and_empty_progress_from_both_sources() {
        let (graph, registry, plan, sink_key) = stateless_fixture();
        let job =
            start_multi_stream_sink::<i32>(&graph, registry, plan, sink_key, &JobConfig::default())
                .unwrap();
        assert_eq!(job.wait().unwrap().status(), JobStatus::Succeeded);
    }

    #[test]
    fn dual_stream_enforces_one_budget_across_both_open_sides() {
        let (graph, registry, plan, route, sink_key) = dual_fixture();
        let config = JobConfig {
            buffer_budget_elements: 1,
            ..JobConfig::default()
        };
        let error = start_dual_side_stream::<i32>(&graph, registry, plan, sink_key, &config, route)
            .unwrap()
            .wait()
            .err()
            .unwrap();
        assert_eq!(error.kind, ErrorKind::Resource);
    }
}
