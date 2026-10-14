//! Independent unbounded sources feeding one fixed or sliding window group.

use super::*;

#[derive(Serialize, Deserialize)]
struct MultiGroupedState {
    version: u8,
    sources: Vec<Option<ProgressSignal>>,
    windows: Vec<u8>,
}

#[allow(clippy::too_many_lines)]
pub(crate) fn start_multi_grouped_stream_sink<T: Element>(
    graph: &Graph,
    registry: Registry,
    plan: PhysicalPlan,
    sink_key: String,
    config: &JobConfig,
    route: MultiGroupedStreamRoute,
) -> Result<StreamJob, Error> {
    let mut source_digest = String::new();
    for (source_key, _) in &route.sources {
        validate_dual_source(graph, &registry, source_key)?;
        if config.checkpoint.is_some() {
            let digest = registry
                .source_config_digest(source_key)
                .ok_or_else(|| Error::unsupported(source_key, "direct", "source-config-digest"))?;
            write!(
                source_digest,
                "{}:{}{}:{}",
                source_key.len(),
                source_key,
                digest.len(),
                digest
            )
            .expect("writing to String cannot fail");
        }
    }
    if config.checkpoint.is_some() && !registry.has_coder(&route.group_input_key) {
        return Err(Error::unsupported(
            &route.group_input_key,
            "direct",
            "stream-window-state-coder",
        ));
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
    let (sequence, sources, windows, buffered) = if let Some((progress, bytes)) = &saved {
        let state: MultiGroupedState = serde_json::from_slice(bytes)
            .map_err(|error| Error::recovery("checkpoint", error.to_string()))?;
        if state.version != 1
            || state.sources.len() != route.sources.len()
            || state
                .sources
                .iter()
                .zip(&route.sources)
                .any(|(signal, (key, _))| {
                    signal.as_ref().is_some_and(|signal| {
                        signal.source_key != *key || signal.split_id.is_empty()
                    })
                })
        {
            return Err(Error::incompatible(
                "multi-group stream checkpoint identity mismatch",
            ));
        }
        let cursor = progress
            .checkpoint_position
            .as_deref()
            .ok_or_else(|| Error::recovery("checkpoint", "missing multi-group sequence"))?;
        let sequence = u64::from_le_bytes(
            cursor
                .try_into()
                .map_err(|_| Error::recovery("checkpoint", "invalid multi-group sequence"))?,
        );
        if *progress != multi_progress(sequence, &state.sources) {
            return Err(Error::recovery(
                "checkpoint",
                "multi-group progress does not match cursors",
            ));
        }
        let (windows, buffered) = restore_window_rows(
            &registry,
            &route.group_input_key,
            &state.windows,
            config.buffer_budget_elements,
        )?;
        (sequence, state.sources, windows, buffered)
    } else {
        (0, vec![None; route.sources.len()], BTreeMap::new(), 0)
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
        if !windows.is_empty() {
            return Err(Error::recovery(
                &route.group_key,
                "completed stream checkpoint retains open windows",
            ));
        }
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
                run_multi_grouped_stream(
                    &registry,
                    &route,
                    &plan.summary.graph_fingerprint,
                    &sink_key,
                    &worker_config,
                    &worker_handle,
                    &worker_progress,
                    checkpoint.as_ref(),
                    sequence,
                    sources,
                    windows,
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

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn run_multi_grouped_stream(
    registry: &Registry,
    route: &MultiGroupedStreamRoute,
    graph_fingerprint: &str,
    sink_key: &str,
    config: &JobConfig,
    handle: &JobHandle,
    shared_progress: &Arc<Mutex<Option<ProgressSignal>>>,
    checkpoint: Option<&StreamCheckpoint>,
    mut sequence: u64,
    mut sources: Vec<Option<ProgressSignal>>,
    mut windows: WindowRows,
    mut buffered: usize,
) -> Result<(), Error> {
    let mut readers = route
        .sources
        .iter()
        .zip(&sources)
        .map(|((source_key, _), progress)| {
            if progress.as_ref().is_some_and(|progress| progress.completed) {
                Ok(None)
            } else {
                catch_unwind(AssertUnwindSafe(|| {
                    registry.open_unbounded_source(
                        source_key,
                        progress
                            .clone()
                            .filter(|signal| signal.checkpoint_position.is_some()),
                    )
                }))
                .unwrap_or_else(|_| {
                    Err(Error::execution(
                        source_key,
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
        for (index, (source_key, stages)) in route.sources.iter().enumerate() {
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
                    source_key,
                    "direct",
                    "source poll panicked",
                ))
            })?;
            let UnboundedPoll::Ready(batch) = polled else {
                continue;
            };
            ready = true;
            if batch.progress.source_key != *source_key || batch.progress.split_id.is_empty() {
                return Err(Error::execution(
                    source_key,
                    "source",
                    "stream progress identity changed",
                ));
            }
            if sources[index]
                .as_ref()
                .is_some_and(|previous| previous.split_id != batch.progress.split_id)
            {
                return Err(Error::execution(
                    source_key,
                    "source",
                    "source split changed after checkpoint",
                ));
            }
            if !batch.elements.is_empty()
                && sources[index].as_ref().is_some_and(|previous| {
                    previous.checkpoint_position == batch.progress.checkpoint_position
                })
            {
                return Err(Error::execution(
                    source_key,
                    "source",
                    "data batch did not advance its cursor",
                ));
            }
            if sources[index].as_ref().is_some_and(|previous| {
                previous
                    .event_time_ns
                    .zip(batch.progress.event_time_ns)
                    .is_some_and(|(old, new)| new < old)
            }) {
                return Err(Error::execution(
                    source_key,
                    "source",
                    "source watermark moved backward",
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
            let old_watermark = multi_progress(sequence, &sources).event_time_ns;
            for input in batch.elements {
                for event in apply_grouped_stages(registry, stages, input, config, handle)? {
                    let window = stream_window(&event, &route.group_key)?;
                    if old_watermark.is_some_and(|watermark| window.1 <= watermark) {
                        continue;
                    }
                    if buffered >= config.buffer_budget_elements {
                        return Err(Error::resource(
                            &route.group_key,
                            "stateful stream input exceeds buffer budget",
                        ));
                    }
                    windows.entry(window).or_default().push(event);
                    buffered += 1;
                }
            }
            let next = merge_progress(sources[index].as_ref(), batch.progress);
            if next.completed {
                readers[index] = None;
            }
            sources[index] = Some(next);
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| Error::resource("checkpoint", "multi-group sequence exhausted"))?;
            let combined = multi_progress(sequence, &sources);
            let closing_watermark = if combined.completed {
                Some(i64::MAX)
            } else {
                combined.event_time_ns
            };
            let closing = windows
                .keys()
                .copied()
                .filter(|(_, end)| closing_watermark.is_some_and(|watermark| *end <= watermark))
                .collect::<Vec<_>>();
            let instruction_id = bundle_id(graph_fingerprint, &[sink_key], &combined);
            let mut session = None;
            let mut written = 0_u64;
            let result = (|| {
                for window in closing {
                    let events = windows.remove(&window).expect("selected open window");
                    buffered -= events.len();
                    let grouped = catch_unwind(AssertUnwindSafe(|| {
                        registry.apply_aggregate(&route.group_key, vec![events])
                    }))
                    .unwrap_or_else(|_| {
                        Err(Error::execution(
                            &route.group_key,
                            "direct",
                            "user aggregate panicked",
                        ))
                    })?;
                    for event in grouped {
                        for output in apply_grouped_stages(
                            registry,
                            &route.after_group,
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
                let state = serde_json::to_vec(&MultiGroupedState {
                    version: 1,
                    sources: sources.clone(),
                    windows: encode_window_rows(
                        registry,
                        &route.group_input_key,
                        &windows,
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
