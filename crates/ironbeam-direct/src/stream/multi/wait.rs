//! Windowed barriers over independent signal sources.

use super::*;

#[derive(Serialize, Deserialize)]
struct MultiWaitState {
    version: u8,
    sources: Vec<Option<ProgressSignal>>,
    main: Vec<u8>,
}

#[allow(clippy::too_many_lines)]
pub(crate) fn start_multi_wait_stream_sink<T: Element>(
    graph: &Graph,
    registry: Registry,
    plan: PhysicalPlan,
    sink_key: String,
    config: &JobConfig,
    route: MultiWaitStreamRoute,
) -> Result<StreamJob, Error> {
    let mut source_digest = String::new();
    for (key, _) in &route.sources {
        validate_dual_source(graph, &registry, key)?;
        if config.checkpoint.is_some() {
            let digest = registry
                .source_config_digest(key)
                .ok_or_else(|| Error::unsupported(key, "direct", "source-config-digest"))?;
            write!(
                source_digest,
                "{}:{}{}:{}",
                key.len(),
                key,
                digest.len(),
                digest
            )
            .expect("writing to String cannot fail");
        }
    }
    if config.checkpoint.is_some() && !registry.has_coder(&route.main_input_key) {
        return Err(Error::unsupported(
            &route.main_input_key,
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
        let state: MultiWaitState = serde_json::from_slice(bytes)
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
                "multi-wait checkpoint identity mismatch",
            ));
        }
        let cursor = progress
            .checkpoint_position
            .as_deref()
            .ok_or_else(|| Error::recovery("checkpoint", "missing multi-wait sequence"))?;
        let sequence = u64::from_le_bytes(
            cursor
                .try_into()
                .map_err(|_| Error::recovery("checkpoint", "invalid multi-wait sequence"))?,
        );
        if *progress != multi_progress(sequence, &state.sources) {
            return Err(Error::recovery(
                "checkpoint",
                "multi-wait progress does not match cursors",
            ));
        }
        let (windows, buffered) = restore_window_rows(
            &registry,
            &route.main_input_key,
            &state.main,
            config.buffer_budget_elements,
        )?;
        (sequence, state.sources, windows, buffered)
    } else {
        (0, vec![None; route.sources.len()], WindowRows::new(), 0)
    };
    for ((key, _), signal) in route.sources.iter().zip(&sources) {
        resume_source_finalization(&registry, key, checkpoint.as_ref(), signal.as_ref())?;
    }
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
                &route.wait_key,
                "completed multi-wait retains open windows",
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
                run_multi_wait_stream(
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
fn run_multi_wait_stream(
    registry: &Registry,
    route: &MultiWaitStreamRoute,
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
        .map(|((key, _), progress)| {
            if progress.as_ref().is_some_and(|signal| signal.completed) {
                Ok(None)
            } else {
                catch_unwind(AssertUnwindSafe(|| {
                    registry.open_unbounded_source(
                        key,
                        progress
                            .clone()
                            .filter(|signal| signal.checkpoint_position.is_some()),
                    )
                }))
                .unwrap_or_else(|_| Err(Error::execution(key, "direct", "source factory panicked")))
                .map(Some)
            }
        })
        .collect::<Result<Vec<_>, Error>>()?;
    loop {
        let mut ready = false;
        for (index, (key, stages)) in route.sources.iter().enumerate() {
            if config.cancellation.load(Ordering::Acquire) {
                return Err(cancelled());
            }
            let Some(reader) = readers[index].as_mut() else {
                continue;
            };
            let polled = catch_unwind(AssertUnwindSafe(|| {
                reader.poll(config.buffer_budget_elements)
            }))
            .unwrap_or_else(|_| Err(Error::execution(key, "direct", "source poll panicked")))?;
            let UnboundedPoll::Ready(batch) = polled else {
                continue;
            };
            ready = true;
            if !batch.elements.is_empty()
                && sources[index].as_ref().is_some_and(|previous| {
                    previous.checkpoint_position == batch.progress.checkpoint_position
                })
            {
                return Err(Error::execution(
                    key,
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
            for input in batch.elements {
                for event in apply_grouped_stages(registry, stages, input, config, handle)? {
                    if index == 0 {
                        let window = stream_window(&event, &route.wait_key)?;
                        if buffered >= config.buffer_budget_elements {
                            return Err(Error::resource(
                                &route.wait_key,
                                "multi-wait buffered rows exceed budget",
                            ));
                        }
                        windows.entry(window).or_default().push(event);
                        buffered += 1;
                    }
                }
            }
            let next = merge_progress(sources[index].as_ref(), batch.progress);
            if next.completed {
                readers[index] = None;
            }
            sources[index] = Some(next);
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| Error::resource("checkpoint", "multi-wait sequence exhausted"))?;
            let combined = multi_progress(sequence, &sources);
            let signal_watermark = sources[1..]
                .iter()
                .map(|source| {
                    source.as_ref().and_then(|signal| {
                        signal
                            .completed
                            .then_some(i64::MAX)
                            .or(signal.event_time_ns)
                    })
                })
                .collect::<Option<Vec<_>>>()
                .and_then(|watermarks| watermarks.into_iter().min());
            let closing = windows
                .keys()
                .copied()
                .filter(|(_, end)| signal_watermark.is_some_and(|watermark| *end <= watermark))
                .collect::<Vec<_>>();
            let instruction_id = bundle_id(graph_fingerprint, &[sink_key], &combined);
            let mut session = None;
            let mut written = 0_u64;
            let result = (|| {
                for window in closing {
                    let events = windows.remove(&window).expect("ready window exists");
                    buffered -= events.len();
                    for event in events {
                        for output in apply_grouped_stages(
                            registry,
                            &route.after_wait,
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
                let state = serde_json::to_vec(&MultiWaitState {
                    version: 1,
                    sources: sources.clone(),
                    main: encode_window_rows(
                        registry,
                        &route.main_input_key,
                        &windows,
                        config.memory_budget_bytes,
                    )?,
                })
                .map_err(|error| Error::recovery("checkpoint", error.to_string()))?;
                checkpoint.save_state(&combined, &state, config.memory_budget_bytes)?;
            }
            if let Some(signal) = sources[index]
                .as_ref()
                .filter(|signal| signal.checkpoint_position.is_some())
            {
                registry.finalize_source_progress(key, signal)?;
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
                if !windows.is_empty() {
                    return Err(Error::recovery(
                        &route.wait_key,
                        "completed signals left open main windows",
                    ));
                }
                return Ok(());
            }
        }
        if !ready {
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
