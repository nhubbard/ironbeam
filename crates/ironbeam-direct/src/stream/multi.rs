//! Multi-source Direct streaming with route-local progress and shared window checkpoints.

#![allow(clippy::wildcard_imports)] // The child module shares the parent stream state machine.

use super::*;
use std::fmt::Write as _;
mod dual_late;
mod grouped;
mod grouped_state;
mod session;
mod wait;
use dual_late::{ClosedPanes, CodedClosedPane, DualFiring, FiringClock};
pub(super) use grouped::start_multi_grouped_stream_sink;
pub(super) use wait::start_multi_wait_stream_sink;

const MULTI_SOURCE_KEY: &str = "ironbeam:multi-source";

#[derive(Serialize, Deserialize)]
struct MultiSourceState {
    version: u8,
    sources: Vec<Option<ProgressSignal>>,
}

struct BoundedStreamReader {
    source: ironbeam_core::SourceIterator,
    source_key: String,
    position: u64,
}

impl BoundedStreamReader {
    fn open(
        registry: &Registry,
        key: &str,
        progress: Option<&ProgressSignal>,
        budget_elements: usize,
    ) -> Result<Self, Error> {
        let position = if let Some(progress) = progress {
            if progress.split_id != "bounded" || progress.source_key != key {
                return Err(Error::incompatible(
                    "bounded stream source identity mismatch",
                ));
            }
            let bytes = progress.checkpoint_position.as_deref().ok_or_else(|| {
                Error::recovery(key, "bounded stream checkpoint lacks a position")
            })?;
            u64::from_le_bytes(
                bytes
                    .try_into()
                    .map_err(|_| Error::recovery(key, "invalid bounded stream position"))?,
            )
        } else {
            0
        };
        let mut source = open_source_safe(registry, key)?;
        let mut remaining = position;
        while remaining > 0 {
            let limit = usize::try_from(remaining)
                .unwrap_or(usize::MAX)
                .min(budget_elements);
            let batch = read_source_batch(&mut source, limit, key)?;
            if batch.len() != limit {
                return Err(Error::incompatible(
                    "bounded stream source changed on resume",
                ));
            }
            remaining -= u64::try_from(batch.len()).expect("batch count fits u64");
        }
        Ok(Self {
            source,
            source_key: key.to_owned(),
            position,
        })
    }
}

impl ironbeam_core::UnboundedSource<Envelope> for BoundedStreamReader {
    fn poll(&mut self, max_elements: usize) -> Result<UnboundedPoll<Envelope>, Error> {
        let elements = read_source_batch(&mut self.source, max_elements, &self.source_key)?;
        self.position = self
            .position
            .checked_add(u64::try_from(elements.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| Error::resource(&self.source_key, "bounded stream position overflow"))?;
        let completed = elements.len() < max_elements;
        Ok(UnboundedPoll::Ready(ironbeam_core::UnboundedBatch {
            elements,
            progress: ProgressSignal {
                source_key: self.source_key.clone(),
                split_id: "bounded".into(),
                checkpoint_position: Some(self.position.to_le_bytes().to_vec()),
                event_time_ns: None,
                completed,
            },
        }))
    }
}

#[derive(Serialize, Deserialize)]
struct DualWindowState {
    version: u8,
    sources: [Option<ProgressSignal>; 2],
    main: Vec<u8>,
    side: Vec<u8>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    closed: Vec<CodedClosedPane>,
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
    let shared_prefix_len = route
        .main_stages
        .iter()
        .zip(&route.side_stages)
        .take_while(|(main, side)| main == side)
        .count();
    let split_key = route.main_stages[..shared_prefix_len]
        .last()
        .unwrap_or(&route.source_key);
    if route.source_key == route.side_source_key && !registry.has_coder(split_key) {
        return Err(Error::unsupported(
            &route.aggregate_key,
            "direct",
            "shared-stream-branch-coder",
        ));
    }
    for key in [&route.source_key, &route.side_source_key] {
        validate_dual_source(graph, &registry, key)?;
    }
    if matches!(route.window, Window::Session { .. })
        && !registry.has_join_coder(&route.aggregate_key)
        && !registry.has_cogroup_coder(&route.aggregate_key)
    {
        return Err(Error::unsupported(
            &route.aggregate_key,
            "direct",
            "session-key-coder",
        ));
    }
    let aggregate = graph
        .node(&route.aggregate_key)
        .ok_or_else(|| Error::graph(&route.aggregate_key, "dual-source aggregate missing"))?;
    let lateness_ns = aggregate.time.allowed_lateness_ns;
    let (late_count, accumulating) = match aggregate.time.trigger {
        WindowTrigger::Default => (None, false),
        WindowTrigger::LateElementCount { count } if lateness_ns > 0 => (Some(count), false),
        WindowTrigger::LateElementCountAccumulating { count } if lateness_ns > 0 => {
            (Some(count), true)
        }
        _ => {
            return Err(Error::unsupported(
                &route.aggregate_key,
                "direct",
                "dual-source-trigger",
            ));
        }
    };
    if lateness_ns > 0
        && !registry.has_join_coder(&route.aggregate_key)
        && !registry.has_cogroup_coder(&route.aggregate_key)
    {
        return Err(Error::unsupported(
            &route.aggregate_key,
            "direct",
            "dual-source-late-key-coder",
        ));
    }
    if config.checkpoint.is_some()
        || lateness_ns > 0
        || !route.other_aggregates.is_empty()
        || matches!(route.window, Window::Session { .. })
    {
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
                write!(digest, "{}:{}{}:{}", key.len(), key, part.len(), part)
                    .expect("writing to String cannot fail");
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
    let (sequence, progress_by_source, main_windows, side_windows, buffered, closed) =
        if let Some((combined, bytes)) = &saved {
            let state: DualWindowState = serde_json::from_slice(bytes)
                .map_err(|error| Error::recovery("checkpoint", error.to_string()))?;
            if state.version
                != if accumulating {
                    3
                } else if lateness_ns > 0 {
                    2
                } else {
                    1
                }
                || state.sources[0]
                    .as_ref()
                    .is_some_and(|p| p.source_key != route.source_key || p.split_id.is_empty())
                || state.sources[1]
                    .as_ref()
                    .is_some_and(|p| p.source_key != route.side_source_key || p.split_id.is_empty())
                || (route.source_key == route.side_source_key
                    && state.sources[0] != state.sources[1])
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
            let closed = if lateness_ns > 0 {
                dual_late::restore_closed(
                    state.closed,
                    dual_watermark(&state.sources),
                    lateness_ns,
                    config.buffer_budget_elements,
                    accumulating,
                )?
            } else if state.closed.is_empty() {
                ClosedPanes::new()
            } else {
                return Err(Error::recovery(
                    "checkpoint",
                    "unexpected dual-source pane state",
                ));
            };
            if combined.completed && !closed.is_empty() {
                return Err(Error::recovery(
                    "checkpoint",
                    "completed dual-source checkpoint retains panes",
                ));
            }
            if accumulating {
                dual_late::validate_retained_counts(&registry, &route, &main, &side, &closed)?;
            }
            (sequence, state.sources, main, side, buffered, closed)
        } else {
            (
                0,
                [None, None],
                BTreeMap::new(),
                BTreeMap::new(),
                0,
                ClosedPanes::new(),
            )
        };
    for (key, signal) in [route.source_key.as_str(), route.side_source_key.as_str()]
        .into_iter()
        .zip(&progress_by_source)
    {
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
                    closed,
                    lateness_ns,
                    late_count,
                    accumulating,
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
        if node.boundedness == ironbeam_core::Boundedness::Unbounded
            && (!progress.stable_split_id
                || progress.replay == Replay::None
                || !registry.has_unbounded_source(&route.source_key))
        {
            return Err(Error::unsupported(
                &route.source_key,
                "direct",
                "resumable-stream-source",
            ));
        }
        if node.boundedness == ironbeam_core::Boundedness::Bounded
            && (!registry.has_source(&route.source_key)
                || registry.has_unbounded_source(&route.source_key))
        {
            return Err(Error::unsupported(
                &route.source_key,
                "direct",
                "bounded-stream-source",
            ));
        }
        if config.checkpoint.is_some() {
            let digest = registry
                .source_config_digest(&route.source_key)
                .ok_or_else(|| {
                    Error::unsupported(&route.source_key, "direct", "source-config-digest")
                })?;
            write!(
                source_digest,
                "{}:{}{}:{}",
                route.source_key.len(),
                route.source_key,
                digest.len(),
                digest
            )
            .expect("writing to String cannot fail");
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
    for (route, signal) in plan.routes.iter().zip(&source_progress) {
        resume_source_finalization(
            &registry,
            &route.source_key,
            checkpoint.as_ref(),
            signal.as_ref(),
        )?;
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
            } else if !registry.has_unbounded_source(&route.source_key) {
                BoundedStreamReader::open(
                    registry,
                    &route.source_key,
                    progress.as_ref(),
                    config.buffer_budget_elements,
                )
                .map(|reader| Some(Box::new(reader) as ironbeam_core::UnboundedSourceReader))
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
            if let Some(signal) = source_progress[index]
                .as_ref()
                .filter(|signal| signal.checkpoint_position.is_some())
            {
                registry.finalize_source_progress(&route.source_key, signal)?;
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

fn copy_window_events(
    registry: &Registry,
    input_key: &str,
    events: &[Envelope],
) -> Result<Vec<Envelope>, Error> {
    events
        .iter()
        .map(|event| {
            let bytes = catch_unwind(AssertUnwindSafe(|| {
                registry.encode_element(input_key, event.value.as_ref())
            }))
            .unwrap_or_else(|_| {
                Err(Error::execution(
                    input_key,
                    "direct",
                    "fanout coder panicked",
                ))
            })?;
            let value = catch_unwind(AssertUnwindSafe(|| {
                registry.decode_element(input_key, &bytes)
            }))
            .unwrap_or_else(|_| {
                Err(Error::execution(
                    input_key,
                    "direct",
                    "fanout coder panicked",
                ))
            })?;
            Ok(Envelope {
                pane: event.pane,
                value,
                timestamp_ns: event.timestamp_ns,
                window: event.window.clone(),
                assigned_window: event.assigned_window,
            })
        })
        .collect()
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
    mut closed: ClosedPanes,
    lateness_ns: i64,
    late_count: Option<u64>,
    accumulating: bool,
) -> Result<(), Error> {
    let shared_source = route.source_key == route.side_source_key;
    let source_keys = [&route.source_key, &route.side_source_key];
    let stages = [&route.main_stages, &route.side_stages];
    let shared_prefix_len = stages[0]
        .iter()
        .zip(stages[1])
        .take_while(|(main, side)| main == side)
        .count();
    let split_key = stages[0][..shared_prefix_len]
        .last()
        .unwrap_or(&route.source_key);
    let mut readers = source_keys
        .iter()
        .zip(&progress)
        .enumerate()
        .map(|(index, (key, saved))| {
            if (shared_source && index == 1) || saved.as_ref().is_some_and(|saved| saved.completed)
            {
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
            let old_watermark = if route.wait_on_signal {
                progress[0].as_ref().and_then(|signal| signal.event_time_ns)
            } else {
                dual_watermark(&progress)
            };
            for input in batch.elements {
                let common_outputs = if shared_source {
                    apply_grouped_stages(
                        registry,
                        &stages[0][..shared_prefix_len],
                        input,
                        config,
                        handle,
                    )?
                } else {
                    vec![input]
                };
                for common_output in common_outputs {
                    let branch_inputs = if shared_source {
                        let copy = copy_window_events(
                            registry,
                            split_key,
                            std::slice::from_ref(&common_output),
                        )?
                        .pop()
                        .expect("one copied branch element");
                        vec![(0, copy), (1, common_output)]
                    } else {
                        vec![(index, common_output)]
                    };
                    for (branch, input) in branch_inputs {
                        if route.wait_on_signal && branch == 1 {
                            // The signal's values carry no barrier data. Its
                            // watermark alone decides readiness.
                            apply_grouped_stages(
                                registry,
                                &stages[1][if shared_source { shared_prefix_len } else { 0 }..],
                                input,
                                config,
                                handle,
                            )?;
                            continue;
                        }
                        let branch_stages = if shared_source {
                            &stages[branch][shared_prefix_len..]
                        } else {
                            stages[branch].as_slice()
                        };
                        for event in
                            apply_grouped_stages(registry, branch_stages, input, config, handle)?
                        {
                            let window = stream_window(&event, &route.aggregate_key)?;
                            let original_expired = old_watermark.is_some_and(|watermark| {
                                window.1.saturating_add(lateness_ns) <= watermark
                            });
                            let resolved_end = if original_expired
                                && matches!(route.window, Window::Session { .. })
                                && !route.wait_on_signal
                            {
                                session::resolved_dual_session_end(
                                    registry,
                                    route,
                                    &main_windows,
                                    &side_windows,
                                    branch,
                                    &event,
                                )?
                            } else {
                                window.1
                            };
                            if old_watermark.is_some_and(|watermark| {
                                resolved_end.saturating_add(lateness_ns) <= watermark
                            }) {
                                continue;
                            }
                            if buffered >= config.buffer_budget_elements {
                                return Err(Error::resource(
                                    &route.aggregate_key,
                                    "stateful stream input exceeds buffer budget",
                                ));
                            }
                            let windows = if branch == 0 {
                                &mut main_windows
                            } else {
                                &mut side_windows
                            };
                            windows.entry(window).or_default().push(event);
                            buffered += 1;
                        }
                    }
                }
            }
            let next = merge_progress(progress[index].as_ref(), batch.progress);
            if next.completed {
                readers[index] = None;
            }
            if shared_source {
                progress[1] = Some(next.clone());
            }
            progress[index] = Some(next);
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| Error::resource("checkpoint", "dual stream sequence exhausted"))?;
            let combined = multi_progress(sequence, &progress);
            let (closing, dropped) = if route.wait_on_signal {
                let signal_watermark = progress[1].as_ref().and_then(|signal| {
                    signal
                        .completed
                        .then_some(i64::MAX)
                        .or(signal.event_time_ns)
                });
                let firings = main_windows
                    .keys()
                    .copied()
                    .filter(|(_, end)| signal_watermark.is_some_and(|watermark| *end <= watermark))
                    .collect::<Vec<_>>()
                    .into_iter()
                    .map(|window| DualFiring {
                        main: main_windows.remove(&window).unwrap_or_default(),
                        side: Vec::new(),
                        pane: None,
                        retained: false,
                    })
                    .collect();
                (firings, 0)
            } else if lateness_ns > 0 {
                let drained = dual_late::drain_ready(
                    registry,
                    route,
                    &mut main_windows,
                    &mut side_windows,
                    &mut closed,
                    FiringClock {
                        prior_watermark: old_watermark,
                        watermark: dual_watermark(&progress),
                        lateness_ns,
                        budget_elements: config.buffer_budget_elements,
                        late_count,
                        accumulating,
                    },
                )?;
                (drained.firings, drained.dropped)
            } else if matches!(route.window, Window::Session { .. }) {
                if let Some(watermark) = dual_watermark(&progress) {
                    let firings = session::close_ready_sessions(
                        registry,
                        route,
                        &mut main_windows,
                        &mut side_windows,
                        watermark,
                    )?
                    .into_iter()
                    .map(|(main, side)| DualFiring {
                        main,
                        side,
                        pane: None,
                        retained: false,
                    })
                    .collect();
                    (firings, 0)
                } else {
                    (Vec::new(), 0)
                }
            } else {
                let firings = main_windows
                    .keys()
                    .chain(side_windows.keys())
                    .copied()
                    .filter(|(_, end)| {
                        dual_watermark(&progress).is_some_and(|watermark| *end <= watermark)
                    })
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .map(|window| DualFiring {
                        main: main_windows.remove(&window).unwrap_or_default(),
                        side: side_windows.remove(&window).unwrap_or_default(),
                        pane: None,
                        retained: false,
                    })
                    .collect();
                (firings, 0)
            };
            buffered = buffered.checked_sub(dropped).ok_or_else(|| {
                Error::recovery(
                    &route.aggregate_key,
                    "dual-source retained row count underflow",
                )
            })?;
            let instruction_id = bundle_id(graph_fingerprint, &[sink_key], &combined);
            let mut session = None;
            let mut written = 0_u64;
            let result =
                (|| {
                    for DualFiring {
                        mut main,
                        mut side,
                        pane,
                        retained,
                    } in closing
                    {
                        if !retained {
                            buffered =
                                buffered
                                    .checked_sub(main.len() + side.len())
                                    .ok_or_else(|| {
                                        Error::recovery(
                                            &route.aggregate_key,
                                            "dual-source buffered row count underflow",
                                        )
                                    })?;
                        }
                        let branches = std::iter::once((
                            route.aggregate_key.as_str(),
                            route.after_aggregate.as_slice(),
                        ))
                        .chain(
                            route
                                .other_aggregates
                                .iter()
                                .map(|(key, stages)| (key.as_str(), stages.as_slice())),
                        )
                        .collect::<Vec<_>>();
                        for (index, (aggregate_key, branch_stages)) in branches.iter().enumerate() {
                            let inputs = if index + 1 == branches.len() {
                                vec![std::mem::take(&mut main), std::mem::take(&mut side)]
                            } else {
                                vec![
                                    copy_window_events(registry, &route.main_input_key, &main)?,
                                    copy_window_events(registry, &route.side_input_key, &side)?,
                                ]
                            };
                            let mut grouped = catch_unwind(AssertUnwindSafe(|| {
                                registry.apply_aggregate(aggregate_key, inputs)
                            }))
                            .unwrap_or_else(|_| {
                                Err(Error::execution(
                                    *aggregate_key,
                                    "direct",
                                    "user aggregate panicked",
                                ))
                            })?;
                            if let Some(pane) = pane {
                                for event in &mut grouped {
                                    event.pane = pane;
                                }
                            }
                            for event in grouped {
                                for branch_output in apply_grouped_stages(
                                    registry,
                                    branch_stages,
                                    event,
                                    config,
                                    handle,
                                )? {
                                    for output in apply_grouped_stages(
                                        registry,
                                        &route.after_fanout,
                                        branch_output,
                                        config,
                                        handle,
                                    )? {
                                        if session.is_none() {
                                            session = Some(begin_sink_bundle(
                                                registry,
                                                sink_key,
                                                &instruction_id,
                                            )?);
                                        }
                                        catch_unwind(AssertUnwindSafe(|| {
                                            session.as_mut().expect("opened sink").write(output)
                                        }))
                                        .unwrap_or_else(|_| {
                                            Err(Error::execution(
                                                sink_key,
                                                "direct",
                                                "sink writer panicked",
                                            ))
                                        })?;
                                        written = written.saturating_add(1);
                                    }
                                }
                            }
                        }
                    }
                    if config.cancellation.load(Ordering::Acquire) {
                        return Err(cancelled());
                    }
                    if let Some(session) = &mut session {
                        catch_unwind(AssertUnwindSafe(|| session.commit())).unwrap_or_else(
                            |_| Err(Error::execution(sink_key, "direct", "sink commit panicked")),
                        )?;
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
                    version: if accumulating {
                        3
                    } else if lateness_ns > 0 {
                        2
                    } else {
                        1
                    },
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
                    closed: dual_late::encode_closed(&closed),
                })
                .map_err(|error| Error::recovery("checkpoint", error.to_string()))?;
                checkpoint.save_state(&combined, &state, config.memory_budget_bytes)?;
            }
            if let Some(signal) = progress[index]
                .as_ref()
                .filter(|signal| signal.checkpoint_position.is_some())
            {
                registry.finalize_source_progress(source_keys[index], signal)?;
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
#[path = "../../tests/unit/stream__multi__tests.rs"]
mod tests;
