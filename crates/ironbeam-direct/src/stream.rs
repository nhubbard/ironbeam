//! One-split Direct execution for a resumable unbounded source.

use crate::multi_output::{self, PairPlan};
use crate::planner::{self, PhysicalPlan};
use crate::stream_checkpoint::StreamCheckpoint;
use crate::{DirectRunner, JobConfig, JobHandle, JobStatus, cancelled, validate_config};
use ironbeam_core::{
    Element, Error, ErrorKind, Operation, ProgressSignal, Registry, Replay, Sink, SinkMode,
    SinkSession, UnboundedPoll,
};
use sha2::{Digest, Sha256};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// A running unbounded Direct job. Dropping it requests cancellation and joins
/// its worker; `wait` preserves the execution error for the caller.
pub struct StreamJob {
    handle: JobHandle,
    progress: Arc<Mutex<Option<ProgressSignal>>>,
    worker: Option<JoinHandle<Result<(), Error>>>,
}

impl StreamJob {
    #[must_use]
    pub fn handle(&self) -> &JobHandle {
        &self.handle
    }

    #[must_use]
    pub fn progress(&self) -> Option<ProgressSignal> {
        self.progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn cancel(&self) {
        self.handle.cancel();
    }

    pub fn wait(mut self) -> Result<JobHandle, Error> {
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| Error::execution("job", "direct", "stream worker panicked"))??;
        }
        Ok(self.handle.clone())
    }
}

impl Drop for StreamJob {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            self.handle.cancel();
            let _ = worker.join();
        }
    }
}

impl DirectRunner {
    /// Start one unbounded source feeding a linear transform route and an
    /// idempotent append sink. Validation and checkpoint recovery finish before
    /// the worker can open either endpoint.
    pub fn start_stream_sink<T: Element>(
        &self,
        sink: &Sink<T>,
        config: &JobConfig,
    ) -> Result<StreamJob, Error> {
        let (graph, registry) = sink.snapshot()?;
        self.validate_with_unbounded(&graph, true)?;
        validate_config(config)?;
        let sink_key = sink.key().to_owned();
        let sink_node = graph
            .node(&sink_key)
            .ok_or_else(|| Error::graph(&sink_key, "sink node missing"))?;
        if !matches!(&sink_node.operation, Operation::Sink { spec }
            if spec.mode == SinkMode::Append && spec.replay == Replay::Idempotent && spec.commit)
        {
            return Err(Error::unsupported(
                &sink_key,
                "direct",
                "idempotent-stream-sink",
            ));
        }
        if !registry.has_sink(&sink_key) {
            return Err(Error::unsupported(&sink_key, "direct", "sink-factory"));
        }
        if config.spill.is_some() {
            return Err(Error::unsupported(&sink_key, "direct", "stream-spill"));
        }
        let plan = planner::lower(&graph, &registry, &sink_key, config.buffer_budget_elements)?;
        if plan.materialized {
            return Err(Error::unsupported(
                &sink_key,
                "direct",
                "stateful-stream-route",
            ));
        }
        let [route] = plan.routes.as_slice() else {
            return Err(Error::unsupported(
                &sink_key,
                "direct",
                "multi-source-stream",
            ));
        };
        let source_node = graph
            .node(&route.source_key)
            .ok_or_else(|| Error::graph(&route.source_key, "stream source node missing"))?;
        let Operation::Source {
            progress: source_progress,
        } = &source_node.operation
        else {
            return Err(Error::graph(
                &route.source_key,
                "stream route has no source",
            ));
        };
        if source_node.boundedness != ironbeam_core::Boundedness::Unbounded
            || !source_progress.stable_split_id
            || source_progress.replay == Replay::None
            || !registry.has_unbounded_source(&route.source_key)
        {
            return Err(Error::unsupported(
                &route.source_key,
                "direct",
                "resumable-stream-source",
            ));
        }
        let checkpoint = config
            .checkpoint
            .as_ref()
            .map(|checkpoint| {
                StreamCheckpoint::new::<T>(
                    checkpoint,
                    &plan.summary.graph_fingerprint,
                    registry
                        .source_config_digest(&route.source_key)
                        .ok_or_else(|| {
                            Error::unsupported(&route.source_key, "direct", "source-config-digest")
                        })?,
                    registry.sink_config_digest(&sink_key).ok_or_else(|| {
                        Error::unsupported(&sink_key, "direct", "sink-config-digest")
                    })?,
                    &route.source_key,
                    &sink_key,
                )
            })
            .transpose()?;
        let recovered = checkpoint
            .as_ref()
            .map(StreamCheckpoint::load)
            .transpose()?
            .flatten();
        #[allow(unused_mut)] // Metrics feature installs a collector before the worker starts.
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
        let progress = Arc::new(Mutex::new(recovered.clone()));
        let worker_progress = Arc::clone(&progress);
        let worker_handle = handle.clone();
        let worker_config = config.clone();
        handle.set_status(JobStatus::Running);
        let worker = std::thread::Builder::new()
            .name(format!("ironbeam-stream-{}", handle.id()))
            .spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    run_stream(
                        &registry,
                        &plan,
                        &sink_key,
                        &worker_config,
                        &worker_handle,
                        &worker_progress,
                        checkpoint,
                        recovered,
                    )
                }))
                .unwrap_or_else(|_| {
                    Err(Error::execution("job", "direct", "stream worker panicked"))
                });
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

    /// Start two idempotent append-sink branches from one resumable source.
    /// Both sinks commit before a batch cursor is checkpointed. A retry can
    /// replay a committed first sink if the second commit failed.
    pub fn start_stream_sink_pair<T: Element, U: Element>(
        &self,
        left: &Sink<T>,
        right: &Sink<U>,
        config: &JobConfig,
    ) -> Result<StreamJob, Error> {
        if !left.shares_pipeline_with(right) || left.key() == right.key() {
            return Err(Error::graph(
                "job",
                "stream sinks must be distinct in one pipeline",
            ));
        }
        let (graph, registry) = left.snapshot()?;
        self.validate_with_unbounded(&graph, true)?;
        validate_config(config)?;
        if config.spill.is_some() {
            return Err(Error::unsupported("job", "direct", "stream-spill"));
        }
        for key in [left.key(), right.key()] {
            let Some(Operation::Sink { spec }) = graph.node(key).map(|node| &node.operation) else {
                return Err(Error::graph(key, "requested terminal is not a sink"));
            };
            if spec.mode != SinkMode::Append
                || spec.replay != Replay::Idempotent
                || !spec.commit
                || !registry.has_sink(key)
            {
                return Err(Error::unsupported(key, "direct", "idempotent-stream-sink"));
            }
        }
        let plan = multi_output::prepare_sinks(&graph, &registry, left.key(), right.key())?;
        let source_node = graph
            .node(&plan.source_key)
            .ok_or_else(|| Error::graph(&plan.source_key, "stream source node missing"))?;
        let Operation::Source {
            progress: source_progress,
        } = &source_node.operation
        else {
            return Err(Error::graph(&plan.source_key, "stream route has no source"));
        };
        if source_node.boundedness != ironbeam_core::Boundedness::Unbounded
            || !source_progress.stable_split_id
            || source_progress.replay == Replay::None
            || !registry.has_unbounded_source(&plan.source_key)
        {
            return Err(Error::unsupported(
                &plan.source_key,
                "direct",
                "resumable-stream-source",
            ));
        }
        let graph_fingerprint = graph.fingerprint()?;
        let checkpoint = config
            .checkpoint
            .as_ref()
            .map(|checkpoint| {
                StreamCheckpoint::new_pair::<T, U>(
                    checkpoint,
                    &graph_fingerprint,
                    registry
                        .source_config_digest(&plan.source_key)
                        .ok_or_else(|| {
                            Error::unsupported(&plan.source_key, "direct", "source-config-digest")
                        })?,
                    &plan.source_key,
                    (
                        left.key(),
                        registry.sink_config_digest(left.key()).ok_or_else(|| {
                            Error::unsupported(left.key(), "direct", "sink-config-digest")
                        })?,
                    ),
                    (
                        right.key(),
                        registry.sink_config_digest(right.key()).ok_or_else(|| {
                            Error::unsupported(right.key(), "direct", "sink-config-digest")
                        })?,
                    ),
                )
            })
            .transpose()?;
        let recovered = checkpoint
            .as_ref()
            .map(StreamCheckpoint::load)
            .transpose()?
            .flatten();
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
        let progress = Arc::new(Mutex::new(recovered.clone()));
        let worker_progress = Arc::clone(&progress);
        let worker_handle = handle.clone();
        let worker_config = config.clone();
        let left_key = left.key().to_owned();
        let right_key = right.key().to_owned();
        handle.set_status(JobStatus::Running);
        let worker = std::thread::Builder::new()
            .name(format!("ironbeam-stream-{}", handle.id()))
            .spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    run_stream_pair(
                        &registry,
                        &plan,
                        &graph_fingerprint,
                        &left_key,
                        &right_key,
                        &worker_config,
                        &worker_handle,
                        &worker_progress,
                        checkpoint,
                        recovered,
                    )
                }))
                .unwrap_or_else(|_| {
                    Err(Error::execution("job", "direct", "stream worker panicked"))
                });
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
}

fn merge_progress(previous: Option<&ProgressSignal>, mut next: ProgressSignal) -> ProgressSignal {
    if let Some(previous) = previous {
        if next.checkpoint_position.is_none() {
            next.checkpoint_position
                .clone_from(&previous.checkpoint_position);
        }
        if next.event_time_ns.is_none() {
            next.event_time_ns = previous.event_time_ns;
        }
    }
    next
}

fn open_sink(registry: &Registry, key: &str) -> Result<Box<dyn SinkSession>, Error> {
    let mut session = catch_unwind(AssertUnwindSafe(|| registry.open_sink(key)))
        .unwrap_or_else(|_| Err(Error::execution(key, "direct", "sink factory panicked")))?;
    let requires_finalization =
        match catch_unwind(AssertUnwindSafe(|| session.requires_bundle_finalization())) {
            Ok(requires_finalization) => requires_finalization,
            Err(_) => {
                let _ = catch_unwind(AssertUnwindSafe(|| session.abort()));
                return Err(Error::execution(
                    key,
                    "direct",
                    "sink finalization probe panicked",
                ));
            }
        };
    if requires_finalization {
        let _ = catch_unwind(AssertUnwindSafe(|| session.abort()));
        return Err(Error::unsupported(
            key,
            "direct",
            "stream-bundle-finalization",
        ));
    }
    Ok(session)
}

fn begin_sink_bundle(
    registry: &Registry,
    key: &str,
    instruction_id: &str,
) -> Result<Box<dyn SinkSession>, Error> {
    let mut session = open_sink(registry, key)?;
    let started = catch_unwind(AssertUnwindSafe(|| session.begin_bundle(instruction_id)))
        .unwrap_or_else(|_| {
            Err(Error::execution(
                key,
                "direct",
                "sink bundle begin panicked",
            ))
        });
    if let Err(error) = started {
        let _ = catch_unwind(AssertUnwindSafe(|| session.abort()));
        return Err(error);
    }
    Ok(session)
}

fn bundle_id(graph_fingerprint: &str, sink_keys: &[&str], progress: &ProgressSignal) -> String {
    let mut hasher = Sha256::new();
    for bytes in std::iter::once(graph_fingerprint.as_bytes())
        .chain(sink_keys.iter().map(|key| key.as_bytes()))
        .chain([
            progress.source_key.as_bytes(),
            progress.split_id.as_bytes(),
            progress.checkpoint_position.as_deref().unwrap_or_default(),
        ])
    {
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn run_stream(
    registry: &Registry,
    plan: &PhysicalPlan,
    sink_key: &str,
    config: &JobConfig,
    handle: &JobHandle,
    shared_progress: &Arc<Mutex<Option<ProgressSignal>>>,
    checkpoint: Option<StreamCheckpoint>,
    mut progress: Option<ProgressSignal>,
) -> Result<(), Error> {
    let route = &plan.routes[0];
    let mut source = catch_unwind(AssertUnwindSafe(|| {
        registry.open_unbounded_source(&route.source_key, progress.clone())
    }))
    .unwrap_or_else(|_| {
        Err(Error::execution(
            &route.source_key,
            "direct",
            "source factory panicked",
        ))
    })?;
    loop {
        if config.cancellation.load(Ordering::Acquire) {
            return Err(cancelled());
        }
        let polled = catch_unwind(AssertUnwindSafe(|| source.poll(plan.summary.batch_limit)))
            .unwrap_or_else(|_| {
                Err(Error::execution(
                    &route.source_key,
                    "direct",
                    "source poll panicked",
                ))
            })?;
        let UnboundedPoll::Ready(batch) = polled else {
            std::thread::sleep(Duration::from_millis(10));
            continue;
        };
        if !batch.elements.is_empty()
            && progress.as_ref().is_some_and(|previous| {
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
            &plan.summary.graph_fingerprint,
            &[sink_key],
            &batch.progress,
        );
        let input_count = u64::try_from(batch.elements.len()).unwrap_or(u64::MAX);
        handle
            .metrics
            .source_elements
            .fetch_add(input_count, Ordering::Relaxed);
        handle
            .metrics
            .peak_batch_elements
            .fetch_max(input_count, Ordering::Relaxed);
        handle.metrics.batches.fetch_add(1, Ordering::Relaxed);
        let mut session = None;
        let mut written = 0_u64;
        let result = (|| {
            for input in batch.elements {
                let mut events = vec![input];
                for key in &route.operations {
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
                            handle
                                .metrics
                                .filtered_elements
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        if next.len().saturating_add(produced.len()) > config.buffer_budget_elements
                        {
                            return Err(Error::resource(
                                key,
                                "stream transform output exceeds buffer budget",
                            ));
                        }
                        next.extend(produced);
                    }
                    events = next;
                }
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
            Ok(())
        })();
        if let Err(error) = result {
            if let Some(session) = &mut session {
                let _ = catch_unwind(AssertUnwindSafe(|| session.abort()));
            }
            return Err(error);
        }
        let next_progress = merge_progress(progress.as_ref(), batch.progress);
        if let Some(checkpoint) = &checkpoint
            && (next_progress.checkpoint_position.is_some() || next_progress.completed)
        {
            checkpoint.save(&next_progress)?;
        }
        let total = handle
            .metrics
            .output_elements
            .fetch_add(written, Ordering::Relaxed)
            .saturating_add(written);
        handle.processed.store(total, Ordering::Relaxed);
        *shared_progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(next_progress.clone());
        if next_progress.completed {
            return Ok(());
        }
        progress = Some(next_progress);
    }
}

fn write_stream_branch(
    registry: &Registry,
    key: &str,
    instruction_id: &str,
    events: Vec<ironbeam_core::Envelope>,
    session: &mut Option<Box<dyn SinkSession>>,
    config: &JobConfig,
) -> Result<u64, Error> {
    let mut written = 0_u64;
    for event in events {
        if config.cancellation.load(Ordering::Acquire) {
            return Err(cancelled());
        }
        if session.is_none() {
            *session = Some(begin_sink_bundle(registry, key, instruction_id)?);
        }
        catch_unwind(AssertUnwindSafe(|| {
            session.as_mut().expect("opened sink").write(event)
        }))
        .unwrap_or_else(|_| Err(Error::execution(key, "direct", "sink writer panicked")))?;
        written = written.saturating_add(1);
    }
    Ok(written)
}

fn commit_stream_branch(
    key: &str,
    session: &mut Option<Box<dyn SinkSession>>,
) -> Result<(), Error> {
    if let Some(session) = session {
        catch_unwind(AssertUnwindSafe(|| session.commit()))
            .unwrap_or_else(|_| Err(Error::execution(key, "direct", "sink commit panicked")))?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_stream_pair(
    registry: &Registry,
    plan: &PairPlan,
    graph_fingerprint: &str,
    left_key: &str,
    right_key: &str,
    config: &JobConfig,
    handle: &JobHandle,
    shared_progress: &Arc<Mutex<Option<ProgressSignal>>>,
    checkpoint: Option<StreamCheckpoint>,
    mut progress: Option<ProgressSignal>,
) -> Result<(), Error> {
    let mut source = catch_unwind(AssertUnwindSafe(|| {
        registry.open_unbounded_source(&plan.source_key, progress.clone())
    }))
    .unwrap_or_else(|_| {
        Err(Error::execution(
            &plan.source_key,
            "direct",
            "source factory panicked",
        ))
    })?;
    loop {
        if config.cancellation.load(Ordering::Acquire) {
            return Err(cancelled());
        }
        let polled = catch_unwind(AssertUnwindSafe(|| {
            source.poll(config.buffer_budget_elements)
        }))
        .unwrap_or_else(|_| {
            Err(Error::execution(
                &plan.source_key,
                "direct",
                "source poll panicked",
            ))
        })?;
        let UnboundedPoll::Ready(batch) = polled else {
            std::thread::sleep(Duration::from_millis(10));
            continue;
        };
        if !batch.elements.is_empty()
            && progress.as_ref().is_some_and(|previous| {
                previous.checkpoint_position == batch.progress.checkpoint_position
            })
        {
            return Err(Error::execution(
                &plan.source_key,
                "source",
                "data batch did not advance its cursor",
            ));
        }
        let instruction_id = bundle_id(graph_fingerprint, &[left_key, right_key], &batch.progress);
        let input_count = u64::try_from(batch.elements.len()).unwrap_or(u64::MAX);
        handle
            .metrics
            .source_elements
            .fetch_add(input_count, Ordering::Relaxed);
        handle
            .metrics
            .peak_batch_elements
            .fetch_max(input_count, Ordering::Relaxed);
        handle.metrics.batches.fetch_add(1, Ordering::Relaxed);
        let mut left_session = None;
        let mut right_session = None;
        let mut written = 0_u64;
        let result = (|| {
            for input in batch.elements {
                let (left_events, right_events) =
                    multi_output::process_event(input, plan, registry, config, &handle.metrics)?;
                if left_events.len().saturating_add(right_events.len())
                    > config.buffer_budget_elements
                {
                    return Err(Error::resource(
                        "job",
                        "stream branch output exceeds buffer budget",
                    ));
                }
                written = written.saturating_add(write_stream_branch(
                    registry,
                    left_key,
                    &instruction_id,
                    left_events,
                    &mut left_session,
                    config,
                )?);
                written = written.saturating_add(write_stream_branch(
                    registry,
                    right_key,
                    &instruction_id,
                    right_events,
                    &mut right_session,
                    config,
                )?);
            }
            if config.cancellation.load(Ordering::Acquire) {
                return Err(cancelled());
            }
            commit_stream_branch(left_key, &mut left_session)?;
            commit_stream_branch(right_key, &mut right_session)?;
            Ok(())
        })();
        if let Err(error) = result {
            if let Some(session) = &mut left_session {
                let _ = catch_unwind(AssertUnwindSafe(|| session.abort()));
            }
            if let Some(session) = &mut right_session {
                let _ = catch_unwind(AssertUnwindSafe(|| session.abort()));
            }
            return Err(error);
        }
        let next_progress = merge_progress(progress.as_ref(), batch.progress);
        if let Some(checkpoint) = &checkpoint
            && (next_progress.checkpoint_position.is_some() || next_progress.completed)
        {
            checkpoint.save(&next_progress)?;
        }
        let total = handle
            .metrics
            .output_elements
            .fetch_add(written, Ordering::Relaxed)
            .saturating_add(written);
        handle.processed.store(total, Ordering::Relaxed);
        *shared_progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(next_progress.clone());
        if next_progress.completed {
            return Ok(());
        }
        progress = Some(next_progress);
    }
}

#[cfg(test)]
mod sink_open_tests {
    use super::open_sink;
    use ironbeam_core::{Error, ErrorKind, Pipeline, Replay, SinkMode, SinkSpec, TypedSink};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    struct FinalizationProbeSink {
        aborts: Arc<AtomicUsize>,
        panic_on_probe: bool,
    }
    impl TypedSink<i32> for FinalizationProbeSink {
        fn write(&mut self, _: i32) -> Result<(), Error> {
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn requires_bundle_finalization(&self) -> bool {
            if self.panic_on_probe {
                panic!("injected finalization probe panic");
            }
            true
        }
    }

    #[test]
    fn unsupported_or_panicking_finalization_probe_aborts_opened_sink() {
        let aborts = Arc::new(AtomicUsize::new(0));
        let panic_on_probe = Arc::new(AtomicBool::new(false));
        let pipeline = Pipeline::default();
        let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
        let sink_aborts = Arc::clone(&aborts);
        let probe_flag = Arc::clone(&panic_on_probe);
        let sink = source
            .sink_named(
                "sink",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                move || {
                    Ok(Box::new(FinalizationProbeSink {
                        aborts: Arc::clone(&sink_aborts),
                        panic_on_probe: probe_flag.load(Ordering::SeqCst),
                    }))
                },
            )
            .unwrap();
        let (_, registry) = sink.snapshot().unwrap();
        assert!(matches!(
            open_sink(&registry, sink.key()).err().unwrap().kind,
            ErrorKind::UnsupportedCapability { .. }
        ));
        assert_eq!(aborts.load(Ordering::SeqCst), 1);
        panic_on_probe.store(true, Ordering::SeqCst);
        assert_eq!(
            open_sink(&registry, sink.key()).err().unwrap().kind,
            ErrorKind::Execution
        );
        assert_eq!(aborts.load(Ordering::SeqCst), 2);
    }
}
