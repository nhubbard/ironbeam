// Unit tests for src/stream/multi/grouped.rs.
use super::*;
use crate::CheckpointConfig;
use ironbeam_core::{
    Pipeline, Replay, SinkSpec, SourceProgress, TestStreamEvent, Timestamped, TypedSink,
    TypedUnboundedSourceReader, UnboundedBatch, UnboundedPoll, UnboundedSource,
};
use std::collections::VecDeque;

struct NullSink;

struct ClockSource(Option<i64>);

impl UnboundedSource<Envelope> for ClockSource {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Envelope>, Error> {
        Ok(UnboundedPoll::Pending)
    }

    fn processing_time_ns(&self) -> Option<i64> {
        self.0
    }
}

#[test]
fn multi_source_clock_tracks_scripted_progress_and_rejects_mixed_domains() {
    let readers = vec![
        Some(Box::new(ClockSource(Some(2))) as Box<dyn UnboundedSource<Envelope>>),
        None,
        Some(Box::new(ClockSource(Some(7))) as Box<dyn UnboundedSource<Envelope>>),
    ];
    assert_eq!(multi_processing_now_ns(&readers, None).unwrap(), 7);
    assert_eq!(multi_processing_now_ns(&readers, Some(11)).unwrap(), 11);
    assert_eq!(
        multi_processing_now_ns(
            &[Some(
                Box::new(ClockSource(Some(-1))) as Box<dyn UnboundedSource<Envelope>>
            )],
            None,
        )
        .err()
        .unwrap()
        .kind,
        ErrorKind::Incompatible
    );
    assert_eq!(
        multi_processing_now_ns(
            &[
                Some(Box::new(ClockSource(Some(0))) as Box<dyn UnboundedSource<Envelope>>),
                Some(Box::new(ClockSource(None)) as Box<dyn UnboundedSource<Envelope>>),
            ],
            None,
        )
        .err()
        .unwrap()
        .kind,
        ErrorKind::UnsupportedCapability {
            capability: "mixed-scripted-and-wall-clocks".into(),
        }
    );
    assert!(
        multi_processing_now_ns(
            &[Some(
                Box::new(ClockSource(None)) as Box<dyn UnboundedSource<Envelope>>
            )],
            None,
        )
        .unwrap()
            > 0
    );
}

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

struct PanicSink {
    panic_on_write: bool,
}

impl TypedSink<i32> for PanicSink {
    fn write(&mut self, _: i32) -> Result<(), Error> {
        assert!(!self.panic_on_write, "sink write failed");
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        assert!(self.panic_on_write, "sink commit failed");
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

fn fixture() -> (
    Graph,
    Registry,
    PhysicalPlan,
    MultiGroupedStreamRoute,
    String,
) {
    fixture_with_sink_fault(None)
}

fn fixture_with_sink_fault(
    fault: Option<bool>,
) -> (
    Graph,
    Registry,
    PhysicalPlan,
    MultiGroupedStreamRoute,
    String,
) {
    let pipeline = Pipeline::default();
    let make = |key: &str, value| {
        pipeline
            .test_stream_source(
                key,
                vec![TestStreamEvent::Elements(vec![((1_i32, value), 1_000_000)])],
            )
            .unwrap()
            .fixed_windows(&format!("{key}-window"), 10_000_000)
            .unwrap()
            .allowed_lateness_named(&format!("{key}-lateness"), 5_000_000)
            .unwrap()
    };
    let first = make("first", 2);
    let second = make("second", 3);
    let sink = first
        .co_group_homogeneous_named("group", &[second])
        .unwrap()
        .map_named("sum", |(_, buckets)| {
            buckets.into_iter().flatten().sum::<i32>()
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "grouped-test-sink-v1".into(),
            move || {
                let sink: Box<dyn TypedSink<i32>> = match fault {
                    Some(panic_on_write) => Box::new(PanicSink { panic_on_write }),
                    None => Box::new(NullSink),
                };
                Ok(sink)
            },
        )
        .unwrap();
    let (graph, registry) = sink.snapshot().unwrap();
    let plan = planner::lower(&graph, &registry, sink.key(), 100).unwrap();
    let route = multi_grouped_stream_route(&graph, sink.key()).unwrap();
    (graph, registry, plan, route, sink.key().into())
}

fn checkpoint_config(path: &std::path::Path) -> JobConfig {
    JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: path.into(),
            interval_batches: 1,
            max_checkpoints: 1,
            recover: true,
            binary_build_id: "grouped-state-test-v1".into(),
        }),
        ..JobConfig::default()
    }
}

fn save_grouped_state(
    registry: &Registry,
    plan: &PhysicalPlan,
    route: &MultiGroupedStreamRoute,
    sink_key: &str,
    config: &JobConfig,
    state: &MultiGroupedState,
    progress: &ProgressSignal,
) {
    let mut digest = String::new();
    for source in &route.sources {
        let part = registry.source_config_digest(&source.key).unwrap();
        write!(
            digest,
            "{}:{}{}:{}",
            source.key.len(),
            source.key,
            part.len(),
            part
        )
        .unwrap();
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
            progress,
            &serde_json::to_vec(state).unwrap(),
            config.memory_budget_bytes,
        )
        .unwrap();
}

fn empty_checkpoint_state(
    registry: &Registry,
    route: &MultiGroupedStreamRoute,
) -> MultiGroupedState {
    MultiGroupedState {
        version: 2,
        processing_clock_ns: None,
        sources: route
            .sources
            .iter()
            .map(|source| {
                Some(ProgressSignal {
                    source_key: source.key.clone(),
                    split_id: "split-0".into(),
                    checkpoint_position: Some(vec![1]),
                    event_time_ns: Some(10_000_000),
                    completed: false,
                })
            })
            .collect(),
        windows: MultiWindowState::empty(5_000_000)
            .encode(registry, route, 1024)
            .unwrap(),
    }
}

#[test]
fn grouped_start_rejects_mismatched_coded_checkpoints() {
    for fault in [
        "version",
        "count",
        "source",
        "split",
        "progress",
        "cursor-length",
        "clock",
    ] {
        let temporary = tempfile::tempdir().unwrap();
        let config = checkpoint_config(temporary.path());
        let (graph, registry, plan, route, sink_key) = fixture();
        let mut state = empty_checkpoint_state(&registry, &route);
        let mut progress = multi_progress(2, &state.sources);
        match fault {
            "version" => state.version = 1,
            "count" => {
                state.sources.pop();
            }
            "source" => state.sources[0].as_mut().unwrap().source_key = "other".into(),
            "split" => state.sources[0].as_mut().unwrap().split_id.clear(),
            "progress" => progress.event_time_ns = Some(99),
            "cursor-length" => progress.checkpoint_position = Some(vec![1]),
            "clock" => state.processing_clock_ns = Some(-1),
            _ => unreachable!(),
        }
        save_grouped_state(
            &registry, &plan, &route, &sink_key, &config, &state, &progress,
        );
        let error = start_multi_grouped_stream_sink::<i32>(
            &graph, registry, plan, sink_key, &config, route,
        )
        .err()
        .unwrap_or_else(|| panic!("{fault}: invalid checkpoint was accepted"));
        assert!(
            matches!(error.kind, ErrorKind::Incompatible | ErrorKind::Recovery),
            "{fault}: {error:?}"
        );
    }
}

#[test]
fn grouped_start_honors_preexisting_cancellation() {
    let (graph, registry, plan, route, sink_key) = fixture();
    let config = JobConfig::default();
    config.cancellation.store(true, Ordering::SeqCst);
    assert_eq!(
        start_multi_grouped_stream_sink::<i32>(&graph, registry, plan, sink_key, &config, route,)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Cancelled
    );
}

#[test]
fn grouped_start_reuses_a_completed_empty_checkpoint() {
    let temporary = tempfile::tempdir().unwrap();
    let config = checkpoint_config(temporary.path());
    let (graph, registry, plan, route, sink_key) = fixture();
    let mut state = empty_checkpoint_state(&registry, &route);
    for signal in state.sources.iter_mut().flatten() {
        signal.completed = true;
    }
    let progress = multi_progress(2, &state.sources);
    save_grouped_state(
        &registry, &plan, &route, &sink_key, &config, &state, &progress,
    );
    let job =
        start_multi_grouped_stream_sink::<i32>(&graph, registry, plan, sink_key, &config, route)
            .unwrap();
    assert!(job.progress().unwrap().completed);
    assert_eq!(job.wait().unwrap().status(), JobStatus::Succeeded);
}

#[test]
fn grouped_start_rejects_completed_checkpoint_with_retained_fold() {
    let temporary = tempfile::tempdir().unwrap();
    let config = checkpoint_config(temporary.path());
    let (graph, registry, plan, route, sink_key) = fixture();
    let mut state = empty_checkpoint_state(&registry, &route);
    state.windows = MultiWindowState::Late {
        folds: BTreeMap::from([(
            (0, 10_000_000),
            (registry.open_fold(&route.group_key).unwrap(), 0),
        )]),
        buffered: 0,
        late_windows: BTreeSet::new(),
        allowed_lateness_ns: 5_000_000,
    }
    .encode(&registry, &route, config.memory_budget_bytes)
    .unwrap();
    for signal in state.sources.iter_mut().flatten() {
        signal.completed = true;
    }
    let progress = multi_progress(2, &state.sources);
    save_grouped_state(
        &registry, &plan, &route, &sink_key, &config, &state, &progress,
    );
    assert_eq!(
        start_multi_grouped_stream_sink::<i32>(&graph, registry, plan, sink_key, &config, route)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
}

#[test]
fn grouped_start_rejects_missing_coder_and_prestart_cancellation() {
    let temporary = tempfile::tempdir().unwrap();
    let config = checkpoint_config(temporary.path());
    let (graph, registry, plan, mut route, sink_key) = fixture();
    route.group_input_key = "missing-coder".into();
    assert!(matches!(
        start_multi_grouped_stream_sink::<i32>(&graph, registry, plan, sink_key, &config, route)
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    let (graph, registry, plan, route, sink_key) = fixture();
    let config = JobConfig::default();
    config.cancellation.store(true, Ordering::SeqCst);
    assert_eq!(
        start_multi_grouped_stream_sink::<i32>(&graph, registry, plan, sink_key, &config, route)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Cancelled
    );
}

#[test]
fn grouped_start_rejects_early_trigger_combined_with_lateness() {
    let (mut graph, registry, plan, route, sink_key) = fixture();
    graph
        .nodes
        .iter_mut()
        .find(|node| node.key == route.group_key)
        .unwrap()
        .time
        .trigger = WindowTrigger::EarlyElementCount { count: 1 };
    let error = start_multi_grouped_stream_sink::<i32>(
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
}

#[test]
fn grouped_start_rejects_early_session_trigger_before_opening_sources() {
    let (mut graph, registry, plan, mut route, sink_key) = fixture();
    let group = graph
        .nodes
        .iter_mut()
        .find(|node| node.key == route.group_key)
        .unwrap();
    group.time.allowed_lateness_ns = 0;
    group.time.trigger = WindowTrigger::EarlyElementCount { count: 1 };
    group.time.window = Window::Session { gap_ns: 5_000_000 };
    route.window = group.time.window.clone();
    let error = start_multi_grouped_stream_sink::<i32>(
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
}

#[test]
fn grouped_start_rejects_combined_count_and_late_count_without_lateness() {
    for (trigger, lateness) in [
        (
            WindowTrigger::EarlyAndLateElementCount {
                early_count: 1,
                late_count: 1,
            },
            5_000_000,
        ),
        (WindowTrigger::LateElementCount { count: 1 }, 0),
    ] {
        let (mut graph, registry, plan, route, sink_key) = fixture();
        let group = graph
            .nodes
            .iter_mut()
            .find(|node| node.key == route.group_key)
            .unwrap();
        group.time.trigger = trigger;
        group.time.allowed_lateness_ns = lateness;
        let error = start_multi_grouped_stream_sink::<i32>(
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
    }
}

#[test]
fn grouped_session_lateness_is_rejected_at_graph_construction() {
    let pipeline = Pipeline::default();
    let error = pipeline
        .test_stream_source(
            "first",
            vec![TestStreamEvent::Elements(vec![((1_i32, 2_i32), 1_000_000)])],
        )
        .unwrap()
        .session_windows_named("first-sessions", 5_000_000)
        .unwrap()
        .allowed_lateness_named("first-lateness", 5_000_000)
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Graph);
    assert!(error.message.contains("fixed or sliding windows"));
}

#[test]
fn grouped_start_reports_missing_group_and_sink_registration() {
    let (mut graph, registry, plan, route, sink_key) = fixture();
    graph.nodes.retain(|node| node.key != route.group_key);
    assert_eq!(
        start_multi_grouped_stream_sink::<i32>(
            &graph,
            registry,
            plan,
            sink_key,
            &JobConfig::default(),
            route,
        )
        .err()
        .unwrap()
        .kind,
        ErrorKind::Graph
    );

    let temporary = tempfile::tempdir().unwrap();
    let config = checkpoint_config(temporary.path());
    let (graph, registry, plan, route, _) = fixture();
    let error = start_multi_grouped_stream_sink::<i32>(
        &graph,
        registry,
        plan,
        "missing-sink".into(),
        &config,
        route,
    )
    .err()
    .unwrap();
    assert!(matches!(
        error.kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
}

#[test]
fn grouped_stream_contains_sink_write_and_commit_panics() {
    for panic_on_write in [true, false] {
        let (graph, registry, plan, route, sink_key) =
            fixture_with_sink_fault(Some(panic_on_write));
        let error = start_multi_grouped_stream_sink::<i32>(
            &graph,
            registry,
            plan,
            sink_key,
            &JobConfig::default(),
            route,
        )
        .unwrap()
        .wait()
        .err()
        .unwrap();
        assert_eq!(error.kind, ErrorKind::Execution);
    }
}

struct FaultSource {
    panic_on_poll: bool,
    polls: VecDeque<UnboundedPoll<Timestamped<(i32, i32)>>>,
}

impl UnboundedSource<Timestamped<(i32, i32)>> for FaultSource {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<(i32, i32)>>, Error> {
        assert!(!self.panic_on_poll, "source poll failed");
        Ok(self.polls.pop_front().unwrap_or(UnboundedPoll::Pending))
    }
}

fn fault_poll(
    source_key: &str,
    split_id: &str,
    cursor: u8,
    watermark: i64,
    row: Option<i32>,
) -> UnboundedPoll<Timestamped<(i32, i32)>> {
    UnboundedPoll::Ready(UnboundedBatch {
        elements: row
            .map(|value| Timestamped {
                value: (1, value),
                timestamp_ns: 1_000_000,
            })
            .into_iter()
            .collect(),
        progress: ProgressSignal {
            source_key: source_key.into(),
            split_id: split_id.into(),
            checkpoint_position: Some(vec![cursor]),
            event_time_ns: Some(watermark),
            completed: false,
        },
    })
}

fn faulty_source_fixture(
    fault: &'static str,
) -> (
    Graph,
    Registry,
    PhysicalPlan,
    MultiGroupedStreamRoute,
    String,
) {
    let pipeline = Pipeline::default();
    let first = pipeline
        .unbounded_source_with_factory(
            "first",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            format!("fault-source-{fault}-v1"),
            0,
            move |_| {
                assert_ne!(fault, "factory-panic", "source factory failed");
                let polls = match fault {
                    "wrong-source" => vec![fault_poll("other", "split-0", 1, 0, None)],
                    "split-change" => vec![
                        fault_poll("first", "split-0", 1, 0, None),
                        fault_poll("first", "split-1", 2, 1, None),
                    ],
                    "stalled-cursor" => vec![
                        fault_poll("first", "split-0", 1, 0, Some(2)),
                        fault_poll("first", "split-0", 1, 1, Some(3)),
                    ],
                    "watermark-regress" => vec![
                        fault_poll("first", "split-0", 1, 10_000_000, None),
                        fault_poll("first", "split-0", 2, 9_000_000, None),
                    ],
                    _ => Vec::new(),
                };
                Ok(Box::new(FaultSource {
                    panic_on_poll: fault == "poll-panic",
                    polls: polls.into(),
                }) as TypedUnboundedSourceReader<(i32, i32)>)
            },
        )
        .unwrap()
        .fixed_windows("first-window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("first-lateness", 5_000_000)
        .unwrap();
    let second = pipeline
        .test_stream_source(
            "second",
            vec![TestStreamEvent::Elements(vec![((1_i32, 4_i32), 1_000_000)])],
        )
        .unwrap()
        .fixed_windows("second-window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("second-lateness", 5_000_000)
        .unwrap();
    let sink = first
        .co_group_homogeneous_named("group", &[second])
        .unwrap()
        .map_named("sum", |(_, buckets)| {
            buckets.into_iter().flatten().sum::<i32>()
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "fault-source-sink-v1".into(),
            || Ok(Box::new(NullSink)),
        )
        .unwrap();
    let (graph, registry) = sink.snapshot().unwrap();
    let plan = planner::lower(&graph, &registry, sink.key(), 100).unwrap();
    let route = multi_grouped_stream_route(&graph, sink.key()).unwrap();
    (graph, registry, plan, route, sink.key().into())
}

#[test]
fn grouped_stream_contains_source_factory_poll_and_progress_failures() {
    for fault in [
        "factory-panic",
        "poll-panic",
        "wrong-source",
        "split-change",
        "stalled-cursor",
        "watermark-regress",
    ] {
        let (graph, registry, plan, route, sink_key) = faulty_source_fixture(fault);
        let error = start_multi_grouped_stream_sink::<i32>(
            &graph,
            registry,
            plan,
            sink_key,
            &JobConfig::default(),
            route,
        )
        .unwrap()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("{fault}: malformed source was accepted"));
        assert_eq!(error.kind, ErrorKind::Execution, "{fault}: {error:?}");
    }
}

#[test]
fn grouped_branch_fanout_copies_rows_for_shared_and_distinct_paths() {
    let (graph, registry, _, route, _) = fixture();
    let source = &route.sources[0];
    let first_stage = graph
        .nodes
        .iter()
        .find(|node| node.input.as_deref() == Some(source.key.as_str()))
        .unwrap()
        .key
        .clone();
    let event = || Envelope {
        pane: ironbeam_core::PaneInfo::NO_FIRING,
        value: Box::new((1_i32, 2_i32)),
        timestamp_ns: 1_000_000,
        window: Window::Global,
        assigned_window: None,
    };
    let config = JobConfig::default();
    let handle = JobHandle::new(config.cancellation.clone(), JobStatus::Running);
    let empty: &[String] = &[];
    let two = apply_branch_slices(
        &registry,
        &source.key,
        &[empty, empty],
        event(),
        &config,
        &handle,
    )
    .unwrap();
    assert_eq!(two.len(), 2);
    let staged = [first_stage];
    let split = apply_branch_slices(
        &registry,
        &source.key,
        &[empty, &staged],
        event(),
        &config,
        &handle,
    )
    .unwrap();
    assert_eq!(split.len(), 2);
}
