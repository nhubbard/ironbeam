// Unit tests for src/stream/multi.rs.
use super::*;
use crate::CheckpointConfig;
use ironbeam_core::{Boundedness, Pipeline, SinkSpec, SourceProgress, TestStreamEvent, TypedSink};

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
        write!(
            digest,
            "{}:{}{}:{}",
            route.source_key.len(),
            route.source_key,
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
        write!(digest, "{}:{}{}:{}", key.len(), key, part.len(), part).unwrap();
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
        let error = start_dual_side_stream::<i32>(&graph, registry, plan, sink_key, &config, route)
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
        let error =
            start_multi_stream_sink::<i32>(&graph, registry, plan, sink_key, &JobConfig::default())
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
    let job = start_multi_stream_sink::<i32>(&graph, registry, plan, sink_key, &config).unwrap();
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
    let job =
        start_dual_side_stream::<i32>(&graph, registry, plan, sink_key, &config, route).unwrap();
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
