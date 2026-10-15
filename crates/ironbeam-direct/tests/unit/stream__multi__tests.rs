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
        let mut state = DualWindowState {
            version: 1,
            sources: [
                Some(source(&route.source_key)),
                Some(source(&route.side_source_key)),
            ],
            main: encode_window_rows(
                &registry,
                &route.main_input_key,
                &BTreeMap::new(),
                config.memory_budget_bytes,
            )
            .unwrap(),
            side: encode_window_rows(
                &registry,
                &route.side_input_key,
                &BTreeMap::new(),
                config.memory_budget_bytes,
            )
            .unwrap(),
            closed: Vec::new(),
        };
        let mut combined = multi_progress(2, &state.sources);
        if fault == "budget" || fault == "completed-open" {
            let event = Envelope {
                pane: ironbeam_core::PaneInfo::NO_FIRING,
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
fn dual_stream_completes_without_checkpoint() {
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
                .unwrap_or_else(|| panic!("{fault}: invalid route was accepted"));
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
fn bounded_stream_resume_rejects_a_shortened_source_and_invalid_position() {
    let pipeline = Pipeline::default();
    pipeline.from_vec("bounded", vec![1_i32]).unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let mut progress = ProgressSignal {
        source_key: "bounded".into(),
        split_id: "bounded".into(),
        checkpoint_position: Some(2_u64.to_le_bytes().to_vec()),
        event_time_ns: None,
        completed: false,
    };
    assert_eq!(
        BoundedStreamReader::open(&registry, "bounded", Some(&progress), 1)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Incompatible
    );
    progress.checkpoint_position = Some(vec![1]);
    assert_eq!(
        BoundedStreamReader::open(&registry, "bounded", Some(&progress), 1)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
    progress.checkpoint_position = None;
    assert_eq!(
        BoundedStreamReader::open(&registry, "bounded", Some(&progress), 1)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
    progress.source_key = "other".into();
    assert_eq!(
        BoundedStreamReader::open(&registry, "bounded", Some(&progress), 1)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Incompatible
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
    route.side_source_key.clone_from(&route.source_key);
    route.main_stages.insert(0, "uncoded-common-stage".into());
    route.side_stages.insert(0, "uncoded-common-stage".into());
    assert!(matches!(
        start_dual_side_stream::<i32>(&graph, registry, plan, sink_key, &config, route)
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    let (graph, registry, plan, mut route, sink_key) = dual_fixture();
    route.side_input_key = "missing-coder".into();
    assert!(matches!(
        start_dual_side_stream::<i32>(&graph, registry, plan, sink_key, &config, route)
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    let (graph, registry, plan, mut route, sink_key) = dual_fixture();
    route.window = Window::Session { gap_ns: 10 };
    route.aggregate_key = "missing-session-coder".into();
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
fn retained_dual_pane_checkpoint_rejects_invalid_keys_indices_and_expiry() {
    let valid = CodedClosedPane {
        start_ns: 0,
        end_ns: 10,
        key: vec![1, 2],
        next_index: 2,
        fired_value_count: 0,
    };
    let restored = dual_late::restore_closed(
        vec![CodedClosedPane {
            start_ns: valid.start_ns,
            end_ns: valid.end_ns,
            key: valid.key.clone(),
            next_index: valid.next_index,
            fired_value_count: valid.fired_value_count,
        }],
        Some(10),
        5,
        1,
        false,
    )
    .unwrap();
    assert_eq!(
        restored
            .get(&(0, 10, vec![1, 2]))
            .map(|state| state.next_index),
        Some(2)
    );
    assert_eq!(dual_late::encode_closed(&restored).len(), 1);
    assert!(matches!(
        dual_late::restore_closed(
            vec![CodedClosedPane {
                start_ns: 0,
                end_ns: 10,
                key: vec![1],
                next_index: 1,
                fired_value_count: 0,
            }],
            Some(10),
            5,
            0,
            false,
        ),
        Err(Error {
            kind: ErrorKind::Resource,
            ..
        })
    ));
    for rows in [
        vec![
            valid,
            CodedClosedPane {
                start_ns: 0,
                end_ns: 10,
                key: vec![1, 2],
                next_index: 1,
                fired_value_count: 0,
            },
        ],
        vec![CodedClosedPane {
            start_ns: 10,
            end_ns: 10,
            key: vec![1],
            next_index: 1,
            fired_value_count: 0,
        }],
        vec![CodedClosedPane {
            start_ns: 0,
            end_ns: 10,
            key: vec![1],
            next_index: 0,
            fired_value_count: 0,
        }],
    ] {
        assert!(matches!(
            dual_late::restore_closed(rows, Some(10), 5, 2, false),
            Err(Error {
                kind: ErrorKind::Recovery,
                ..
            })
        ));
    }
    assert!(
        dual_late::restore_closed(
            vec![CodedClosedPane {
                start_ns: 0,
                end_ns: 10,
                key: vec![1],
                next_index: 1,
                fired_value_count: 0,
            }],
            Some(15),
            5,
            1,
            false,
        )
        .is_err()
    );
}

#[test]
fn accumulating_dual_pane_checkpoint_requires_a_retained_row_count() {
    let row = |fired_value_count| CodedClosedPane {
        start_ns: 0,
        end_ns: 10,
        key: vec![1, 2],
        next_index: 2,
        fired_value_count,
    };
    assert!(dual_late::restore_closed(vec![row(2)], Some(10), 5, 1, true).is_ok());
    assert!(dual_late::restore_closed(vec![row(2)], Some(10), 5, 1, false).is_err());
    assert!(dual_late::restore_closed(vec![row(0)], Some(10), 5, 1, true).is_err());
}

#[test]
fn accumulating_dual_pane_checkpoint_checks_retained_rows_against_coded_key() {
    let (_, registry, _, route, _) = dual_fixture();
    let event = Envelope {
        value: Box::new((1_i32, 1_i32)),
        timestamp_ns: 1,
        window: Window::Fixed { size_ns: 10 },
        assigned_window: Some(ironbeam_core::WindowInterval {
            start_ns: 0,
            end_ns: 10,
        }),
        pane: ironbeam_core::PaneInfo::NO_FIRING,
    };
    let encoded = registry
        .encode_element(&route.main_input_key, event.value.as_ref())
        .unwrap();
    let (key, _) = registry
        .split_join_input(&route.aggregate_key, 0, &encoded, event.timestamp_ns)
        .unwrap();
    let main = BTreeMap::from([((0, 10), vec![event])]);
    let side = BTreeMap::new();
    let closed = |count| {
        dual_late::restore_closed(
            vec![CodedClosedPane {
                start_ns: 0,
                end_ns: 10,
                key: key.clone(),
                next_index: 1,
                fired_value_count: count,
            }],
            Some(10),
            5,
            1,
            true,
        )
        .unwrap()
    };
    assert!(
        dual_late::validate_retained_counts(&registry, &route, &main, &side, &closed(1)).is_ok()
    );
    assert!(
        dual_late::validate_retained_counts(&registry, &route, &main, &side, &closed(2)).is_err()
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
        closed: Vec::new(),
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

#[derive(Clone, Copy)]
enum FoldFault {
    AddPanic,
    ClosedPanic,
    ClosedOverrelease,
    ClosedReleaseOne,
    LatePanic,
    LateOverrelease,
    LateReleaseOne,
    FinalPanic,
    FinalOverrelease,
    FinalReleaseOne,
}

struct FaultFold(FoldFault);

impl FoldSession for FaultFold {
    fn add(&mut self, _: Envelope) -> Result<(), Error> {
        assert!(!matches!(self.0, FoldFault::AddPanic), "fold add failed");
        Ok(())
    }

    fn drain_closed(&mut self, _: i64) -> Result<(Vec<Envelope>, usize), Error> {
        match self.0 {
            FoldFault::ClosedPanic => panic!("closed fold failed"),
            FoldFault::ClosedOverrelease => Ok((Vec::new(), 2)),
            FoldFault::ClosedReleaseOne => Ok((Vec::new(), 1)),
            _ => Ok((Vec::new(), 0)),
        }
    }

    fn drain_late(&mut self, final_firing: bool) -> Result<(Vec<Envelope>, usize), Error> {
        match (self.0, final_firing) {
            (FoldFault::LatePanic, false) => panic!("late fold failed"),
            (FoldFault::LateOverrelease, false) | (FoldFault::FinalOverrelease, true) => {
                Ok((Vec::new(), 2))
            }
            (FoldFault::LateReleaseOne, false) | (FoldFault::FinalReleaseOne, true) => {
                Ok((Vec::new(), 1))
            }
            (FoldFault::FinalPanic, true) => panic!("final fold failed"),
            _ => Ok((Vec::new(), 0)),
        }
    }

    fn finish(self: Box<Self>) -> Result<Vec<Envelope>, Error> {
        Ok(Vec::new())
    }
}

fn fault_group_route() -> MultiGroupedStreamRoute {
    MultiGroupedStreamRoute {
        sources: Vec::new(),
        group_input_key: "input".into(),
        group_key: "group".into(),
        window: Window::Fixed { size_ns: 10 },
        after_group: Vec::new(),
    }
}

fn fault_progress(watermark: i64) -> ProgressSignal {
    ProgressSignal {
        source_key: "source".into(),
        split_id: "split-0".into(),
        checkpoint_position: Some(vec![1]),
        event_time_ns: Some(watermark),
        completed: false,
    }
}

#[test]
fn multi_grouped_fold_panics_and_invalid_release_counts_are_typed_errors() {
    use grouped_state::MultiWindowState;

    let route = fault_group_route();
    let registry = Registry::default();
    let window = (0, 10);
    for (fault, watermark, prior, late_window, expected) in [
        (
            FoldFault::ClosedPanic,
            10,
            None,
            false,
            ErrorKind::Execution,
        ),
        (
            FoldFault::ClosedOverrelease,
            10,
            None,
            false,
            ErrorKind::Recovery,
        ),
        (
            FoldFault::LatePanic,
            11,
            Some(10),
            true,
            ErrorKind::Execution,
        ),
        (
            FoldFault::LateOverrelease,
            11,
            Some(10),
            true,
            ErrorKind::Recovery,
        ),
        (
            FoldFault::FinalPanic,
            15,
            Some(10),
            false,
            ErrorKind::Execution,
        ),
        (
            FoldFault::FinalOverrelease,
            15,
            Some(10),
            false,
            ErrorKind::Recovery,
        ),
    ] {
        let mut state = MultiWindowState::Late {
            folds: BTreeMap::from([(
                window,
                (Box::new(FaultFold(fault)) as Box<dyn FoldSession>, 1),
            )]),
            buffered: 1,
            late_windows: if late_window {
                BTreeSet::from([window])
            } else {
                BTreeSet::new()
            },
            allowed_lateness_ns: 5,
        };
        let error = state
            .drain(&registry, &route, prior, &fault_progress(watermark))
            .err()
            .unwrap();
        assert_eq!(error.kind, expected);
    }
}

#[test]
fn multi_grouped_state_versions_and_empty_snapshots_are_distinct() {
    use grouped_state::MultiWindowState;

    let registry = Registry::default();
    let route = fault_group_route();
    for lateness in [0, 5] {
        let state = MultiWindowState::empty(lateness);
        assert!(state.is_empty());
        let version = state.version();
        assert_eq!(version, if lateness == 0 { 1 } else { 2 });
        let bytes = state.encode(&registry, &route, 1024).unwrap();
        let restored =
            MultiWindowState::restore(&registry, &route, &bytes, version, lateness, 10).unwrap();
        assert!(restored.is_empty());
        assert_eq!(restored.version(), version);
        assert_eq!(
            MultiWindowState::restore(&registry, &route, &bytes, 3 - version, lateness, 10)
                .err()
                .unwrap()
                .kind,
            ErrorKind::Incompatible
        );
        if lateness > 0 {
            assert!(
                MultiWindowState::restore(
                    &registry,
                    &route,
                    b"bad-fold-state",
                    version,
                    lateness,
                    10
                )
                .is_err()
            );
        }
    }
}

#[test]
fn multi_grouped_state_drops_expired_rows_before_charging_the_budget() {
    use grouped_state::MultiWindowState;

    let registry = Registry::default();
    let route = fault_group_route();
    let event = || Envelope {
        pane: ironbeam_core::PaneInfo::NO_FIRING,
        value: Box::new((1_i32, 2_i32)),
        timestamp_ns: 2,
        window: Window::Fixed { size_ns: 10 },
        assigned_window: Some(WindowInterval {
            start_ns: 0,
            end_ns: 10,
        }),
    };
    let mut rows = MultiWindowState::empty(0);
    rows.add(&registry, &route, event(), Some(10), 0).unwrap();
    assert!(rows.is_empty());
    assert_eq!(
        rows.add(&registry, &route, event(), None, 0)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Resource
    );
    let mut late = MultiWindowState::empty(5);
    late.add(&registry, &route, event(), Some(15), 0).unwrap();
    assert!(late.is_empty());
    assert_eq!(
        late.add(&registry, &route, event(), Some(10), 0)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Resource
    );
    assert!(late.add(&registry, &route, event(), Some(10), 1).is_err());
    assert!(late.is_empty());
    let mut panicking = MultiWindowState::Late {
        folds: BTreeMap::from([(
            (0, 10),
            (
                Box::new(FaultFold(FoldFault::AddPanic)) as Box<dyn FoldSession>,
                0,
            ),
        )]),
        buffered: 0,
        late_windows: BTreeSet::new(),
        allowed_lateness_ns: 5,
    };
    assert_eq!(
        panicking
            .add(&registry, &route, event(), Some(10), 1)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Execution
    );
}

#[test]
fn multi_grouped_state_rejects_buffer_accounting_underflow() {
    use grouped_state::MultiWindowState;

    let registry = Registry::default();
    let route = fault_group_route();
    let window = (0, 10);
    let event = Envelope {
        pane: ironbeam_core::PaneInfo::NO_FIRING,
        value: Box::new((1_i32, 2_i32)),
        timestamp_ns: 2,
        window: Window::Fixed { size_ns: 10 },
        assigned_window: Some(WindowInterval {
            start_ns: 0,
            end_ns: 10,
        }),
    };
    let mut rows = MultiWindowState::Rows {
        windows: BTreeMap::from([(window, vec![event])]),
        buffered: 0,
    };
    assert_eq!(
        rows.drain(&registry, &route, None, &fault_progress(10))
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
    for (fault, watermark, prior, late_window) in [
        (FoldFault::ClosedReleaseOne, 10, None, false),
        (FoldFault::LateReleaseOne, 11, Some(10), true),
        (FoldFault::FinalReleaseOne, 15, Some(10), false),
    ] {
        let mut state = MultiWindowState::Late {
            folds: BTreeMap::from([(
                window,
                (Box::new(FaultFold(fault)) as Box<dyn FoldSession>, 1),
            )]),
            buffered: 0,
            late_windows: if late_window {
                BTreeSet::from([window])
            } else {
                BTreeSet::new()
            },
            allowed_lateness_ns: 5,
        };
        assert_eq!(
            state
                .drain(&registry, &route, prior, &fault_progress(watermark))
                .err()
                .unwrap()
                .kind,
            ErrorKind::Recovery
        );
    }
}

#[test]
fn multi_grouped_rows_report_missing_aggregate_without_losing_the_error() {
    use grouped_state::MultiWindowState;

    let registry = Registry::default();
    let route = fault_group_route();
    let event = Envelope {
        pane: ironbeam_core::PaneInfo::NO_FIRING,
        value: Box::new((1_i32, 2_i32)),
        timestamp_ns: 2,
        window: Window::Fixed { size_ns: 10 },
        assigned_window: Some(WindowInterval {
            start_ns: 0,
            end_ns: 10,
        }),
    };
    let mut rows = MultiWindowState::Rows {
        windows: BTreeMap::from([((0, 10), vec![event])]),
        buffered: 1,
    };
    assert_eq!(
        rows.drain(&registry, &route, None, &fault_progress(10))
            .err()
            .unwrap()
            .kind,
        ErrorKind::Execution
    );
}

#[test]
fn multi_grouped_state_waits_for_progress_and_skips_late_delivery_at_expiry() {
    use grouped_state::MultiWindowState;

    let registry = Registry::default();
    let route = fault_group_route();
    let mut rows = MultiWindowState::empty(0);
    let mut pending = fault_progress(0);
    pending.event_time_ns = None;
    assert!(
        rows.drain(&registry, &route, None, &pending)
            .unwrap()
            .is_empty()
    );
    let window = (0, 10);
    let mut late = MultiWindowState::Late {
        folds: BTreeMap::from([(
            window,
            (
                Box::new(FaultFold(FoldFault::ClosedPanic)) as Box<dyn FoldSession>,
                0,
            ),
        )]),
        buffered: 0,
        late_windows: BTreeSet::from([window]),
        allowed_lateness_ns: 5,
    };
    assert!(
        late.drain(&registry, &route, Some(10), &fault_progress(15))
            .unwrap()
            .is_empty()
    );
    assert!(late.is_empty());
}
