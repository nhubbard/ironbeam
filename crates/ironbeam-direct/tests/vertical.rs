use ironbeam_core::{Element, ErrorKind, Pipeline, Replay, SourceProgress};
use ironbeam_direct::{DirectRunner, JobConfig, JobHandle, JobStatus};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[test]
fn small_graph_runs_with_job_owned_settings() {
    let p = Pipeline::default();
    let input = p.from_vec("input", vec![1_i32, 2, 3, 4]).unwrap();
    let doubled = input.map_named("double/v1", |x| x * 2).unwrap();
    let even = doubled.filter_named("even/v1", |x| x % 2 == 0).unwrap();
    let output = even.output("result").unwrap();
    let runner = DirectRunner;
    let (values, job) = runner
        .run_collect(
            &output,
            &JobConfig {
                parallelism: 1,
                ..JobConfig::default()
            },
        )
        .unwrap();
    assert_eq!(values, [2, 4, 6, 8]);
    assert_eq!(job.status(), JobStatus::Succeeded);
    assert_eq!(job.processed(), 4);
    let (values, second) = runner
        .run_collect(
            &output,
            &JobConfig {
                parallelism: 3,
                ..JobConfig::default()
            },
        )
        .unwrap();
    assert_eq!(values, [2, 4, 6, 8]);
    assert_ne!(job.id(), second.id());
}

#[test]
fn distinct_job_pools_are_visible_to_user_code() {
    let p = Pipeline::default();
    let input = p.from_vec("input", vec![1_i32]).unwrap();
    let output = input
        .map_named("threads", |_| {
            u32::try_from(rayon::current_num_threads()).unwrap()
        })
        .unwrap()
        .output("result")
        .unwrap();
    let runner = DirectRunner;
    assert_eq!(
        runner
            .run_collect(
                &output,
                &JobConfig {
                    parallelism: 1,
                    ..JobConfig::default()
                }
            )
            .unwrap()
            .0,
        [1]
    );
    assert_eq!(
        runner
            .run_collect(
                &output,
                &JobConfig {
                    parallelism: 3,
                    ..JobConfig::default()
                }
            )
            .unwrap()
            .0,
        [3]
    );
}

#[test]
fn validation_precedes_source_side_effects() {
    #[derive(Clone)]
    struct Probe(Arc<AtomicUsize>);
    impl Element for Probe {
        const TYPE_ID: &'static str = "test/probe";
        const CODER_ID: &'static str = "test/probe-v1";
    }
    impl Drop for Probe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let p = Pipeline::default();
    let input = p.from_vec("input", vec![Probe(drops.clone())]).unwrap();
    let output = input
        .custom_named("unsupported", "vendor/custom", ironbeam_core::GRAPH_VERSION)
        .unwrap()
        .output("result")
        .unwrap();
    let err = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(
        err.kind,
        ErrorKind::UnsupportedCapability {
            capability: "vendor/custom".into()
        }
    );
    assert_eq!(drops.load(Ordering::Relaxed), 0);
}

#[test]
fn unbounded_fixture_rejected_and_long_lived_handle_cancels() {
    let p = Pipeline::default();
    let events = p
        .unbounded_source::<i32>(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            -1,
        )
        .unwrap();
    events
        .map_named("map", |x| x)
        .unwrap()
        .fixed_windows("window", 100)
        .unwrap()
        .append_sink("sink", Replay::Idempotent)
        .unwrap();
    let (graph, _) = p.snapshot().unwrap();
    let err = DirectRunner.validate(&graph).unwrap_err();
    assert_eq!(err.node.as_deref(), Some("events"));
    assert_eq!(
        err.kind,
        ErrorKind::UnsupportedCapability {
            capability: "unbounded-source".into()
        }
    );
    let handle = JobHandle::running_fixture();
    assert_eq!(handle.status(), JobStatus::Running);
    handle.cancel();
    handle.cancel();
    assert_eq!(handle.status(), JobStatus::Cancelled);
}

#[test]
fn cancelled_job_never_opens_source() {
    let p = Pipeline::default();
    let output = p
        .from_vec("input", vec![1_i32])
        .unwrap()
        .output("result")
        .unwrap();
    let token = Arc::new(AtomicBool::new(true));
    let err = DirectRunner
        .run_collect(
            &output,
            &JobConfig {
                cancellation: token,
                ..JobConfig::default()
            },
        )
        .err()
        .unwrap();
    assert_eq!(err.kind, ErrorKind::Cancelled);
}

#[test]
fn empty_and_filtered_outputs_are_valid() {
    let p = Pipeline::default();
    let output = p
        .from_vec("input", Vec::<i32>::new())
        .unwrap()
        .map_named("map", |x| x + 1)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        Vec::<i32>::new()
    );

    let p = Pipeline::default();
    let output = p
        .from_vec("input", vec![1_i32, 2])
        .unwrap()
        .filter_named("none", |_| false)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        Vec::<i32>::new()
    );
}

#[test]
fn invalid_resources_are_rejected_and_bounded_windows_execute() {
    let p = Pipeline::default();
    let input = p.from_vec("input", vec![1_i32]).unwrap();
    let output = input.output("out").unwrap();
    let bad = JobConfig {
        parallelism: 0,
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner.run_collect(&output, &bad).err().unwrap().kind,
        ErrorKind::Graph
    );
    let windowed = input
        .fixed_windows("window", 100)
        .unwrap()
        .output("window-out")
        .unwrap();
    let (graph, _) = p.snapshot().unwrap();
    DirectRunner.validate(&graph).unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&windowed, &JobConfig::default())
            .unwrap()
            .0,
        vec![1]
    );
}
