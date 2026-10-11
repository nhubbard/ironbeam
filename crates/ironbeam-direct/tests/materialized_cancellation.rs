use ironbeam_core::{
    Error, ErrorKind, Pipeline, Replay, SinkMode, SinkSpec, SourceProgress, TypedSink,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[test]
fn checkpointed_group_can_retry_after_downstream_cancellation() {
    let directory = tempfile::tempdir().unwrap();
    let config = JobConfig {
        buffer_budget_elements: 1,
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().to_path_buf(),
            binary_build_id: "group-cancel-retry-v1".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let cancel_once = Arc::new(AtomicBool::new(true));
    let token = Arc::clone(&config.cancellation);
    let trigger = Arc::clone(&cancel_once);
    let output = Pipeline::default()
        .from_vec("source", vec![(1_i32, 10_i32), (1, 20)])
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .map_named("cancel-after-group", move |row| {
            if trigger.swap(false, Ordering::AcqRel) {
                token.store(true, Ordering::Release);
            }
            row
        })
        .unwrap()
        .filter_named("keep-group", |_| true)
        .unwrap()
        .output("result")
        .unwrap();

    assert_eq!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Cancelled
    );
    assert!(directory.path().read_dir().unwrap().next().is_some());
    config.cancellation.store(false, Ordering::Release);
    let (rows, resumed) = DirectRunner.run_collect(&output, &config).unwrap();
    assert_eq!(rows, [(1, vec![10, 20])]);
    assert_eq!(resumed.metrics().source_elements, 0);
}

#[test]
fn materialized_stage_observes_cancellation_after_reshuffle() {
    let cancellation = Arc::new(AtomicBool::new(false));
    let token = Arc::clone(&cancellation);
    let output = Pipeline::default()
        .from_vec("source", vec![1_i32, 2])
        .unwrap()
        .reshuffle_named("barrier")
        .unwrap()
        .map_named("cancel", move |value| {
            token.store(true, Ordering::Release);
            value
        })
        .unwrap()
        .map_named("must-not-run", |_| -> i32 {
            panic!("cancelled materialized stage continued")
        })
        .unwrap()
        .output("result")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(
                &output,
                &JobConfig {
                    cancellation,
                    ..JobConfig::default()
                },
            )
            .err()
            .unwrap()
            .kind,
        ErrorKind::Cancelled
    );
}

#[test]
fn materialized_group_checks_cancellation_before_state_update() {
    let cancellation = Arc::new(AtomicBool::new(false));
    let token = Arc::clone(&cancellation);
    let output = Pipeline::default()
        .from_vec("source", vec![(1_i32, 10_i32)])
        .unwrap()
        .map_named("cancel-before-barrier", move |row| {
            token.store(true, Ordering::Release);
            row
        })
        .unwrap()
        .reshuffle_named("barrier")
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .output("result")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(
                &output,
                &JobConfig {
                    cancellation,
                    ..JobConfig::default()
                },
            )
            .err()
            .unwrap()
            .kind,
        ErrorKind::Cancelled
    );
}

#[test]
fn materialized_source_stops_before_reading_a_second_batch() {
    let cancellation = Arc::new(AtomicBool::new(false));
    let token = Arc::clone(&cancellation);
    let output = Pipeline::default()
        .bounded_source(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "cancel-after-first-source-row-v1".into(),
            Some(2),
            move || {
                let token = Arc::clone(&token);
                Ok(Box::new(
                    std::iter::once_with(move || {
                        token.store(true, Ordering::Release);
                        Ok(1_i32)
                    })
                    .chain(std::iter::once(Ok(2_i32))),
                ))
            },
        )
        .unwrap()
        .reshuffle_named("barrier")
        .unwrap()
        .output("result")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(
                &output,
                &JobConfig {
                    cancellation,
                    buffer_budget_elements: 1,
                    ..JobConfig::default()
                },
            )
            .err()
            .unwrap()
            .kind,
        ErrorKind::Cancelled
    );
}

#[test]
fn materialized_fan_in_stops_before_opening_the_second_source() {
    let cancellation = Arc::new(AtomicBool::new(false));
    let token = Arc::clone(&cancellation);
    let opens = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let left = pipeline
        .from_vec("left", vec![1_i32])
        .unwrap()
        .map_named("cancel-left", move |value| {
            token.store(true, Ordering::Release);
            value
        })
        .unwrap();
    let source_opens = Arc::clone(&opens);
    let right = pipeline
        .bounded_source(
            "right",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "unopened-right-v1".into(),
            Some(1),
            move || {
                source_opens.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(std::iter::once(Ok(2_i32))))
            },
        )
        .unwrap();
    let output = left
        .flatten_named("flatten", &[right])
        .unwrap()
        .output("result")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(
                &output,
                &JobConfig {
                    cancellation,
                    ..JobConfig::default()
                },
            )
            .err()
            .unwrap()
            .kind,
        ErrorKind::Cancelled
    );
    assert_eq!(opens.load(Ordering::SeqCst), 0);
}

#[test]
fn wait_signal_cancellation_stops_before_main_source_access() {
    struct AbortSink(Arc<AtomicUsize>);
    impl TypedSink<i32> for AbortSink {
        fn write(&mut self, _: i32) -> Result<(), Error> {
            panic!("cancelled wait wrote main data")
        }
        fn commit(&mut self) -> Result<(), Error> {
            panic!("cancelled wait committed")
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    for second_signal_stage in [false, true] {
        let cancellation = Arc::new(AtomicBool::new(false));
        let token = Arc::clone(&cancellation);
        let main_opens = Arc::new(AtomicUsize::new(0));
        let aborts = Arc::new(AtomicUsize::new(0));
        let pipeline = Pipeline::default();
        let signal = pipeline
            .from_vec("signal-source", vec![1_i32, 2])
            .unwrap()
            .map_named("cancel-signal", move |value| {
                token.store(true, Ordering::Release);
                value
            })
            .unwrap();
        let signal = if second_signal_stage {
            signal.filter_named("after-cancel", |_| true).unwrap()
        } else {
            signal
        };
        let opens = Arc::clone(&main_opens);
        let main = pipeline
            .bounded_source(
                "main-source",
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: false,
                },
                "unopened-main-v1".into(),
                Some(1),
                move || {
                    opens.fetch_add(1, Ordering::SeqCst);
                    Ok(Box::new(std::iter::once(Ok(7_i32))))
                },
            )
            .unwrap();
        let sink_aborts = Arc::clone(&aborts);
        let sink = main
            .wait_on_named("wait", &signal)
            .unwrap()
            .sink_named(
                "sink",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::AtLeastOnce,
                    commit: true,
                },
                move || Ok(Box::new(AbortSink(Arc::clone(&sink_aborts)))),
            )
            .unwrap();
        assert_eq!(
            DirectRunner
                .run_sink(
                    &sink,
                    &JobConfig {
                        cancellation,
                        buffer_budget_elements: 1,
                        ..JobConfig::default()
                    },
                )
                .err()
                .unwrap()
                .kind,
            ErrorKind::Cancelled
        );
        assert_eq!(main_opens.load(Ordering::SeqCst), 0);
        assert_eq!(aborts.load(Ordering::SeqCst), 1);
    }
}
