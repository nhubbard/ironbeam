use ironbeam_core::{ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[test]
fn typed_batch_observes_cancellation_before_reading_the_next_batch() {
    let config = JobConfig {
        buffer_budget_elements: 1,
        ..JobConfig::default()
    };
    let token = Arc::clone(&config.cancellation);
    let output = Pipeline::default()
        .from_vec("source", vec![1_i32, 2])
        .unwrap()
        .map_named("cancel", move |value| {
            token.store(true, Ordering::Release);
            value
        })
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
}

#[test]
fn envelope_routes_observe_cancellation_between_transform_stages() {
    for parallelism in [1, 3] {
        let cancellation = Arc::new(AtomicBool::new(false));
        let token = Arc::clone(&cancellation);
        let output = Pipeline::default()
            .from_vec("source", vec![1_i32, 2, 3])
            .unwrap()
            .map_named("cancel-and-change-type", move |value| {
                token.store(true, Ordering::Release);
                value.to_string()
            })
            .unwrap()
            .map_named("must-not-run", |_| -> String {
                panic!("cancelled route continued")
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
                        parallelism,
                        ..JobConfig::default()
                    },
                )
                .err()
                .unwrap()
                .kind,
            ErrorKind::Cancelled
        );
    }
}

#[test]
fn flat_map_route_observes_cancellation_before_downstream_expansion() {
    let cancellation = Arc::new(AtomicBool::new(false));
    let token = Arc::clone(&cancellation);
    let output = Pipeline::default()
        .from_vec("source", vec![1_i32, 2, 3])
        .unwrap()
        .flat_map_named("cancel-and-expand", move |value| {
            token.store(true, Ordering::Release);
            vec![value, value + 10]
        })
        .unwrap()
        .map_named("must-not-run", |_| -> i32 {
            panic!("cancelled fan-out continued")
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
                    parallelism: 3,
                    ..JobConfig::default()
                },
            )
            .err()
            .unwrap()
            .kind,
        ErrorKind::Cancelled
    );
}
