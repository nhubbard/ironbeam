use ironbeam_core::Pipeline;
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_transforms::LogElementsExt;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[test]
fn logging_is_a_passthrough_and_runs_at_execution() {
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", vec![1_i32, 2, 3]).unwrap();
    let seen = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&seen);
    let output = input
        .log_elements_with_named("format", move |value| {
            calls.fetch_add(1, Ordering::Relaxed);
            format!("value={value}")
        })
        .unwrap()
        .log_elements_named("debug")
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(seen.load(Ordering::Relaxed), 0);
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        [1, 2, 3]
    );
    assert_eq!(seen.load(Ordering::Relaxed), 3);
}
