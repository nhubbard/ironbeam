use ironbeam_core::Pipeline;
use ironbeam_direct::{DirectRunner, JobConfig};

#[test]
fn bounded_multi_signal_wait_keeps_main_values_and_event_time() {
    let pipeline = Pipeline::default();
    let main = pipeline
        .from_vec("main", vec![(2_i64, 7_i32), (12, 8)])
        .unwrap()
        .timestamps_from_pairs_named("main-time")
        .unwrap()
        .fixed_windows("main-window", 10)
        .unwrap();
    let first = pipeline
        .from_vec("first", vec![(1_i64, 99_i32)])
        .unwrap()
        .timestamps_from_pairs_named("first-time")
        .unwrap()
        .fixed_windows("first-window", 10)
        .unwrap();
    let second = pipeline
        .from_vec("second", Vec::<(i64, i32)>::new())
        .unwrap()
        .timestamps_from_pairs_named("second-time")
        .unwrap()
        .fixed_windows("second-window", 10)
        .unwrap();
    let output = main
        .wait_on_all_windowed_named("wait", &[first, second])
        .unwrap()
        .reify_timestamps_named("output-time")
        .unwrap()
        .output("result")
        .unwrap();
    let mut rows = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap()
        .0;
    rows.sort();
    assert_eq!(rows, [(2, 7), (12, 8)]);
}
