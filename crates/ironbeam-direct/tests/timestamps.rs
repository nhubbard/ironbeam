use ironbeam_core::{Pipeline, WindowInterval};
use ironbeam_direct::{DirectRunner, JobConfig};

#[test]
fn timestamp_pairs_round_trip_through_event_time_and_windows() {
    let pipeline = Pipeline::default();
    let values = pipeline
        .from_vec(
            "pairs",
            vec![(-2_i64, "before".to_owned()), (12, "after".to_owned())],
        )
        .unwrap()
        .timestamps_from_pairs_named("event-time")
        .unwrap();
    let reified = values
        .reify_timestamps_named("reified")
        .unwrap()
        .output("reified-output")
        .unwrap();
    let windowed = values
        .fixed_windows("windows", 10)
        .unwrap()
        .with_window_named("window-values")
        .unwrap()
        .output("window-output")
        .unwrap();
    for parallelism in [1, 3] {
        let config = JobConfig {
            parallelism,
            ..JobConfig::default()
        };
        assert_eq!(
            DirectRunner.run_collect(&reified, &config).unwrap().0,
            vec![(-2, "before".to_owned()), (12, "after".to_owned())]
        );
        assert_eq!(
            DirectRunner.run_collect(&windowed, &config).unwrap().0,
            vec![
                (
                    WindowInterval {
                        start_ns: -10,
                        end_ns: 0,
                    },
                    "before".to_owned(),
                ),
                (
                    WindowInterval {
                        start_ns: 10,
                        end_ns: 20,
                    },
                    "after".to_owned(),
                ),
            ]
        );
    }
    let already_windowed = pipeline
        .from_vec("late-pairs", vec![(1_i64, 1_i32)])
        .unwrap()
        .fixed_windows("late-window", 10)
        .unwrap();
    assert!(
        already_windowed
            .timestamps_from_pairs_named("too-late")
            .is_err()
    );
}
