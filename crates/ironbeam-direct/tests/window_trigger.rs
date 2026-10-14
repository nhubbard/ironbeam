use ironbeam_core::{PaneInfo, PaneTiming, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};

#[test]
fn bounded_count_trigger_emits_one_early_pane_for_a_bundle() {
    let pipeline = Pipeline::default();
    let grouped = pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32), ("a".to_owned(), 3)])
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .early_count_trigger_named("count-trigger", 1)
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .output("result")
        .unwrap();
    let (rows, _) = DirectRunner
        .run_collect(&grouped, &JobConfig::default())
        .unwrap();
    assert_eq!(
        rows,
        vec![(
            PaneInfo {
                is_first: true,
                is_last: false,
                timing: PaneTiming::Early,
                index: 0,
                nonspeculative_index: -1,
                contains_element_metadata: false,
            },
            ("a".to_owned(), vec![2, 3]),
        )]
    );
}

#[test]
fn bounded_count_trigger_keeps_unreached_group_on_time() {
    let pipeline = Pipeline::default();
    let grouped = pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32)])
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .early_count_trigger_named("count-trigger", 2)
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .output("result")
        .unwrap();
    let (rows, _) = DirectRunner
        .run_collect(&grouped, &JobConfig::default())
        .unwrap();
    assert_eq!(
        rows,
        vec![(PaneInfo::FIRST_LAST_ON_TIME, ("a".into(), vec![2]))]
    );
}

#[test]
fn bounded_accumulating_count_retains_values_in_on_time_pane() {
    let pipeline = Pipeline::default();
    let grouped = pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32), ("a".to_owned(), 3)])
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .accumulating_count_trigger_named("count-trigger", 1)
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .output("result")
        .unwrap();
    let (rows, _) = DirectRunner
        .run_collect(&grouped, &JobConfig::default())
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0.timing, PaneTiming::Early);
    assert_eq!(rows[0].0.index, 0);
    assert_eq!(rows[0].1, ("a".to_owned(), vec![2, 3]));
    assert_eq!(rows[1].0.timing, PaneTiming::OnTime);
    assert_eq!(rows[1].0.index, 1);
    assert_eq!(rows[1].1, ("a".to_owned(), vec![2, 3]));
}

#[test]
fn sliding_count_trigger_fires_independently_in_overlapping_windows() {
    let pipeline = Pipeline::default();
    let grouped = pipeline
        .from_vec(
            "values",
            vec![
                (6_000_000_i64, ("a".to_owned(), 2_i32)),
                (7_000_000, ("a".to_owned(), 3)),
            ],
        )
        .unwrap()
        .timestamps_from_pairs_named("timestamps")
        .unwrap()
        .sliding_windows("windows", 10_000_000, 5_000_000, 0)
        .unwrap()
        .early_count_trigger_named("count-trigger", 1)
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .with_window_named("interval")
        .unwrap()
        .output("result")
        .unwrap();
    let (mut rows, _) = DirectRunner
        .run_collect(&grouped, &JobConfig::default())
        .unwrap();
    rows.sort_by_key(|row| row.0.start_ns);
    assert_eq!(rows.len(), 2);
    assert_eq!((rows[0].0.start_ns, rows[0].0.end_ns), (0, 10_000_000));
    assert_eq!(
        (rows[1].0.start_ns, rows[1].0.end_ns),
        (5_000_000, 15_000_000)
    );
    for (_, (pane, (key, values))) in rows {
        assert_eq!(pane.timing, PaneTiming::Early);
        assert_eq!(pane.index, 0);
        assert_eq!(key, "a");
        assert_eq!(values, vec![2, 3]);
    }
}
