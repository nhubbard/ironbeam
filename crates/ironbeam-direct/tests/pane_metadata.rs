use ironbeam_core::{PaneInfo, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};

#[test]
fn default_pane_metadata_survives_elementwise_steps_and_marks_group_firing() {
    let pipeline = Pipeline::default();
    let values = pipeline
        .from_vec(
            "events",
            vec![(1_i64, ("a".to_owned(), 2_i32)), (2, ("a".to_owned(), 3))],
        )
        .unwrap()
        .timestamps_from_pairs_named("event-time")
        .unwrap()
        .fixed_windows("windows", 10)
        .unwrap();
    let raw = values
        .map_named("identity", |value| value)
        .unwrap()
        .reify_panes_named("source-panes")
        .unwrap()
        .output("source-result")
        .unwrap();
    let grouped = values
        .group_by_key_named("group")
        .unwrap()
        .reify_panes_named("group-panes")
        .unwrap()
        .output("group-result")
        .unwrap();
    let config = JobConfig::default();
    let (raw_rows, _) = DirectRunner.run_collect(&raw, &config).unwrap();
    assert_eq!(raw_rows.len(), 2);
    assert!(
        raw_rows
            .iter()
            .all(|(pane, _)| *pane == PaneInfo::NO_FIRING)
    );
    let (group_rows, _) = DirectRunner.run_collect(&grouped, &config).unwrap();
    assert_eq!(group_rows.len(), 1);
    assert_eq!(group_rows[0].0, PaneInfo::FIRST_LAST_ON_TIME);
    assert_eq!(group_rows[0].1.0, "a");
    assert_eq!(group_rows[0].1.1, [2, 3]);
}
