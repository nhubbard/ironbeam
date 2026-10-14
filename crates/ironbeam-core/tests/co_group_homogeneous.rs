use ironbeam_core::{Pipeline, Replay, SourceProgress};

#[test]
fn homogeneous_cogroup_accepts_twelve_inputs_and_validates_the_graph() {
    let pipeline = Pipeline::default();
    let inputs = (0..12)
        .map(|index| {
            pipeline
                .from_vec(&format!("input-{index}"), vec![(1_i32, index)])
                .unwrap()
        })
        .collect::<Vec<_>>();
    inputs[0]
        .co_group_homogeneous_named("group", &inputs[1..])
        .unwrap()
        .output("result")
        .unwrap();
    pipeline.snapshot().unwrap().0.validate().unwrap();
}

#[test]
fn homogeneous_cogroup_rejects_mismatched_inputs_before_adding_nodes() {
    let pipeline = Pipeline::default();
    let main = pipeline.from_vec("main", vec![(1_i32, 2_i32)]).unwrap();
    let foreign = Pipeline::default()
        .from_vec("foreign", vec![(1_i32, 3_i32)])
        .unwrap();
    let windowed = main.fixed_windows("windowed", 10).unwrap();
    let unbounded = pipeline
        .unbounded_source::<(i32, i32)>(
            "unbounded",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            0,
        )
        .unwrap();
    let before = pipeline.snapshot().unwrap().0.nodes.len();
    for (key, other) in [
        ("foreign-group", foreign),
        ("window-group", windowed),
        ("stream-group", unbounded),
    ] {
        assert!(main.co_group_homogeneous_named(key, &[other]).is_err());
    }
    assert_eq!(pipeline.snapshot().unwrap().0.nodes.len(), before);
}
