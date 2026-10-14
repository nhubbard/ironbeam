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

#[test]
fn homogeneous_cogroup_accepts_finite_unbounded_inputs_but_rejects_mixed_sources() {
    let pipeline = Pipeline::default();
    let progress = SourceProgress {
        stable_split_id: true,
        replay: Replay::AtLeastOnce,
        reports_event_time: true,
    };
    let first = pipeline
        .unbounded_source::<(i32, i32)>("first", progress.clone(), 0)
        .unwrap()
        .fixed_windows("first-window", 10_000_000)
        .unwrap();
    let second = pipeline
        .unbounded_source::<(i32, i32)>("second", progress, 0)
        .unwrap()
        .fixed_windows("second-window", 10_000_000)
        .unwrap();
    first
        .co_group_homogeneous_named("unbounded-group", &[second])
        .unwrap()
        .output("result")
        .unwrap();
    let session_a = pipeline
        .unbounded_source::<(i32, i32)>(
            "session-a",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            0,
        )
        .unwrap()
        .session_windows_named("session-a-window", 5_000_000)
        .unwrap();
    let session_b = pipeline
        .unbounded_source::<(i32, i32)>(
            "session-b",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            0,
        )
        .unwrap()
        .session_windows_named("session-b-window", 5_000_000)
        .unwrap();
    session_a
        .co_group_homogeneous_named("session-group", &[session_b])
        .unwrap()
        .output("session-result")
        .unwrap();
    pipeline.snapshot().unwrap().0.validate().unwrap();

    let bounded = pipeline
        .from_vec("bounded", vec![(1_i32, 2_i32)])
        .unwrap()
        .fixed_windows("bounded-window", 10_000_000)
        .unwrap();
    let before = pipeline.snapshot().unwrap().0.nodes.len();
    assert!(
        first
            .co_group_homogeneous_named("mixed-group", &[bounded])
            .is_err()
    );
    assert_eq!(pipeline.snapshot().unwrap().0.nodes.len(), before);
}
