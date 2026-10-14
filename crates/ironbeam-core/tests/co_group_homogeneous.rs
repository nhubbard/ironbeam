use ironbeam_core::{Envelope, ErrorKind, PaneInfo, Pipeline, Replay, SourceProgress, Window};

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
fn homogeneous_cogroup_accepts_finite_unbounded_and_mixed_inputs() {
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
    first
        .co_group_homogeneous_named("mixed-group", std::slice::from_ref(&bounded))
        .unwrap()
        .output("mixed-result")
        .unwrap();
    bounded
        .co_group_homogeneous_named("mixed-reverse", &[first])
        .unwrap()
        .output("mixed-reverse-result")
        .unwrap();
    pipeline.snapshot().unwrap().0.validate().unwrap();
}

#[test]
fn homogeneous_cogroup_rejects_an_undeclared_input_tag() {
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", vec![(1_i32, 2_i32)]).unwrap();
    input.co_group_homogeneous_named("group", &[]).unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let malformed = Envelope {
        pane: PaneInfo::NO_FIRING,
        value: Box::new((1_i32, vec![(1_u32, 2_i32)])),
        timestamp_ns: 0,
        window: Window::Global,
        assigned_window: None,
    };
    assert_eq!(
        registry.apply_many("group", malformed).err().unwrap().kind,
        ErrorKind::Execution
    );
}
