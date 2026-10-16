use super::*;

#[tokio::test]
async fn prism_and_direct_reject_keyed_batch_length_change() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-invalid-keyed-batch.txt");
    let portable_path = directory.path().join("portable-invalid-keyed-batch.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink = invalid_keyed_batch_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    let error = DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("invalid-keyed-batch"));
    assert!(error.message.contains("batch value map changed length"));
    assert!(!direct_path.exists());

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    invalid_keyed_batch_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "invalid-keyed-batch",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_BAD_KEYED_BATCH".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 4,
        "Prism keyed batch failure state: {terminal:?}"
    );
    assert!(
        terminal.1.iter().any(|message| {
            message.contains("invalid-keyed-batch")
                && message.contains("batch value map changed length")
        }),
        "Prism did not report the keyed batch error: {:?}",
        terminal.1
    );
    assert!(!portable_path.exists());
}

#[tokio::test]
async fn prism_emits_full_and_final_partial_unbounded_keyed_batches() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    for (windowed, upstream_trigger, variable) in [
        (false, false, "IRONBEAM_V5_PRISM_GLOBAL_KEYED_BATCHES"),
        (true, false, "IRONBEAM_V5_PRISM_WINDOWED_KEYED_BATCHES"),
        (
            false,
            true,
            "IRONBEAM_V5_PRISM_GLOBAL_KEYED_BATCHES_UPSTREAM_TRIGGER",
        ),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("batches.txt");
        let pipeline = Pipeline::default();
        if upstream_trigger {
            global_keyed_batches_with_upstream_trigger_fixture(&pipeline, path.clone()).unwrap();
        } else if windowed {
            windowed_keyed_batches_fixture(&pipeline, path.clone()).unwrap();
        } else {
            global_keyed_batches_fixture(&pipeline, path.clone()).unwrap();
        }
        let terminal = run_shared_fixture(
            &endpoint,
            &pipeline,
            "keyed-batches",
            HashMap::from([(variable.into(), path.to_string_lossy().into_owned())]),
        )
        .await;
        assert_eq!(terminal.0, 3, "keyed batch job state: {terminal:?}");
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort_unstable();
        assert_eq!(
            rows,
            [
                "a:[2, 3]:Early:true:false:0:-1",
                "a:[4]:OnTime:false:true:1:0",
            ],
            "windowed: {windowed}; upstream trigger: {upstream_trigger}"
        );
    }
}

#[tokio::test]
async fn prism_flushes_late_keyed_batches_at_the_batch_count() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("late-batches.txt");
    let pipeline = Pipeline::default();
    late_keyed_batches_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "late-keyed-batches",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_LATE_KEYED_BATCHES".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "late keyed batch job state: {terminal:?}");
    let rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        [
            "a:[2, 3]:Early:true:false:0:-1",
            "a:[4, 5]:Late:false:false:1:0"
        ]
    );
}

#[tokio::test]
async fn prism_flushes_keyed_batches_on_time_and_count() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("timed-batches.txt");
    let pipeline = Pipeline::default();
    timed_keyed_batches_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "timed-keyed-batches",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_TIMED_KEYED_BATCHES".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "timed batch job state: {terminal:?}");
    let mut rows = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort_unstable();
    assert_eq!(
        rows,
        [
            "a:[2]:Early:true:false:0:-1",
            "a:[3, 4]:Early:false:false:1:-1",
        ]
    );
}

#[tokio::test]
async fn prism_flushes_timed_and_late_keyed_batches() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("timed-late-batches.txt");
    let pipeline = Pipeline::default();
    timed_late_keyed_batches_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "timed-late-keyed-batches",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_TIMED_LATE_KEYED_BATCHES".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "timed late keyed batch job state: {terminal:?}"
    );
    let rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        [
            "a:[2, 3]:Early:true:false:0:-1",
            "a:[4]:Early:false:false:1:-1",
            "a:[5, 7]:Late:false:false:2:0",
        ]
    );
}

#[tokio::test]
#[ignore = "pinned Prism emits extra Early and OnTime panes before the final Late pane"]
async fn prism_matches_direct_accumulating_timed_and_late_group_panes() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-accumulating-timed-late");
    let portable_path = directory.path().join("portable-accumulating-timed-late");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        accumulating_timed_late_group_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let mut expected = [
        "a:[2, 3]:Early:true:false:0:-1",
        "a:[2, 3, 4]:Early:false:false:1:-1",
        "a:[2, 3, 4]:OnTime:false:false:2:0",
        "a:[2, 3, 4, 5, 7]:Late:false:false:3:1",
    ];
    expected.sort();
    assert_eq!(read_rows(&direct_path), expected);
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    accumulating_timed_late_group_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "accumulating-timed-late-group",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_ACCUMULATING_TIMED_LATE_GROUP".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism timed-late group failed: {terminal:?}");
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_matches_direct_conjunctive_early_and_late_count_panes() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-and-time-late");
    let portable_path = directory.path().join("portable-and-time-late");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        count_and_time_late_group_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let mut expected = [
        "a:[2, 3]:Early:true:false:0:-1",
        "a:[5, 7]:Late:false:false:1:0",
    ];
    expected.sort();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    assert_eq!(read_rows(&direct_path), expected);
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    count_and_time_late_group_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "count-and-time-late-group",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_COUNT_AND_TIME_LATE_GROUP".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "Prism conjunctive early and late group failed: {terminal:?}"
    );
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_replays_conjunctive_early_pane_after_execution_child_loss() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let portable_path = directory.path().join("replayed-and-time-late");
    let trigger = directory.path().join("early-pane-trigger");
    let killed = directory.path().join("early-pane-killed");
    let pipeline = Pipeline::default();
    count_and_time_late_group_fixture(&pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "replayed-count-and-time-late-group",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_COUNT_AND_TIME_LATE_GROUP".into(),
                portable_path.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_KILL_COUNT_AND_TIME_LATE_EARLY".into(),
                "1".into(),
            ),
            ("IRONBEAM_V5_SUPERVISE_EXECUTION".into(), "1".into()),
            (
                "IRONBEAM_V5_SUPERVISOR_TRIGGER".into(),
                trigger.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_SUPERVISOR_KILLED".into(),
                killed.to_string_lossy().into_owned(),
            ),
        ]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "Prism failed to replay the early pane: {terminal:?}"
    );
    assert_eq!(
        std::fs::read_to_string(&killed).unwrap(),
        std::fs::read_to_string(&trigger).unwrap()
    );
    let mut rows = std::fs::read_dir(&portable_path)
        .unwrap()
        .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>();
    rows.sort();
    assert_eq!(
        rows,
        [
            "a:[2, 3]:Early:true:false:0:-1",
            "a:[5, 7]:Late:false:false:1:0",
        ]
    );
}

#[tokio::test]
async fn prism_matches_direct_two_source_conjunctive_early_and_late_panes() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-dual-and-late");
    let portable_path = directory.path().join("portable-dual-and-late");
    let direct = Pipeline::default();
    let sink = dual_count_and_time_late_cogroup_fixture(&direct, direct_path.clone()).unwrap();
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let mut expected = [
        "1:[[2], [4]]:Early:true:false:0:-1",
        "1:[[5], [7]]:Late:false:false:1:0",
    ];
    expected.sort();
    assert_eq!(read_rows(&direct_path), expected);
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable = Pipeline::default();
    dual_count_and_time_late_cogroup_fixture(&portable, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "dual-count-and-time-late",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_DUAL_COUNT_AND_TIME_LATE".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "Prism dual count-and-time late job failed: {terminal:?}"
    );
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_matches_direct_three_source_conjunctive_early_and_late_panes() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-triple-and-late");
    let portable_path = directory.path().join("portable-triple-and-late");
    let direct = Pipeline::default();
    let sink = triple_count_and_time_late_cogroup_fixture(&direct, direct_path.clone()).unwrap();
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let mut expected = [
        "1:[[2], [4], [6]]:Early:true:false:0:-1",
        "1:[[5], [7], []]:Late:false:false:1:0",
    ];
    expected.sort();
    assert_eq!(read_rows(&direct_path), expected);
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable = Pipeline::default();
    triple_count_and_time_late_cogroup_fixture(&portable, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "triple-count-and-time-late",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_TRIPLE_COUNT_AND_TIME_LATE".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "Prism triple count-and-time late job failed: {terminal:?}"
    );
    assert_eq!(read_rows(&portable_path), expected);
}
