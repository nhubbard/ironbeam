use super::*;

#[tokio::test]
async fn prism_matches_direct_per_window_distinct() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-distinct.txt");
    let portable_path = directory.path().join("portable-distinct.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink = distinct_window_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let expected = ["0:1000000:a", "10000000:11000000:a", "10000000:12000000:b"];
    assert_eq!(read_rows(&direct_path), expected);
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    distinct_window_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "distinct-window",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_DISTINCT_WINDOW".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "distinct-window job state: {terminal:?}");
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_matches_direct_representative_distinct() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-representatives.txt");
    let portable_path = directory.path().join("portable-representatives.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        representative_distinct_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let expected = ["0:1000000:a1", "0:3000000:b1", "10000000:11000000:a3"];
    assert_eq!(read_rows(&direct_path), expected);
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    representative_distinct_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "representative-distinct",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_REPRESENTATIVE_DISTINCT".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "representative distinct job state: {terminal:?}"
    );
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_matches_direct_streaming_representative_distinct() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-stream-representatives");
    let portable_path = directory.path().join("portable-stream-representatives");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        streaming_representative_distinct_fixture(&direct_pipeline, direct_path.clone()).unwrap();
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
    let expected = ["0:1000000:a1", "0:3000000:b1", "10000000:11000000:a3"];
    assert_eq!(read_rows(&direct_path), expected);
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    streaming_representative_distinct_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "stream-representative-distinct",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_STREAM_REPRESENTATIVE_DISTINCT".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "stream representative distinct job state: {terminal:?}"
    );
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_matches_direct_triggered_per_window_distinct() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-stream-distinct");
    let portable_path = directory.path().join("portable-stream-distinct");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        streaming_distinct_window_fixture(&direct_pipeline, direct_path.clone()).unwrap();
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
    let expected = [
        "a:1000000:Early:0",
        "a:3000000:Early:1",
        "b:5000000:OnTime:0",
    ];
    assert_eq!(read_rows(&direct_path), expected);
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    streaming_distinct_window_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "stream-distinct-window",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_STREAMING_DISTINCT_WINDOW".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "streaming distinct job state: {terminal:?}");
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_matches_direct_late_per_window_distinct() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-late-distinct");
    let portable_path = directory.path().join("portable-late-distinct");
    let direct_pipeline = Pipeline::default();
    let direct_sink = late_distinct_window_fixture(&direct_pipeline, direct_path.clone()).unwrap();
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
    let expected = ["a:1000000:OnTime:0", "a:2000000:Late:1"];
    assert_eq!(read_rows(&direct_path), expected);
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    late_distinct_window_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "late-distinct-window",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_LATE_DISTINCT_WINDOW".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "late distinct job state: {terminal:?}");
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_replays_late_distinct_pane_after_execution_child_loss() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let portable_path = directory.path().join("portable-replayed-late-distinct");
    let trigger = directory.path().join("late-distinct-trigger");
    let killed = directory.path().join("killed-late-distinct-child");
    let pipeline = Pipeline::default();
    late_distinct_window_fixture(&pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "replayed-late-distinct",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_LATE_DISTINCT_WINDOW".into(),
                portable_path.to_string_lossy().into_owned(),
            ),
            ("IRONBEAM_V5_PRISM_KILL_LATE_DISTINCT".into(), "1".into()),
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
        "Prism late Distinct replay failed: {terminal:?}"
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
    assert_eq!(rows, ["a:1000000:OnTime:0", "a:2000000:Late:1"]);
}
