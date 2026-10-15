use super::*;

#[tokio::test]
async fn prism_matches_direct_event_timer_callback_and_output() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-timers");
    let portable_path = directory.path().join("portable-timers");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        ironbeam_prism_proof::streaming_event_timer_fixture(&direct_pipeline, direct_path.clone())
            .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let portable_pipeline = Pipeline::default();
    ironbeam_prism_proof::streaming_event_timer_fixture(&portable_pipeline, portable_path.clone())
        .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "streaming-event-timers",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_EVENT_TIMER".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism event timers failed: {terminal:?}");
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    assert_eq!(read_rows(&direct_path), ["1:3", "2:4"]);
    assert_eq!(read_rows(&portable_path), ["1:3", "2:4"]);
}

#[tokio::test]
async fn prism_matches_direct_processing_timer_callback_and_hold_timestamp() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-processing-timer");
    let portable_path = directory.path().join("portable-processing-timer");
    let direct_pipeline = Pipeline::default();
    let direct_sink = ironbeam_prism_proof::streaming_processing_timer_fixture(
        &direct_pipeline,
        direct_path.clone(),
    )
    .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let portable_pipeline = Pipeline::default();
    ironbeam_prism_proof::streaming_processing_timer_fixture(
        &portable_pipeline,
        portable_path.clone(),
    )
    .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "streaming-processing-timer",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_PROCESSING_TIMER".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism processing timer failed: {terminal:?}");
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    assert_eq!(read_rows(&direct_path), ["1:3"]);
    assert_eq!(read_rows(&portable_path), ["1:3"]);
}

#[tokio::test]
async fn prism_recovers_an_armed_event_timer_after_execution_child_loss() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let portable_path = directory.path().join("portable-recovered-timer");
    let trigger = directory.path().join("timer-source-reopen-trigger");
    let killed = directory.path().join("killed-timer-child");
    let portable_pipeline = Pipeline::default();
    ironbeam_prism_proof::production_recovered_event_timer_fixture(
        &portable_pipeline,
        portable_path.clone(),
    )
    .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "replayed-production-event-timer",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_PRODUCTION_EVENT_TIMER".into(),
                portable_path.to_string_lossy().into_owned(),
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
        "Prism event timer recovery failed: {terminal:?}"
    );
    assert_eq!(
        std::fs::read_to_string(&killed).unwrap(),
        std::fs::read_to_string(&trigger).unwrap()
    );
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    assert_eq!(read_rows(&portable_path), ["1:24"]);
}
