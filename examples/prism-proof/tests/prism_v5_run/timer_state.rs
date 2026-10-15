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

#[tokio::test]
async fn prism_replays_timer_input_after_execution_child_dies_in_callback() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let portable_path = directory.path().join("portable-replayed-timer-callback");
    let trigger = directory.path().join("timer-callback-trigger");
    let killed = directory.path().join("killed-timer-callback-child");
    let portable_pipeline = Pipeline::default();
    ironbeam_prism_proof::streaming_event_timer_fixture(&portable_pipeline, portable_path.clone())
        .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "replayed-event-timer-callback",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_EVENT_TIMER".into(),
                portable_path.to_string_lossy().into_owned(),
            ),
            ("IRONBEAM_V5_PRISM_KILL_TIMER_CALLBACK".into(), "1".into()),
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
    assert_eq!(terminal.0, 3, "Prism timer replay failed: {terminal:?}");
    assert_eq!(
        std::fs::read_to_string(&killed).unwrap(),
        std::fs::read_to_string(&trigger).unwrap()
    );
    let mut rows = std::fs::read_dir(&portable_path)
        .unwrap()
        .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>();
    rows.sort();
    assert_eq!(rows, ["1:3", "2:4"]);
}

#[tokio::test]
async fn prism_replays_interval_window_timer_after_callback_child_loss() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let portable_path = directory.path().join("portable-replayed-windowed-timer");
    let trigger = directory.path().join("windowed-timer-callback-trigger");
    let killed = directory.path().join("killed-windowed-timer-child");
    let portable_pipeline = Pipeline::default();
    ironbeam_prism_proof::streaming_windowed_timer_hold_fixture(
        &portable_pipeline,
        portable_path.clone(),
    )
    .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "replayed-windowed-timer-callback",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_WINDOWED_TIMER_HOLD".into(),
                portable_path.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_KILL_WINDOWED_TIMER_CALLBACK".into(),
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
        "Prism interval timer replay failed: {terminal:?}"
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
        ["2000000:3:0:10000000\n", "22000000:4:20000000:30000000\n"]
    );
}

#[tokio::test]
async fn prism_replays_window_expiration_after_callback_child_loss() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let portable_path = directory.path().join("portable-replayed-expiration");
    let trigger = directory.path().join("expiration-callback-trigger");
    let killed = directory.path().join("killed-expiration-child");
    let portable_pipeline = Pipeline::default();
    ironbeam_prism_proof::streaming_window_expiration_fixture(
        &portable_pipeline,
        portable_path.clone(),
    )
    .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "replayed-window-expiration-callback",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_WINDOW_EXPIRATION".into(),
                portable_path.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_KILL_EXPIRATION_CALLBACK".into(),
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
        "Prism expiration replay failed: {terminal:?}"
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
        ["29000000:4:20000000:30000000\n", "9000000:3:0:10000000\n"]
    );
}

#[tokio::test]
async fn prism_matches_direct_fixed_window_timer_output_hold_and_metadata() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-windowed-timers");
    let portable_path = directory.path().join("portable-windowed-timers");
    let direct_pipeline = Pipeline::default();
    let direct_sink = ironbeam_prism_proof::streaming_windowed_timer_hold_fixture(
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
    ironbeam_prism_proof::streaming_windowed_timer_hold_fixture(
        &portable_pipeline,
        portable_path.clone(),
    )
    .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "windowed-timer-output-hold",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_WINDOWED_TIMER_HOLD".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism windowed timers failed: {terminal:?}");
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let expected = ["2000000:3:0:10000000\n", "22000000:4:20000000:30000000\n"];
    assert_eq!(read_rows(&direct_path), expected);
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_matches_direct_overlapping_sliding_timer_windows() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-sliding-timers");
    let portable_path = directory.path().join("portable-sliding-timers");
    let direct_pipeline = Pipeline::default();
    let direct_sink = ironbeam_prism_proof::streaming_sliding_timer_hold_fixture(
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
    ironbeam_prism_proof::streaming_sliding_timer_hold_fixture(
        &portable_pipeline,
        portable_path.clone(),
    )
    .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "sliding-timer-output-hold",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_SLIDING_TIMER_HOLD".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism sliding timers failed: {terminal:?}");
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let expected = ["7000000:3:0:10000000\n", "7000000:4:5000000:15000000\n"];
    assert_eq!(read_rows(&direct_path), expected);
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_matches_direct_window_expiration_callbacks() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-window-expiration");
    let portable_path = directory.path().join("portable-window-expiration");
    let direct_pipeline = Pipeline::default();
    let direct_sink = ironbeam_prism_proof::streaming_window_expiration_fixture(
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
    ironbeam_prism_proof::streaming_window_expiration_fixture(
        &portable_pipeline,
        portable_path.clone(),
    )
    .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "window-expiration-callback",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_WINDOW_EXPIRATION".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism expiration failed: {terminal:?}");
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let expected = ["29000000:4:20000000:30000000\n", "9000000:3:0:10000000\n"];
    assert_eq!(read_rows(&direct_path), expected);
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_matches_direct_multi_key_window_expiration_callbacks() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-multi-key-expiration");
    let portable_path = directory.path().join("portable-multi-key-expiration");
    let direct_pipeline = Pipeline::default();
    let direct_sink = ironbeam_prism_proof::streaming_multi_key_window_expiration_fixture(
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
    ironbeam_prism_proof::streaming_multi_key_window_expiration_fixture(
        &portable_pipeline,
        portable_path.clone(),
    )
    .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "multi-key-window-expiration",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_MULTI_KEY_EXPIRATION".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "Prism multi-key expiration failed: {terminal:?}"
    );
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let expected = [
        "29000000:4:20000000:30000000\n",
        "29000000:6:20000000:30000000\n",
        "9000000:3:0:10000000\n",
        "9000000:5:0:10000000\n",
    ];
    assert_eq!(read_rows(&direct_path), expected);
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_matches_direct_distinct_key_same_batch_expiration_callbacks() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-same-batch-expiration");
    let portable_path = directory.path().join("portable-same-batch-expiration");
    let direct_pipeline = Pipeline::default();
    let direct_sink = ironbeam_prism_proof::streaming_same_batch_distinct_key_expiration_fixture(
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
    ironbeam_prism_proof::streaming_same_batch_distinct_key_expiration_fixture(
        &portable_pipeline,
        portable_path.clone(),
    )
    .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "distinct-key-same-batch-expiration",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_SAME_BATCH_EXPIRATION".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "Prism same-batch expiration failed: {terminal:?}"
    );
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let expected = ["29000000:6:20000000:30000000\n", "9000000:3:0:10000000\n"];
    assert_eq!(read_rows(&direct_path), expected);
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
#[ignore = "pinned Prism omits the later expiration for one key in two windows in one source batch"]
async fn prism_same_key_same_batch_expiration_matches_direct() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-same-key-expiration");
    let portable_path = directory.path().join("portable-same-key-expiration");
    let direct_pipeline = Pipeline::default();
    let direct_sink = ironbeam_prism_proof::streaming_same_key_batch_expiration_fixture(
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
    ironbeam_prism_proof::streaming_same_key_batch_expiration_fixture(
        &portable_pipeline,
        portable_path.clone(),
    )
    .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "same-key-same-batch-expiration",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_SAME_KEY_EXPIRATION".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "Prism same-key expiration failed: {terminal:?}"
    );
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let expected = ["29000000:6:20000000:30000000\n", "9000000:3:0:10000000\n"];
    assert_eq!(read_rows(&direct_path), expected);
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_matches_direct_offset_window_expiration_callbacks() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-offset-expiration");
    let portable_path = directory.path().join("portable-offset-expiration");
    let direct_pipeline = Pipeline::default();
    let direct_sink = ironbeam_prism_proof::streaming_offset_window_expiration_fixture(
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
    ironbeam_prism_proof::streaming_offset_window_expiration_fixture(
        &portable_pipeline,
        portable_path.clone(),
    )
    .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "offset-window-expiration",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_OFFSET_EXPIRATION".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "Prism offset expiration failed: {terminal:?}"
    );
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let expected = [
        "1000000:3:-8000000:2000000\n",
        "21000000:4:12000000:22000000\n",
    ];
    assert_eq!(read_rows(&direct_path), expected);
    assert_eq!(read_rows(&portable_path), expected);
}
