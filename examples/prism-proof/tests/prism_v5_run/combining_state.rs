use super::*;

#[tokio::test]
async fn prism_matches_direct_combining_state_mutations() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-combining-state");
    let portable_path = directory.path().join("portable-combining-state");
    let direct_pipeline = Pipeline::default();
    let direct_sink = ironbeam_prism_proof::bounded_combining_state_fixture(
        &direct_pipeline,
        direct_path.clone(),
    )
    .unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let portable_pipeline = Pipeline::default();
    ironbeam_prism_proof::bounded_combining_state_fixture(
        &portable_pipeline,
        portable_path.clone(),
    )
    .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "bounded-combining-state",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_COMBINING_STATE".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism combining state failed: {terminal:?}");
    let rows = |path: &Path| {
        let mut values = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        values.sort();
        values
    };
    let expected = ["1:-1:30", "1:-2:0", "1:10:0", "1:20:10", "1:5:50", "2:30:0"];
    assert_eq!(rows(&direct_path), expected);
    assert_eq!(rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_matches_direct_windowed_combining_across_batches() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-windowed-combining");
    let portable_path = directory.path().join("portable-windowed-combining");
    let direct_pipeline = Pipeline::default();
    let direct_sink = ironbeam_prism_proof::streaming_windowed_combining_state_fixture(
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
    ironbeam_prism_proof::streaming_windowed_combining_state_fixture(
        &portable_pipeline,
        portable_path.clone(),
    )
    .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "streaming-windowed-combining-state",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_STREAMING_COMBINING_STATE".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "Prism windowed combining failed: {terminal:?}"
    );
    let rows = |path: &Path| {
        let mut values = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        values.sort();
        values
    };
    let expected = [
        "0:10000000:1:10:0",
        "0:10000000:1:5:10",
        "20000000:30000000:1:30:0",
        "20000000:30000000:1:7:30",
    ];
    assert_eq!(rows(&direct_path), expected);
    assert_eq!(rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_replays_combining_state_after_child_loss() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let portable_path = directory.path().join("production-combining");
    let trigger = directory.path().join("combining-source-reopen-trigger");
    let killed = directory.path().join("killed-combining-child");
    let pipeline = Pipeline::default();
    ironbeam_prism_proof::production_combining_state_fixture(&pipeline, portable_path.clone())
        .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "replayed-production-unbounded-combining-state",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_PRODUCTION_COMBINING_STATE".into(),
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
    assert_eq!(terminal.0, 3, "Prism combining replay failed: {terminal:?}");
    assert_eq!(
        std::fs::read_to_string(&killed).unwrap(),
        std::fs::read_to_string(&trigger).unwrap()
    );
    let mut rows = std::fs::read_dir(&portable_path)
        .unwrap()
        .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>();
    rows.sort();
    let sequential = ["1:7:0", "1:8:7", "1:9:15"];
    let residual_first = ["1:7:0", "1:8:16", "1:9:7"];
    assert!(
        rows == sequential || rows == residual_first,
        "unexpected combining replay: {rows:?}"
    );
}
