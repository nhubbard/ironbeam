use super::*;

#[tokio::test]
async fn prism_matches_direct_unbounded_fixed_window_set_membership() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-windowed-set");
    let portable_path = directory.path().join("portable-windowed-set");
    let direct_pipeline = Pipeline::default();
    let direct_sink = ironbeam_prism_proof::streaming_windowed_set_state_fixture(
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
    ironbeam_prism_proof::streaming_windowed_set_state_fixture(
        &portable_pipeline,
        portable_path.clone(),
    )
    .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "unbounded-windowed-set-membership",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_STREAMING_WINDOWED_SET_STATE".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism windowed set failed: {terminal:?}");
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let expected = [
        "0:10000000:1:11:0:0",
        "0:10000000:1:11:1:1",
        "20000000:30000000:1:17:0:0",
        "20000000:30000000:1:17:1:1",
    ];
    assert_eq!(read_rows(&direct_path), expected);
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_replays_production_unbounded_set_state_after_child_loss() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let portable_path = directory.path().join("production-set");
    let trigger = directory.path().join("set-source-reopen-trigger");
    let killed = directory.path().join("killed-set-child");
    let pipeline = Pipeline::default();
    ironbeam_prism_proof::production_set_state_fixture(&pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "replayed-production-unbounded-set-state",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_PRODUCTION_SET_STATE".into(),
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
        "Prism production set replay failed: {terminal:?}"
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
    assert_eq!(rows, ["1:7:0:0", "1:8:0:2", "1:9:0:1"]);
}

#[tokio::test]
async fn prism_matches_direct_unbounded_set_membership() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-stream-set");
    let portable_path = directory.path().join("portable-stream-set");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        ironbeam_prism_proof::streaming_set_state_fixture(&direct_pipeline, direct_path.clone())
            .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let portable_pipeline = Pipeline::default();
    ironbeam_prism_proof::streaming_set_state_fixture(&portable_pipeline, portable_path.clone())
        .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "unbounded-set-membership",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_STREAMING_SET_STATE".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism unbounded set failed: {terminal:?}");
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let expected = ["1:11:0:0", "1:11:1:1", "1:23:0:1", "2:17:0:0", "2:19:0:1"];
    assert_eq!(read_rows(&direct_path), expected);
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_matches_direct_set_membership_and_mutations() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-set-mutations");
    let portable_path = directory.path().join("portable-set-mutations");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        ironbeam_prism_proof::bounded_set_mutations_fixture(&direct_pipeline, direct_path.clone())
            .unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let portable_pipeline = Pipeline::default();
    ironbeam_prism_proof::bounded_set_mutations_fixture(&portable_pipeline, portable_path.clone())
        .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "bounded-set-mutations",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_SET_MUTATIONS".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism set mutations failed: {terminal:?}");
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let expected = [
        "1:-1:0:2", "1:-2:0:1", "1:-3:0:2", "1:0:0:1", "1:10:0:0", "1:10:0:1", "1:10:1:1",
        "1:10:1:2", "1:11:0:0", "1:20:0:1", "2:10:0:0",
    ];
    assert_eq!(read_rows(&direct_path), expected);
    assert_eq!(read_rows(&portable_path), expected);
}
