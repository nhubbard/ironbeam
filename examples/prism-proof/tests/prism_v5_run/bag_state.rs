use super::*;

#[tokio::test]
async fn prism_replays_production_unbounded_bag_state_after_child_loss() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let portable_path = directory.path().join("production-bag");
    let trigger = directory.path().join("bag-source-reopen-trigger");
    let killed = directory.path().join("killed-bag-child");
    let pipeline = Pipeline::default();
    ironbeam_prism_proof::production_bag_state_fixture(&pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "replayed-production-unbounded-bag-state",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_PRODUCTION_BAG_STATE".into(),
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
        "Prism production bag replay failed: {terminal:?}"
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
    assert_eq!(rows, ["1:7:1", "1:8:3", "1:9:2"]);
}

#[tokio::test]
async fn prism_matches_direct_bag_mutations_and_duplicate_contents() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-bag-mutations");
    let portable_path = directory.path().join("portable-bag-mutations");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        ironbeam_prism_proof::bounded_bag_mutations_fixture(&direct_pipeline, direct_path.clone())
            .unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let portable_pipeline = Pipeline::default();
    ironbeam_prism_proof::bounded_bag_mutations_fixture(&portable_pipeline, portable_path.clone())
        .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "bounded-bag-mutations",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_BAG_MUTATIONS".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism bag mutations failed: {terminal:?}");
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let expected = [
        "1:-1:11,13,13",
        "1:-2:17",
        "1:0:11,13,13",
        "1:11:",
        "1:13:11",
        "1:13:11,13",
        "1:17:",
        "1:19:99",
        "2:7:",
    ];
    assert_eq!(read_rows(&direct_path), expected);
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_matches_direct_unbounded_fixed_window_bag_state() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-windowed-bag-state");
    let portable_path = directory.path().join("portable-windowed-bag-state");
    let direct_pipeline = Pipeline::default();
    let direct_sink = ironbeam_prism_proof::streaming_windowed_bag_state_fixture(
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
    ironbeam_prism_proof::streaming_windowed_bag_state_fixture(
        &portable_pipeline,
        portable_path.clone(),
    )
    .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "unbounded-fixed-window-bag-state",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_STREAMING_WINDOWED_BAG_STATE".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "Prism fixed-window bag state failed: {terminal:?}"
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
        "0:10000000:1:11:1",
        "0:10000000:1:13:2",
        "20000000:30000000:1:17:1",
        "20000000:30000000:1:19:2",
    ];
    assert_eq!(read_rows(&direct_path), expected);
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_matches_direct_unbounded_bag_state() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-stream-bag-state");
    let portable_path = directory.path().join("portable-stream-bag-state");

    let direct_pipeline = Pipeline::default();
    let direct_sink =
        ironbeam_prism_proof::streaming_bag_state_fixture(&direct_pipeline, direct_path.clone())
            .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();

    let portable_pipeline = Pipeline::default();
    ironbeam_prism_proof::streaming_bag_state_fixture(&portable_pipeline, portable_path.clone())
        .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "unbounded-bag-state",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_STREAMING_BAG_STATE".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "Prism unbounded bag state failed: {terminal:?}"
    );
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let expected = ["1:11:1", "1:13:2", "1:23:3", "2:17:1", "2:19:2"];
    assert_eq!(read_rows(&direct_path), expected);
    assert_eq!(read_rows(&portable_path), expected);
}
