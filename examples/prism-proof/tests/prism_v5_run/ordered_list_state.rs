use super::*;

#[tokio::test]
async fn prism_matches_direct_ordered_list_mutations() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-ordered-list");
    let portable_path = directory.path().join("portable-ordered-list");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        ironbeam_prism_proof::bounded_ordered_list_fixture(&direct_pipeline, direct_path.clone())
            .unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let portable_pipeline = Pipeline::default();
    ironbeam_prism_proof::bounded_ordered_list_fixture(&portable_pipeline, portable_path.clone())
        .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "bounded-ordered-list-mutations",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_ORDERED_LIST_MUTATIONS".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "Prism ordered-list mutation failed: {terminal:?}"
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
        "1:-10:[20]",
        "1:-1:[-10, 10, 10, 20]",
        "1:10:[-10, 10, 20]",
        "1:10:[-10, 20]",
        "1:15:[-10, 20]",
        "1:20:[]",
        "2:30:[]",
    ];
    assert_eq!(read_rows(&direct_path), expected);
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_matches_direct_windowed_ordered_list_across_batches() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-windowed-ordered-list");
    let portable_path = directory.path().join("portable-windowed-ordered-list");
    let direct_pipeline = Pipeline::default();
    let direct_sink = ironbeam_prism_proof::streaming_windowed_ordered_list_fixture(
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
    ironbeam_prism_proof::streaming_windowed_ordered_list_fixture(
        &portable_pipeline,
        portable_path.clone(),
    )
    .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "streaming-windowed-ordered-list",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_STREAMING_WINDOWED_ORDERED_LIST".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "Prism windowed ordered-list failed: {terminal:?}"
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
        "0:10000000:1:10:[20]",
        "0:10000000:1:20:[]",
        "20000000:30000000:1:25:[30]",
        "20000000:30000000:1:30:[]",
    ];
    assert_eq!(read_rows(&direct_path), expected);
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_replays_production_ordered_list_after_child_loss() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let portable_path = directory.path().join("production-ordered-list");
    let trigger = directory.path().join("ordered-list-source-reopen-trigger");
    let killed = directory.path().join("killed-ordered-list-child");
    let pipeline = Pipeline::default();
    ironbeam_prism_proof::production_ordered_list_fixture(&pipeline, portable_path.clone())
        .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "replayed-production-unbounded-ordered-list-state",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_PRODUCTION_ORDERED_LIST".into(),
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
        "Prism ordered-list replay failed: {terminal:?}"
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
    let sequential = ["1:7:[]", "1:8:[7]", "1:9:[7, 8]"];
    let residual_first = ["1:7:[]", "1:8:[7, 9]", "1:9:[7]"];
    assert!(
        rows == sequential || rows == residual_first,
        "unexpected ordered-list replay: {rows:?}"
    );
}
