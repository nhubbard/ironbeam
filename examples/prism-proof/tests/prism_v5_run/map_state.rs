use super::*;

#[tokio::test]
async fn prism_matches_direct_unbounded_map_updates() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-stream-map");
    let portable_path = directory.path().join("portable-stream-map");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        ironbeam_prism_proof::streaming_map_state_fixture(&direct_pipeline, direct_path.clone())
            .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let portable_pipeline = Pipeline::default();
    ironbeam_prism_proof::streaming_map_state_fixture(&portable_pipeline, portable_path.clone())
        .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "unbounded-map-updates",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_STREAMING_MAP_STATE".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism unbounded map failed: {terminal:?}");
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let expected = [
        "1:11:-1:0",
        "1:11:11:1",
        "1:23:-1:1",
        "2:17:-1:0",
        "2:19:-1:1",
    ];
    assert_eq!(read_rows(&direct_path), expected);
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_matches_direct_map_mutations() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-map-mutations");
    let portable_path = directory.path().join("portable-map-mutations");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        ironbeam_prism_proof::bounded_map_mutations_fixture(&direct_pipeline, direct_path.clone())
            .unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let portable_pipeline = Pipeline::default();
    ironbeam_prism_proof::bounded_map_mutations_fixture(&portable_pipeline, portable_path.clone())
        .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "bounded-map-mutations",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_MAP_MUTATIONS".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism map mutations failed: {terminal:?}");
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let expected = [
        "1:-1:100:2",
        "1:-2:-1:1",
        "1:-3:100:2",
        "1:0:100:2",
        "1:10:-1:0",
        "1:10:100:2",
        "1:11:-1:0",
        "1:20:100:1",
        "2:10:-1:0",
    ];
    assert_eq!(read_rows(&direct_path), expected);
    assert_eq!(read_rows(&portable_path), expected);
}
