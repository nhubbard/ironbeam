use super::*;

#[tokio::test]
async fn value_and_map_state_branches_recover_one_shared_source_cursor() {
    let directory = tempfile::tempdir().unwrap();
    let direct_value = directory.path().join("direct-value");
    let direct_map = directory.path().join("direct-map");
    let direct = Pipeline::default();
    let (value_sink, map_sink) = ironbeam_prism_proof::production_state_fanout_fixture(
        &direct,
        direct_value.clone(),
        direct_map.clone(),
    )
    .unwrap();
    DirectRunner
        .start_stream_sink_pair(&value_sink, &map_sink, &JobConfig::default())
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
    let expected_value = ["1:7:1", "1:8:3", "1:9:2"];
    let expected_map = ["1:7:-1", "1:8:9", "1:9:7"];
    assert_eq!(read_rows(&direct_value), expected_value);
    assert_eq!(read_rows(&direct_map), expected_map);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_value = directory.path().join("portable-value");
    let portable_map = directory.path().join("portable-map");
    let trigger = directory.path().join("state-fanout-trigger");
    let killed = directory.path().join("state-fanout-killed");
    let portable = Pipeline::default();
    ironbeam_prism_proof::production_state_fanout_fixture(
        &portable,
        portable_value.clone(),
        portable_map.clone(),
    )
    .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "replayed-dual-state-fanout",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_STATE_FANOUT_VALUE".into(),
                portable_value.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_STATE_FANOUT_MAP".into(),
                portable_map.to_string_lossy().into_owned(),
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
    assert_eq!(terminal.0, 3, "Prism state fanout failed: {terminal:?}");
    assert_eq!(
        std::fs::read_to_string(&killed).unwrap(),
        std::fs::read_to_string(&trigger).unwrap()
    );
    assert_eq!(read_rows(&portable_value), expected_value);
    assert_eq!(read_rows(&portable_map), expected_map);
}
