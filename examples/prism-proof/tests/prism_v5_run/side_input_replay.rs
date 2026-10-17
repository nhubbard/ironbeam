use super::*;

#[tokio::test]
async fn changing_windowed_side_view_replays_each_row_once_after_worker_loss() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-side-rows");
    let direct = Pipeline::default();
    let sink = ironbeam_prism_proof::dual_stream_windowed_side_replay_fixture(
        &direct,
        direct_path.clone(),
    )
    .unwrap();
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
    let expected = [
        "0..2000000:1000000:11",
        "2000000..4000000:3000000:43",
        "4000000..6000000:5000000:3",
    ];
    assert_eq!(read_rows(&direct_path), expected);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_path = directory.path().join("portable-side-rows");
    let trigger = directory.path().join("side-replay-trigger");
    let killed = directory.path().join("killed-side-child");
    let portable = Pipeline::default();
    ironbeam_prism_proof::dual_stream_windowed_side_replay_fixture(
        &portable,
        portable_path.clone(),
    )
    .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "changing-windowed-side-replay",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_DUAL_WINDOWED_SIDE_REPLAY".into(),
                portable_path.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_KILL_DUAL_WINDOWED_SIDE".into(),
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
    assert_eq!(terminal.0, 3, "Prism side replay failed: {terminal:?}");
    assert_eq!(
        std::fs::read_to_string(&killed).unwrap(),
        std::fs::read_to_string(&trigger).unwrap()
    );
    assert_eq!(read_rows(&portable_path), expected);
}
