use super::*;
use ironbeam_prism_proof::count_or_finally_trigger_fixture;

#[tokio::test]
async fn prism_matches_direct_count_or_finally_panes() {
    let directory = tempfile::tempdir().unwrap();
    let endpoint = std::env::var("IRONBEAM_PRISM_ENDPOINT").ok();
    let _prism_job = prism_job_lock().lock().await;
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    for (accumulating, final_count) in [(false, 7), (true, 7), (false, 11), (true, 11)] {
        let direct_path = directory
            .path()
            .join(format!("direct-{accumulating}-{final_count}"));
        let direct_pipeline = Pipeline::default();
        let direct_sink = count_or_finally_trigger_fixture(
            &direct_pipeline,
            direct_path.clone(),
            accumulating,
            final_count,
        )
        .unwrap();
        DirectRunner
            .start_stream_sink(&direct_sink, &JobConfig::default())
            .unwrap()
            .wait()
            .unwrap();
        let expected = if (accumulating, final_count) == (true, 7) {
            [
                "a:[1, 2]:Early:0",
                "a:[1, 2, 3, 4]:Early:1",
                "a:[1, 2, 3, 4, 5, 6]:Early:2",
                "a:[1, 2, 3, 4, 5, 6, 7]:Early:3",
                "a:[1, 2, 3, 4, 5, 6, 7, 8, 9]:OnTime:4",
            ]
        } else if (accumulating, final_count) == (false, 7) {
            [
                "a:[1, 2]:Early:0",
                "a:[3, 4]:Early:1",
                "a:[5, 6]:Early:2",
                "a:[7]:Early:3",
                "a:[8, 9]:OnTime:4",
            ]
        } else if accumulating {
            [
                "a:[1, 2]:Early:0",
                "a:[1, 2, 3, 4]:Early:1",
                "a:[1, 2, 3, 4, 5, 6]:Early:2",
                "a:[1, 2, 3, 4, 5, 6, 7, 8]:Early:3",
                "a:[1, 2, 3, 4, 5, 6, 7, 8, 9]:OnTime:4",
            ]
        } else {
            [
                "a:[1, 2]:Early:0",
                "a:[3, 4]:Early:1",
                "a:[5, 6]:Early:2",
                "a:[7, 8]:Early:3",
                "a:[9]:OnTime:4",
            ]
        };
        let mut expected = expected.map(str::to_owned).to_vec();
        expected.sort();
        assert_eq!(read_rows(&direct_path), expected);
        let Some(endpoint) = endpoint.as_deref() else {
            continue;
        };
        let portable_path = directory
            .path()
            .join(format!("portable-{accumulating}-{final_count}"));
        let pipeline = Pipeline::default();
        count_or_finally_trigger_fixture(
            &pipeline,
            portable_path.clone(),
            accumulating,
            final_count,
        )
        .unwrap();
        let terminal = run_shared_fixture(
            endpoint,
            &pipeline,
            &format!("count-or-finally-{accumulating}-{final_count}"),
            HashMap::from([
                (
                    "IRONBEAM_V5_PRISM_COUNT_OR_FINALLY".into(),
                    portable_path.to_string_lossy().into_owned(),
                ),
                (
                    "IRONBEAM_V5_PRISM_COUNT_OR_FINALLY_ACCUMULATING".into(),
                    if accumulating { "1" } else { "0" }.into(),
                ),
                (
                    "IRONBEAM_V5_PRISM_COUNT_OR_FINALLY_FINAL_COUNT".into(),
                    final_count.to_string(),
                ),
            ]),
        )
        .await;
        assert_eq!(terminal.0, 3, "OrFinally job state: {terminal:?}");
        assert_eq!(read_rows(&portable_path), expected);
    }
}

#[tokio::test]
async fn prism_matches_direct_latest_timestamp_after_discarding_early_pane() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-latest-discard.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        discarding_latest_on_time_fixture(&direct_pipeline, direct_path.clone()).unwrap();
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
        rows.sort_unstable();
        rows
    };
    let expected = [
        "a:[3]:3000000:OnTime:1".to_owned(),
        "a:[7, 8]:8000000:Early:0".to_owned(),
    ];
    assert_eq!(read_rows(&direct_path), expected);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_path = directory.path().join("portable-latest-discard.txt");
    let portable_pipeline = Pipeline::default();
    discarding_latest_on_time_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "latest-discard",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_LATEST_DISCARD".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism latest-discard state: {terminal:?}");
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_omits_requested_empty_final_pane() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-closing-always.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        pane_metadata_fixture_closing_always(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    };
    let direct_rows = rows(&direct_path);
    assert_eq!(
        direct_rows,
        [
            "group:a:[2, 3]:Early:true:false:0:-1",
            "group:a:[]:Late:false:true:1:0",
            "source:a:2:Unknown:true:true:0:0",
            "source:a:3:Unknown:true:true:0:0",
        ]
    );
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_path = directory.path().join("portable-closing-always.txt");
    let portable_pipeline = Pipeline::default();
    pane_metadata_fixture_closing_always(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "closing-always",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_CLOSING_ALWAYS".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism closing-always state: {terminal:?}");
    let portable_rows = rows(&portable_path);
    assert_eq!(portable_rows.len(), direct_rows.len() - 1);
    assert_eq!(
        portable_rows,
        [
            "group:a:[2, 3]:Early:true:false:0:-1",
            "source:a:2:Unknown:true:true:0:0",
            "source:a:3:Unknown:true:true:0:0",
        ]
    );
}

#[tokio::test]
async fn prism_omits_requested_empty_on_time_pane_after_early_firing() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-fire-always.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        pane_metadata_fixture_fire_always(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    };
    let direct_rows = rows(&direct_path);
    assert_eq!(
        direct_rows,
        [
            "group:a:[2, 3]:Early:true:false:0:-1",
            "group:a:[]:OnTime:false:true:1:0",
            "source:a:2:Unknown:true:true:0:0",
            "source:a:3:Unknown:true:true:0:0",
        ]
    );
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_path = directory.path().join("portable-fire-always.txt");
    let portable_pipeline = Pipeline::default();
    pane_metadata_fixture_fire_always(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "fire-always",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_FIRE_ALWAYS".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism fire-always state: {terminal:?}");
    let portable_rows = rows(&portable_path);
    assert_eq!(portable_rows.len(), direct_rows.len() - 1);
    assert_eq!(
        portable_rows,
        [
            "group:a:[2, 3]:Early:true:false:0:-1",
            "source:a:2:Unknown:true:true:0:0",
            "source:a:3:Unknown:true:true:0:0",
        ]
    );
}

async fn assert_default_pane_metadata(
    window: PaneFixtureWindow,
    expected_direct: &[&str],
    expected_prism: &[&str],
) {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-panes.txt");
    let portable_path = directory.path().join("portable-panes.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        pane_metadata_fixture_for(&direct_pipeline, direct_path.clone(), window).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    };
    let direct_rows = rows(&direct_path);
    assert_eq!(direct_rows, expected_direct);
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    pane_metadata_fixture_for(&portable_pipeline, portable_path.clone(), window).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        &format!("panes-{window:?}"),
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_PANES".into(),
                portable_path.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_PANES_WINDOW".into(),
                format!("{window:?}").to_ascii_lowercase(),
            ),
        ]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism pane metadata state: {terminal:?}");
    assert_eq!(rows(&portable_path), expected_prism);
}

#[tokio::test]
async fn prism_matches_direct_default_pane_metadata_after_native_grouping() {
    assert_default_pane_metadata(
        PaneFixtureWindow::Fixed,
        &[
            "group:a:[2, 3]:OnTime:true:true:0:0",
            "source:a:2:Unknown:true:true:0:0",
            "source:a:3:Unknown:true:true:0:0",
        ],
        &[
            "group:a:[2, 3]:OnTime:true:true:0:0",
            "source:a:2:Unknown:true:true:0:0",
            "source:a:3:Unknown:true:true:0:0",
        ],
    )
    .await;
}

#[tokio::test]
async fn prism_matches_direct_default_panes_in_overlapping_sliding_windows() {
    assert_default_pane_metadata(
        PaneFixtureWindow::Sliding,
        &[
            "group:a:[2, 3]:OnTime:true:true:0:0",
            "group:a:[2, 3]:OnTime:true:true:0:0",
            "source:a:2:Unknown:true:true:0:0",
            "source:a:2:Unknown:true:true:0:0",
            "source:a:3:Unknown:true:true:0:0",
            "source:a:3:Unknown:true:true:0:0",
        ],
        &[
            "group:a:[2, 3]:OnTime:true:true:0:0",
            "group:a:[2, 3]:OnTime:true:true:0:0",
            "source:a:2:Unknown:true:true:0:0",
            "source:a:2:Unknown:true:true:0:0",
            "source:a:3:Unknown:true:true:0:0",
            "source:a:3:Unknown:true:true:0:0",
        ],
    )
    .await;
}

#[tokio::test]
async fn prism_reports_early_default_pane_for_merged_session() {
    // The pinned Prism runner emits one Early pane for this merged session.
    // Beam's default trigger describes an on-time firing at window closure;
    // keep the runner difference visible until its cause is resolved.
    assert_default_pane_metadata(
        PaneFixtureWindow::Session,
        &[
            "group:a:[2, 3]:OnTime:true:true:0:0",
            "source:a:2:Unknown:true:true:0:0",
            "source:a:3:Unknown:true:true:0:0",
        ],
        &[
            "group:a:[2, 3]:Early:false:false:0:-1",
            "source:a:2:Unknown:true:true:0:0",
            "source:a:3:Unknown:true:true:0:0",
        ],
    )
    .await;
}

#[tokio::test]
async fn prism_count_trigger_fires_one_early_pane_for_a_bounded_bundle() {
    // The same public graph runs through Direct and the pinned Prism runner.
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-count-trigger.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink = pane_metadata_fixture_with_trigger(
        &direct_pipeline,
        direct_path.clone(),
        PaneFixtureWindow::Fixed,
        true,
    )
    .unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let path = directory.path().join("count-trigger.txt");
    let pipeline = Pipeline::default();
    pane_metadata_fixture_with_trigger(&pipeline, path.clone(), PaneFixtureWindow::Fixed, true)
        .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "count-trigger-probe",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_COUNT_BOUNDED".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "count trigger job state: {terminal:?}");
    let mut rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort();
    let mut direct_rows = std::fs::read_to_string(direct_path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    direct_rows.sort();
    assert_eq!(direct_rows, rows);
    assert_eq!(
        rows,
        [
            "group:a:[2, 3]:Early:true:false:0:-1".to_owned(),
            "source:a:2:Unknown:true:true:0:0".to_owned(),
            "source:a:3:Unknown:true:true:0:0".to_owned(),
        ]
    );
}

#[tokio::test]
async fn prism_and_direct_count_trigger_match_overlapping_sliding_windows() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    for accumulating in [false, true] {
        let mode = if accumulating {
            "accumulating"
        } else {
            "discarding"
        };
        let direct_path = directory.path().join(format!("direct-sliding-{mode}.txt"));
        let direct_pipeline = Pipeline::default();
        let direct_sink = if accumulating {
            pane_metadata_fixture_with_accumulating_count(
                &direct_pipeline,
                direct_path.clone(),
                PaneFixtureWindow::Sliding,
            )
        } else {
            pane_metadata_fixture_with_trigger(
                &direct_pipeline,
                direct_path.clone(),
                PaneFixtureWindow::Sliding,
                true,
            )
        }
        .unwrap();
        DirectRunner
            .run_sink(&direct_sink, &JobConfig::default())
            .unwrap();
        let path = directory.path().join(format!("sliding-{mode}.txt"));
        let pipeline = Pipeline::default();
        if accumulating {
            pane_metadata_fixture_with_accumulating_count(
                &pipeline,
                path.clone(),
                PaneFixtureWindow::Sliding,
            )
        } else {
            pane_metadata_fixture_with_trigger(
                &pipeline,
                path.clone(),
                PaneFixtureWindow::Sliding,
                true,
            )
        }
        .unwrap();
        let environment = if accumulating {
            HashMap::from([(
                "IRONBEAM_V5_PRISM_COUNT_ACCUMULATING_BOUNDED".into(),
                path.to_string_lossy().into_owned(),
            )])
        } else {
            HashMap::from([
                (
                    "IRONBEAM_V5_PRISM_COUNT_BOUNDED".into(),
                    path.to_string_lossy().into_owned(),
                ),
                ("IRONBEAM_V5_PRISM_COUNT_WINDOW".into(), "sliding".into()),
            ])
        };
        let terminal = run_shared_fixture(
            &endpoint,
            &pipeline,
            &format!("sliding-{mode}-count-trigger-probe"),
            environment,
        )
        .await;
        assert_eq!(
            terminal.0, 3,
            "sliding {mode} count job state: {terminal:?}"
        );
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort();
        let mut direct_rows = std::fs::read_to_string(direct_path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        direct_rows.sort();
        assert_eq!(direct_rows, rows, "sliding {mode} panes differ");
    }
}

#[tokio::test]
async fn count_and_processing_time_firing_order_matches_prism_where_supported() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let endpoint = std::env::var("IRONBEAM_PRISM_ENDPOINT").ok();
    let _prism_job = if endpoint.is_some() {
        Some(prism_job_lock().lock().await)
    } else {
        None
    };
    let directory = tempfile::tempdir().unwrap();
    for count_first in [true, false] {
        for accumulating in [false, true] {
            let portable_path = directory
                .path()
                .join(format!("count-and-time-{count_first}-{accumulating}.txt"));
            let direct_rows = Arc::new(Mutex::new(Vec::new()));
            let accepted = Arc::new(Mutex::new(HashSet::new()));
            let direct_pipeline = Pipeline::default();
            let direct_sink = count_and_processing_time_trigger_collection(
                &direct_pipeline,
                accumulating,
                count_first,
            )
            .unwrap()
            .sink_named_with_config(
                "count-and-time-result",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                format!("count-and-time-direct-{count_first}-{accumulating}-v1"),
                {
                    let rows = Arc::clone(&direct_rows);
                    let accepted = Arc::clone(&accepted);
                    move || {
                        Ok(Box::new(DirectRows {
                            rows: Arc::clone(&rows),
                            accepted: Arc::clone(&accepted),
                            staged: Vec::new(),
                            bundle: String::new(),
                        }))
                    }
                },
            )
            .unwrap();
            DirectRunner
                .start_stream_sink(&direct_sink, &JobConfig::default())
                .unwrap()
                .wait()
                .unwrap();
            let mut direct = direct_rows.lock().unwrap().clone();
            direct.sort();
            let early = "group:a:[2, 3]:Early:true:false:0:-1".to_owned();
            assert_eq!(direct.as_slice(), std::slice::from_ref(&early), "Direct count-and-time panes: count_first={count_first}, accumulating={accumulating}");
            let Some(endpoint) = endpoint.as_deref() else {
                continue;
            };
            let pipeline = Pipeline::default();
            count_and_processing_time_trigger_fixture(
                &pipeline,
                portable_path.clone(),
                accumulating,
                count_first,
            )
            .unwrap();
            let terminal = run_shared_fixture(
                endpoint,
                &pipeline,
                &format!("count-and-time-{count_first}-{accumulating}"),
                HashMap::from([
                    (
                        "IRONBEAM_V5_PRISM_COUNT_AND_TIME".into(),
                        portable_path.to_string_lossy().into_owned(),
                    ),
                    (
                        "IRONBEAM_V5_PRISM_COUNT_AND_TIME_ACCUMULATING".into(),
                        if accumulating { "1" } else { "0" }.into(),
                    ),
                    (
                        "IRONBEAM_V5_PRISM_COUNT_AND_TIME_COUNT_FIRST".into(),
                        if count_first { "1" } else { "0" }.into(),
                    ),
                ]),
            )
            .await;
            assert_eq!(terminal.0, 3, "count-and-time job state: {terminal:?}");
            let mut portable = std::fs::read_to_string(&portable_path)
                .unwrap()
                .lines()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            portable.sort();
            if accumulating {
                // Beam's FIRE_IF_NONEMPTY requires new input since the early
                // firing. Pinned Prism currently emits this extra on-time pane
                // even though both values were already included in the early pane.
                assert_eq!(
                    portable,
                    [early, "group:a:[2, 3]:OnTime:false:true:1:0".to_owned()]
                );
            } else {
                assert_eq!(
                    portable, direct,
                    "count-and-time panes differ: count_first={count_first}"
                );
            }
        }
    }
}

#[tokio::test]
async fn prism_matches_direct_sequential_count_then_processing_time_panes() {
    let directory = tempfile::tempdir().unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let endpoint = std::env::var("IRONBEAM_PRISM_ENDPOINT").ok();
    let _prism_job = prism_job_lock().lock().await;
    for accumulating in [false, true] {
        let direct_path = directory.path().join(format!("direct-{accumulating}"));
        let portable_path = directory.path().join(format!("portable-{accumulating}"));
        let direct_pipeline = Pipeline::default();
        let direct_sink = count_then_processing_time_trigger_fixture(
            &direct_pipeline,
            direct_path.clone(),
            accumulating,
        )
        .unwrap();
        DirectRunner
            .start_stream_sink(&direct_sink, &JobConfig::default())
            .unwrap()
            .wait()
            .unwrap();
        let expected = if accumulating {
            vec![
                "group:a:[2, 3, 4]:OnTime:false:true:2:0".to_owned(),
                "group:a:[2, 3, 4]:Early:false:false:1:-1".to_owned(),
                "group:a:[2, 3]:Early:true:false:0:-1".to_owned(),
            ]
        } else {
            vec![
                "group:a:[2, 3]:Early:true:false:0:-1".to_owned(),
                "group:a:[4]:Early:false:false:1:-1".to_owned(),
            ]
        };
        let mut expected = expected;
        expected.sort();
        assert_eq!(read_rows(&direct_path), expected);
        let Some(endpoint) = endpoint.as_deref() else {
            continue;
        };
        let pipeline = Pipeline::default();
        count_then_processing_time_trigger_fixture(&pipeline, portable_path.clone(), accumulating)
            .unwrap();
        let terminal = run_shared_fixture(
            endpoint,
            &pipeline,
            &format!("count-then-time-{accumulating}"),
            HashMap::from([
                (
                    "IRONBEAM_V5_PRISM_COUNT_THEN_TIME".into(),
                    portable_path.to_string_lossy().into_owned(),
                ),
                (
                    "IRONBEAM_V5_PRISM_COUNT_THEN_TIME_ACCUMULATING".into(),
                    if accumulating { "1" } else { "0" }.into(),
                ),
            ]),
        )
        .await;
        assert_eq!(terminal.0, 3, "sequential trigger job state: {terminal:?}");
        assert_eq!(read_rows(&portable_path), expected);
    }
}

#[tokio::test]
async fn prism_matches_direct_processing_time_early_panes() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    for accumulating in [false, true] {
        let portable_path = directory
            .path()
            .join(format!("portable-{accumulating}.txt"));
        let direct_rows = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(Mutex::new(HashSet::new()));
        let direct_pipeline = Pipeline::default();
        let direct_sink = processing_time_trigger_collection(&direct_pipeline, accumulating)
            .unwrap()
            .sink_named_with_config(
                "processing-result",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                format!("processing-direct-{accumulating}-v1"),
                {
                    let rows = Arc::clone(&direct_rows);
                    let accepted = Arc::clone(&accepted);
                    move || {
                        Ok(Box::new(DirectRows {
                            rows: Arc::clone(&rows),
                            accepted: Arc::clone(&accepted),
                            staged: Vec::new(),
                            bundle: String::new(),
                        }))
                    }
                },
            )
            .unwrap();
        DirectRunner
            .start_stream_sink(&direct_sink, &JobConfig::default())
            .unwrap()
            .wait()
            .unwrap();
        let pipeline = Pipeline::default();
        processing_time_trigger_fixture(&pipeline, portable_path.clone(), accumulating).unwrap();
        let terminal = run_shared_fixture(
            &endpoint,
            &pipeline,
            &format!("processing-time-{accumulating}"),
            HashMap::from([
                (
                    "IRONBEAM_V5_PRISM_PROCESSING_TIME".into(),
                    portable_path.to_string_lossy().into_owned(),
                ),
                (
                    "IRONBEAM_V5_PRISM_PROCESSING_ACCUMULATING".into(),
                    if accumulating { "1" } else { "0" }.into(),
                ),
            ]),
        )
        .await;
        assert_eq!(terminal.0, 3, "processing-time job state: {terminal:?}");
        let mut direct = direct_rows.lock().unwrap().clone();
        let mut portable = std::fs::read_to_string(&portable_path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        direct.sort();
        portable.sort();
        assert_eq!(portable, direct, "processing-time panes differ");
    }
}

#[tokio::test]
async fn prism_matches_direct_two_source_processing_time_panes() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    for accumulating in [false, true] {
        let portable_path = directory
            .path()
            .join(format!("portable-dual-processing-{accumulating}.txt"));
        let direct_rows = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(Mutex::new(HashSet::new()));
        let direct_pipeline = Pipeline::default();
        let direct_sink = dual_processing_time_cogroup_collection(&direct_pipeline, accumulating)
            .unwrap()
            .sink_named_with_config(
                "dual-processing-result",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                format!("dual-processing-direct-{accumulating}-v1"),
                {
                    let rows = Arc::clone(&direct_rows);
                    let accepted = Arc::clone(&accepted);
                    move || {
                        Ok(Box::new(DirectRows {
                            rows: Arc::clone(&rows),
                            accepted: Arc::clone(&accepted),
                            staged: Vec::new(),
                            bundle: String::new(),
                        }))
                    }
                },
            )
            .unwrap();
        DirectRunner
            .start_stream_sink(&direct_sink, &JobConfig::default())
            .unwrap()
            .wait()
            .unwrap();
        let portable_pipeline = Pipeline::default();
        dual_processing_time_cogroup_fixture(
            &portable_pipeline,
            portable_path.clone(),
            accumulating,
        )
        .unwrap();
        let terminal = run_shared_fixture(
            &endpoint,
            &portable_pipeline,
            &format!("dual-processing-time-{accumulating}"),
            HashMap::from([
                (
                    "IRONBEAM_V5_PRISM_DUAL_PROCESSING_TIME".into(),
                    portable_path.to_string_lossy().into_owned(),
                ),
                (
                    "IRONBEAM_V5_PRISM_DUAL_PROCESSING_ACCUMULATING".into(),
                    if accumulating { "1" } else { "0" }.into(),
                ),
            ]),
        )
        .await;
        assert_eq!(
            terminal.0, 3,
            "dual processing-time job state: {terminal:?}"
        );
        let mut direct = direct_rows.lock().unwrap().clone();
        let mut portable = std::fs::read_to_string(&portable_path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        direct.sort();
        portable.sort();
        assert_eq!(direct.len(), if accumulating { 2 } else { 1 });
        assert_eq!(portable, direct, "two-source processing-time panes differ");
    }
}

#[tokio::test]
async fn prism_matches_direct_two_source_count_and_time_panes() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    for accumulating in [false, true] {
        let portable_path = directory
            .path()
            .join(format!("dual-and-{accumulating}.txt"));
        let rows = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(Mutex::new(HashSet::new()));
        let direct_pipeline = Pipeline::default();
        let direct_sink = dual_count_and_time_cogroup_collection(&direct_pipeline, accumulating)
            .unwrap()
            .sink_named_with_config(
                "dual-and-result",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                format!("dual-and-direct-{accumulating}-v1"),
                {
                    let rows = Arc::clone(&rows);
                    let accepted = Arc::clone(&accepted);
                    move || {
                        Ok(Box::new(DirectRows {
                            rows: Arc::clone(&rows),
                            accepted: Arc::clone(&accepted),
                            staged: Vec::new(),
                            bundle: String::new(),
                        }))
                    }
                },
            )
            .unwrap();
        DirectRunner
            .start_stream_sink(&direct_sink, &JobConfig::default())
            .unwrap()
            .wait()
            .unwrap();
        let portable_pipeline = Pipeline::default();
        dual_count_and_time_cogroup_fixture(
            &portable_pipeline,
            portable_path.clone(),
            accumulating,
        )
        .unwrap();
        let terminal = run_shared_fixture(
            &endpoint,
            &portable_pipeline,
            &format!("dual-and-{accumulating}"),
            HashMap::from([
                (
                    "IRONBEAM_V5_PRISM_DUAL_COUNT_AND_TIME".into(),
                    portable_path.to_string_lossy().into_owned(),
                ),
                (
                    "IRONBEAM_V5_PRISM_DUAL_COUNT_AND_TIME_ACCUMULATING".into(),
                    if accumulating { "1" } else { "0" }.into(),
                ),
            ]),
        )
        .await;
        assert_eq!(
            terminal.0, 3,
            "two-source count-and-time job state: {terminal:?}"
        );
        let mut direct = rows.lock().unwrap().clone();
        let mut portable = std::fs::read_to_string(&portable_path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        direct.sort();
        portable.sort();
        let early = "group:1:[[2], [4]]:Early:true:false:0:-1";
        if accumulating {
            assert_eq!(direct, [early, "group:1:[[2], [4]]:OnTime:false:true:1:0",]);
        } else {
            assert_eq!(direct, [early]);
        }
        assert_eq!(portable, direct, "two-source count-and-time panes differ");
    }
}

#[tokio::test]
async fn prism_characterizes_two_source_accumulating_count_or_time_panes() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dual-count-time.txt");
    let pipeline = Pipeline::default();
    dual_count_or_time_cogroup_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "dual-count-time",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_DUAL_COUNT_OR_TIME".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "two-source count-or-time job: {terminal:?}");
    let mut rows = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort_unstable();
    assert_eq!(
        rows,
        [
            "1:[[2, 3], [4]]:Early:2",
            "1:[[2, 3], [4]]:OnTime:3",
            "1:[[2], [4]]:Early:0",
            "1:[[2], [4]]:Early:1",
        ]
    );
}

#[tokio::test]
async fn prism_count_trigger_stream_advances_early_pane_indices() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("count-stream.txt");
    let pipeline = Pipeline::default();
    count_trigger_stream_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "count-trigger-stream-probe",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_COUNT_STREAM".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "count stream job state: {terminal:?}");
    let rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        [
            "group:a:[2]:Early:true:false:0:-1".to_owned(),
            "group:a:[3]:Early:false:false:1:-1".to_owned(),
        ]
    );
}

#[tokio::test]
async fn prism_accumulating_count_stream_retains_prior_values() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("accumulating-count-stream.txt");
    let pipeline = Pipeline::default();
    accumulating_count_trigger_stream_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "accumulating-count-stream-probe",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_COUNT_ACCUMULATING".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "accumulating count stream job state: {terminal:?}"
    );
    let rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        [
            "group:a:[2]:Early:true:false:0:-1".to_owned(),
            "group:a:[2, 3]:Early:false:false:1:-1".to_owned(),
            "group:a:[2, 3]:OnTime:false:true:2:0".to_owned(),
        ]
    );
}

#[tokio::test]
async fn prism_accumulating_count_stream_uses_latest_output_time() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("latest-count-stream.txt");
    let pipeline = Pipeline::default();
    accumulating_latest_count_trigger_stream_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "latest-count-stream-probe",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_COUNT_LATEST".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "latest count job state: {terminal:?}");
    let rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        [
            "group:a:[2]:1000000:Early:true:false:0:-1".to_owned(),
            "group:a:[2, 3]:3000000:Early:false:false:1:-1".to_owned(),
            "group:a:[2, 3]:3000000:OnTime:false:true:2:0".to_owned(),
        ]
    );
}

#[tokio::test]
async fn prism_emits_late_pane_before_allowed_lateness_expires() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("allowed-lateness.txt");
    let pipeline = Pipeline::default();
    allowed_lateness_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "allowed-lateness-probe",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_ALLOWED_LATENESS".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "late pane job state: {terminal:?}");
    let rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        [
            "a:[2]:2000000:OnTime:true:false:0:0",
            "a:[3]:3000000:Late:false:false:1:1",
        ]
    );
}

#[tokio::test]
async fn prism_repeats_late_count_and_flushes_partial_pane() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("late-count.txt");
    let pipeline = Pipeline::default();
    late_count_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "late-count-probe",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_LATE_COUNT".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "late count job state: {terminal:?}");
    let rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        [
            "a:[2]:2000000:OnTime:true:false:0:0",
            "a:[3, 5]:3000000:Late:false:false:1:1",
            "a:[7]:7000000:Late:false:true:2:2",
        ]
    );
}

#[tokio::test]
async fn prism_combines_early_and_late_count_firings() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("combined-count.txt");
    let pipeline = Pipeline::default();
    combined_count_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "combined-count-probe",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_COMBINED_COUNT".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "combined count job state: {terminal:?}");
    let rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        [
            "a:[2, 3]:2000000:Early:true:false:0:-1",
            "a:[4]:4000000:OnTime:false:false:1:0",
            "a:[5, 7]:5000000:Late:false:false:2:1",
            "a:[11]:8000000:Late:false:true:3:2",
        ]
    );
}

#[tokio::test]
async fn prism_characterizes_accumulating_early_and_late_count_schedule() {
    let directory = tempfile::tempdir().unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let direct_path = directory.path().join("combined-accumulating-direct");
    let direct = Pipeline::default();
    let direct_sink = accumulating_combined_count_fixture(&direct, direct_path.clone()).unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let direct_rows = read_rows(&direct_path);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_path = directory.path().join("combined-accumulating-prism");
    let portable = Pipeline::default();
    accumulating_combined_count_fixture(&portable, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "combined-accumulating-count",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_COMBINED_COUNT_ACCUMULATING".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "combined accumulating job: {terminal:?}");
    let portable_rows = read_rows(&portable_path);
    let mut expected_direct = vec![
        "a:[2, 3]:2000000:Early:true:false:0:-1",
        "a:[2, 3]:2000000:OnTime:false:false:1:0",
        "a:[2, 3, 5, 7]:2000000:Late:false:false:2:1",
    ];
    expected_direct.sort();
    assert_eq!(direct_rows, expected_direct);
    // Pinned Prism restarts the accumulating early/on-time trigger for the
    // late bundle before firing its final late pane on this TestStream schedule.
    let mut expected_prism = vec![
        "a:[2, 3]:2000000:Early:true:false:0:-1",
        "a:[2, 3]:2000000:OnTime:false:false:1:0",
        "a:[2, 3, 5, 7]:2000000:Early:false:false:2:-1",
        "a:[2, 3, 5, 7]:2000000:OnTime:false:false:3:0",
        "a:[2, 3, 5, 7]:2000000:Late:false:true:4:1",
    ];
    expected_prism.sort();
    assert_eq!(portable_rows, expected_prism);
}

#[tokio::test]
async fn prism_accumulating_late_count_coalesces_until_expiry() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("accumulating-late-count.txt");
    let pipeline = Pipeline::default();
    accumulating_late_count_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "accumulating-late-count-probe",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_LATE_COUNT_ACCUMULATING".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    // Pinned Prism holds accumulating late input until expiry on this schedule.
    // Direct emits a threshold pane earlier, but the final values agree.
    assert_eq!(
        terminal.0, 3,
        "accumulating late count job state: {terminal:?}"
    );
    let rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        [
            "a:[2]:2000000:OnTime:true:false:0:0",
            "a:[2, 3, 5, 7]:2000000:Late:false:true:1:1",
        ]
    );
}

#[tokio::test]
async fn prism_sliding_windows_keep_independent_late_pane_indices() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sliding-lateness.txt");
    let pipeline = Pipeline::default();
    sliding_allowed_lateness_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "sliding-lateness-probe",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_SLIDING_LATENESS".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "sliding late job state: {terminal:?}");
    let mut rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort();
    let mut expected = vec![
        "0..10000000:a:[2]:6000000:OnTime:true:false:0:0",
        "0..10000000:a:[3]:7000000:Late:false:true:1:1",
        "5000000..15000000:a:[2, 3]:6000000:OnTime:true:false:0:0",
        "5000000..15000000:a:[4]:8000000:Late:false:false:1:1",
    ];
    expected.sort();
    assert_eq!(rows, expected);
}
