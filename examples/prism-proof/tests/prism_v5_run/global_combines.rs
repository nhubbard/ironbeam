use super::*;
use ironbeam_prism_proof::{
    streaming_global_combine_fixture, streaming_global_combine_panes_fixture,
    streaming_late_global_combine_fixture, LateGlobalCombineRoute, StreamingGlobalCombineRoute,
};

const EXPECTED: [&str; 6] = [
    "count:0:1:3",
    "count:10:11:3",
    "sum:0:1:6",
    "sum:10:11:15",
    "vector:0:1:[1, 2, 3]",
    "vector:10:11:[4, 5, 6]",
];

fn read_rows(path: &Path) -> Vec<String> {
    let mut rows = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

#[test]
fn direct_runs_windowed_global_combines() {
    let directory = tempfile::tempdir().unwrap();
    let mut rows = Vec::new();
    for (name, route) in [
        ("vector", StreamingGlobalCombineRoute::Vector),
        ("count", StreamingGlobalCombineRoute::Count),
        ("sum", StreamingGlobalCombineRoute::Sum),
    ] {
        let path = directory.path().join(name);
        let pipeline = Pipeline::default();
        let sink = streaming_global_combine_fixture(&pipeline, path.clone(), route).unwrap();
        DirectRunner
            .start_stream_sink(&sink, &JobConfig::default())
            .unwrap()
            .wait()
            .unwrap();
        rows.extend(read_rows(&path));
    }
    rows.sort();
    assert_eq!(rows, EXPECTED);
}

#[tokio::test]
async fn prism_runs_windowed_global_combines() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("portable-global-combines");
    let pipeline = Pipeline::default();
    streaming_global_combine_fixture(&pipeline, path.clone(), StreamingGlobalCombineRoute::All)
        .unwrap();
    let (state, messages) = run_shared_fixture(
        &endpoint,
        &pipeline,
        "streaming-global-combines",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_STREAMING_GLOBAL_COMBINES".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(state, 3, "Prism global combines failed: {messages:?}");
    assert_eq!(read_rows(&path), EXPECTED);
}

const EXPECTED_PANES: [&str; 4] = [
    "count:Early:0:3:2",
    "count:OnTime:1:6:1",
    "sum:Early:0:3:3",
    "sum:OnTime:1:6:3",
];
const EXPECTED_ACCUM_PANES: [&str; 4] = [
    "count:Early:0:3:2",
    "count:OnTime:1:6:3",
    "sum:Early:0:3:3",
    "sum:OnTime:1:6:6",
];

#[test]
fn direct_runs_early_global_combine_panes() {
    let directory = tempfile::tempdir().unwrap();
    for (accumulating, expected) in [(false, EXPECTED_PANES), (true, EXPECTED_ACCUM_PANES)] {
        let mut rows = Vec::new();
        for (name, route) in [
            ("count", StreamingGlobalCombineRoute::Count),
            ("sum", StreamingGlobalCombineRoute::Sum),
        ] {
            let path = directory.path().join(format!("{accumulating}-{name}"));
            let pipeline = Pipeline::default();
            let sink = streaming_global_combine_panes_fixture(
                &pipeline,
                path.clone(),
                route,
                accumulating,
            )
            .unwrap();
            DirectRunner
                .start_stream_sink(&sink, &JobConfig::default())
                .unwrap()
                .wait()
                .unwrap();
            rows.extend(read_rows(&path));
        }
        rows.sort();
        assert_eq!(rows, expected);
    }
}

#[tokio::test]
async fn prism_runs_early_global_combine_panes() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("portable-global-panes");
    let pipeline = Pipeline::default();
    streaming_global_combine_panes_fixture(
        &pipeline,
        path.clone(),
        StreamingGlobalCombineRoute::All,
        false,
    )
    .unwrap();
    let (state, messages) = run_shared_fixture(
        &endpoint,
        &pipeline,
        "streaming-global-panes",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_STREAMING_GLOBAL_PANES".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(state, 3, "Prism global panes failed: {messages:?}");
    assert_eq!(read_rows(&path), EXPECTED_PANES);
}

#[tokio::test]
async fn prism_runs_accumulating_global_combine_panes() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("portable-global-accum-panes");
    let pipeline = Pipeline::default();
    streaming_global_combine_panes_fixture(
        &pipeline,
        path.clone(),
        StreamingGlobalCombineRoute::All,
        true,
    )
    .unwrap();
    let (state, messages) = run_shared_fixture(
        &endpoint,
        &pipeline,
        "streaming-global-accum-panes",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_STREAMING_GLOBAL_ACCUM_PANES".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        state, 3,
        "Prism accumulating global panes failed: {messages:?}"
    );
    assert_eq!(read_rows(&path), EXPECTED_ACCUM_PANES);
}

const EXPECTED_LATE: [&str; 4] = [
    "count:Late:1:4:1",
    "count:OnTime:0:1:2",
    "sum:Late:1:4:3",
    "sum:OnTime:0:1:3",
];
const EXPECTED_ACCUM_LATE: [&str; 4] = [
    "count:Late:1:1:3",
    "count:OnTime:0:1:2",
    "sum:Late:1:1:6",
    "sum:OnTime:0:1:3",
];

#[test]
fn direct_runs_late_global_combine_panes() {
    let directory = tempfile::tempdir().unwrap();
    for (accumulating, expected) in [(false, EXPECTED_LATE), (true, EXPECTED_ACCUM_LATE)] {
        let mut rows = Vec::new();
        for (name, route) in [
            ("count", LateGlobalCombineRoute::Count),
            ("sum", LateGlobalCombineRoute::Sum),
        ] {
            let path = directory.path().join(format!("{accumulating}-{name}"));
            let pipeline = Pipeline::default();
            let sink =
                streaming_late_global_combine_fixture(&pipeline, path.clone(), route, accumulating)
                    .unwrap();
            DirectRunner
                .start_stream_sink(&sink, &JobConfig::default())
                .unwrap()
                .wait()
                .unwrap();
            rows.extend(read_rows(&path));
        }
        rows.sort();
        assert_eq!(rows, expected);
    }
}

#[tokio::test]
async fn prism_runs_late_global_combine_panes() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("portable-global-late");
    let pipeline = Pipeline::default();
    streaming_late_global_combine_fixture(
        &pipeline,
        path.clone(),
        LateGlobalCombineRoute::All,
        false,
    )
    .unwrap();
    let (state, messages) = run_shared_fixture(
        &endpoint,
        &pipeline,
        "streaming-global-late",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_STREAMING_GLOBAL_LATE".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(state, 3, "Prism late global combines failed: {messages:?}");
    assert_eq!(read_rows(&path), EXPECTED_LATE);
}

#[tokio::test]
async fn prism_runs_accumulating_late_global_combine_panes() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("portable-global-accum-late");
    let pipeline = Pipeline::default();
    streaming_late_global_combine_fixture(
        &pipeline,
        path.clone(),
        LateGlobalCombineRoute::All,
        true,
    )
    .unwrap();
    let (state, messages) = run_shared_fixture(
        &endpoint,
        &pipeline,
        "streaming-global-accum-late",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_STREAMING_GLOBAL_ACCUM_LATE".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        state, 3,
        "Prism accumulating late global combines failed: {messages:?}"
    );
    assert_eq!(read_rows(&path), EXPECTED_ACCUM_LATE);
}
