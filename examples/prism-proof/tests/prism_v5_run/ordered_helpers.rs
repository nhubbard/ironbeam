use super::*;
use ironbeam_prism_proof::{
    ordered_helpers_fixture, streaming_ordered_helpers_fixture, StreamingOrderedHelperRoute,
};

const STREAMING_ORDERED_ROWS: [&str; 16] = [
    "distinct:0:1:10",
    "distinct:0:3:20",
    "distinct:10:4:10",
    "distinct:10:6:30",
    "first:0:1:10",
    "first:10:4:10",
    "list:0:0:1:10",
    "list:0:1:2:10",
    "list:0:2:3:20",
    "list:10:0:4:10",
    "list:10:1:5:10",
    "list:10:2:6:30",
    "take:0:1:10",
    "take:0:2:10",
    "take:10:4:10",
    "take:10:5:10",
];

fn read_row_directory(path: &Path) -> Vec<String> {
    let mut rows = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

#[tokio::test]
async fn prism_matches_direct_explicit_order_helpers() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-ordered-helpers");
    let portable_path = directory.path().join("portable-ordered-helpers");
    let direct = Pipeline::default();
    let sink = ordered_helpers_fixture(&direct, direct_path.clone()).unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    let portable = Pipeline::default();
    ordered_helpers_fixture(&portable, portable_path.clone()).unwrap();
    let (state, messages) = run_shared_fixture(
        &endpoint,
        &portable,
        "ordered-helpers",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_ORDERED_HELPERS".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(state, 3, "Prism ordered helpers failed: {messages:?}");
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let expected = [
        "distinct:1:10",
        "distinct:2:30",
        "distinct:3:20",
        "first:1:10",
        "list:0:1:10",
        "list:1:2:30",
        "list:2:3:20",
        "list:3:4:10",
        "take:1:10",
        "take:2:30",
    ];
    assert_eq!(read_rows(&direct_path), expected);
    assert_eq!(read_rows(&portable_path), expected);
}

#[test]
fn direct_runs_windowed_streaming_order_helpers() {
    let directory = tempfile::tempdir().unwrap();
    let mut rows = Vec::new();
    for (name, route) in [
        ("list", StreamingOrderedHelperRoute::List),
        ("take", StreamingOrderedHelperRoute::Take),
        ("first", StreamingOrderedHelperRoute::First),
        ("distinct", StreamingOrderedHelperRoute::Distinct),
    ] {
        let path = directory.path().join(name);
        let pipeline = Pipeline::default();
        let sink = streaming_ordered_helpers_fixture(&pipeline, path.clone(), route).unwrap();
        DirectRunner
            .start_stream_sink(&sink, &JobConfig::default())
            .unwrap()
            .wait()
            .unwrap();
        rows.extend(read_row_directory(&path));
    }
    rows.sort();
    assert_eq!(rows, STREAMING_ORDERED_ROWS);
}

#[tokio::test]
async fn prism_runs_windowed_streaming_order_helpers() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let portable_path = directory.path().join("portable-windowed-helpers");
    let portable = Pipeline::default();
    streaming_ordered_helpers_fixture(
        &portable,
        portable_path.clone(),
        StreamingOrderedHelperRoute::All,
    )
    .unwrap();
    let (state, messages) = run_shared_fixture(
        &endpoint,
        &portable,
        "streaming-ordered-helpers",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_STREAMING_ORDERED_HELPERS".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        state, 3,
        "Prism streaming ordered helpers failed: {messages:?}"
    );
    assert_eq!(read_row_directory(&portable_path), STREAMING_ORDERED_ROWS);
}
