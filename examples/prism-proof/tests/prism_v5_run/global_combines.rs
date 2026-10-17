use super::*;
use ironbeam_prism_proof::{streaming_global_combine_fixture, StreamingGlobalCombineRoute};

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
