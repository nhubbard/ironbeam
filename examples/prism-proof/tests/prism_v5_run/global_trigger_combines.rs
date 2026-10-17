use super::*;
use ironbeam_prism_proof::{streaming_global_trigger_fixture, GlobalTriggerRoute};

const EXPECTED: [&str; 8] = [
    "count:Early:0:2:2",
    "count:OnTime:1:4:1",
    "ordered:Early:0:2:[2, 3]",
    "ordered:OnTime:1:4:[4]",
    "sum:Early:0:2:5",
    "sum:OnTime:1:4:4",
    "vector:Early:0:2:[2, 3]",
    "vector:OnTime:1:4:[4]",
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
fn direct_runs_unbounded_global_combine_trigger() {
    let directory = tempfile::tempdir().unwrap();
    let mut rows = Vec::new();
    for (name, route) in [
        ("vector", GlobalTriggerRoute::Vector),
        ("count", GlobalTriggerRoute::Count),
        ("sum", GlobalTriggerRoute::Sum),
        ("ordered", GlobalTriggerRoute::Ordered),
    ] {
        let path = directory.path().join(name);
        let pipeline = Pipeline::default();
        let sink = streaming_global_trigger_fixture(&pipeline, path.clone(), route).unwrap();
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
async fn prism_runs_unbounded_global_combine_trigger() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("portable-global-trigger");
    let pipeline = Pipeline::default();
    streaming_global_trigger_fixture(&pipeline, path.clone(), GlobalTriggerRoute::All).unwrap();
    let (state, messages) = run_shared_fixture(
        &endpoint,
        &pipeline,
        "global-trigger-combines",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_GLOBAL_TRIGGER_COMBINES".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        state, 3,
        "Prism global trigger combines failed: {messages:?}"
    );
    assert_eq!(read_rows(&path), EXPECTED);
}
