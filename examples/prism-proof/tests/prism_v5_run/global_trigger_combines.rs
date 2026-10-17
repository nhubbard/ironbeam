use super::*;
use ironbeam_prism_proof::{
    streaming_global_trigger_fixture, GlobalTriggerPolicy, GlobalTriggerRoute,
};

const EXPECTED: [&str; 10] = [
    "count:Early:0:2:2",
    "count:OnTime:1:4:1",
    "latest:Early:0:3:3",
    "latest:OnTime:1:4:4",
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
    direct_runs_global_trigger(GlobalTriggerPolicy::Count);
}

#[test]
fn direct_runs_unbounded_global_processing_time_trigger() {
    direct_runs_global_trigger(GlobalTriggerPolicy::ProcessingTime);
}

fn direct_runs_global_trigger(trigger: GlobalTriggerPolicy) {
    let directory = tempfile::tempdir().unwrap();
    let mut rows = Vec::new();
    for (name, route) in [
        ("vector", GlobalTriggerRoute::Vector),
        ("count", GlobalTriggerRoute::Count),
        ("sum", GlobalTriggerRoute::Sum),
        ("ordered", GlobalTriggerRoute::Ordered),
        ("latest", GlobalTriggerRoute::Latest),
    ] {
        let path = directory.path().join(name);
        let pipeline = Pipeline::default();
        let sink =
            streaming_global_trigger_fixture(&pipeline, path.clone(), route, trigger).unwrap();
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
    prism_runs_global_trigger(
        "IRONBEAM_V5_PRISM_GLOBAL_TRIGGER_COMBINES",
        GlobalTriggerPolicy::Count,
    )
    .await;
}

#[tokio::test]
#[ignore = "pinned Prism panics in aggregateStageKind.buildProcessingTimeBundle for the global window"]
async fn prism_runs_unbounded_global_processing_time_trigger() {
    prism_runs_global_trigger(
        "IRONBEAM_V5_PRISM_GLOBAL_TIME_TRIGGER_COMBINES",
        GlobalTriggerPolicy::ProcessingTime,
    )
    .await;
}

async fn prism_runs_global_trigger(env: &str, trigger: GlobalTriggerPolicy) {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("portable-global-trigger");
    let pipeline = Pipeline::default();
    streaming_global_trigger_fixture(&pipeline, path.clone(), GlobalTriggerRoute::All, trigger)
        .unwrap();
    let (state, messages) = run_shared_fixture(
        &endpoint,
        &pipeline,
        "global-trigger-combines",
        HashMap::from([(env.into(), path.to_string_lossy().into_owned())]),
    )
    .await;
    assert_eq!(
        state, 3,
        "Prism global trigger combines failed: {messages:?}"
    );
    assert_eq!(read_rows(&path), EXPECTED);
}
