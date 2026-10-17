use super::*;
use ironbeam_prism_proof::streaming_global_distinct_count_per_key_fixture;

const EXPECTED: [&str; 2] = ["k:1:Early:0", "k:2:Early:1"];

fn read_rows(path: &Path) -> Vec<String> {
    let mut rows = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

#[test]
fn direct_counts_unique_values_in_each_global_pane() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("direct-global-distinct-count");
    let pipeline = Pipeline::default();
    let sink = streaming_global_distinct_count_per_key_fixture(&pipeline, path.clone()).unwrap();
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(read_rows(&path), EXPECTED);
}

#[tokio::test]
async fn prism_counts_unique_values_in_each_global_pane() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("portable-global-distinct-count");
    let pipeline = Pipeline::default();
    streaming_global_distinct_count_per_key_fixture(&pipeline, path.clone()).unwrap();
    let (state, messages) = run_shared_fixture(
        &endpoint,
        &pipeline,
        "global-distinct-count-per-key",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_GLOBAL_DISTINCT_COUNT_PER_KEY".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(state, 3, "Prism global distinct count failed: {messages:?}");
    assert_eq!(read_rows(&path), EXPECTED);
}
