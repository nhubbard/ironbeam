use super::*;
use ironbeam_prism_proof::streaming_distinct_count_per_key_fixture;

const EXPECTED: [&str; 3] = ["0:k:2", "0:q:1", "10000000:k:1"];

fn read_rows(path: &Path) -> Vec<String> {
    let mut rows = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

#[test]
fn direct_counts_unique_keyed_values_in_two_streaming_windows() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("direct-distinct-count");
    let pipeline = Pipeline::default();
    let sink = streaming_distinct_count_per_key_fixture(&pipeline, path.clone()).unwrap();
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(read_rows(&path), EXPECTED);
}

#[tokio::test]
async fn prism_counts_unique_keyed_values_in_two_streaming_windows() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("portable-distinct-count");
    let pipeline = Pipeline::default();
    streaming_distinct_count_per_key_fixture(&pipeline, path.clone()).unwrap();
    let (state, messages) = run_shared_fixture(
        &endpoint,
        &pipeline,
        "streaming-distinct-count-per-key",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_STREAM_DISTINCT_COUNT_PER_KEY".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(state, 3, "Prism distinct count failed: {messages:?}");
    assert_eq!(read_rows(&path), EXPECTED);
}
