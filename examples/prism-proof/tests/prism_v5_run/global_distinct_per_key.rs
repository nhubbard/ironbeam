use super::*;
use ironbeam_prism_proof::streaming_global_distinct_per_key_fixture;

const EXPECTED: [&str; 3] = ["k:a:Early:0", "k:a:Early:1", "k:b:Early:1"];

fn read_rows(path: &Path) -> Vec<String> {
    let mut rows = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

#[test]
fn direct_deduplicates_keyed_values_within_each_global_pane() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("direct-global-distinct");
    let pipeline = Pipeline::default();
    let sink = streaming_global_distinct_per_key_fixture(&pipeline, path.clone()).unwrap();
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(read_rows(&path), EXPECTED);
}

#[tokio::test]
async fn prism_deduplicates_keyed_values_within_each_global_pane() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("portable-global-distinct");
    let pipeline = Pipeline::default();
    streaming_global_distinct_per_key_fixture(&pipeline, path.clone()).unwrap();
    let (state, messages) = run_shared_fixture(
        &endpoint,
        &pipeline,
        "global-distinct-per-key",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_GLOBAL_DISTINCT_PER_KEY".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(state, 3, "Prism global keyed distinct failed: {messages:?}");
    assert_eq!(read_rows(&path), EXPECTED);
}
