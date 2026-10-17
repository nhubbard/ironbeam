use super::*;
use ironbeam_prism_proof::streaming_global_count_per_element_fixture;

const EXPECTED: [&str; 3] = [
    "a:Early:0:1000000:2",
    "a:OnTime:1:11000000:1",
    "b:OnTime:0:3000000:1",
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
fn direct_counts_repeated_elements_in_global_trigger_panes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("direct-global-counts");
    let pipeline = Pipeline::default();
    let sink = streaming_global_count_per_element_fixture(&pipeline, path.clone()).unwrap();
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(read_rows(&path), EXPECTED);
}

#[tokio::test]
async fn prism_counts_repeated_elements_in_global_trigger_panes() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("portable-global-counts");
    let pipeline = Pipeline::default();
    streaming_global_count_per_element_fixture(&pipeline, path.clone()).unwrap();
    let (state, messages) = run_shared_fixture(
        &endpoint,
        &pipeline,
        "global-count-per-element",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_GLOBAL_COUNT_PER_ELEMENT".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        state, 3,
        "Prism global count-per-element failed: {messages:?}"
    );
    assert_eq!(read_rows(&path), EXPECTED);
}
