use super::*;
use ironbeam_prism_proof::streaming_global_ordered_keyed_fixture;

const EXPECTED: [&str; 3] = [
    "1:Early:0:2000000:23",
    "1:OnTime:1:4000000:4",
    "2:OnTime:0:3000000:9",
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
fn direct_folds_keys_in_explicit_order_in_global_trigger_panes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("direct-global-ordered");
    let pipeline = Pipeline::default();
    let sink = streaming_global_ordered_keyed_fixture(&pipeline, path.clone()).unwrap();
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(read_rows(&path), EXPECTED);
}

#[tokio::test]
async fn prism_folds_keys_in_explicit_order_in_global_trigger_panes() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("portable-global-ordered");
    let pipeline = Pipeline::default();
    streaming_global_ordered_keyed_fixture(&pipeline, path.clone()).unwrap();
    let (state, messages) = run_shared_fixture(
        &endpoint,
        &pipeline,
        "global-ordered-keyed",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_ORDERED_GLOBAL_STREAM".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        state, 3,
        "Prism global ordered keyed fold failed: {messages:?}"
    );
    assert_eq!(read_rows(&path), EXPECTED);
}
