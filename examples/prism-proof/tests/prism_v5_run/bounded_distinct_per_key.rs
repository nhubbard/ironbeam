use super::*;
use ironbeam_prism_proof::bounded_distinct_per_key_fixture;

const EXPECTED: [&str; 3] = ["k:a", "k:b", "q:a"];

fn read_rows(path: &Path) -> Vec<String> {
    let mut rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

#[test]
fn direct_keeps_one_value_per_key_on_bounded_input() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("direct-keyed-distinct");
    let pipeline = Pipeline::default();
    let sink = bounded_distinct_per_key_fixture(&pipeline, path.clone()).unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    assert_eq!(read_rows(&path), EXPECTED);
}

#[tokio::test]
async fn prism_keeps_one_value_per_key_on_bounded_input() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("portable-keyed-distinct");
    let pipeline = Pipeline::default();
    bounded_distinct_per_key_fixture(&pipeline, path.clone()).unwrap();
    let (state, messages) = run_shared_fixture(
        &endpoint,
        &pipeline,
        "bounded-distinct-per-key",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_BOUNDED_DISTINCT_PER_KEY".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        state, 3,
        "Prism bounded keyed distinct failed: {messages:?}"
    );
    assert_eq!(read_rows(&path), EXPECTED);
}
