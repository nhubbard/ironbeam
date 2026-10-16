use super::*;
use ironbeam_prism_proof::source_finalization_fixture;

#[tokio::test]
async fn prism_finalizes_a_replayable_source_bundle() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    source_finalization_fixture(&pipeline, directory.path()).unwrap();
    let journal_dir = directory.path().join("journal");
    let (state, messages) = run_shared_fixture(
        &endpoint,
        &pipeline,
        "source-finalization",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_SOURCE_FINALIZATION".into(),
                directory.path().to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_FINALIZATION_JOURNAL_DIR".into(),
                journal_dir.to_string_lossy().into_owned(),
            ),
        ]),
    )
    .await;
    assert_eq!(state, 3, "Prism job failed: {messages:?}");
    let identity = prepare_v5(&pipeline, "local-process", "prism-run-fixture")
        .unwrap()
        .identity;
    let records = FileFinalizationJournal::new(journal_dir, identity)
        .unwrap()
        .snapshot()
        .unwrap();
    let acknowledgment = std::fs::read(directory.path().join("source-acknowledged"));
    assert!(
        matches!(&acknowledgment, Ok(bytes) if bytes == b"7"),
        "acknowledgment {acknowledgment:?}; finalization journal after Prism completion: {records:?}"
    );
    assert_eq!(records.len(), 1);
    let record = records.values().next().unwrap();
    assert!(record.completed);
    assert!(record.sinks.is_empty());
    assert_eq!(
        record.sources[0].checkpoint_position.as_deref(),
        Some(&[7][..])
    );
}
