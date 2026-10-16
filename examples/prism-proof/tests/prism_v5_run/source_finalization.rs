use super::*;
use ironbeam_prism_proof::{finalization_probe_store, source_finalization_fixture};

#[tokio::test]
async fn prism_finalizes_each_replayable_source_bundle() {
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
    assert_eq!(
        records
            .values()
            .filter(|record| !record.sources.is_empty())
            .count(),
        2,
        "finalization journal: {records:?}"
    );
    assert!(
        records
            .values()
            .any(|record| !record.sinks.is_empty() && record.completed),
        "finalization journal has no completed sink: {records:?}"
    );
    let fragments = finalization_probe_store(directory.path().join("sink"), "source-ack-sink")
        .unwrap()
        .accepted_fragments()
        .unwrap();
    assert_eq!(fragments.len(), 2);
    assert_eq!(
        fragments.iter().map(|fragment| fragment.rows).sum::<u64>(),
        2
    );
    for value in [7, 8] {
        let acknowledgment = std::fs::read(
            directory
                .path()
                .join(format!("source-acknowledged-{value}")),
        );
        assert!(
            matches!(&acknowledgment, Ok(bytes) if bytes.as_slice() == [value]),
            "acknowledgment {acknowledgment:?}; finalization journal after Prism completion: {records:?}"
        );
        assert!(records.values().any(|record| {
            record.completed
                && record.sinks.is_empty()
                && record.sources.len() == 1
                && record.sources[0].checkpoint_position.as_deref() == Some(&[value][..])
        }));
    }
}
