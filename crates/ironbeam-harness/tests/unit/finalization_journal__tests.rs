// Unit tests for src/finalization_journal.rs.
use super::*;
use ironbeam_cloud_objects::LocalObjectIO;
use std::process::Command;

fn identity(graph: &str) -> ExecutionIdentity {
    ExecutionIdentity {
        graph_fingerprint: graph.into(),
        binary_build_id: "build".into(),
        registry_manifest: "registry".into(),
        coder_manifest: "coder".into(),
        source_config_digest: "source".into(),
        sink_config_digest: "sink".into(),
    }
}

#[test]
fn journal_crash_child() {
    let Ok(directory) = std::env::var("IRONBEAM_FINALIZATION_CHILD_DIRECTORY") else {
        return;
    };
    let stage = std::env::var("IRONBEAM_FINALIZATION_EXIT_STAGE").unwrap();
    let journal = FileFinalizationJournal::new(directory, identity("graph")).unwrap();
    match stage.as_str() {
        "journal_record_named" => journal.record("bundle", &["sink".into()]).unwrap(),
        "journal_complete_linked" => journal.complete("bundle").unwrap(),
        _ => panic!("unexpected journal crash stage"),
    }
    panic!("journal child did not exit at its fault stage");
}

#[test]
fn journal_recovers_after_process_exit_and_unsynced_entry_loss() {
    for stage in ["journal_record_named", "journal_complete_linked"] {
        for lose_unsynced_entry in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let journal =
                FileFinalizationJournal::new(directory.path(), identity("graph")).unwrap();
            if stage == "journal_complete_linked" {
                journal.record("bundle", &["sink".into()]).unwrap();
            }
            let result = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "finalization_journal::tests::journal_crash_child",
                ])
                .env("IRONBEAM_FINALIZATION_CHILD_DIRECTORY", directory.path())
                .env("IRONBEAM_FINALIZATION_EXIT_STAGE", stage)
                .output()
                .unwrap();
            assert_eq!(result.status.code(), Some(86), "stage {stage}: {result:?}");
            if lose_unsynced_entry {
                let (record, complete) = journal.paths("bundle").unwrap();
                fs::remove_file(if stage == "journal_record_named" {
                    record
                } else {
                    complete
                })
                .unwrap();
            }
            let reopened =
                FileFinalizationJournal::new(directory.path(), identity("graph")).unwrap();
            reopened.record("bundle", &["sink".into()]).unwrap();
            reopened.complete("bundle").unwrap();
            assert_eq!(
                reopened
                    .completed_manifest(["sink"])
                    .unwrap()
                    .attempts_for_sink("sink")
                    .unwrap(),
                ["bundle"]
            );
        }
    }
}

#[test]
fn journal_reopens_exact_sink_set_and_completed_marker() {
    let dir = tempfile::tempdir().unwrap();
    assert!(FileFinalizationJournal::new("relative", identity("graph")).is_err());
    let journal = FileFinalizationJournal::new(dir.path(), identity("graph")).unwrap();
    assert!(journal.record("", &["sink".into()]).is_err());
    assert!(journal.record("bundle", &["z".into(), "a".into()]).is_err());
    assert!(journal.load("bundle").unwrap().is_none());
    journal.record("bundle", &["a".into(), "z".into()]).unwrap();
    let reopened = FileFinalizationJournal::new(dir.path(), identity("graph")).unwrap();
    reopened
        .record("bundle", &["a".into(), "z".into()])
        .unwrap();
    assert!(journal.completed_attempts_for_sink("a").is_err());
    assert_eq!(
        journal.load("bundle").unwrap(),
        Some(JournalEntry {
            sinks: vec!["a".into(), "z".into()],
            completed: false,
        })
    );
    assert!(journal.record("bundle", &["a".into(), "x".into()]).is_err());
    let wrong = FileFinalizationJournal::new(dir.path(), identity("other")).unwrap();
    assert!(wrong.load("bundle").is_err());
    journal.complete("bundle").unwrap();
    reopened.complete("bundle").unwrap();
    assert!(journal.load("bundle").unwrap().unwrap().completed);
    assert_eq!(journal.snapshot().unwrap().len(), 1);
    assert_eq!(journal.snapshot().unwrap()["bundle"].sinks, ["a", "z"]);
    assert_eq!(
        journal.completed_attempts_for_sink("a").unwrap(),
        ["bundle"]
    );
    assert!(journal
        .completed_attempts_for_sink("other")
        .unwrap()
        .is_empty());
    assert!(journal.completed_attempts_for_sink("").is_err());
}

#[test]
fn nested_journal_directory_recovers_completed_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("new").join("nested").join("journal");
    let journal = FileFinalizationJournal::new(&root, identity("graph")).unwrap();
    journal.record("bundle", &["sink".into()]).unwrap();
    let reopened = FileFinalizationJournal::new(&root, identity("graph")).unwrap();
    reopened.complete("bundle").unwrap();
    assert_eq!(
        reopened
            .completed_manifest(["sink"])
            .unwrap()
            .attempts_for_sink("sink")
            .unwrap(),
        ["bundle"]
    );
}

#[test]
fn whole_job_manifest_rejects_undeclared_and_incomplete_sinks() {
    let dir = tempfile::tempdir().unwrap();
    let journal = FileFinalizationJournal::new(dir.path(), identity("graph")).unwrap();
    assert!(journal.completed_manifest(Vec::<String>::new()).is_err());
    assert!(journal.completed_manifest(["a", "a"]).is_err());
    assert!(journal.completed_manifest(["a", ""]).is_err());
    journal.record("bundle-1", &["a".into()]).unwrap();
    assert!(journal.completed_manifest(["a", "b"]).is_err());
    journal.complete("bundle-1").unwrap();
    assert!(journal.completed_manifest(["b"]).is_err());
    let manifest = journal.completed_manifest(["a", "b"]).unwrap();
    assert_eq!(manifest.sinks().collect::<Vec<_>>(), ["a", "b"]);
    assert_eq!(manifest.attempts_for_sink("a").unwrap(), ["bundle-1"]);
    assert!(manifest.attempts_for_sink("b").unwrap().is_empty());
    assert!(manifest.attempts_for_sink("unknown").is_err());
    journal.record("bundle-2", &["b".into()]).unwrap();
    assert!(journal.completed_manifest(["a", "b"]).is_err());
    journal.complete("bundle-2").unwrap();
    let manifest = journal.completed_manifest(["a", "b"]).unwrap();
    assert_eq!(manifest.attempts_for_sink("b").unwrap(), ["bundle-2"]);
}

#[test]
fn snapshot_rejects_orphan_corrupt_and_foreign_entries() {
    let dir = tempfile::tempdir().unwrap();
    let journal = FileFinalizationJournal::new(dir.path(), identity("graph")).unwrap();
    assert!(journal.snapshot().unwrap().is_empty());
    journal.record("bundle", &["sink".into()]).unwrap();
    let (record, marker) = journal.paths("bundle").unwrap();
    let scratch = dir.path().join("orphan-tempfile");
    fs::write(&scratch, b"incomplete").unwrap();
    assert!(journal.snapshot().is_err());
    fs::remove_file(&scratch).unwrap();
    journal.complete("bundle").unwrap();
    assert!(journal.snapshot().unwrap()["bundle"].completed);
    fs::remove_file(&record).unwrap();
    assert!(journal.snapshot().is_err());
    fs::remove_file(&marker).unwrap();
    let foreign = FileFinalizationJournal::new(dir.path(), identity("other")).unwrap();
    foreign.record("bundle", &["sink".into()]).unwrap();
    assert!(journal.snapshot().is_err());
    assert_eq!(foreign.snapshot().unwrap().len(), 1);
    let wrong_name = dir.path().join("bundle-wrong.json");
    fs::rename(&record, &wrong_name).unwrap();
    assert!(foreign.snapshot().is_err());
}

#[test]
fn journal_rejects_corrupt_record_or_completion_marker() {
    let dir = tempfile::tempdir().unwrap();
    let journal = FileFinalizationJournal::new(dir.path(), identity("graph")).unwrap();
    journal.record("bundle", &["sink".into()]).unwrap();
    let (path, completed) = journal.paths("bundle").unwrap();
    fs::write(&path, b"invalid JSON").unwrap();
    assert!(journal.load("bundle").is_err());
    fs::remove_file(&path).unwrap();
    journal.record("bundle", &["sink".into()]).unwrap();
    journal.complete("bundle").unwrap();
    fs::remove_file(&completed).unwrap();
    fs::write(&completed, b"different").unwrap();
    assert!(journal.load("bundle").is_err());
}

#[test]
fn object_journal_reopens_exact_record_and_rejects_conflicts() {
    let dir = tempfile::tempdir().unwrap();
    let mut stores = ObjectStores::new();
    stores
        .register("file", LocalObjectIO::new(dir.path()).unwrap())
        .unwrap();
    let stores = Arc::new(stores);
    assert!(ObjectFinalizationJournal::new(
        Arc::clone(&stores),
        "file://bucket/jobs",
        identity("graph")
    )
    .is_err());
    let journal = ObjectFinalizationJournal::new(
        Arc::clone(&stores),
        "file://bucket/jobs/",
        identity("graph"),
    )
    .unwrap();
    assert!(journal.record("bundle", &["z".into(), "a".into()]).is_err());
    journal.record("bundle", &["a".into(), "z".into()]).unwrap();
    journal.record("bundle", &["a".into(), "z".into()]).unwrap();
    assert!(journal.record("bundle", &["a".into(), "x".into()]).is_err());
    assert_eq!(
        journal.load("bundle").unwrap(),
        Some(JournalEntry {
            sinks: vec!["a".into(), "z".into()],
            completed: false,
        })
    );
    let wrong = ObjectFinalizationJournal::new(
        Arc::clone(&stores),
        "file://bucket/jobs/",
        identity("other"),
    )
    .unwrap();
    assert!(wrong.load("bundle").is_err());
    journal.complete("bundle").unwrap();
    journal.complete("bundle").unwrap();
    assert!(journal.load("bundle").unwrap().unwrap().completed);
    let (_, marker) = journal.uris("bundle").unwrap();
    stores.write(&marker, b"different").unwrap();
    assert!(journal.load("bundle").is_err());
}

#[test]
fn object_journal_manifest_requires_complete_validated_attempts() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("bucket/jobs")).unwrap();
    let mut stores = ObjectStores::new();
    stores
        .register("file", LocalObjectIO::new(dir.path()).unwrap())
        .unwrap();
    let stores = Arc::new(stores);
    let journal = ObjectFinalizationJournal::new(
        Arc::clone(&stores),
        "file://bucket/jobs/",
        identity("graph"),
    )
    .unwrap();
    assert!(journal.snapshot().unwrap().is_empty());
    assert!(journal
        .completed_manifest(["a"])
        .unwrap()
        .attempts_for_sink("a")
        .unwrap()
        .is_empty());
    journal
        .record("bundle-1", &["a".into(), "b".into()])
        .unwrap();
    assert!(journal.completed_manifest(["a", "b"]).is_err());
    journal.complete("bundle-1").unwrap();
    journal.record("bundle-2", &["b".into()]).unwrap();
    journal.complete("bundle-2").unwrap();
    let manifest = journal.completed_manifest(["a", "b"]).unwrap();
    assert_eq!(manifest.attempts_for_sink("a").unwrap(), ["bundle-1"]);
    assert_eq!(
        manifest.attempts_for_sink("b").unwrap(),
        ["bundle-1", "bundle-2"]
    );
    assert_eq!(
        journal.completed_attempts_for_sink("b").unwrap(),
        ["bundle-1", "bundle-2"]
    );
    assert!(journal.completed_manifest(["a"]).is_err());
    assert!(journal.completed_attempts_for_sink("").is_err());

    let (record, marker) = journal.uris("bundle-1").unwrap();
    stores.write(&marker, b"corrupt").unwrap();
    assert!(journal.snapshot().is_err());
    stores
        .write(&marker, &stores.read(&record).unwrap())
        .unwrap();
    stores
        .write("file://bucket/jobs/scratch", b"unknown")
        .unwrap();
    assert!(journal.snapshot().is_err());
    stores.delete("file://bucket/jobs/scratch").unwrap();
    stores.delete(&record).unwrap();
    assert!(journal.snapshot().is_err());
}
