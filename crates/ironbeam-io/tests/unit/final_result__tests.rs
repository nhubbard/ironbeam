// Unit tests for src/final_result.rs.
use super::*;
use std::process::Command;

#[test]
fn final_result_crash_child() {
    let Ok(directory) = std::env::var("IRONBEAM_FINAL_RESULT_CHILD_DIRECTORY") else {
        return;
    };
    let stage = std::env::var("IRONBEAM_FINAL_RESULT_EXIT_STAGE").unwrap();
    let root = Path::new(&directory);
    let store = LocalFinalResult::new(root.join("stage"), "job", "graph", "sink").unwrap();
    match stage.as_str() {
        "fragment_staged" => store.stage("bundle", 1, b"row").unwrap(),
        "fragment_linked" => store.accept("bundle").unwrap(),
        "result_persisted" => {
            store
                .require_bundle_attempts(["bundle"])
                .unwrap()
                .publish_concatenated(1, root.join("result.bin"))
                .unwrap();
        }
        _ => panic!("unexpected final-result crash stage"),
    }
    panic!("final-result child did not exit at its fault stage");
}

#[test]
fn final_result_recovers_from_process_exit_at_publication_boundaries() {
    for stage in ["fragment_staged", "fragment_linked", "result_persisted"] {
        for lose_unsynced_entry in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let root = directory.path();
            let store = LocalFinalResult::new(root.join("stage"), "job", "graph", "sink").unwrap();
            if stage != "fragment_staged" {
                store.stage("bundle", 1, b"row").unwrap();
            }
            if stage == "result_persisted" {
                store.accept("bundle").unwrap();
            }
            let result = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "final_result::tests::final_result_crash_child"])
                .env("IRONBEAM_FINAL_RESULT_CHILD_DIRECTORY", root)
                .env("IRONBEAM_FINAL_RESULT_EXIT_STAGE", stage)
                .output()
                .unwrap();
            assert_eq!(result.status.code(), Some(86), "stage {stage}: {result:?}");
            if lose_unsynced_entry {
                let unsynced = match stage {
                    "fragment_staged" => store.path("bundle", "pending").unwrap(),
                    "fragment_linked" => store.path("bundle", "accepted").unwrap(),
                    "result_persisted" => root.join("result.bin"),
                    _ => unreachable!(),
                };
                fs::remove_file(unsynced).unwrap();
            }

            let reopened =
                LocalFinalResult::new(root.join("stage"), "job", "graph", "sink").unwrap();
            if stage == "fragment_staged" {
                reopened.stage("bundle", 1, b"row").unwrap();
            }
            reopened.accept("bundle").unwrap();
            let published = reopened
                .require_bundle_attempts(["bundle"])
                .unwrap()
                .publish_concatenated(1, root.join("result.bin"))
                .unwrap();
            assert_eq!(published.rows, 1);
            assert_eq!(fs::read(root.join("result.bin")).unwrap(), b"row");
        }
    }
}

#[test]
fn accepted_fragments_publish_once_after_exact_row_accounting() {
    let temp = tempfile::tempdir().unwrap();
    let store =
        LocalFinalResult::new(temp.path().join("stage"), "job-1", "graph-1", "sink-1").unwrap();
    let output = temp.path().join("result.bin");
    store.stage("bundle-a", 1, b"alpha").unwrap();
    assert!(store.publish_concatenated(1, &output).is_err());
    assert!(!output.exists());
    store.stage("bundle-b", 2, b"beta").unwrap();
    store.accept("bundle-a").unwrap();
    store.accept("bundle-b").unwrap();
    store.accept("bundle-a").unwrap();
    assert!(store.publish_concatenated(3, &output).is_err());
    assert!(!output.exists());
    let bound = store
        .require_bundle_attempts(["bundle-a", "bundle-b"])
        .unwrap();
    assert!(bound.publish_concatenated(4, &output).is_err());
    assert!(!output.exists());
    let fragments = store.accepted_fragments().unwrap();
    assert_eq!(
        fragments
            .iter()
            .map(AcceptedFragment::bundle_digest_hex)
            .collect::<std::collections::BTreeSet<_>>(),
        ["bundle-a", "bundle-b"]
            .into_iter()
            .map(|id| hex(&Sha256::digest(id.as_bytes())))
            .collect()
    );
    let mut expected = Vec::new();
    for fragment in &fragments {
        fragment.copy_payload(&mut expected).unwrap();
    }
    let published = bound.publish_concatenated(3, &output).unwrap();
    assert_eq!(published.rows, 3);
    assert_eq!(published.fragments, 2);
    assert_eq!(published.bytes, expected.len() as u64);
    assert_eq!(fs::read(&output).unwrap(), expected);
    assert_eq!(bound.publish_concatenated(3, &output).unwrap(), published);
    fs::write(&output, b"short").unwrap();
    assert!(bound.publish_concatenated(3, &output).is_err());
    fs::write(&output, vec![b'X'; expected.len()]).unwrap();
    assert!(bound.publish_concatenated(3, &output).is_err());
}

#[test]
fn attempts_are_immutable_and_scoped_to_one_job_graph_and_sink() {
    let temp = tempfile::tempdir().unwrap();
    let stage = temp.path().join("stage");
    let store = LocalFinalResult::new(&stage, "job-1", "graph-1", "sink-1").unwrap();
    store.stage("bundle-a", 1, b"payload").unwrap();
    assert!(store.stage("bundle-a", 1, b"other").is_err());
    let pending = store.path("bundle-a", "pending").unwrap();
    assert!(pending.exists());
    store.accept("bundle-a").unwrap();
    assert!(store.stage("bundle-a", 1, b"other").is_err());
    let other = LocalFinalResult::new(&stage, "job-2", "graph-1", "sink-1").unwrap();
    assert!(other.accepted_fragments().is_err());
    let another_graph = LocalFinalResult::new(&stage, "job-1", "graph-2", "sink-1").unwrap();
    assert!(another_graph.accepted_fragments().is_err());
    let another_sink = LocalFinalResult::new(&stage, "job-1", "graph-1", "sink-2").unwrap();
    assert!(another_sink.accepted_fragments().is_err());
}

#[test]
fn identical_pending_attempt_can_be_staged_again_after_restart() {
    let temp = tempfile::tempdir().unwrap();
    let stage = temp.path().join("stage");
    let store = LocalFinalResult::new(&stage, "job", "graph", "sink").unwrap();
    store.stage("bundle", 1, b"payload").unwrap();
    let pending = store.path("bundle", "pending").unwrap();
    let original = fs::read(&pending).unwrap();

    let reopened = LocalFinalResult::new(&stage, "job", "graph", "sink").unwrap();
    reopened.stage("bundle", 1, b"payload").unwrap();
    assert_eq!(fs::read(&pending).unwrap(), original);
    assert!(reopened.stage("bundle", 2, b"payload").is_err());
    assert!(reopened.stage("bundle", 1, b"different").is_err());

    fs::write(&pending, b"corrupt").unwrap();
    assert!(reopened.stage("bundle", 1, b"payload").is_err());
    assert_eq!(fs::read(&pending).unwrap(), b"corrupt");
}

#[test]
fn nested_staging_and_output_directories_publish_after_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let staging = temp.path().join("new").join("nested").join("stage");
    let output = temp.path().join("result").join("nested").join("rows.bin");
    let store = LocalFinalResult::new(&staging, "job", "graph", "sink").unwrap();
    store.stage("bundle", 1, b"row").unwrap();
    let reopened = LocalFinalResult::new(&staging, "job", "graph", "sink").unwrap();
    reopened.accept("bundle").unwrap();
    let bound = reopened.require_bundle_attempts(["bundle"]).unwrap();
    assert_eq!(bound.publish_concatenated(1, &output).unwrap().bytes, 3);
    assert_eq!(bound.publish_concatenated(1, &output).unwrap().bytes, 3);
    assert_eq!(fs::read(output).unwrap(), b"row");
}

#[test]
fn corrupt_or_abandoned_attempts_cannot_publish() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalFinalResult::new(temp.path().join("stage"), "job", "graph", "sink").unwrap();
    store.abandon("never-staged").unwrap();
    store.stage("abandoned", 1, b"x").unwrap();
    store.abandon("abandoned").unwrap();
    store.abandon("abandoned").unwrap();
    assert!(store.accepted_fragments().unwrap().is_empty());
    store.stage("accepted", 1, b"payload").unwrap();
    store.accept("accepted").unwrap();
    let path = store.path("accepted", "accepted").unwrap();
    let mut file = fs::OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(HEADER_LEN)).unwrap();
    file.write_all(b"X").unwrap();
    assert!(store.accepted_fragments().is_err());
    assert!(!temp.path().join("result.bin").exists());
}

#[test]
fn invalid_identity_and_format_are_rejected_before_publication() {
    let temp = tempfile::tempdir().unwrap();
    assert!(LocalFinalResult::new(temp.path(), "", "graph", "sink").is_err());
    assert!(LocalFinalResult::new(temp.path(), "job", "", "sink").is_err());
    assert!(LocalFinalResult::new(temp.path(), "job", "graph", "").is_err());
    let store = LocalFinalResult::new(temp.path().join("stage"), "job", "graph", "sink").unwrap();
    assert!(store.stage("", 1, b"row").is_err());
    store.stage("bundle", 1, b"bad framing").unwrap();
    store.accept("bundle").unwrap();
    let output = temp.path().join("result.bin");
    assert!(
        store
            .publish_validated_concatenated(1, &output, |_| { Err(invalid("invalid row framing")) })
            .is_err()
    );
    assert!(!output.exists());
    let accepted = store.path("bundle", "accepted").unwrap();
    let original = fs::read(&accepted).unwrap();
    assert!(
        store
            .read_fragment(&accepted, Some("another-bundle"))
            .is_err()
    );
    let mut wrong_magic = original.clone();
    wrong_magic[0] = b'X';
    fs::write(&accepted, &wrong_magic).unwrap();
    assert!(store.accepted_fragments().is_err());
    fs::write(&accepted, &original).unwrap();
    let mut wrong_size = original.clone();
    wrong_size[80..88].copy_from_slice(&1_u64.to_le_bytes());
    fs::write(&accepted, &wrong_size).unwrap();
    assert!(store.accepted_fragments().is_err());
    wrong_size[80..88].copy_from_slice(&u64::MAX.to_le_bytes());
    fs::write(&accepted, &wrong_size).unwrap();
    assert!(store.accepted_fragments().is_err());
    fs::write(&accepted, &original).unwrap();
    let fragment = store.accepted_fragments().unwrap().remove(0);
    fs::write(&accepted, &original[..original.len() - 1]).unwrap();
    assert!(fragment.copy_payload(&mut Vec::new()).is_err());
    fs::write(&accepted, &original).unwrap();
    fs::write(store.staging.join("unrelated"), b"ignored").unwrap();
    assert_eq!(store.accepted_fragments().unwrap().len(), 1);
    let renamed = accepted.with_file_name("incorrect.accepted");
    fs::rename(&accepted, &renamed).unwrap();
    assert!(store.accepted_fragments().is_err());
}

#[test]
fn interrupted_acceptance_resumes_only_matching_attempt() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalFinalResult::new(temp.path().join("stage"), "job", "graph", "sink").unwrap();
    store.stage("bundle", 1, b"row").unwrap();
    let pending = store.path("bundle", "pending").unwrap();
    let accepted = store.path("bundle", "accepted").unwrap();
    fs::hard_link(&pending, &accepted).unwrap();
    let reopened = LocalFinalResult::new(&store.staging, "job", "graph", "sink").unwrap();
    reopened.accept("bundle").unwrap();
    assert!(!pending.exists());
    assert_eq!(reopened.accepted_fragments().unwrap().len(), 1);
    assert_eq!(
        reopened
            .require_bundle_attempts(["bundle"])
            .unwrap()
            .publish_concatenated(1, temp.path().join("result.bin"))
            .unwrap()
            .rows,
        1
    );
    reopened.accept("bundle").unwrap();

    let other = LocalFinalResult::new(temp.path().join("other"), "job", "graph", "sink").unwrap();
    other.stage("bundle", 1, b"different").unwrap();
    let different = other.path("bundle", "pending").unwrap();
    fs::copy(&different, &pending).unwrap();
    assert!(reopened.accept("bundle").is_err());
    assert!(pending.exists());
    assert!(accepted.exists());
    fs::remove_file(&pending).unwrap();
    let directory = store.path("directory-bundle", "pending").unwrap();
    fs::create_dir(&directory).unwrap();
    assert!(store.abandon("directory-bundle").is_err());
}

#[test]
fn streamed_fragments_cross_buffer_boundaries_and_fail_atomically() {
    struct FailingReader {
        first: bool,
    }
    impl Read for FailingReader {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            if !self.first {
                return Err(std::io::Error::other("source failed"));
            }
            self.first = false;
            buffer[0] = b'X';
            Ok(1)
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let store = LocalFinalResult::new(temp.path().join("stage"), "job", "graph", "sink").unwrap();
    assert!(
        store
            .stage_reader("failed", 1, FailingReader { first: true })
            .is_err()
    );
    assert!(!store.path("failed", "pending").unwrap().exists());
    let payload = vec![b'A'; 150_000];
    store
        .stage_reader("large", 1, std::io::Cursor::new(&payload))
        .unwrap();
    store.accept("large").unwrap();
    let output = temp.path().join("large.bin");
    assert_eq!(
        store
            .require_bundle_attempts(["large"])
            .unwrap()
            .publish_concatenated(1, &output)
            .unwrap()
            .bytes,
        payload.len() as u64
    );
    assert_eq!(fs::read(output).unwrap(), payload);
}

#[test]
fn matching_row_count_cannot_hide_pending_or_orphaned_fragments() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalFinalResult::new(temp.path().join("stage"), "job", "graph", "sink").unwrap();
    store.stage("accepted", 1, b"one").unwrap();
    store.accept("accepted").unwrap();
    store.stage("pending-empty", 0, b"").unwrap();
    let output = temp.path().join("result.bin");
    let bound = store.require_bundle_attempts(["accepted"]).unwrap();
    assert!(bound.publish_concatenated(1, &output).is_err());
    assert!(!output.exists());
    store.abandon("pending-empty").unwrap();
    let orphan = store.staging.join("orphaned-tempfile");
    fs::write(&orphan, b"incomplete").unwrap();
    assert!(bound.publish_concatenated(1, &output).is_err());
    fs::remove_file(orphan).unwrap();
    assert_eq!(bound.publish_concatenated(1, &output).unwrap().rows, 1);
}

#[test]
fn runner_manifest_rejects_missing_extra_and_duplicate_attempts() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalFinalResult::new(temp.path().join("stage"), "job", "graph", "sink").unwrap();
    let output = temp.path().join("result.bin");
    store.stage("bundle-a", 1, b"a").unwrap();
    store.accept("bundle-a").unwrap();
    assert!(store.require_bundle_attempts(["", "bundle-a"]).is_err());
    assert!(
        store
            .require_bundle_attempts(["bundle-a", "bundle-a"])
            .is_err()
    );
    assert!(
        store
            .require_bundle_attempts(["bundle-b"])
            .unwrap()
            .publish_concatenated(1, &output)
            .is_err()
    );
    assert!(
        store
            .require_bundle_attempts(["bundle-a", "bundle-b"])
            .unwrap()
            .publish_concatenated(1, &output)
            .is_err()
    );
    assert!(!output.exists());
    let bound = store.require_bundle_attempts(["bundle-a"]).unwrap();
    assert_eq!(bound.publish_concatenated(1, &output).unwrap().rows, 1);
    store.stage("bundle-b", 0, b"").unwrap();
    store.accept("bundle-b").unwrap();
    assert!(bound.publish_concatenated(1, &output).is_err());
}

#[test]
fn merged_publication_checks_decoded_rows_before_visibility() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalFinalResult::new(temp.path().join("stage"), "job", "graph", "sink").unwrap();
    store.stage("bundle", 2, b"one").unwrap();
    store.accept("bundle").unwrap();
    let store = store.require_bundle_attempts(["bundle"]).unwrap();
    let output = temp.path().join("merged.bin");
    assert!(
        store
            .publish_merged(2, &output, |parts, _, file| {
                for part in parts {
                    part.copy_payload(file)?;
                }
                Ok(1)
            })
            .is_err()
    );
    assert!(!output.exists());
    assert_eq!(
        store
            .publish_merged(2, &output, |parts, _, file| {
                for part in parts {
                    part.copy_payload(file)?;
                }
                Ok(2)
            })
            .unwrap()
            .bytes,
        3
    );
}
