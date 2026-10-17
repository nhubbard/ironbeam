// Unit tests for src/lib.rs.
use super::*;

fn probe(directory: &Path) -> FinalizationProbe<i32> {
    FinalizationProbe {
        store: finalization_probe_store(directory, "probe").unwrap(),
        count: 0,
        payload: Vec::new(),
        bundle_id: None,
        staged: false,
        fail_after_stage: false,
        marker: std::marker::PhantomData,
    }
}

#[test]
fn stable_bundle_attempt_names_are_idempotent_and_conflicts_fail_closed() {
    let directory = tempfile::tempdir().unwrap();
    let mut first = probe(directory.path());
    assert!(first.commit().is_err());
    first.begin_bundle("bundle-A").unwrap();
    assert!(first.begin_bundle("bundle-B").is_err());
    first.write(1).unwrap();
    first.write(2).unwrap();
    first.commit().unwrap();
    let pending = directory.path().join(format!(
        "part-{}.pending",
        hex::encode(Sha256::digest(b"bundle-A"))
    ));
    assert!(pending.exists());
    let mut overlapping = probe(directory.path());
    overlapping.begin_bundle("bundle-A").unwrap();
    assert!(overlapping.commit().is_err());
    overlapping.abort().unwrap();
    assert!(pending.exists());
    first.finalize_bundle().unwrap();
    first.finalize_bundle().unwrap();
    assert!(!pending.exists());
    assert!(pending.with_extension("accepted").exists());
    let output = directory.path().join("result.bin");
    let bound = first.store.require_bundle_attempts(["bundle-A"]).unwrap();
    assert_eq!(bound.publish_concatenated(2, &output).unwrap().rows, 2);
    let bytes = std::fs::read(&output).unwrap();
    assert_eq!(&bytes[0..8], &4_u64.to_le_bytes());
    assert_eq!(&bytes[8..12], &1_i32.to_le_bytes());

    let mut replay = probe(directory.path());
    replay.begin_bundle("bundle-A").unwrap();
    replay.write(3).unwrap();
    assert!(replay.commit().is_err());
}
