// Unit tests for src/main.rs.
use super::*;
use ironbeam_direct::{DirectRunner, JobConfig};

#[test]
fn v5_wordcount_preserves_the_expected_counts_and_portable_contract() {
    let pipeline = Pipeline::default();
    let output = counts(&pipeline).unwrap().output("test-counts").unwrap();
    let (mut rows, _) = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap();
    rows.sort();
    assert_eq!(
        rows,
        [
            ("again".into(), 1),
            ("brown".into(), 2),
            ("dog".into(), 2),
            ("fox".into(), 2),
            ("jumps".into(), 2),
            ("lazy".into(), 2),
            ("over".into(), 1),
            ("quick".into(), 2),
            ("the".into(), 4),
        ]
    );

    let portable = Pipeline::default();
    build(&portable).unwrap();
    ironbeam_dataflow::prepare_v5(&portable, "example/worker:local", "ironbeam-wordcount-v5")
        .unwrap();
}
