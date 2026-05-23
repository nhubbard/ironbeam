// tests/joins.rs
use anyhow::Result;
use ironbeam::testing::*;
use ironbeam::*;

fn sorted<T: Ord>(mut v: Vec<T>) -> Vec<T> {
    v.sort();
    v
}

#[test]
fn inner_join_basic_seq_par() -> Result<()> {
    let p = TestPipeline::new();
    let left = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("b".to_string(), 2),
            ("a".to_string(), 3),
        ],
    );
    let right = from_vec(
        &p,
        vec![
            ("a".to_string(), 10i32),
            ("c".to_string(), 30),
            ("a".to_string(), 40),
            ("b".to_string(), 20),
        ],
    );

    let joined = left.join_inner(&right);
    let seq = sorted(joined.clone().collect_seq()?);
    let par = sorted(joined.collect_par(None, None)?);

    let expected = sorted(vec![
        ("a".to_string(), (1u32, 10i32)),
        ("a".to_string(), (1u32, 40i32)),
        ("a".to_string(), (3u32, 10i32)),
        ("a".to_string(), (3u32, 40i32)),
        ("b".to_string(), (2u32, 20i32)),
    ]);
    assert_eq!(seq, expected);
    assert_eq!(par, expected);
    Ok(())
}

/// Verify that `CoGroup` left/right branches execute correctly when run concurrently
/// via `rayon::join`. This exercises the parallel `CoGroup` arm in `exec_par`.
#[test]
fn join_branches_produce_correct_result_in_parallel() -> Result<()> {
    let p = Pipeline::default();
    let left = from_vec(&p, vec![("a".to_string(), 1u32), ("b".to_string(), 2)]);
    let right = from_vec(&p, vec![("a".to_string(), 10u32), ("b".to_string(), 20)]);
    let mut out = left.join_inner(&right).collect_par(None, Some(4))?;
    out.sort_unstable_by_key(|(k, _)| k.clone());
    assert_eq!(
        out,
        vec![
            ("a".to_string(), (1u32, 10u32)),
            ("b".to_string(), (2u32, 20u32)),
        ]
    );
    Ok(())
}

#[test]
fn left_right_full_outer() -> Result<()> {
    let p = TestPipeline::new();
    let left = from_vec(
        &p,
        vec![
            (1u32, "L1".to_string()),
            (2, "L2".to_string()),
            (1, "L1b".to_string()),
        ],
    );
    let right = from_vec(&p, vec![(1u32, "R1".to_string()), (3, "R3".to_string())]);

    // left
    let leftj = left.join_left(&right);
    let mut left_out = leftj.collect_par(None, None)?;
    left_out.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.0.cmp(&b.1.0)));
    assert_eq!(
        left_out,
        vec![
            (1, ("L1".to_string(), Some("R1".to_string()))),
            (1, ("L1b".to_string(), Some("R1".to_string()))),
            (2, ("L2".to_string(), None)),
        ]
    );

    // right
    let rightj = left.join_right(&right);
    let mut right_out = rightj.collect_seq()?;
    right_out.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.1.cmp(&b.1.1)));
    assert_eq!(
        right_out,
        vec![
            (1, (Some("L1".to_string()), "R1".to_string())),
            (1, (Some("L1b".to_string()), "R1".to_string())),
            (3, (None, "R3".to_string())),
        ]
    );

    // full
    let fullj = left.join_full(&right);
    let mut full_out = fullj.collect_par(None, None)?;
    full_out.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then(a.1.0.clone().cmp(&b.1.0.clone()))
            .then(a.1.1.clone().cmp(&b.1.1.clone()))
    });
    assert_eq!(
        full_out,
        vec![
            (1, (Some("L1".to_string()), Some("R1".to_string()))),
            (1, (Some("L1b".to_string()), Some("R1".to_string()))),
            (2, (Some("L2".to_string()), None)),
            (3, (None, Some("R3".to_string()))),
        ]
    );

    Ok(())
}

// ── 3.12 Bloom Filter Semi-Join correctness ───────────────────────────────────

/// Inner join with sparse overlap: only matching keys appear in output.
/// Verifies that the Bloom semi-join does not produce false negatives.
#[test]
fn inner_join_sparse_overlap_correctness() -> Result<()> {
    let p = Pipeline::default();
    // Left: keys 0..5. Right: keys 3..8. Overlap: 3, 4.
    let left = from_vec(&p, (0u32..5).map(|i| (i, i * 10)).collect::<Vec<_>>());
    let right = from_vec(&p, (3u32..8).map(|i| (i, i * 100)).collect::<Vec<_>>());
    let mut out = left.join_inner(&right).collect_seq()?;
    out.sort_unstable_by_key(|(k, _)| *k);
    assert_eq!(
        out,
        vec![(3u32, (30u32, 300u32)), (4, (40, 400))],
        "only keys present on both sides must appear in the inner join output"
    );
    Ok(())
}

/// Inner join with no overlap produces empty output.
#[test]
fn inner_join_no_overlap_empty() -> Result<()> {
    let p = Pipeline::default();
    let left = from_vec(&p, vec![("a".to_string(), 1u32), ("b".to_string(), 2)]);
    let right = from_vec(&p, vec![("c".to_string(), 10u32), ("d".to_string(), 20)]);
    let out = left.join_inner(&right).collect_seq()?;
    assert!(
        out.is_empty(),
        "inner join with disjoint key sets must produce no output"
    );
    Ok(())
}

/// Inner join with complete overlap is identical to a regular hash join.
/// All pairs are emitted — no false negatives from the Bloom filter.
#[test]
fn inner_join_full_overlap_no_false_negatives() -> Result<()> {
    let p = Pipeline::default();
    let left = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("b".to_string(), 2),
            ("c".to_string(), 3),
        ],
    );
    let right = from_vec(
        &p,
        vec![
            ("a".to_string(), 10u32),
            ("b".to_string(), 20),
            ("c".to_string(), 30),
        ],
    );
    let mut out = left.join_inner(&right).collect_seq()?;
    out.sort_unstable_by_key(|(k, _)| k.clone());
    assert_eq!(
        out,
        vec![
            ("a".to_string(), (1u32, 10u32)),
            ("b".to_string(), (2, 20)),
            ("c".to_string(), (3, 30)),
        ]
    );
    Ok(())
}

/// Left outer join: all left rows appear; only matched right rows appear.
/// The Bloom filter on the right side must not drop right rows that match.
#[test]
fn left_join_bloom_preserves_all_left_rows() -> Result<()> {
    let p = Pipeline::default();
    let left = from_vec(&p, vec![("a".to_string(), 1u32), ("b".to_string(), 2)]);
    // Right only has "a"; "b" has no right counterpart.
    let right = from_vec(&p, vec![("a".to_string(), "ax".to_string())]);
    let mut out = left.join_left(&right).collect_seq()?;
    out.sort_unstable_by_key(|(k, _)| k.clone());
    assert_eq!(
        out,
        vec![
            ("a".to_string(), (1u32, Some("ax".to_string()))),
            ("b".to_string(), (2u32, None)),
        ],
        "all left rows must be present; right=None for unmatched"
    );
    Ok(())
}

/// Right outer join: all right rows appear; only matched left rows appear.
#[test]
fn right_join_bloom_preserves_all_right_rows() -> Result<()> {
    let p = Pipeline::default();
    // Left only has "a"; "c" has no left counterpart.
    let left = from_vec(&p, vec![("a".to_string(), 1u32)]);
    let right = from_vec(
        &p,
        vec![
            ("a".to_string(), "ax".to_string()),
            ("c".to_string(), "cx".to_string()),
        ],
    );
    let mut out = left.join_right(&right).collect_seq()?;
    out.sort_unstable_by_key(|(k, _)| k.clone());
    assert_eq!(
        out,
        vec![
            ("a".to_string(), (Some(1u32), "ax".to_string())),
            ("c".to_string(), (None, "cx".to_string())),
        ],
        "all right rows must be present; left=None for unmatched"
    );
    Ok(())
}

/// Bloom semi-join works correctly in parallel execution mode.
#[test]
fn inner_join_bloom_correct_parallel() -> Result<()> {
    let p = Pipeline::default();
    let left = from_vec(&p, (0u32..5).map(|i| (i, i * 10)).collect::<Vec<_>>());
    let right = from_vec(&p, (3u32..8).map(|i| (i, i * 100)).collect::<Vec<_>>());
    let mut out = left.join_inner(&right).collect_par(None, None)?;
    out.sort_unstable_by_key(|(k, _)| *k);
    assert_eq!(out, vec![(3u32, (30u32, 300u32)), (4, (40, 400))]);
    Ok(())
}
