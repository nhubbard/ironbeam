// tests/joins.rs
use anyhow::Result;
use rustflow::*;

fn sorted<T: Ord>(mut v: Vec<T>) -> Vec<T> {
    v.sort();
    v
}

#[test]
fn inner_join_basic_seq_par() -> Result<()> {
    let p = Pipeline::default();
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

#[test]
fn left_right_full_outer() -> Result<()> {
    let p = Pipeline::default();
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
    let leftj = left.clone().join_left(&right);
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
    let rightj = left.clone().join_right(&right);
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
            .then(
                a.1.0
                    .as_ref()
                    .map(|s| s.clone())
                    .cmp(&b.1.0.as_ref().map(|s| s.clone())),
            )
            .then(
                a.1.1
                    .as_ref()
                    .map(|s| s.clone())
                    .cmp(&b.1.1.as_ref().map(|s| s.clone())),
            )
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
