//! Stable observations against published Ironbeam 4.1.1.
use ironbeam_v4::{Pipeline, from_vec};

#[test]
fn elementwise_empty_singleton_and_order() {
    for input in [vec![], vec![1], vec![1, 2, 3, 4]] {
        let expected: Vec<_> = input.iter().map(|n| n * 2).filter(|n| n % 4 == 0).collect();
        let p = Pipeline::default();
        let actual = from_vec(&p, input.clone())
            .map(|n| n * 2)
            .filter(|n| n % 4 == 0)
            .collect_seq()
            .unwrap();
        assert_eq!(actual, expected);

        let p = Pipeline::default();
        let mut actual = from_vec(&p, input)
            .map(|n| n * 2)
            .filter(|n| n % 4 == 0)
            .collect_par(Some(2), Some(2))
            .unwrap();
        actual.sort_unstable();
        assert_eq!(actual, expected);
    }
}

#[test]
fn keyed_group_and_join_multisets() {
    let p = Pipeline::default();
    let mut grouped = from_vec(
        &p,
        vec![
            ("a".to_string(), 1),
            ("b".to_string(), 3),
            ("a".to_string(), 2),
        ],
    )
    .group_by_key()
    .collect_seq()
    .unwrap();
    grouped.sort_by(|(a, _), (b, _)| a.cmp(b));
    for (_, values) in &mut grouped {
        values.sort_unstable();
    }
    assert_eq!(
        grouped,
        vec![("a".to_string(), vec![1, 2]), ("b".to_string(), vec![3])]
    );

    let p = Pipeline::default();
    let left = from_vec(
        &p,
        vec![
            ("a".to_string(), 1),
            ("a".to_string(), 2),
            ("b".to_string(), 3),
        ],
    );
    let right = from_vec(&p, vec![("a".to_string(), 10), ("c".to_string(), 20)]);
    let mut joined = left
        .join_inner(&right)
        .collect_par(Some(2), Some(2))
        .unwrap();
    joined.sort_unstable();
    assert_eq!(
        joined,
        vec![("a".to_string(), (1, 10)), ("a".to_string(), (2, 10))]
    );
}

#[test]
fn jsonl_round_trip_is_bounded_and_immediate() {
    use ironbeam_v4::read_jsonl;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("rows.jsonl");
    let p = Pipeline::default();
    let written = from_vec(&p, vec![1u32, 2, 3]).write_jsonl(&path).unwrap();
    assert_eq!(written, 3);
    let p = Pipeline::default();
    assert_eq!(
        read_jsonl::<u32>(&p, &path).unwrap().collect_seq().unwrap(),
        vec![1, 2, 3]
    );
}
