//! Tests for [`PCollection::reify_timestamps`].

use anyhow::Result;
use ironbeam::window::Timestamped;
use ironbeam::*;

// --- Basic correctness -------------------------------------------------------

#[test]
fn reify_empty_collection() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec::<Timestamped<u32>>(&p, vec![])
        .reify_timestamps()
        .collect_seq()?;
    assert!(result.is_empty());
    Ok(())
}

#[test]
fn reify_single_element_timestamp() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![Timestamped::new(42u64, 99u32)])
        .reify_timestamps()
        .collect_seq()?;
    assert_eq!(result, vec![(42u64, 99u32)]);
    Ok(())
}

#[test]
fn reify_preserves_timestamp_value() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(
        &p,
        vec![Timestamped::new(
            1_700_000_000_000u64,
            "payload".to_string(),
        )],
    )
    .reify_timestamps()
    .collect_seq()?;
    assert_eq!(result[0].0, 1_700_000_000_000u64);
    Ok(())
}

#[test]
fn reify_preserves_element_value() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![Timestamped::new(1u64, "hello".to_string())])
        .reify_timestamps()
        .collect_seq()?;
    assert_eq!(result[0].1, "hello");
    Ok(())
}

// --- Multiple elements -------------------------------------------------------

#[test]
fn reify_multiple_elements_all_preserved() -> Result<()> {
    let p = Pipeline::default();
    let input = vec![
        Timestamped::new(100u64, 1u32),
        Timestamped::new(200u64, 2u32),
        Timestamped::new(300u64, 3u32),
    ];
    let mut result = from_vec(&p, input).reify_timestamps().collect_seq()?;
    result.sort_unstable_by_key(|&(ts, _)| ts);
    assert_eq!(result, vec![(100u64, 1u32), (200u64, 2u32), (300u64, 3u32)]);
    Ok(())
}

#[test]
#[allow(clippy::cast_possible_truncation)]
fn reify_distinct_timestamps_preserved() -> Result<()> {
    let p = Pipeline::default();
    let input: Vec<Timestamped<u32>> = (1u64..=5)
        .map(|ts| Timestamped::new(ts * 1000, ts as u32))
        .collect();
    let mut result = from_vec(&p, input).reify_timestamps().collect_seq()?;
    result.sort_unstable_by_key(|&(ts, _)| ts);
    let expected: Vec<(u64, u32)> = (1u64..=5).map(|ts| (ts * 1000, ts as u32)).collect();
    assert_eq!(result, expected);
    Ok(())
}

#[test]
fn reify_duplicate_timestamps_all_emitted() -> Result<()> {
    let p = Pipeline::default();
    let input = vec![
        Timestamped::new(999u64, 10u32),
        Timestamped::new(999u64, 20u32),
        Timestamped::new(999u64, 30u32),
    ];
    let mut result = from_vec(&p, input).reify_timestamps().collect_seq()?;
    result.sort_unstable_by_key(|&(_, v)| v);
    assert_eq!(
        result,
        vec![(999u64, 10u32), (999u64, 20u32), (999u64, 30u32)]
    );
    Ok(())
}

// --- Element types -----------------------------------------------------------

#[test]
fn reify_string_values() -> Result<()> {
    let p = Pipeline::default();
    let input = vec![
        Timestamped::new(1u64, "alpha".to_string()),
        Timestamped::new(2u64, "beta".to_string()),
    ];
    let mut result = from_vec(&p, input).reify_timestamps().collect_seq()?;
    result.sort_unstable_by_key(|(_, v)| v.clone());
    assert_eq!(
        result,
        vec![(1u64, "alpha".to_string()), (2u64, "beta".to_string())]
    );
    Ok(())
}

#[test]
fn reify_struct_values() -> Result<()> {
    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct Point {
        x: i32,
        y: i32,
    }

    let p = Pipeline::default();
    let input = vec![
        Timestamped::new(10u64, Point { x: 1, y: 2 }),
        Timestamped::new(20u64, Point { x: 3, y: 4 }),
    ];
    let mut result = from_vec(&p, input).reify_timestamps().collect_seq()?;
    result.sort_unstable_by_key(|(ts, _)| *ts);
    assert_eq!(
        result,
        vec![(10u64, Point { x: 1, y: 2 }), (20u64, Point { x: 3, y: 4 })]
    );
    Ok(())
}

#[test]
fn reify_tuple_values() -> Result<()> {
    let p = Pipeline::default();
    let input = vec![
        Timestamped::new(5u64, (1u32, "a".to_string())),
        Timestamped::new(10u64, (2u32, "b".to_string())),
    ];
    let mut result = from_vec(&p, input).reify_timestamps().collect_seq()?;
    result.sort_unstable_by_key(|&(ts, _)| ts);
    assert_eq!(
        result,
        vec![
            (5u64, (1u32, "a".to_string())),
            (10u64, (2u32, "b".to_string())),
        ]
    );
    Ok(())
}

// --- Round-trip / inverse ----------------------------------------------------

#[test]
fn reify_is_inverse_of_to_timestamped() -> Result<()> {
    let p = Pipeline::default();
    let pairs = vec![(100u64, 1u32), (200u64, 2u32), (300u64, 3u32)];
    let mut result = from_vec(&p, pairs.clone())
        .to_timestamped()
        .reify_timestamps()
        .collect_seq()?;
    result.sort_unstable_by_key(|&(ts, _)| ts);
    assert_eq!(result, pairs);
    Ok(())
}

#[test]
fn to_timestamped_is_inverse_of_reify() -> Result<()> {
    let p = Pipeline::default();
    let input = vec![
        Timestamped::new(50u64, "x".to_string()),
        Timestamped::new(75u64, "y".to_string()),
    ];
    let mut result = from_vec(&p, input.clone())
        .reify_timestamps()
        .to_timestamped()
        .collect_seq()?;
    result.sort_unstable_by_key(|ts| ts.ts);
    assert_eq!(result, input);
    Ok(())
}

// --- Composition -------------------------------------------------------------

#[test]
fn reify_chainable_with_map() -> Result<()> {
    let p = Pipeline::default();
    let input = vec![Timestamped::new(1u64, 10u32), Timestamped::new(2u64, 20u32)];
    let mut result = from_vec(&p, input)
        .reify_timestamps()
        .map(|(ts, v): &(u64, u32)| (*ts, v * 2))
        .collect_seq()?;
    result.sort_unstable_by_key(|&(ts, _)| ts);
    assert_eq!(result, vec![(1u64, 20u32), (2u64, 40u32)]);
    Ok(())
}

#[test]
fn reify_after_attach_timestamps() -> Result<()> {
    let p = Pipeline::default();
    let input = vec![1u32, 2, 3];
    let mut result = from_vec(&p, input)
        .attach_timestamps(|&v| u64::from(v) * 100)
        .reify_timestamps()
        .collect_seq()?;
    result.sort_unstable_by_key(|&(ts, _)| ts);
    assert_eq!(result, vec![(100u64, 1u32), (200u64, 2u32), (300u64, 3u32)]);
    Ok(())
}

#[test]
fn reify_chainable_with_filter() -> Result<()> {
    let p = Pipeline::default();
    let input = vec![
        Timestamped::new(1u64, 1u32),
        Timestamped::new(2u64, 2u32),
        Timestamped::new(3u64, 3u32),
        Timestamped::new(4u64, 4u32),
    ];
    let mut result = from_vec(&p, input)
        .reify_timestamps()
        .filter(|(_, v): &(u64, u32)| v % 2 == 0)
        .collect_seq()?;
    result.sort_unstable_by_key(|&(ts, _)| ts);
    assert_eq!(result, vec![(2u64, 2u32), (4u64, 4u32)]);
    Ok(())
}

// --- Scalability -------------------------------------------------------------

#[test]
#[allow(clippy::cast_possible_truncation)]
fn reify_large_collection() -> Result<()> {
    let p = Pipeline::default();
    let n = 1_000u64;
    let input: Vec<Timestamped<u64>> = (0..n).map(|i| Timestamped::new(i * 10, i)).collect();
    let mut result = from_vec(&p, input).reify_timestamps().collect_seq()?;
    assert_eq!(result.len(), n as usize);
    result.sort_unstable_by_key(|&(ts, _)| ts);
    let expected: Vec<(u64, u64)> = (0..n).map(|i| (i * 10, i)).collect();
    assert_eq!(result, expected);
    Ok(())
}
