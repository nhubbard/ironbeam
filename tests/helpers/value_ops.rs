use anyhow::Result;
use ironbeam::testing::*;
use ironbeam::{Sum, from_vec};

#[test]
fn map_values_and_filter_values_work_and_reorder_safely() -> Result<()> {
    let p = TestPipeline::new();
    let data: Vec<(u32, u32)> = (0..10_000).map(|i| (i % 7, i)).collect();

    // Two logically equivalent orders (planner may reorder filters earlier):
    let a = from_vec(&p, data.clone())
        .map_values(|v: &u32| v + 1)
        .filter_values(|v: &u32| !v.is_multiple_of(3))
        .combine_values(Sum::<u32>::new())
        .collect_par_sorted_by_key(Some(4), None)?;

    let b = from_vec(&p, data)
        .filter_values(|v: &u32| !v.is_multiple_of(3))
        .map_values(|v: &u32| v + 1)
        .combine_values(Sum::<u32>::new())
        .collect_par_sorted_by_key(Some(4), None)?;

    assert_eq!(a, b);
    Ok(())
}

#[test]
fn map_values_changes_only_value_preserves_keys() -> Result<()> {
    let p = TestPipeline::new();
    let input: Vec<(String, u32)> = vec![
        ("a".into(), 1),
        ("b".into(), 2),
        ("a".into(), 3),
        ("b".into(), 4),
    ];

    let out = from_vec(&p, input)
        .map_values(|v: &u32| v * 10)
        .collect_par_sorted_by_key(Some(2), None)?;

    assert_eq!(
        out,
        vec![
            ("a".into(), 10),
            ("a".into(), 30),
            ("b".into(), 20),
            ("b".into(), 40),
        ]
    );
    Ok(())
}
