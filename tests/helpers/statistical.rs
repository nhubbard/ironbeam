use anyhow::Result;
use ironbeam::*;

// ─────────────────────────────── approx_median_globally ──────────────────────

#[test]
fn approx_median_globally_odd_count() -> Result<()> {
    let p = Pipeline::default();
    // Median of 1..=101 is 51
    let result = from_vec(&p, (1u32..=101).collect::<Vec<_>>())
        .approx_median_globally(100.0)
        .collect_seq()?;
    assert_eq!(result.len(), 1);
    // Allow up to 5% error
    assert!(
        (result[0] - 51.0).abs() < 5.0,
        "expected ~51, got {}",
        result[0]
    );
    Ok(())
}

#[test]
fn approx_median_globally_even_count() -> Result<()> {
    let p = Pipeline::default();
    // Median of 1..=100 is ~50.5
    let result = from_vec(&p, (1u32..=100).collect::<Vec<_>>())
        .approx_median_globally(100.0)
        .collect_seq()?;
    assert_eq!(result.len(), 1);
    assert!(
        (result[0] - 50.5).abs() < 5.0,
        "expected ~50.5, got {}",
        result[0]
    );
    Ok(())
}

#[test]
fn approx_median_globally_single_element() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![42u32])
        .approx_median_globally(100.0)
        .collect_seq()?;
    // Single element: median is that element
    assert_eq!(result.len(), 1);
    assert!((result[0] - 42.0).abs() < 1.0);
    Ok(())
}

#[test]
fn approx_median_globally_empty_returns_nan() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec::<u32>(&p, vec![])
        .approx_median_globally(100.0)
        .collect_seq()?;
    assert_eq!(result.len(), 1);
    assert!(result[0].is_nan());
    Ok(())
}

#[test]
fn approx_median_globally_higher_compression_is_accurate() -> Result<()> {
    let p = Pipeline::default();
    // With compression=1000 over a large dataset, the estimate should be in the right ballpark.
    // t-digest is probabilistic; allow up to 20% relative error on a 1000-element input.
    let result = from_vec(&p, (1u32..=1000).collect::<Vec<_>>())
        .approx_median_globally(1000.0)
        .collect_seq()?;
    // True median of 1..=1000 is 500.5; allow ±100 (20%) as a generous bound.
    assert!(
        (result[0] - 500.5).abs() < 100.0,
        "expected ~500.5, got {}",
        result[0]
    );
    Ok(())
}

// ─────────────────────────────── approx_median_per_key ───────────────────────

#[test]
fn approx_median_per_key_basic() -> Result<()> {
    let p = Pipeline::default();
    // "a": [1,2,3,4,5] median ≈ 3.0; "b": [10,20,30] median ≈ 20.0
    let mut result = from_vec(
        &p,
        vec![
            ("a", 1.0f64),
            ("a", 2.0),
            ("a", 3.0),
            ("a", 4.0),
            ("a", 5.0),
            ("b", 10.0),
            ("b", 20.0),
            ("b", 30.0),
        ],
    )
    .approx_median_per_key(100.0)
    .collect_seq()?;
    result.sort_by(|a, b| a.0.cmp(b.0));

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].0, "a");
    assert!((result[0].1 - 3.0).abs() < 1.0, "a median: {}", result[0].1);
    assert_eq!(result[1].0, "b");
    assert!(
        (result[1].1 - 20.0).abs() < 5.0,
        "b median: {}",
        result[1].1
    );
    Ok(())
}

#[test]
fn approx_median_per_key_single_entry_per_key() -> Result<()> {
    let p = Pipeline::default();
    let mut result = from_vec(&p, vec![("x", 7.0f64), ("y", 42.0)])
        .approx_median_per_key(100.0)
        .collect_seq()?;
    result.sort_by(|a, b| a.0.cmp(b.0));
    assert!((result[0].1 - 7.0).abs() < 1.0);
    assert!((result[1].1 - 42.0).abs() < 1.0);
    Ok(())
}

// ─────────────────────────────── approx_quantiles_globally ───────────────────

#[test]
fn approx_quantiles_globally_quartiles() -> Result<()> {
    let p = Pipeline::default();
    // 1..=100; Q1 ≈ 25, Q2 ≈ 50, Q3 ≈ 75
    let result = from_vec(&p, (1u32..=100).collect::<Vec<_>>())
        .approx_quantiles_globally(vec![0.25, 0.5, 0.75], 100.0)
        .collect_seq()?;
    assert_eq!(result.len(), 1);
    let qs = &result[0];
    assert_eq!(qs.len(), 3);
    assert!((qs[0] - 25.0).abs() < 5.0, "Q1: {}", qs[0]);
    assert!((qs[1] - 50.5).abs() < 5.0, "Q2: {}", qs[1]);
    assert!((qs[2] - 75.0).abs() < 5.0, "Q3: {}", qs[2]);
    Ok(())
}

#[test]
fn approx_quantiles_globally_min_max() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, (1u32..=100).collect::<Vec<_>>())
        .approx_quantiles_globally(vec![0.0, 1.0], 100.0)
        .collect_seq()?;
    let qs = &result[0];
    assert_eq!(qs.len(), 2);
    assert!((qs[0] - 1.0).abs() < 1.0, "min: {}", qs[0]);
    assert!((qs[1] - 100.0).abs() < 1.0, "max: {}", qs[1]);
    Ok(())
}

#[test]
fn approx_quantiles_globally_single_quantile() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, (1u32..=100).collect::<Vec<_>>())
        .approx_quantiles_globally(vec![0.5], 100.0)
        .collect_seq()?;
    let qs = &result[0];
    assert_eq!(qs.len(), 1);
    assert!((qs[0] - 50.5).abs() < 5.0);
    Ok(())
}

#[test]
fn approx_quantiles_globally_empty_returns_nan() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec::<u32>(&p, vec![])
        .approx_quantiles_globally(vec![0.25, 0.5, 0.75], 100.0)
        .collect_seq()?;
    let qs = &result[0];
    assert_eq!(qs.len(), 3);
    for &q in qs {
        assert!(q.is_nan());
    }
    Ok(())
}

// ─────────────────────────────── approx_quantiles_per_key ────────────────────

#[test]
fn approx_quantiles_per_key_basic() -> Result<()> {
    let p = Pipeline::default();
    // "a": [1,2,3,4,5]; quantiles [0.0, 0.5, 1.0]
    let mut result = from_vec(
        &p,
        vec![
            ("a", 1.0f64),
            ("a", 2.0),
            ("a", 3.0),
            ("a", 4.0),
            ("a", 5.0),
        ],
    )
    .approx_quantiles_per_key(vec![0.0, 0.5, 1.0], 100.0)
    .collect_seq()?;
    result.sort_by(|a, b| a.0.cmp(b.0));

    assert_eq!(result.len(), 1);
    let qs = &result[0].1;
    assert_eq!(qs.len(), 3);
    // min ≈ 1, median ≈ 3, max ≈ 5
    assert!((qs[0] - 1.0).abs() < 1.0, "min: {}", qs[0]);
    assert!((qs[1] - 3.0).abs() < 1.0, "median: {}", qs[1]);
    assert!((qs[2] - 5.0).abs() < 1.0, "max: {}", qs[2]);
    Ok(())
}

#[test]
fn approx_quantiles_per_key_multiple_keys() -> Result<()> {
    let p = Pipeline::default();
    let mut result = from_vec(
        &p,
        vec![
            ("a", 1.0f64),
            ("a", 2.0),
            ("a", 3.0),
            ("b", 10.0),
            ("b", 20.0),
            ("b", 30.0),
        ],
    )
    .approx_quantiles_per_key(vec![0.5], 100.0)
    .collect_seq()?;
    result.sort_by(|a, b| a.0.cmp(b.0));

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].1.len(), 1); // "a" median
    assert_eq!(result[1].1.len(), 1); // "b" median
    assert!((result[0].1[0] - 2.0).abs() < 1.0, "a: {}", result[0].1[0]);
    assert!((result[1].1[0] - 20.0).abs() < 5.0, "b: {}", result[1].1[0]);
    Ok(())
}

// ─────────────────────────────── consistency checks ──────────────────────────

#[test]
fn approx_median_globally_consistent_with_quantiles_at_half() -> Result<()> {
    let p = Pipeline::default();
    let data: Vec<u32> = (1..=200).collect();

    let median = from_vec(&p, data.clone())
        .approx_median_globally(100.0)
        .collect_seq()?[0];
    let qs = from_vec(&p, data)
        .approx_quantiles_globally(vec![0.5], 100.0)
        .collect_seq()?;

    // Both should be close to each other (both approximate)
    assert!(
        (median - qs[0][0]).abs() < 5.0,
        "median={median}, qs[0.5]={}",
        qs[0][0]
    );
    Ok(())
}
