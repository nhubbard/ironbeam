use anyhow::Result;
use rustflow::{Pipeline, from_vec, Count};

#[test]
fn gbk_then_combine_lifted_equals_classic_combine() -> Result<()> {
    let p = Pipeline::default();
    let words: Vec<String> = (0..20_000).map(|i| format!("w{}", i % 137)).collect();

    // Baseline: key-by then direct combine
    let direct = from_vec(&p, words.clone())
        .key_by(|w: &String| w.clone())
        .combine_values(Count)
        .collect_par_sorted_by_key(Some(8), None)?;

    // Pipeline that groups first, then uses lifted combine
    let with_lift = from_vec(&p, words)
        .key_by(|w: &String| w.clone())
        .group_by_key()
        .combine_values_lifted(Count)
        .collect_par_sorted_by_key(Some(8), None)?;

    assert_eq!(direct, with_lift);
    Ok(())
}