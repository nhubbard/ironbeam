use anyhow::Result;
use rustflow::*;
use rustflow::testing::*;

#[test]
fn reservoir_global_vec_and_flatten() -> Result<()> {
    let p = TestPipeline::new();
    let data: Vec<u32> = (0..10_000).collect();

    let v1 = from_vec(&p, data.clone())
        .sample_reservoir_vec(128, 42)
        .collect_seq()?;
    assert_eq!(v1.len(), 1);
    assert_eq!(v1[0].len(), 128);

    // determinism (same seed → same sample)
    let v2 = from_vec(&p, data)
        .sample_reservoir_vec(128, 42)
        .collect_seq()?;
    assert_eq!(v1, v2);

    // flatten form
    let f1 = from_vec(&p, (0..1000u32).collect())
        .sample_reservoir(50, 99)
        .collect_seq()?;
    assert_eq!(f1.len(), 50);

    Ok(())
}

#[test]
fn reservoir_per_key_vec_and_flatten() -> Result<()> {
    let p = TestPipeline::new();
    // Make 5 keys with 100 items each
    let input: Vec<(String, u32)> = (0..5)
        .flat_map(|k| (0..100u32).map(move |i| (format!("k{k}"), i)))
        .collect();

    let per_key = from_vec(&p, input.clone())
        .sample_values_reservoir_vec(7, 1234)
        .collect_seq_sorted()?; // (K, Vec<V>) where K: Ord for sorting
    assert_eq!(per_key.len(), 5);
    for (_k, vs) in &per_key {
        assert_eq!(vs.len(), 7);
    }

    // determinism
    let per_key_2 = from_vec(&p, input)
        .sample_values_reservoir_vec(7, 1234)
        .collect_seq_sorted()?;
    assert_eq!(per_key, per_key_2);

    // flatten variant
    let flat = from_vec(
        &p,
        (0..3)
            .flat_map(|k| (0..20).map(move |i| (format!("k{k}"), i)))
            .collect::<Vec<_>>(),
    )
    .sample_values_reservoir(5, 77)
    .collect_seq()?;
    // 3 keys * 5 samples per key
    assert_eq!(flat.len(), 15);

    Ok(())
}
