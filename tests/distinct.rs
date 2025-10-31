use rustflow::*;

#[test]
fn distinct_global_exact() -> anyhow::Result<()> {
    let p = Pipeline::default();
    let out = from_vec(&p, vec![1, 1, 2, 3, 3, 3]).distinct();
    let mut v = out.collect_seq()?;
    v.sort();
    assert_eq!(v, vec![1, 2, 3]);
    Ok(())
}

#[test]
fn distinct_per_key_exact() -> anyhow::Result<()> {
    let p = Pipeline::default();
    let kv = vec![
        ("a".to_string(), 1),
        ("a".to_string(), 1),
        ("a".to_string(), 2),
        ("b".to_string(), 7),
        ("b".to_string(), 7),
    ];
    let out = from_vec(&p, kv).distinct_per_key();
    let mut v = out.collect_seq()?;
    v.sort();
    assert_eq!(
        v,
        vec![
            ("a".to_string(), 1),
            ("a".to_string(), 2),
            ("b".to_string(), 7),
        ]
    );
    Ok(())
}

#[test]
fn approx_distinct_global_kmv() -> anyhow::Result<()> {
    let p = Pipeline::default();
    // 10_000 values with ~1234 uniques
    let pc = from_vec(&p, (0..10_000u64).map(|n| n % 1234).collect::<Vec<_>>());
    let est = pc.approx_distinct_count(256).collect_seq()?[0];
    println!("est={:?}", est);
    assert!(est > 1150.0 && est < 1320.0);
    Ok(())
}

#[test]
fn approx_distinct_per_key_kmv() -> anyhow::Result<()> {
    let p = Pipeline::default();
    // Two keys with different cardinalities
    let mut data = Vec::new();
    for i in 0..1000 {
        data.push(("a".to_string(), i % 37)); // 37 uniques
        data.push(("b".to_string(), i % 7)); // 7 uniques
    }
    let out = from_vec(&p, data).approx_distinct_count_per_key(96);
    let mut v = out.collect_seq()?;
    v.sort_by(|a, b| a.0.cmp(&b.0));
    let (ka, ea) = (&v[0].0, v[0].1);
    let (kb, eb) = (&v[1].0, v[1].1);
    println!("ea={:?},eb={:?}", ea, eb);
    assert_eq!(ka, "a");
    assert!(ea > 30.0 && ea < 45.0);
    assert_eq!(kb, "b");
    assert!(eb > 5.0 && eb < 9.5);
    Ok(())
}
