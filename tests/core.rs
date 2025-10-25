use std::collections::HashMap;
use rustflow::{from_vec, Pipeline};
use rustflow::collection::Count;

#[test]
fn map_filter_flatmap_chain() -> anyhow::Result<()> {
    let p = Pipeline::default();
    let lines = from_vec(
        &p,
        vec![
            "The quick brown fox".to_string(),
            "jumps over the lazy dog".to_string(),
        ],
    );

    let words = lines.flat_map(|s: &String| {
        s.split_whitespace().map(|w| w.to_lowercase()).collect::<Vec<_>>()
    });
    let filtered = words.filter(|w: &String| w.len() >= 4);

    // either .collect() if you added the shim, or .collect_seq()
    let out = filtered.collect_seq()?;

    assert_eq!(
        out,
        vec![
            "quick".to_string(),
            "brown".to_string(),
            "jumps".to_string(),
            "over".to_string(),
            "lazy".to_string()
        ]
    );
    Ok(())
}

#[test]
fn key_by_and_group_by_key_counts_words() -> anyhow::Result<()> {
    let p = Pipeline::default();
    let words = from_vec(
        &p,
        vec!["a".to_string(), "b".to_string(), "a".to_string(), "c".to_string(), "b".to_string()],
    );
    let keyed = words.key_by(|w: &String| w.clone()); // (word, word)
    let grouped = keyed.group_by_key();               // (word, Vec<word>)
    let out = grouped.collect_seq()?;

    // Explicit map type to satisfy inference on v.len()
    let mut m: HashMap<String, usize> = HashMap::new();
    for (k, v) in out {
        m.insert(k, v.len());
    }
    assert_eq!(m.get("a"), Some(&2usize));
    assert_eq!(m.get("b"), Some(&2usize));
    assert_eq!(m.get("c"), Some(&1usize));
    Ok(())
}

#[test]
fn combine_values_count() -> anyhow::Result<()> {
    let p = Pipeline::default();
    let words = from_vec(
        &p,
        vec!["a".to_string(), "b".to_string(), "a".to_string(), "c".to_string(), "b".to_string()],
    );

    let counts = words
        .flat_map(|w: &String| vec![w.clone()])            // keep as Vec<String>
        .key_by(|w: &String| w.clone())                    // (String, String)
        .map_values(|_v: &String| 1u64)                    // (String, u64)
        .combine_values(Count);                            // (String, u64)

    // either .collect() if shim exists, or .collect_seq()
    let mut m: HashMap<String, u64> = HashMap::new();
    for (k, v) in counts.collect_seq()? {
        m.insert(k, v);
    }
    assert_eq!(m.get("a"), Some(&2u64));
    assert_eq!(m.get("b"), Some(&2u64));
    assert_eq!(m.get("c"), Some(&1u64));
    Ok(())
}

#[test]
fn map_values_transforms_payloads() -> anyhow::Result<()> {
    let p = Pipeline::default();
    let nums = from_vec(&p, vec![1u32, 2, 3, 4, 5]);

    let kv = nums.key_by(|n: &u32| if (*n).is_multiple_of(2) { "even" } else { "odd" }.to_string());
    let doubled = kv.map_values(|v: &u32| v * 2);

    // either .collect() if shim exists, or .collect_seq()
    let out = doubled.collect_seq()?;

    assert_eq!(out.len(), 5);
    for (_k, v) in out {
        assert_eq!(v % 2, 0);
    }
    Ok(())
}

#[test]
fn stateless_seq_vs_par_equivalent() -> anyhow::Result<()> {
    let p = Pipeline::default();
    let lines = from_vec(&p, (0..1000).map(|i| format!("w{i} w{i}")).collect::<Vec<_>>());
    let words = lines.flat_map(|s: &String| s.split_whitespace().map(|w| w.to_string()).collect::<Vec<_>>());
    let filtered = words.filter(|w: &String| w.len() >= 2);

    let a = filtered.clone().collect_seq()?;
    let b = filtered.collect_par(Some(4), Some(8))?;
    assert_eq!(a, b);
    Ok(())
}