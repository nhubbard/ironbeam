use ironbeam::collection::Count;
use ironbeam::from_vec;
use ironbeam::testing::*;
use std::collections::HashMap;

#[test]
fn map_filter_flatmap_chain() -> anyhow::Result<()> {
    let p = TestPipeline::new();
    let lines = from_vec(
        &p,
        vec![
            "The quick brown fox".to_string(),
            "jumps over the lazy dog".to_string(),
        ],
    );

    let words = lines.flat_map(|s: &String| {
        s.split_whitespace()
            .map(str::to_lowercase)
            .collect::<Vec<_>>()
    });
    let filtered = words.filter(|w: &String| w.len() >= 4);

    let out = filtered.collect_seq()?;

    assert_collections_equal(
        &out,
        &[
            "quick".to_string(),
            "brown".to_string(),
            "jumps".to_string(),
            "over".to_string(),
            "lazy".to_string(),
        ],
    );
    Ok(())
}

#[test]
fn key_by_and_group_by_key_counts_words() -> anyhow::Result<()> {
    let p = TestPipeline::new();
    let words = from_vec(
        &p,
        vec![
            "a".to_string(),
            "b".to_string(),
            "a".to_string(),
            "c".to_string(),
            "b".to_string(),
        ],
    );
    let keyed = words.key_by(|w: &String| w.clone());
    let grouped = keyed.group_by_key();
    let out = grouped.collect_seq()?;

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
    let p = TestPipeline::new();
    let words = from_vec(
        &p,
        vec![
            "a".to_string(),
            "b".to_string(),
            "a".to_string(),
            "c".to_string(),
            "b".to_string(),
        ],
    );

    let counts = words
        .flat_map(|w: &String| vec![w.clone()])
        .key_by(|w: &String| w.clone())
        .map_values(|_v: &String| 1u64)
        .combine_values(Count);

    let out = counts.collect_seq()?;
    let mut m: HashMap<String, u64> = HashMap::new();
    for (k, v) in out {
        m.insert(k, v);
    }
    assert_eq!(m.get("a"), Some(&2u64));
    assert_eq!(m.get("b"), Some(&2u64));
    assert_eq!(m.get("c"), Some(&1u64));
    Ok(())
}

#[test]
fn map_values_transforms_payloads() -> anyhow::Result<()> {
    let p = TestPipeline::new();
    let nums = from_vec(&p, vec![1u32, 2, 3, 4, 5]);

    let kv = nums.key_by(|n: &u32| {
        if (*n).is_multiple_of(2) {
            "even"
        } else {
            "odd"
        }
        .to_string()
    });
    let doubled = kv.map_values(|v: &u32| v * 2);

    let out = doubled.collect_seq()?;

    assert_collection_size(&out, 5);
    assert_all(&out, |(_k, v)| *v % 2 == 0);
    Ok(())
}

#[test]
fn stateless_seq_vs_par_equivalent() -> anyhow::Result<()> {
    let p = TestPipeline::new();
    let lines = from_vec(
        &p,
        (0..1000).map(|i| format!("w{i} w{i}")).collect::<Vec<_>>(),
    );
    let words = lines.flat_map(|s: &String| {
        s.split_whitespace()
            .map(str::to_string)
            .collect::<Vec<_>>()
    });
    let filtered = words.filter(|w: &String| w.len() >= 2);

    let a = filtered.clone().collect_seq()?;
    let b = filtered.collect_par(Some(4), Some(8))?;
    assert_collections_equal(&a, &b);
    Ok(())
}
