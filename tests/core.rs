use rustflow::{from_vec, Count, Pipeline};

#[test]
fn word_count_pipeline() -> anyhow::Result<()> {
    let p = Pipeline::default();

    let lines = from_vec(
        &p,
        vec![
            "The quick brown fox".to_string(),
            "Jumps over the lazy dog".to_string()
        ]
    );

    let words = lines.flat_map(|s: &String| {
        s.split_whitespace()
            .map(|w| w.to_lowercase())
            .collect::<Vec<_>>()
    });

    let filtered = words.filter(|w: &String| w.len() >= 3);

    let out = filtered.collect()?;

    assert_eq!(out.len(), 9);

    let expected = vec!["the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"];
    assert_eq!(expected, out);

    Ok(())
}

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
    let out = filtered.collect()?;

    assert_eq!(out, vec![
        "quick".to_string(), "brown".to_string(),
        "jumps".to_string(), "over".to_string(),
        "lazy".to_string()
    ]);
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
    let out = grouped.collect()?;

    // Convert to a map for stable assertions
    let mut m = std::collections::HashMap::new();
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
        .key_by(|w: &String| w.clone()) // (word, word)
        .map_values(|_v: &String| 1u32) // (word, 1)
        .combine_values(Count);         // (word, u64)

    let mut m = std::collections::HashMap::new();
    for (k, v) in counts.collect()? {
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
    let out = doubled.collect()?;

    // verify there are five elements and values are doubled
    assert_eq!(out.len(), 5);
    for (_k, v) in out {
        assert_eq!(v % 2, 0);
    }
    Ok(())
}