use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use ironbeam::testing::*;
use ironbeam::{from_vec, side_hashmap, side_multimap, side_singleton, side_vec};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Product {
    sku: String,
}

#[test]
fn map_with_side_map_enriches_records() -> Result<()> {
    let p = TestPipeline::new();

    // Input stream of product SKUs
    let items = vec![
        Product { sku: "A".into() },
        Product { sku: "B".into() },
        Product { sku: "C".into() },
        Product { sku: "D".into() }, // not in the price map
    ];
    let input = from_vec(&p, items);

    // Side map: price list
    let price_side = side_hashmap::<String, u32>(vec![
        ("A".into(), 100),
        ("B".into(), 250),
        ("C".into(), 250),
    ]);

    // Enrich with price, defaulting missing SKUs to 0
    let enriched = input.map_with_side_map(&price_side, |prod, prices| {
        let price = prices.get(&prod.sku).copied().unwrap_or(0);
        (prod.sku.clone(), price)
    });

    // Collect (either path should work; use par to exercise concurrency)
    let out = enriched.collect_par(Some(4), None)?;
    let mut m: HashMap<String, u32> = HashMap::new();
    for (sku, price) in out {
        m.insert(sku, price);
    }

    assert_eq!(m.get("A"), Some(&100));
    assert_eq!(m.get("B"), Some(&250));
    assert_eq!(m.get("C"), Some(&250));
    assert_eq!(m.get("D"), Some(&0)); // default for missing
    Ok(())
}

#[test]
fn filter_with_side_allows_only_whitelisted() -> Result<()> {
    let p = TestPipeline::new();

    // Input: countries seen in events
    let input = from_vec(
        &p,
        vec![
            "us".to_string(),
            "de".to_string(),
            "xx".to_string(),
            "jp".to_string(),
        ],
    );

    // Side vec whitelist (we'll turn it into a set inside the closure)
    let whitelist = side_vec::<String>(vec!["us".into(), "de".into(), "jp".into()]);

    // Keep only whitelisted countries
    let filtered = input.filter_with_side(&whitelist, |code, allowed| {
        let set: HashSet<&String> = allowed.iter().collect();
        set.contains(code)
    });

    // Deterministic check: sort results
    let mut out = filtered.collect_par_sorted(Some(4), None)?;
    out.sort();
    assert_eq!(
        out,
        vec!["de".to_string(), "jp".to_string(), "us".to_string()]
    );
    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// filter_with_side_map
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn filter_with_side_map_keeps_matching_keys() -> Result<()> {
    let p = TestPipeline::new();
    let items = from_vec(&p, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    let allowed = side_hashmap::<String, ()>(vec![("a".into(), ()), ("b".into(), ())]);

    let valid = items.filter_with_side_map(&allowed, |item, m| m.contains_key(item));

    let mut out = valid.collect_par(Some(2), None)?;
    out.sort();
    assert_eq!(out, vec!["a".to_string(), "b".to_string()]);
    Ok(())
}

#[test]
fn filter_with_side_map_empty_result_when_no_match() -> Result<()> {
    let p = TestPipeline::new();
    let items = from_vec(&p, vec!["x".to_string(), "y".to_string()]);
    let allowed = side_hashmap::<String, ()>(vec![("a".into(), ())]);

    let valid = items.filter_with_side_map(&allowed, |item, m| m.contains_key(item));

    let out = valid.collect_par(Some(2), None)?;
    assert!(out.is_empty());
    Ok(())
}

#[test]
fn filter_with_side_map_uses_value_for_predicate() -> Result<()> {
    let p = TestPipeline::new();
    // Keep elements whose value in the map exceeds 5
    let scores = from_vec(
        &p,
        vec!["alice".to_string(), "bob".to_string(), "carol".to_string()],
    );
    let thresholds = side_hashmap::<String, u32>(vec![
        ("alice".into(), 10),
        ("bob".into(), 3),
        ("carol".into(), 7),
    ]);

    let passing =
        scores.filter_with_side_map(&thresholds, |name, m| m.get(name).copied().unwrap_or(0) > 5);

    let mut out = passing.collect_par(Some(2), None)?;
    out.sort();
    assert_eq!(out, vec!["alice".to_string(), "carol".to_string()]);
    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// side_singleton / map_with_singleton / filter_with_singleton
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn side_singleton_constructs_correctly() {
    let s = side_singleton(42u32);
    assert_eq!(*s.0, 42u32);
}

#[test]
fn map_with_singleton_scales_all_elements() -> Result<()> {
    let p = TestPipeline::new();
    let data = from_vec(&p, vec![1u32, 2, 3, 4]);
    let multiplier = side_singleton(10u32);

    let scaled = data.map_with_singleton(&multiplier, |x, m| x * m);

    let mut out = scaled.collect_par(Some(2), None)?;
    out.sort_unstable();
    assert_eq!(out, vec![10, 20, 30, 40]);
    Ok(())
}

#[test]
fn map_with_singleton_string_prefix() -> Result<()> {
    let p = TestPipeline::new();
    let words = from_vec(&p, vec!["world".to_string(), "rust".to_string()]);
    let prefix = side_singleton("hello_".to_string());

    let prefixed = words.map_with_singleton(&prefix, |w, pre| format!("{pre}{w}"));

    let mut out = prefixed.collect_par(Some(2), None)?;
    out.sort();
    assert_eq!(
        out,
        vec!["hello_rust".to_string(), "hello_world".to_string()]
    );
    Ok(())
}

#[test]
fn filter_with_singleton_threshold() -> Result<()> {
    let p = TestPipeline::new();
    let data = from_vec(&p, vec![1u32, 5, 10, 2, 8]);
    let threshold = side_singleton(5u32);

    let above = data.filter_with_singleton(&threshold, |x, t| x > t);

    let mut out = above.collect_par(Some(2), None)?;
    out.sort_unstable();
    assert_eq!(out, vec![8, 10]);
    Ok(())
}

#[test]
fn filter_with_singleton_all_pass() -> Result<()> {
    let p = TestPipeline::new();
    let data = from_vec(&p, vec![10u32, 20, 30]);
    let threshold = side_singleton(0u32);

    let above = data.filter_with_singleton(&threshold, |x, t| x > t);

    let mut out = above.collect_par(Some(2), None)?;
    out.sort_unstable();
    assert_eq!(out, vec![10, 20, 30]);
    Ok(())
}

#[test]
fn filter_with_singleton_none_pass() -> Result<()> {
    let p = TestPipeline::new();
    let data = from_vec(&p, vec![1u32, 2, 3]);
    let threshold = side_singleton(100u32);

    let above = data.filter_with_singleton(&threshold, |x, t| x > t);

    let out = above.collect_par(Some(2), None)?;
    assert!(out.is_empty());
    Ok(())
}

#[test]
fn singleton_can_be_shared_across_two_collections() -> Result<()> {
    let p = TestPipeline::new();
    let singleton = side_singleton(10u32);

    // Two independent input collections using the same singleton
    let data_a = from_vec(&p, vec![1u32, 2, 3]);
    let data_b = from_vec(&p, vec![1u32, 2, 3]);

    let a = data_a.map_with_singleton(&singleton, |x, m| x + m);
    let b = data_b.map_with_singleton(&singleton, |x, m| x * m);

    let mut out_a = a.collect_par(Some(2), None)?;
    let mut out_b = b.collect_par(Some(2), None)?;
    out_a.sort_unstable();
    out_b.sort_unstable();
    assert_eq!(out_a, vec![11, 12, 13]);
    assert_eq!(out_b, vec![10, 20, 30]);
    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// side_multimap / map_with_side_multimap / filter_with_side_multimap
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn side_multimap_groups_duplicate_keys() {
    let m = side_multimap(vec![("alice", "admin"), ("alice", "user"), ("bob", "user")]);
    let inner = &*m.0;
    let mut alice_tags = inner["alice"].clone();
    alice_tags.sort_unstable();
    assert_eq!(alice_tags, vec!["admin", "user"]);
    assert_eq!(inner["bob"], vec!["user"]);
}

#[test]
fn map_with_side_multimap_enriches_users() -> Result<()> {
    let p = TestPipeline::new();
    let users = from_vec(
        &p,
        vec!["alice".to_string(), "bob".to_string(), "carol".to_string()],
    );
    let tags = side_multimap(vec![
        ("alice".to_string(), "admin".to_string()),
        ("alice".to_string(), "user".to_string()),
        ("bob".to_string(), "user".to_string()),
    ]);

    let with_tags = users.map_with_side_multimap(&tags, |name, m| {
        let mut ts: Vec<String> = m.get(name).cloned().unwrap_or_default();
        ts.sort();
        (name.clone(), ts)
    });

    let out: HashMap<String, Vec<String>> =
        with_tags.collect_par(Some(2), None)?.into_iter().collect();

    let mut alice_tags = out["alice"].clone();
    alice_tags.sort();
    assert_eq!(alice_tags, vec!["admin".to_string(), "user".to_string()]);
    assert_eq!(out["bob"], vec!["user".to_string()]);
    assert_eq!(out.get("carol"), Some(&vec![])); // not in map → empty vec
    Ok(())
}

#[test]
fn map_with_side_multimap_count_tags() -> Result<()> {
    let p = TestPipeline::new();
    let users = from_vec(&p, vec!["alice".to_string(), "bob".to_string()]);
    let tags = side_multimap(vec![
        ("alice".to_string(), "a".to_string()),
        ("alice".to_string(), "b".to_string()),
        ("alice".to_string(), "c".to_string()),
        ("bob".to_string(), "x".to_string()),
    ]);

    let counts = users.map_with_side_multimap(&tags, |name, m| {
        (name.clone(), m.get(name).map_or(0, Vec::len))
    });

    let out: HashMap<String, usize> = counts.collect_par(Some(2), None)?.into_iter().collect();
    assert_eq!(out["alice"], 3);
    assert_eq!(out["bob"], 1);
    Ok(())
}

#[test]
fn filter_with_side_multimap_keeps_users_with_admin_tag() -> Result<()> {
    let p = TestPipeline::new();
    let users = from_vec(
        &p,
        vec!["alice".to_string(), "bob".to_string(), "carol".to_string()],
    );
    let tags = side_multimap(vec![
        ("alice".to_string(), "admin".to_string()),
        ("alice".to_string(), "user".to_string()),
        ("bob".to_string(), "user".to_string()),
    ]);

    let admins = users.filter_with_side_multimap(&tags, |name, m| {
        m.get(name)
            .is_some_and(|ts| ts.contains(&"admin".to_string()))
    });

    let out = admins.collect_par(Some(2), None)?;
    assert_eq!(out, vec!["alice".to_string()]);
    Ok(())
}

#[test]
fn filter_with_side_multimap_empty_map() -> Result<()> {
    let p = TestPipeline::new();
    let users = from_vec(&p, vec!["alice".to_string(), "bob".to_string()]);
    let tags = side_multimap::<String, String>(vec![]);

    let result = users.filter_with_side_multimap(&tags, |name, m| m.contains_key(name));

    let out = result.collect_par(Some(2), None)?;
    assert!(out.is_empty());
    Ok(())
}

#[test]
fn side_multimap_single_key_many_values() -> Result<()> {
    let p = TestPipeline::new();
    let keys = from_vec(&p, vec!["k".to_string()]);
    let mm = side_multimap(vec![
        ("k".to_string(), 1u32),
        ("k".to_string(), 2),
        ("k".to_string(), 3),
        ("k".to_string(), 4),
        ("k".to_string(), 5),
    ]);

    let sums = keys.map_with_side_multimap(&mm, |key, m| {
        let total: u32 = m.get(key).map_or(0, |vs| vs.iter().sum());
        (key.clone(), total)
    });

    let out = sums.collect_par(Some(1), None)?;
    assert_eq!(out, vec![("k".to_string(), 15u32)]);
    Ok(())
}
