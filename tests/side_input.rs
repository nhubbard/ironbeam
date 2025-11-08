use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use rustflow::{from_vec, side_hashmap, side_vec, Pipeline};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Product {
    sku: String,
}

#[test]
fn map_with_side_map_enriches_records() -> anyhow::Result<()> {
    let p = Pipeline::default();

    // Input stream of product SKUs
    let items = vec![
        Product { sku: "A".into() },
        Product { sku: "B".into() },
        Product { sku: "C".into() },
        Product { sku: "D".into() }, // not in price map
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
fn filter_with_side_allows_only_whitelisted() -> anyhow::Result<()> {
    let p = Pipeline::default();

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
