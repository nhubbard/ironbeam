//! Two-, three-, and five-input `CoGroupByKey` with heterogeneous values.
//!
//! Run with `cargo run --example co_gbk`.

use ironbeam::v5::{Pipeline, cogroup_by_key_named, prelude::*};

// Keep the three arities together so the example can be read top to bottom.
#[allow(clippy::too_many_lines)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let p = Pipeline::default();
    let users = p.from_vec(
        "users",
        vec![
            ("user1".to_string(), "Alice".to_string()),
            ("user2".to_string(), "Bob".to_string()),
            ("user3".to_string(), "Charlie".to_string()),
        ],
    )?;
    let orders = p.from_vec(
        "orders",
        vec![
            ("user1".to_string(), 100_u32),
            ("user1".to_string(), 200),
            ("user2".to_string(), 150),
        ],
    )?;
    let mut pairs = cogroup_by_key_named!("users-orders", users, orders)?
        .output("pairs-result")?
        .collect_seq()?;
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(pairs.len(), 3);
    assert_eq!(pairs[0].1.1, vec![100, 200]);
    assert_eq!(pairs[2].1.1, Vec::<u32>::new());
    for (user, (names, amounts)) in &pairs {
        println!("{user}: {names:?}, orders {amounts:?}");
    }

    let addresses = p.from_vec(
        "addresses",
        vec![
            ("user1".to_string(), "123 Main St".to_string()),
            ("user2".to_string(), "456 Oak Ave".to_string()),
        ],
    )?;
    let mut reports = cogroup_by_key_named!("users-orders-addresses", users, orders, addresses)?
        .map_named("format-reports", |(id, (names, amounts, addresses))| {
            format!(
                "{id} ({}): {} order{} totaling ${}, address: {}",
                names.first().map_or("Unknown", String::as_str),
                amounts.len(),
                if amounts.len() == 1 { "" } else { "s" },
                amounts.iter().sum::<u32>(),
                addresses.first().map_or("No address", String::as_str),
            )
        })?
        .output("reports-result")?
        .collect_seq()?;
    reports.sort();
    assert_eq!(reports.len(), 3);
    assert!(reports[0].contains("totaling $300"));
    for report in &reports {
        println!("{report}");
    }

    let c1 = p.from_vec("five-1", vec![("key".to_string(), "one".to_string())])?;
    let c2 = p.from_vec("five-2", vec![("key".to_string(), 2_u32)])?;
    let c3 = p.from_vec("five-3", vec![("key".to_string(), true)])?;
    let c4 = p.from_vec("five-4", vec![("key".to_string(), -4_i32)])?;
    let c5 = p.from_vec("five-5", vec![("key".to_string(), 5_u64)])?;
    let five = cogroup_by_key_named!("five-way", c1, c2, c3, c4, c5)?
        .output("five-result")?
        .collect_seq()?;
    assert_eq!(five.len(), 1);
    assert_eq!(
        five[0].1,
        (
            vec!["one".to_string()],
            vec![2],
            vec![true],
            vec![-4],
            vec![5],
        )
    );
    println!("Five-way co-group: {five:?}");

    let products = p.from_vec(
        "products",
        vec![
            ("prod1".to_string(), "Widget".to_string()),
            ("prod2".to_string(), "Gadget".to_string()),
            ("prod3".to_string(), "Doohickey".to_string()),
        ],
    )?;
    let sales = p.from_vec(
        "sales",
        vec![
            ("prod1".to_string(), 50_u32),
            ("prod1".to_string(), 30),
            ("prod2".to_string(), 10),
        ],
    )?;
    let reviews = p.from_vec(
        "reviews",
        vec![
            ("prod1".to_string(), 5_u32),
            ("prod1".to_string(), 4),
            ("prod3".to_string(), 3),
        ],
    )?;
    let popular = cogroup_by_key_named!("product-sales-reviews", products, sales, reviews)?
        .filter_map_named("popular-products", |(id, (names, sales, ratings))| {
            if sales.is_empty() || ratings.is_empty() {
                return None;
            }
            let name = names.first()?.clone();
            Some(format!(
                "{id} ({name}): {} sales",
                sales.iter().sum::<u32>()
            ))
        })?
        .output("popular-result")?
        .collect_seq()?;
    assert_eq!(popular, vec!["prod1 (Widget): 80 sales".to_string()]);
    println!("Popular: {popular:?}");
    Ok(())
}
