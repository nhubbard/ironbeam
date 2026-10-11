//! Inner, left, right, full, and chained joins on the v5 graph.
//!
//! Run with `cargo run --example advanced_joins`.

use ironbeam::v5::{Pipeline, prelude::*};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipeline = Pipeline::default();
    let users = pipeline.from_vec("users", vec![
        (1_u32, "Alice".to_string()),
        (2, "Bob".to_string()),
        (3, "Charlie".to_string()),
        (4, "Diana".to_string()),
    ])?;
    let orders = pipeline.from_vec("orders", vec![
        (1_u32, (101_u32, "Product_A".to_string())),
        (1, (102, "Product_B".to_string())),
        (2, (103, "Product_A".to_string())),
        (5, (104, "Product_C".to_string())),
    ])?;
    let prices = pipeline.from_vec("prices", vec![
        ("Product_A".to_string(), 29.99_f64),
        ("Product_B".to_string(), 49.99),
        ("Product_C".to_string(), 19.99),
        ("Product_D".to_string(), 99.99),
    ])?;

    let mut inner = orders.join_inner_named("inner-orders-users", &users)?
        .output("inner-result")?.collect_seq()?;
    inner.sort_by_key(|(_, ((order_id, _), _))| *order_id);
    assert_eq!(inner.len(), 3);
    for (_, ((order_id, product), name)) in &inner {
        println!("Inner: order #{order_id}: {name} ordered {product}");
    }

    let mut left = orders.join_left_named("left-orders-users", &users)?
        .output("left-result")?.collect_seq()?;
    left.sort_by_key(|(_, ((order_id, _), _))| *order_id);
    assert_eq!(left.len(), 4);
    assert!(left.iter().any(|(id, (_, name))| *id == 5 && name.is_none()));

    let mut right = orders.join_right_named("right-orders-users", &users)?
        .output("right-result")?.collect_seq()?;
    right.sort_by_key(|(id, (order, _))| (*id, order.as_ref().map_or(0, |(number, _)| *number)));
    assert_eq!(right.len(), 5);
    assert!(right.iter().any(|(id, (order, _))| *id == 4 && order.is_none()));

    let mut full = orders.join_full_named("full-orders-users", &users)?
        .output("full-result")?.collect_seq()?;
    full.sort_by_key(|(id, (order, _))| (*id, order.as_ref().map_or(0, |(number, _)| *number)));
    assert_eq!(full.len(), 6);
    assert!(full.iter().any(|(id, (order, name))| *id == 5 && order.is_some() && name.is_none()));
    assert!(full.iter().any(|(id, (order, name))| *id == 4 && order.is_none() && name.is_some()));

    // The second join consumes a graph node directly.
    let by_product = orders.join_inner_named("enrich-users", &users)?
        .map_named("key-by-product", |(_, ((order_id, product), name))| {
            (product, (order_id, name))
        })?;
    let mut enriched = by_product.join_inner_named("enrich-prices", &prices)?
        .output("enriched-result")?.collect_seq()?;
    enriched.sort_by_key(|(_, ((order_id, _), _))| *order_id);
    assert_eq!(enriched.len(), 3);
    println!("Order# | Customer | Product   | Price");
    for (product, ((order_id, name), price)) in enriched {
        println!("{order_id:<7}| {name:<9}| {product:<10}| ${price:.2}");
    }
    println!("Verified inner, left, right, full, and chained joins.");
    Ok(())
}
