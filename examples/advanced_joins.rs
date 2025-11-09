//! Advanced join patterns and co-grouping example.
//!
//! Demonstrates:
//! - All four join types (inner, left, right, full)
//! - Multiple dataset joins
//! - Data enrichment patterns
//! - Handling mismatched keys
//!
//! Run with: cargo run --example advanced_joins

use anyhow::Result;
use rustflow::*;

fn main() -> Result<()> {
    println!("🔗 Advanced Joins and Co-Grouping Example\n");

    let pipeline = Pipeline::default();

    // Sample datasets: Users, Orders, and Products
    let users = from_vec(
        &pipeline,
        vec![
            (1u32, "Alice".to_string()),
            (2u32, "Bob".to_string()),
            (3u32, "Charlie".to_string()),
            (4u32, "Diana".to_string()),
        ],
    );

    // Orders stored as 3-tuples for demonstration
    let orders_raw = vec![
        (101u32, 1u32, "Product_A".to_string()), // Alice's order
        (102u32, 1u32, "Product_B".to_string()), // Alice's another order
        (103u32, 2u32, "Product_A".to_string()), // Bob's order
        (104u32, 5u32, "Product_C".to_string()), // Non-existent user
    ];

    let product_prices = from_vec(
        &pipeline,
        vec![
            ("Product_A".to_string(), 29.99),
            ("Product_B".to_string(), 49.99),
            ("Product_C".to_string(), 19.99),
            ("Product_D".to_string(), 99.99), // No orders for this
        ],
    );

    // =============================================================================
    // EXAMPLE 1: Inner Join - Only matching keys
    // =============================================================================
    println!("📊 Example 1: Inner Join (Users ⋈ Orders)");
    println!("Only shows users who have placed orders\n");

    // Create join-ready collection by keying orders by user_id
    let orders_by_user = from_vec(
        &pipeline,
        orders_raw
            .iter()
            .map(|(order_id, user_id, product)| (*user_id, (*order_id, product.clone())))
            .collect::<Vec<_>>(),
    );

    let user_orders = orders_by_user.clone().join_inner(&users.clone());

    let results = user_orders.collect_seq_sorted()?;
    for (_user_id, ((order_id, product), name)) in results {
        println!("  Order #{}: {} ordered {}", order_id, name, product);
    }

    // =============================================================================
    // EXAMPLE 2: Left Join - Keep all left records
    // =============================================================================
    println!("\n📊 Example 2: Left Join (Orders ⟕ Users)");
    println!("Shows all orders, even if user doesn't exist\n");

    let all_orders_with_users = orders_by_user.clone().join_left(&users.clone());

    let left_results = all_orders_with_users.collect_seq_sorted()?;
    for (user_id, ((order_id, product), maybe_name)) in left_results {
        match maybe_name {
            Some(name) => println!("  Order #{}: {} ordered {}", order_id, name, product),
            None => println!(
                "  Order #{}: Unknown user {} ordered {}",
                order_id, user_id, product
            ),
        }
    }

    // =============================================================================
    // EXAMPLE 3: Right Join - Keep all right records
    // =============================================================================
    println!("\n📊 Example 3: Right Join (Orders ⟖ Users)");
    println!("Shows all users, even those without orders\n");

    let users_with_maybe_orders = orders_by_user.clone().join_right(&users.clone());

    let right_results = users_with_maybe_orders.collect_seq_sorted()?;
    for (_user_id, (maybe_order, name)) in right_results {
        match maybe_order {
            Some((order_id, product)) => {
                println!("  {}: Order #{} for {}", name, order_id, product)
            }
            None => println!("  {}: No orders", name),
        }
    }

    // =============================================================================
    // EXAMPLE 4: Full Outer Join - Keep everything
    // =============================================================================
    println!("\n📊 Example 4: Full Outer Join (Orders ⟗ Users)");
    println!("Shows all orders and all users\n");

    let full_join = orders_by_user.clone().join_full(&users.clone());

    let full_results = full_join.collect_seq_sorted()?;
    for (user_id, (maybe_order, maybe_name)) in full_results {
        match (maybe_order, maybe_name) {
            (Some((order_id, product)), Some(name)) => {
                println!(
                    "  User {}: {} ordered {} (Order #{})",
                    user_id, name, product, order_id
                )
            }
            (Some((order_id, product)), None) => {
                println!(
                    "  User {}: Unknown user ordered {} (Order #{})",
                    user_id, product, order_id
                )
            }
            (None, Some(name)) => {
                println!("  User {}: {} has no orders", user_id, name)
            }
            (None, None) => unreachable!("Full join can't have both None"),
        }
    }

    // =============================================================================
    // EXAMPLE 5: Multi-way join (Orders + Users + Prices)
    // =============================================================================
    println!("\n📊 Example 5: Multi-way Join (Enrichment Pattern)");
    println!("Combine orders with user names and product prices\n");

    // Strategy: Instead of chaining joins (which creates nested CoGroup),
    // collect intermediate results and rejoin

    // Step 1: Join orders with users
    let orders_with_users = orders_by_user.clone().join_inner(&users);
    let step1_results = orders_with_users.collect_seq()?;

    // Step 2: Transform to (product, (order_id, user_id, name))
    let orders_by_product = from_vec(
        &pipeline,
        step1_results
            .into_iter()
            .map(|(_user_id, ((order_id, product), name))| (product, (order_id, name)))
            .collect::<Vec<_>>(),
    );

    // Step 3: Join with prices
    let complete_orders = orders_by_product.join_inner(&product_prices);

    let mut enriched = complete_orders.collect_seq()?;
    enriched.sort_by_key(|(_product, ((order_id, _name), _price))| *order_id);
    println!("Order# | Customer | Product   | Price");
    println!("{:-<50}", "");
    for (product, ((order_id, name), price)) in enriched {
        println!(
            "{:<7}| {:<9}| {:<10}| ${:.2}",
            order_id, name, product, price
        );
    }

    println!("\n✅ Joins Complete!");
    println!("\nKey Concepts:");
    println!("  • Inner join: Only matching keys");
    println!("  • Left join: All left + matching right");
    println!("  • Right join: All right + matching left");
    println!("  • Full join: Everything from both sides");
    println!("  • Multi-way joins: Chain joins for enrichment");

    Ok(())
}
