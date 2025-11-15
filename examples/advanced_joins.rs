//! Advanced join patterns and co-grouping example.
//!
//! Demonstrates:
//! - All four join types (inner, left, right, full)
//! - Multiple dataset joins
//! - Data enrichment patterns
//! - Handling mismatched keys
//!
//! Run with: `cargo run --example advanced_joins`

use anyhow::Result;
use ironbeam::{Pipeline, from_vec};

#[allow(clippy::too_many_lines)]
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
    let orders_raw = [
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

    let user_orders = orders_by_user.join_inner(&users);

    let results = user_orders.collect_seq_sorted()?;
    for (_user_id, ((order_id, product), name)) in results {
        println!("  Order #{order_id}: {name} ordered {product}");
    }

    // =============================================================================
    // EXAMPLE 2: Left Join - Keep all left records
    // =============================================================================
    println!("\n📊 Example 2: Left Join (Orders ⟕ Users)");
    println!("Shows all orders, even if user doesn't exist\n");

    let all_orders_with_users = orders_by_user.join_left(&users);

    let left_results = all_orders_with_users.collect_seq_sorted()?;
    for (user_id, ((order_id, product), maybe_name)) in left_results {
        match maybe_name {
            Some(name) => println!("  Order #{order_id}: {name} ordered {product}"),
            None => println!(
                "  Order #{order_id}: Unknown user {user_id} ordered {product}"
            ),
        }
    }

    // =============================================================================
    // EXAMPLE 3: Right Join - Keep all right records
    // =============================================================================
    println!("\n📊 Example 3: Right Join (Orders ⟖ Users)");
    println!("Shows all users, even those without orders\n");

    let users_with_maybe_orders = orders_by_user.join_right(&users);

    let right_results = users_with_maybe_orders.collect_seq_sorted()?;
    for (_user_id, (maybe_order, name)) in right_results {
        match maybe_order {
            Some((order_id, product)) => {
                println!("  {name}: Order #{order_id} for {product}");
            }
            None => println!("  {name}: No orders"),
        }
    }

    // =============================================================================
    // EXAMPLE 4: Full Outer Join - Keep everything
    // =============================================================================
    println!("\n📊 Example 4: Full Outer Join (Orders ⟗ Users)");
    println!("Shows all orders and all users\n");

    let full_join = orders_by_user.join_full(&users);

    let full_results = full_join.collect_seq_sorted()?;
    for (user_id, (maybe_order, maybe_name)) in full_results {
        match (maybe_order, maybe_name) {
            (Some((order_id, product)), Some(name)) => {
                println!(
                    "  User {user_id}: {name} ordered {product} (Order #{order_id})"
                );
            }
            (Some((order_id, product)), None) => {
                println!(
                    "  User {user_id}: Unknown user ordered {product} (Order #{order_id})"
                );
            }
            (None, Some(name)) => {
                println!("  User {user_id}: {name} has no orders");
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
    let orders_with_users = orders_by_user.join_inner(&users);
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
            "{order_id:<7}| {name:<9}| {product:<10}| ${price:.2}"
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
