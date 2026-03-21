//! Example demonstrating multi-way `CoGroupByKey` operations
//!
//! This example shows how to use the `cogroup_by_key!` macro to perform
//! N-way full outer joins on multiple keyed collections.

use anyhow::Result;
use ironbeam::{cogroup_by_key, Pipeline, PCollection, from_vec};

#[allow(clippy::too_many_lines)]
fn main() -> Result<()> {
    // Create a pipeline
    let p = Pipeline::default();

    // Example 1: 2-way cogroup - Simple user and order data
    println!("=== 2-Way CoGroupByKey Example ===\n");

    let users: PCollection<(String, String)> = from_vec(
        &p,
        vec![
            ("user1".to_string(), "Alice".to_string()),
            ("user2".to_string(), "Bob".to_string()),
            ("user3".to_string(), "Charlie".to_string()),
        ],
    );

    let orders: PCollection<(String, u32)> = from_vec(
        &p,
        vec![
            ("user1".to_string(), 100),
            ("user1".to_string(), 200),
            ("user2".to_string(), 150),
        ],
    );

    let cogroup2 = cogroup_by_key!(users, orders);

    println!("User and Order Data:");
    for (user_id, (user_names, order_amounts)) in cogroup2.collect_seq()? {
        println!(
            "User {}: {} names, {} orders (total: ${})",
            user_id,
            user_names.len(),
            order_amounts.len(),
            order_amounts.iter().sum::<u32>()
        );
    }

    // Example 2: 3-way cogroup - User, Orders, and Addresses
    println!("\n=== 3-Way CoGroupByKey Example ===\n");

    let users2: PCollection<(String, String)> = from_vec(
        &p,
        vec![
            ("user1".to_string(), "Alice".to_string()),
            ("user2".to_string(), "Bob".to_string()),
        ],
    );

    let orders2: PCollection<(String, u32)> = from_vec(
        &p,
        vec![
            ("user1".to_string(), 100),
            ("user1".to_string(), 200),
            ("user3".to_string(), 300),
        ],
    );

    let addresses: PCollection<(String, String)> = from_vec(
        &p,
        vec![
            ("user1".to_string(), "123 Main St".to_string()),
            ("user2".to_string(), "456 Oak Ave".to_string()),
        ],
    );

    let cogroup3 = cogroup_by_key!(users2, orders2, addresses);

    // Custom join logic: Create a report for each user
    let reports = cogroup3.map(|(user_id, (users, orders, addrs))| {
        let user_name = users.first().cloned().unwrap_or_else(|| "Unknown".to_string());
        let total_orders = orders.iter().sum::<u32>();
        let address = addrs
            .first()
            .cloned()
            .unwrap_or_else(|| "No address".to_string());

        format!(
            "{} ({}): {} orders totaling ${}, address: {}",
            user_id,
            user_name,
            orders.len(),
            total_orders,
            address
        )
    });

    println!("User Reports:");
    for report in reports.collect_seq()? {
        println!("  {report}");
    }

    // Example 3: 5-way cogroup - Demonstrating larger N-way joins
    println!("\n=== 5-Way CoGroupByKey Example ===\n");

    let c1: PCollection<(String, String)> = from_vec(
        &p,
        vec![("key1".to_string(), "data1".to_string())],
    );
    let c2: PCollection<(String, String)> = from_vec(
        &p,
        vec![("key1".to_string(), "data2".to_string())],
    );
    let c3: PCollection<(String, String)> = from_vec(
        &p,
        vec![("key1".to_string(), "data3".to_string())],
    );
    let c4: PCollection<(String, String)> = from_vec(
        &p,
        vec![("key1".to_string(), "data4".to_string())],
    );
    let c5: PCollection<(String, String)> = from_vec(
        &p,
        vec![("key1".to_string(), "data5".to_string())],
    );

    let cogroup5 = cogroup_by_key!(c1, c2, c3, c4, c5);

    println!("5-Way Cogroup Result:");
    for (key, (v1, v2, v3, v4, v5)) in cogroup5.collect_seq()? {
        println!(
            "Key: {key}, Values: {v1:?}, {v2:?}, {v3:?}, {v4:?}, {v5:?}"
        );
    }

    // Example 4: Filtering based on cogroup results
    println!("\n=== CoGroupByKey with Custom Filter ===\n");

    let products: PCollection<(String, String)> = from_vec(
        &p,
        vec![
            ("prod1".to_string(), "Widget".to_string()),
            ("prod2".to_string(), "Gadget".to_string()),
            ("prod3".to_string(), "Doohickey".to_string()),
        ],
    );

    let sales: PCollection<(String, u32)> = from_vec(
        &p,
        vec![
            ("prod1".to_string(), 50),
            ("prod1".to_string(), 30),
            ("prod2".to_string(), 10),
        ],
    );

    let reviews: PCollection<(String, u32)> = from_vec(
        &p,
        vec![
            ("prod1".to_string(), 5),
            ("prod1".to_string(), 4),
            ("prod3".to_string(), 3),
        ],
    );

    let cogroup = cogroup_by_key!(products, sales, reviews);

    // Filter to only products with both sales AND reviews
    let popular_products = cogroup.filter_map(|(product_id, (names, sales_list, reviews_list))| {
        if !sales_list.is_empty() && !reviews_list.is_empty() {
            let name = names.first()?.clone();
            let total_sales: u32 = sales_list.iter().sum();
            #[allow(clippy::cast_precision_loss)]
            let avg_rating = f64::from(reviews_list.iter().sum::<u32>()) / reviews_list.len() as f64;

            Some(format!(
                "{product_id} ({name}): {total_sales} sales, {avg_rating:.1} avg rating"
            ))
        } else {
            None
        }
    });

    println!("Popular Products (with both sales and reviews):");
    for product in popular_products.collect_seq()? {
        println!("  {product}");
    }

    Ok(())
}
