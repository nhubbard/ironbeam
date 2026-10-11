//! Read, transform, and write SQLite rows through the v5 SQL extension.
//!
//! Run with `cargo run --example sql_pipeline --features io-sql`.

use ironbeam::v5::{
    DirectRunner, Element, JobConfig, Pipeline, prelude::*, sql_sink_with, sql_source,
};
use sqlx::{AnyPool, FromRow};

#[derive(Clone, Debug, PartialEq, FromRow)]
struct Order {
    id: i64,
    customer: String,
    amount: f64,
}

impl Element for Order {
    const TYPE_ID: &'static str = "examples/sql-order";
    const CODER_ID: &'static str = "examples/sql-order-local";
}

fn bind_order(
    mut separated: sqlx::query_builder::Separated<'_, sqlx::Any, &'static str>,
    row: &Order,
) {
    separated
        .push_bind(row.id)
        .push_bind(row.customer.clone())
        .push_bind(row.amount);
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    sqlx::any::install_default_drivers();
    let dir = tempfile::tempdir()?;
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("orders.db").display()
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let pool = AnyPool::connect(&url).await?;
        sqlx::query("CREATE TABLE orders (id INTEGER, customer TEXT, amount REAL)")
            .execute(&pool)
            .await?;
        sqlx::query("CREATE TABLE big_orders (id INTEGER, customer TEXT, amount REAL)")
            .execute(&pool)
            .await?;
        Ok::<(), sqlx::Error>(())
    })?;

    let seed = Pipeline::default();
    let input = seed.from_vec(
        "seed-orders",
        vec![
            Order {
                id: 1,
                customer: "Alice".into(),
                amount: 120.0,
            },
            Order {
                id: 2,
                customer: "Bob".into(),
                amount: 45.5,
            },
            Order {
                id: 3,
                customer: "Carol".into(),
                amount: 300.25,
            },
            Order {
                id: 4,
                customer: "Dave".into(),
                amount: 12.75,
            },
        ],
    )?;
    DirectRunner.run_sink(
        &sql_sink_with(
            &input,
            "write-seed",
            &url,
            "INSERT INTO orders (id, customer, amount)",
            bind_order,
        )?,
        &JobConfig::default(),
    )?;

    let processing = Pipeline::default();
    let selected = sql_source::<Order>(
        &processing,
        "read-orders",
        &url,
        "SELECT id, customer, amount FROM orders WHERE amount > 50",
    )?
    .map_named("uppercase-customers", |order| Order {
        id: order.id,
        customer: order.customer.to_uppercase(),
        amount: order.amount,
    })?;
    DirectRunner.run_sink(
        &sql_sink_with(
            &selected,
            "write-big-orders",
            &url,
            "INSERT INTO big_orders (id, customer, amount)",
            bind_order,
        )?,
        &JobConfig::default(),
    )?;

    let verify = Pipeline::default();
    let rows = sql_source::<Order>(
        &verify,
        "verify",
        &url,
        "SELECT id, customer, amount FROM big_orders ORDER BY id",
    )?
    .collect_seq()?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].customer, "ALICE");
    assert_eq!(rows[1].customer, "CAROL");
    for row in rows {
        println!("#{:3} {:10} ${:.2}", row.id, row.customer, row.amount);
    }
    Ok(())
}
