//! SQL Database I/O pipeline example (feature: `io-sql`).
//!
//! Demonstrates a full pipeline: seed a SQLite database, read rows into a `PCollection` via
//! [`ironbeam::read_sql`], transform them, and write the results back to a new table via
//! [`ironbeam::PCollection::write_sql_with`].
//!
//! Every low-level `io-sql` function opens and drops its own connection pool per call, and a
//! bare `sqlite::memory:` database only lives as long as a single connection stays open. So
//! this example uses a real SQLite database file in a temp directory (`?mode=rwc` to
//! auto-create it) instead, letting all of those independent connections observe the same
//! data. No Docker or external process is required.
//!
//! Run with:
//! ```bash
//! cargo run --example sql_pipeline --features io-sql
//! ```

use anyhow::Result;

#[cfg(feature = "io-sql")]
#[derive(Clone, Debug, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
struct Order {
    id: i64,
    customer: String,
    amount: f64,
}

#[cfg(feature = "io-sql")]
fn bind_order(mut sep: sqlx::query_builder::Separated<'_, sqlx::Any, &'static str>, row: &Order) {
    sep.push_bind(row.id)
        .push_bind(row.customer.clone())
        .push_bind(row.amount);
}

/// Run arbitrary DDL against `url` using a throwaway connection pool, independent of the
/// crate's internal SQL runtime (which only ever runs reads/writes, never DDL).
#[cfg(feature = "io-sql")]
fn create_table(url: &str, ddl: &str) -> Result<()> {
    sqlx::any::install_default_drivers();
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let pool = sqlx::AnyPool::connect(url).await?;
        sqlx::query(sqlx::AssertSqlSafe(ddl.to_owned()))
            .execute(&pool)
            .await?;
        Ok::<(), sqlx::Error>(())
    })?;
    Ok(())
}

#[cfg(feature = "io-sql")]
fn main() -> Result<()> {
    use ironbeam::{Pipeline, from_vec, read_sql};

    println!("🚀 SQL Pipeline Example: Order Processing\n");

    let tmp = tempfile::tempdir()?;
    let db_url = format!(
        "sqlite://{}?mode=rwc",
        tmp.path().join("orders.db").display()
    );

    println!(
        "📐 Creating tables in a SQLite database at {}...",
        tmp.path().display()
    );
    create_table(
        &db_url,
        "CREATE TABLE orders (id INTEGER, customer TEXT, amount REAL)",
    )?;
    create_table(
        &db_url,
        "CREATE TABLE big_orders (id INTEGER, customer TEXT, amount REAL)",
    )?;
    println!("  ✓ Tables created\n");

    let p = Pipeline::default();

    println!("📥 Seeding the `orders` table...");
    let seed = from_vec(
        &p,
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
    );
    let seeded = seed.write_sql_with(
        &db_url,
        "INSERT INTO orders (id, customer, amount)",
        bind_order,
    )?;
    println!("  ✓ Inserted {seeded} orders\n");

    println!("📖 Reading + transforming orders over $50...");
    let big_orders = read_sql::<Order>(
        &p,
        &db_url,
        "SELECT id, customer, amount FROM orders WHERE amount > 50",
    )?
    .map(|o: &Order| Order {
        id: o.id,
        customer: o.customer.to_uppercase(),
        amount: o.amount,
    });

    println!("💾 Writing results to `big_orders`...");
    let written = big_orders.write_sql_with(
        &db_url,
        "INSERT INTO big_orders (id, customer, amount)",
        bind_order,
    )?;
    println!("  ✓ Wrote {written} big orders\n");

    println!("📊 Final contents of `big_orders`:");
    let result = read_sql::<Order>(
        &p,
        &db_url,
        "SELECT id, customer, amount FROM big_orders ORDER BY id",
    )?
    .collect_seq()?;
    for order in &result {
        println!(
            "  #{:<3} {:<10} ${:.2}",
            order.id, order.customer, order.amount
        );
    }

    println!("\n✅ SQL Pipeline Complete!");
    Ok(())
}

#[cfg(not(feature = "io-sql"))]
fn main() {
    println!("This example requires the 'io-sql' feature.");
    println!("Run with: cargo run --example sql_pipeline --features io-sql");
}
