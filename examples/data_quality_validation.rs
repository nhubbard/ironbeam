//! Example demonstrating data quality and validation features.
//!
//! This example shows how to:
//! - Define validation rules for data types
//! - Handle bad records with different modes (skip/log/fail)
//! - Collect and report validation errors
//! - Use built-in validators for common patterns
//! - Integrate validation into a production ETL pipeline

use rustflow::validation::*;
use rustflow::*;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Transaction {
    transaction_id: String,
    customer_id: u32,
    amount: f64,
    currency: String,
    email: String,
    timestamp: u64,
}

impl Validate for Transaction {
    fn validate(&self) -> ValidationResult {
        // Combine multiple validation checks
        combine_validations(vec![
            validators::not_empty("transaction_id", &self.transaction_id),
            validators::not_empty("currency", &self.currency),
            validators::in_range("amount", &self.amount, &0.0, &1_000_000.0),
            validators::is_email("email", &self.email),
            validators::min_length("currency", &self.currency, 3),
            validators::max_length("currency", &self.currency, 3),
        ])
    }
}

fn main() -> anyhow::Result<()> {
    println!("=== Data Quality & Validation Example ===\n");

    // Example 1: Skip invalid records silently
    println!("1. Skip Invalid Records Mode:");
    skip_invalid_example()?;

    // Example 2: Log and continue processing
    println!("\n2. Log and Continue Mode:");
    log_and_continue_example()?;

    // Example 3: Production ETL pipeline with validation
    println!("\n3. Production ETL Pipeline:");
    production_etl_example()?;

    // Example 4: Validate keyed collections
    println!("\n4. Validate Keyed Collections:");
    validate_keyed_example()?;

    Ok(())
}

fn skip_invalid_example() -> anyhow::Result<()> {
    let p = Pipeline::default();

    let transactions = from_vec(
        &p,
        vec![
            Transaction {
                transaction_id: "tx001".into(),
                customer_id: 1,
                amount: 100.50,
                currency: "USD".into(),
                email: "alice@example.com".into(),
                timestamp: 1234567890,
            },
            Transaction {
                transaction_id: "".into(), // Invalid: empty ID
                customer_id: 2,
                amount: 200.00,
                currency: "EUR".into(),
                email: "bob@example.com".into(),
                timestamp: 1234567891,
            },
            Transaction {
                transaction_id: "tx003".into(),
                customer_id: 3,
                amount: -50.00, // Invalid: negative amount
                currency: "GBP".into(),
                email: "charlie@example.com".into(),
                timestamp: 1234567892,
            },
            Transaction {
                transaction_id: "tx004".into(),
                customer_id: 4,
                amount: 300.00,
                currency: "USD".into(),
                email: "invalid-email".into(), // Invalid: bad email
                timestamp: 1234567893,
            },
            Transaction {
                transaction_id: "tx005".into(),
                customer_id: 5,
                amount: 150.75,
                currency: "USD".into(),
                email: "eve@example.com".into(),
                timestamp: 1234567894,
            },
        ],
    );

    let valid = transactions.validate_skip_invalid();
    let results = valid.collect_seq()?;

    println!("  Input records: 5");
    println!("  Valid records: {}", results.len());
    println!("  Skipped records: {}", 5 - results.len());

    for tx in results {
        println!("    ✓ {} - ${:.2}", tx.transaction_id, tx.amount);
    }

    Ok(())
}

fn log_and_continue_example() -> anyhow::Result<()> {
    let p = Pipeline::default();

    let transactions = from_vec(
        &p,
        vec![
            Transaction {
                transaction_id: "tx001".into(),
                customer_id: 1,
                amount: 100.50,
                currency: "USD".into(),
                email: "alice@example.com".into(),
                timestamp: 1234567890,
            },
            Transaction {
                transaction_id: "".into(),
                customer_id: 2,
                amount: 200.00,
                currency: "EUR".into(),
                email: "bob@example.com".into(),
                timestamp: 1234567891,
            },
            Transaction {
                transaction_id: "tx003".into(),
                customer_id: 3,
                amount: -50.00,
                currency: "GBP".into(),
                email: "charlie@example.com".into(),
                timestamp: 1234567892,
            },
        ],
    );

    let collector = Arc::new(Mutex::new(ErrorCollector::new()));
    let valid = transactions
        .validate_with_mode(ValidationMode::LogAndContinue, Some(Arc::clone(&collector)));
    let results = valid.collect_seq()?;

    let errors = collector.lock().unwrap();
    println!("  Input records: 3");
    println!("  Valid records: {}", results.len());
    println!("  Validation errors: {}\n", errors.error_count());

    println!("  Error Details:");
    for (idx, error) in errors.errors().iter().enumerate() {
        println!(
            "    Error #{}: {}",
            idx + 1,
            error.record_id.as_ref().unwrap()
        );
        for e in &error.errors {
            println!("      - {}", e);
        }
    }

    // Export errors to JSON
    println!("\n  Exporting errors to JSON:");
    let json = errors.to_json()?;
    println!("{}", json);

    Ok(())
}

fn production_etl_example() -> anyhow::Result<()> {
    let p = Pipeline::default();

    // Simulate reading from a data source with mixed quality data
    let raw_transactions = from_vec(
        &p,
        vec![
            Transaction {
                transaction_id: "tx001".into(),
                customer_id: 1,
                amount: 100.50,
                currency: "USD".into(),
                email: "alice@example.com".into(),
                timestamp: 1234567890,
            },
            Transaction {
                transaction_id: "tx002".into(),
                customer_id: 2,
                amount: 5000000.00, // Invalid: exceeds limit
                currency: "USD".into(),
                email: "bob@example.com".into(),
                timestamp: 1234567891,
            },
            Transaction {
                transaction_id: "tx003".into(),
                customer_id: 3,
                amount: 75.25,
                currency: "EUR".into(),
                email: "charlie@example.com".into(),
                timestamp: 1234567892,
            },
        ],
    );

    // Set up error collection
    let validation_errors = Arc::new(Mutex::new(ErrorCollector::new()));

    // Validate and filter bad records
    let clean_transactions = raw_transactions.validate_with_mode(
        ValidationMode::LogAndContinue,
        Some(Arc::clone(&validation_errors)),
    );

    // Continue with ETL transformations
    let by_currency = clean_transactions
        .key_by(|tx: &Transaction| tx.currency.clone())
        .map_values(|tx: &Transaction| tx.amount)
        .combine_values(Sum::<f64>::default());

    let results = by_currency.collect_seq()?;

    println!("  Total by Currency:");
    for (currency, total) in results {
        println!("    {}: ${:.2}", currency, total);
    }

    let errors = validation_errors.lock().unwrap();
    println!("\n  Data Quality Report:");
    println!("    - Records processed: 3");
    println!("    - Valid records: 2");
    println!("    - Invalid records: {}", errors.error_count());

    if errors.error_count() > 0 {
        println!("\n  Validation Errors:");
        errors.print_errors();
    }

    Ok(())
}

fn validate_keyed_example() -> anyhow::Result<()> {
    let p = Pipeline::default();

    // Customer transactions keyed by customer ID
    let customer_transactions = from_vec(
        &p,
        vec![
            (
                1u32,
                Transaction {
                    transaction_id: "tx001".into(),
                    customer_id: 1,
                    amount: 100.50,
                    currency: "USD".into(),
                    email: "alice@example.com".into(),
                    timestamp: 1234567890,
                },
            ),
            (
                2u32,
                Transaction {
                    transaction_id: "".into(), // Invalid
                    customer_id: 2,
                    amount: 200.00,
                    currency: "EUR".into(),
                    email: "bob@example.com".into(),
                    timestamp: 1234567891,
                },
            ),
            (
                1u32,
                Transaction {
                    transaction_id: "tx003".into(),
                    customer_id: 1,
                    amount: 50.00,
                    currency: "USD".into(),
                    email: "alice@example.com".into(),
                    timestamp: 1234567892,
                },
            ),
        ],
    );

    // Validate only the values (transactions) while keeping keys (customer IDs)
    let valid_transactions = customer_transactions.validate_values_skip_invalid();

    // Group by customer and sum amounts
    let customer_totals = valid_transactions
        .map_values(|tx: &Transaction| tx.amount)
        .combine_values(Sum::<f64>::default());

    let results = customer_totals.collect_seq()?;

    println!("  Customer Totals (after validation):");
    for (customer_id, total) in results {
        println!("    Customer {}: ${:.2}", customer_id, total);
    }

    Ok(())
}
