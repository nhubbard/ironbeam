//! Example demonstrating data quality and validation features.
//!
//! This example shows how to:
//! - Define validation rules for data types
//! - Handle bad records with different modes (skip/log/fail)
//! - Collect and report validation errors
//! - Use built-in validators for common patterns
//! - Integrate validation into a production ETL pipeline

use anyhow::Result;
use ironbeam::v5::{
    Element, ErrorCollector, Pipeline, Validate, ValidationMode, ValidationResult,
    combine_validations,
    prelude::*,
    validators::{in_range, is_email, max_length, min_length, not_empty},
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Transaction {
    id: String,
    customer_id: u32,
    amount: f64,
    currency: String,
    email: String,
    timestamp: u64,
}

impl Element for Transaction {
    const TYPE_ID: &'static str = "examples/validation-transaction";
    const CODER_ID: &'static str = "examples/validation-transaction-local";
}

impl Validate for Transaction {
    fn validate(&self) -> ValidationResult {
        // Combine multiple validation checks
        combine_validations(vec![
            not_empty("transaction_id", &self.id),
            not_empty("currency", &self.currency),
            in_range("amount", &self.amount, &0.0, &1_000_000.0),
            is_email("email", &self.email),
            min_length("currency", &self.currency, 3),
            max_length("currency", &self.currency, 3),
        ])
    }
}

fn main() -> Result<()> {
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

fn skip_invalid_example() -> Result<()> {
    let p = Pipeline::default();

    let transactions = p.from_vec(
        "skip-input",
        vec![
            Transaction {
                id: "tx001".into(),
                customer_id: 1,
                amount: 100.50,
                currency: "USD".into(),
                email: "alice@example.com".into(),
                timestamp: 1_234_567_890,
            },
            Transaction {
                id: String::new(), // Invalid: empty ID
                customer_id: 2,
                amount: 200.00,
                currency: "EUR".into(),
                email: "bob@example.com".into(),
                timestamp: 1_234_567_891,
            },
            Transaction {
                id: "tx003".into(),
                customer_id: 3,
                amount: -50.00, // Invalid: negative amount
                currency: "GBP".into(),
                email: "charlie@example.com".into(),
                timestamp: 1_234_567_892,
            },
            Transaction {
                id: "tx004".into(),
                customer_id: 4,
                amount: 300.00,
                currency: "USD".into(),
                email: "invalid-email".into(), // Invalid: bad email
                timestamp: 1_234_567_893,
            },
            Transaction {
                id: "tx005".into(),
                customer_id: 5,
                amount: 150.75,
                currency: "USD".into(),
                email: "eve@example.com".into(),
                timestamp: 1_234_567_894,
            },
        ],
    )?;

    let valid = transactions.validate_skip_invalid_named("skip-invalid")?;
    let results = valid.collect_seq()?;
    assert_eq!(
        results.iter().map(|tx| tx.id.as_str()).collect::<Vec<_>>(),
        ["tx001", "tx005"]
    );

    println!("  Input records: 5");
    println!("  Valid records: {}", results.len());
    println!("  Skipped records: {}", 5 - results.len());

    for tx in results {
        println!("    ✓ {} - ${:.2}", tx.id, tx.amount);
    }

    Ok(())
}

fn log_and_continue_example() -> Result<()> {
    let p = Pipeline::default();

    let transactions = p.from_vec(
        "log-input",
        vec![
            Transaction {
                id: "tx001".into(),
                customer_id: 1,
                amount: 100.50,
                currency: "USD".into(),
                email: "alice@example.com".into(),
                timestamp: 1_234_567_890,
            },
            Transaction {
                id: String::new(),
                customer_id: 2,
                amount: 200.00,
                currency: "EUR".into(),
                email: "bob@example.com".into(),
                timestamp: 1_234_567_891,
            },
            Transaction {
                id: "tx003".into(),
                customer_id: 3,
                amount: -50.00,
                currency: "GBP".into(),
                email: "charlie@example.com".into(),
                timestamp: 1_234_567_892,
            },
        ],
    )?;

    let collector = Arc::new(Mutex::new(ErrorCollector::new()));
    let valid = transactions.validate_with_mode_named(
        "log-invalid",
        ValidationMode::LogAndContinue,
        Some(Arc::clone(&collector)),
    )?;
    let results = valid.collect_seq()?;

    let errors = collector.lock().unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(errors.error_count(), 2);
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
            println!("      - {e}");
        }
    }

    // Export errors to JSON
    println!("\n  Exporting errors to JSON:");
    let json = errors.to_json()?;
    println!("{json}");
    drop(errors);
    Ok(())
}

fn production_etl_example() -> Result<()> {
    let p = Pipeline::default();

    // Simulate reading from a data source with mixed quality data
    let raw_transactions = p.from_vec(
        "etl-input",
        vec![
            Transaction {
                id: "tx001".into(),
                customer_id: 1,
                amount: 100.50,
                currency: "USD".into(),
                email: "alice@example.com".into(),
                timestamp: 1_234_567_890,
            },
            Transaction {
                id: "tx002".into(),
                customer_id: 2,
                amount: 5_000_000.00, // Invalid: exceeds limit
                currency: "USD".into(),
                email: "bob@example.com".into(),
                timestamp: 1_234_567_891,
            },
            Transaction {
                id: "tx003".into(),
                customer_id: 3,
                amount: 75.25,
                currency: "EUR".into(),
                email: "charlie@example.com".into(),
                timestamp: 1_234_567_892,
            },
        ],
    )?;

    // Set up an error collector
    let validation_errors = Arc::new(Mutex::new(ErrorCollector::new()));

    // Validate and filter bad records
    let clean_transactions = raw_transactions.validate_with_mode_named(
        "etl-validation",
        ValidationMode::LogAndContinue,
        Some(Arc::clone(&validation_errors)),
    )?;

    // Continue with ETL transformations
    let by_currency = clean_transactions
        .key_by_named("key-by-currency", |tx: &Transaction| tx.currency.clone())?
        .map_values_named("transaction-amount", |tx: Transaction| tx.amount)?
        .sum_per_key_named("currency-total")?;

    let results = by_currency.collect_seq()?;
    let totals: std::collections::HashMap<_, _> = results.iter().cloned().collect();
    assert_eq!(totals.get("USD"), Some(&100.50));
    assert_eq!(totals.get("EUR"), Some(&75.25));

    println!("  Total by Currency:");
    for (currency, total) in results {
        println!("    {currency}: ${total:.2}");
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
    drop(errors);
    Ok(())
}

fn validate_keyed_example() -> Result<()> {
    let p = Pipeline::default();

    // Customer transactions keyed by customer ID
    let customer_transactions = p.from_vec(
        "keyed-input",
        vec![
            (
                1u32,
                Transaction {
                    id: "tx001".into(),
                    customer_id: 1,
                    amount: 100.50,
                    currency: "USD".into(),
                    email: "alice@example.com".into(),
                    timestamp: 1_234_567_890,
                },
            ),
            (
                2u32,
                Transaction {
                    id: String::new(), // Invalid
                    customer_id: 2,
                    amount: 200.00,
                    currency: "EUR".into(),
                    email: "bob@example.com".into(),
                    timestamp: 1_234_567_891,
                },
            ),
            (
                1u32,
                Transaction {
                    id: "tx003".into(),
                    customer_id: 1,
                    amount: 50.00,
                    currency: "USD".into(),
                    email: "alice@example.com".into(),
                    timestamp: 1_234_567_892,
                },
            ),
        ],
    )?;

    // Validate only the values (transactions) while keeping keys (customer IDs)
    let valid_transactions =
        customer_transactions.validate_values_skip_invalid_named("keyed-validation")?;

    // Group by customer and sum amounts
    let customer_totals = valid_transactions
        .map_values_named("keyed-amount", |tx: Transaction| tx.amount)?
        .sum_per_key_named("customer-total")?;

    let results = customer_totals.collect_seq()?;
    assert_eq!(results, vec![(1, 150.50)]);

    println!("  Customer Totals (after validation):");
    for (customer_id, total) in results {
        println!("    Customer {customer_id}: ${total:.2}");
    }

    Ok(())
}
