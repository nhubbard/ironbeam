//! Example demonstrating the multi-output (side outputs) pattern.
//!
//! This example shows how to split a data stream into multiple independent outputs
//! using enums and the `partition!` macro. This is Ironbeam's implementation of the
//! "side outputs" pattern from Apache Beam.
//!
//! Run with:
//! ```sh
//! cargo run --example multi_output
//! ```

#![allow(clippy::wildcard_imports)]
#![allow(clippy::too_many_lines)]

use anyhow::Result;
use ironbeam::*;

// Define an enum with variants for each output type
#[derive(Clone)]
enum RecordValidation {
    Valid(String),
    Warning(String),
    Error(String),
}

// User struct for data quality example
#[derive(Clone, Debug)]
struct User {
    id: u32,
    email: String,
    age: i32,
}

#[derive(Clone)]
enum UserValidation {
    Valid(User),
    InvalidEmail(User),
    InvalidAge(User),
    MultipleErrors(User),
}

fn main() -> Result<()> {
    println!("=== Multi-Output Pattern Example ===\n");

    let p = Pipeline::default();

    // Create sample data: web server log entries
    let logs = from_vec(
        &p,
        vec![
            "GET /api/users 200",
            "POST /api/login 401",
            "GET /api/data 500",
            "GET /index.html 200",
            "POST /api/upload 403",
            "GET /api/status 200",
            "DELETE /api/users/123 500",
        ],
    );

    // Step 1: Classify each log entry into an enum variant
    let classified = logs.flat_map(|log: &&str| {
        let parts: Vec<&str> = log.split_whitespace().collect();
        if parts.len() >= 3 {
            let status_code = parts[2];
            match status_code {
                "401" | "403" => vec![RecordValidation::Warning(log.to_string())],
                "500" => vec![RecordValidation::Error(log.to_string())],
                _ => vec![RecordValidation::Valid(log.to_string())],
            }
        } else {
            vec![RecordValidation::Error(format!("Invalid log format: {log}"))]
        }
    });

    // Step 2: Use the partition! macro to split into separate streams
    partition!(classified, RecordValidation, {
        Valid => successful_requests,
        Warning => auth_failures,
        Error => server_errors
    });

    // Now we have three independent collections that can be processed differently
    println!("Successful Requests (200):");
    for request in successful_requests.collect_seq()? {
        println!("  ✓ {request}");
    }

    println!("\nAuthentication/Authorization Failures (401/403):");
    for failure in auth_failures.collect_seq()? {
        println!("  ⚠ {failure}");
    }

    println!("\nServer Errors (500):");
    for error in server_errors.collect_seq()? {
        println!("  ✗ {error}");
    }

    println!("\n=== Advanced: Data Quality Pipeline ===\n");

    // More complex example: validating user records
    let p2 = Pipeline::default();
    let users = from_vec(
        &p2,
        vec![
            User {
                id: 1,
                email: "alice@example.com".into(),
                age: 30,
            },
            User {
                id: 2,
                email: "invalid_email".into(),
                age: 25,
            },
            User {
                id: 3,
                email: "bob@example.com".into(),
                age: -5,
            },
            User {
                id: 4,
                email: "no_at_sign".into(),
                age: 200,
            },
        ],
    );

    let validated = users.flat_map(|user: &User| {
        let has_valid_email = user.email.contains('@');
        let has_valid_age = user.age > 0 && user.age < 150;

        match (has_valid_email, has_valid_age) {
            (true, true) => vec![UserValidation::Valid(user.clone())],
            (false, true) => vec![UserValidation::InvalidEmail(user.clone())],
            (true, false) => vec![UserValidation::InvalidAge(user.clone())],
            (false, false) => vec![UserValidation::MultipleErrors(user.clone())],
        }
    });

    partition!(validated, UserValidation, {
        Valid => valid_users,
        InvalidEmail => email_errors,
        InvalidAge => age_errors,
        MultipleErrors => multiple_errors
    });

    println!("Valid Users:");
    for user in valid_users.collect_seq()? {
        println!("  ✓ User #{}: {} (age {})", user.id, user.email, user.age);
    }

    println!("\nInvalid Email:");
    for user in email_errors.collect_seq()? {
        println!("  ⚠ User #{}: {} (age {})", user.id, user.email, user.age);
    }

    println!("\nInvalid Age:");
    for user in age_errors.collect_seq()? {
        println!("  ⚠ User #{}: {} (age {})", user.id, user.email, user.age);
    }

    println!("\nMultiple Validation Errors:");
    for user in multiple_errors.collect_seq()? {
        println!("  ✗ User #{}: {} (age {})", user.id, user.email, user.age);
    }

    println!("\n=== Key Benefits ===");
    println!("1. Type-safe: All output types are known at compile time");
    println!("2. Explicit: Enum variants make the logic clear");
    println!("3. Independent processing: Each output can be handled differently");
    println!("4. Easy to extend: Just add new enum variants");

    Ok(())
}
