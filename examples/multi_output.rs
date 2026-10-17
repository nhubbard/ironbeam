//! Example demonstrating the multi-output (side outputs) pattern.
//!
//! This example classifies each bounded input once and returns typed result buckets.
//! These buckets are local terminal results; graph-level side outputs remain a
//! separate runner capability.
//!
//! Run with:
//! ```sh
//! cargo run --example multi_output
//! ```

#![allow(clippy::too_many_lines)]

use anyhow::Result;
use ironbeam::v5::{Element, Error, JobConfig, Partition3, Partition4, Pipeline, prelude::*};
use serde::{Deserialize, Serialize};

// Define an enum with variants for each output type
#[derive(Clone, Serialize, Deserialize)]
enum RecordValidation {
    Valid(String),
    Warning(String),
    Error(String),
}

impl Element for RecordValidation {
    const TYPE_ID: &'static str = "examples/record-validation/v1";
    const CODER_ID: &'static str = "examples/record-validation/json-v1";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> std::result::Result<Vec<u8>, Error> {
        serde_json::to_vec(self).map_err(|error| Error::incompatible(error.to_string()))
    }
    fn decode(bytes: &[u8]) -> std::result::Result<Self, Error> {
        serde_json::from_slice(bytes).map_err(|error| Error::incompatible(error.to_string()))
    }
}

// User struct for data quality example
#[derive(Clone, Debug, Serialize, Deserialize)]
struct User {
    id: u32,
    email: String,
    age: i32,
}

#[derive(Clone, Serialize, Deserialize)]
enum UserValidation {
    Valid(User),
    InvalidEmail(User),
    InvalidAge(User),
    MultipleErrors(User),
}

impl Element for User {
    const TYPE_ID: &'static str = "examples/validated-user/v1";
    const CODER_ID: &'static str = "examples/validated-user/json-v1";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> std::result::Result<Vec<u8>, Error> {
        serde_json::to_vec(self).map_err(|error| Error::incompatible(error.to_string()))
    }
    fn decode(bytes: &[u8]) -> std::result::Result<Self, Error> {
        serde_json::from_slice(bytes).map_err(|error| Error::incompatible(error.to_string()))
    }
}

impl Element for UserValidation {
    const TYPE_ID: &'static str = "examples/user-validation/v1";
    const CODER_ID: &'static str = "examples/user-validation/json-v1";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> std::result::Result<Vec<u8>, Error> {
        serde_json::to_vec(self).map_err(|error| Error::incompatible(error.to_string()))
    }
    fn decode(bytes: &[u8]) -> std::result::Result<Self, Error> {
        serde_json::from_slice(bytes).map_err(|error| Error::incompatible(error.to_string()))
    }
}

fn main() -> Result<()> {
    println!("=== Multi-Output Pattern Example ===\n");

    let p = Pipeline::default();

    // Create sample data: web server log entries
    let logs = p.from_vec(
        "logs",
        vec![
            "GET /api/users 200".to_string(),
            "POST /api/login 401".to_string(),
            "GET /api/data 500".to_string(),
            "GET /index.html 200".to_string(),
            "POST /api/upload 403".to_string(),
            "GET /api/status 200".to_string(),
            "DELETE /api/users/123 500".to_string(),
        ],
    )?;

    // Step 1: Classify each log entry into an enum variant
    let classified = logs.map_named("classify-logs", |log| {
        let parts: Vec<&str> = log.split_whitespace().collect();
        if parts.len() >= 3 {
            let status_code = parts[2];
            match status_code {
                "401" | "403" => RecordValidation::Warning(log),
                "500" => RecordValidation::Error(log),
                _ => RecordValidation::Valid(log),
            }
        } else {
            RecordValidation::Error(format!("Invalid log format: {log}"))
        }
    })?;

    let (successful_requests, auth_failures, server_errors) =
        classified.collect_partitioned3_named("log-results", &JobConfig::default(), |record| {
            match record {
                RecordValidation::Valid(value) => Partition3::First(value),
                RecordValidation::Warning(value) => Partition3::Second(value),
                RecordValidation::Error(value) => Partition3::Third(value),
            }
        })?;

    println!("Successful Requests (200):");
    for request in successful_requests {
        println!("  ✓ {request}");
    }

    println!("\nAuthentication/Authorization Failures (401/403):");
    for failure in auth_failures {
        println!("  ⚠ {failure}");
    }

    println!("\nServer Errors (500):");
    for error in server_errors {
        println!("  ✗ {error}");
    }

    println!("\n=== Advanced: Data Quality Pipeline ===\n");

    // More complex example: validating user records
    let p2 = Pipeline::default();
    let users = p2.from_vec(
        "users",
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
    )?;

    let validated = users.map_named("validate-users", |user| {
        let has_valid_email = user.email.contains('@');
        let has_valid_age = user.age > 0 && user.age < 150;

        match (has_valid_email, has_valid_age) {
            (true, true) => UserValidation::Valid(user),
            (false, true) => UserValidation::InvalidEmail(user),
            (true, false) => UserValidation::InvalidAge(user),
            (false, false) => UserValidation::MultipleErrors(user),
        }
    })?;

    let (valid_users, email_errors, age_errors, multiple_errors) =
        validated.collect_partitioned4_named("user-results", &JobConfig::default(), |result| {
            match result {
                UserValidation::Valid(value) => Partition4::First(value),
                UserValidation::InvalidEmail(value) => Partition4::Second(value),
                UserValidation::InvalidAge(value) => Partition4::Third(value),
                UserValidation::MultipleErrors(value) => Partition4::Fourth(value),
            }
        })?;

    println!("Valid Users:");
    for user in valid_users {
        println!("  ✓ User #{}: {} (age {})", user.id, user.email, user.age);
    }

    println!("\nInvalid Email:");
    for user in email_errors {
        println!("  ⚠ User #{}: {} (age {})", user.id, user.email, user.age);
    }

    println!("\nInvalid Age:");
    for user in age_errors {
        println!("  ⚠ User #{}: {} (age {})", user.id, user.email, user.age);
    }

    println!("\nMultiple Validation Errors:");
    for user in multiple_errors {
        println!("  ✗ User #{}: {} (age {})", user.id, user.email, user.age);
    }

    println!("\n=== Key Benefits ===");
    println!("1. Type-safe: All output types are known at compile time");
    println!("2. Explicit: Enum variants make the logic clear");
    println!("3. One Direct job: Classification runs once per input");
    println!("4. Easy to extend: Just add new enum variants");

    Ok(())
}
