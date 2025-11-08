//! Data quality and validation utilities for production pipelines.
//!
//! This module provides tools for handling bad data gracefully with configurable
//! error handling modes (skip/log/fail), schema validation helpers, and error
//! collection utilities.
//!
//! # Overview
//!
//! Production data pipelines must handle malformed or invalid data robustly.
//! This module provides:
//! - **Validation trait** - Define custom validation rules for your data types
//! - **Error handling modes** - Skip bad records, log and continue, or fail fast
//! - **Error collectors** - Accumulate validation errors for batch reporting
//! - **Built-in validators** - Common validation patterns
//!
//! # Example
//!
//! ```no_run
//! use rustflow::*;
//! use rustflow::validation::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Debug, Serialize, Deserialize)]
//! struct UserRecord {
//!     id: u32,
//!     email: String,
//!     age: i32,
//! }
//!
//! impl Validate for UserRecord {
//!     fn validate(&self) -> ValidationResult {
//!         let mut errors = Vec::new();
//!
//!         if self.email.is_empty() || !self.email.contains('@') {
//!             errors.push(ValidationError::field("email", "Invalid email format"));
//!         }
//!
//!         if self.age < 0 || self.age > 150 {
//!             errors.push(ValidationError::field("age", "Age must be between 0 and 150"));
//!         }
//!
//!         if errors.is_empty() {
//!             Ok(())
//!         } else {
//!             Err(errors)
//!         }
//!     }
//! }
//!
//! # fn main() -> anyhow::Result<()> {
//! use std::sync::{Arc, Mutex};
//! let p = Pipeline::default();
//! let records = from_vec(&p, vec![
//!     UserRecord { id: 1, email: "alice@example.com".into(), age: 30 },
//!     UserRecord { id: 2, email: "invalid".into(), age: 25 },
//!     UserRecord { id: 3, email: "bob@example.com".into(), age: -5 },
//! ]);
//!
//! // Skip invalid records and collect errors
//! let collector = Arc::new(Mutex::new(ErrorCollector::new()));
//! let valid_records = records.validate_with_mode(
//!     ValidationMode::LogAndContinue,
//!     Some(Arc::clone(&collector))
//! );
//!
//! let results = valid_records.collect_seq()?;
//! println!("Valid records: {}", results.len());
//! println!("Errors: {}", collector.lock().unwrap().error_count());
//! # Ok(())
//! # }
//! ```

use serde::{Deserialize, Serialize};
use std::{fmt, io};
use std::io::Error;
use std::path::Path;

/// Result type for validation operations.
pub type ValidationResult = Result<(), Vec<ValidationError>>;

/// Trait for types that can be validated.
///
/// Implement this trait on your data types to define validation rules.
pub trait Validate {
    /// Validate this instance and return a list of errors if invalid.
    fn validate(&self) -> ValidationResult;
}

/// A single validation error with context.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationError {
    /// The field that failed validation (optional)
    pub field: Option<String>,
    /// Human-readable error message
    pub message: String,
    /// Error code for categorization (optional)
    pub code: Option<String>,
}

impl ValidationError {
    /// Create a new validation error with just a message.
    pub fn new<S: Into<String>>(message: S) -> Self {
        Self {
            field: None,
            message: message.into(),
            code: None,
        }
    }

    /// Create a validation error for a specific field.
    pub fn field<S: Into<String>, M: Into<String>>(field: S, message: M) -> Self {
        Self {
            field: Some(field.into()),
            message: message.into(),
            code: None,
        }
    }

    /// Create a validation error with an error code.
    pub fn with_code<S: Into<String>>(mut self, code: S) -> Self {
        self.code = Some(code.into());
        self
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref field) = self.field {
            write!(f, "[{}] {}", field, self.message)?;
        } else {
            write!(f, "{}", self.message)?;
        }
        if let Some(ref code) = self.code {
            write!(f, " (code: {})", code)?;
        }
        Ok(())
    }
}

impl std::error::Error for ValidationError {}

/// Defines how to handle validation failures in a pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationMode {
    /// Skip invalid records silently and continue processing
    SkipInvalid,
    /// Log invalid records to error collector and continue processing
    LogAndContinue,
    /// Fail immediately on the first validation error
    FailFast,
}

/// Collects validation errors for batch reporting.
///
/// Use this to accumulate errors when using [`ValidationMode::LogAndContinue`].
#[derive(Debug, Clone, Default)]
pub struct ErrorCollector {
    errors: Vec<RecordError>,
}

/// A validation error with optional record context.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordError {
    /// Index or identifier of the record that failed
    pub record_id: Option<String>,
    /// The validation errors for this record
    pub errors: Vec<ValidationError>,
}

impl ErrorCollector {
    /// Create a new empty error collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a validation error for a record.
    pub fn add_error(&mut self, record_id: Option<String>, errors: Vec<ValidationError>) {
        self.errors.push(RecordError { record_id, errors });
    }

    /// Get the total number of failed records.
    pub fn error_count(&self) -> usize {
        self.errors.len()
    }

    /// Get all collected errors.
    pub fn errors(&self) -> &[RecordError] {
        &self.errors
    }

    /// Clear all collected errors.
    pub fn clear(&mut self) {
        self.errors.clear();
    }

    /// Print all errors to stderr.
    pub fn print_errors(&self) {
        for (idx, record_err) in self.errors.iter().enumerate() {
            if let Some(ref id) = record_err.record_id {
                eprintln!("Record {}: {}", id, format_errors(&record_err.errors));
            } else {
                eprintln!("Record #{}: {}", idx, format_errors(&record_err.errors));
            }
        }
    }

    /// Export errors to JSON format.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(&self.errors)
    }

    /// Write errors to a file in JSON format.
    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let json = self.to_json().map_err(|e| {
            Error::other(e)
        })?;
        std::fs::write(path, json)
    }
}

impl fmt::Display for ErrorCollector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ErrorCollector({} errors)", self.error_count())
    }
}

fn format_errors(errors: &[ValidationError]) -> String {
    errors
        .iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Validation helper for common patterns.
pub mod validators {
    use super::{ValidationError, ValidationResult};

    /// Validate that a string is not empty.
    pub fn not_empty(field: &str, value: &str) -> ValidationResult {
        if value.is_empty() {
            Err(vec![ValidationError::field(field, "must not be empty")])
        } else {
            Ok(())
        }
    }

    /// Validate that a string contains a substring.
    pub fn contains(field: &str, value: &str, substring: &str) -> ValidationResult {
        if value.contains(substring) {
            Ok(())
        } else {
            Err(vec![ValidationError::field(
                field,
                format!("must contain '{}'", substring),
            )])
        }
    }

    /// Validate that a numeric value is within a range.
    pub fn in_range<T: PartialOrd + fmt::Display>(
        field: &str,
        value: T,
        min: T,
        max: T,
    ) -> ValidationResult {
        if value >= min && value <= max {
            Ok(())
        } else {
            Err(vec![ValidationError::field(
                field,
                format!("must be between {} and {}", min, max),
            )])
        }
    }

    /// Validate that a string matches a basic email pattern.
    pub fn is_email(field: &str, value: &str) -> ValidationResult {
        // Basic email validation: must have @ symbol with non-empty local and domain parts
        if let Some(at_pos) = value.find('@') {
            let local = &value[..at_pos];
            let domain = &value[at_pos + 1..];

            // Local part must not be empty
            // Domain must not be empty and must contain at least one dot
            if !local.is_empty() && !domain.is_empty() && domain.contains('.') {
                // Domain must have at least one character after the dot
                if let Some(dot_pos) = domain.rfind('.') {
                    if dot_pos < domain.len() - 1 {
                        return Ok(());
                    }
                }
            }
        }
        Err(vec![ValidationError::field(field, "invalid email format")])
    }

    /// Validate minimum length.
    pub fn min_length(field: &str, value: &str, min: usize) -> ValidationResult {
        if value.len() >= min {
            Ok(())
        } else {
            Err(vec![ValidationError::field(
                field,
                format!("must have at least {} characters", min),
            )])
        }
    }

    /// Validate maximum length.
    pub fn max_length(field: &str, value: &str, max: usize) -> ValidationResult {
        if value.len() <= max {
            Ok(())
        } else {
            Err(vec![ValidationError::field(
                field,
                format!("must have at most {} characters", max),
            )])
        }
    }

    use std::fmt;
}

/// Combine multiple validation results.
pub fn combine_validations(results: Vec<ValidationResult>) -> ValidationResult {
    let mut all_errors = Vec::new();
    for result in results {
        if let Err(mut errors) = result {
            all_errors.append(&mut errors);
        }
    }
    if all_errors.is_empty() {
        Ok(())
    } else {
        Err(all_errors)
    }
}
