//! Tests for validation functionality.

use rustflow::validation::*;
use rustflow::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct User {
    id: u32,
    email: String,
    age: i32,
}

impl Validate for User {
    fn validate(&self) -> ValidationResult {
        let mut errors = Vec::new();

        if self.email.is_empty() || !self.email.contains('@') {
            errors.push(ValidationError::field("email", "Invalid email format"));
        }

        if self.age < 0 || self.age > 150 {
            errors.push(ValidationError::field(
                "age",
                "Age must be between 0 and 150",
            ));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

#[test]
fn test_validate_skip_invalid() {
    let p = Pipeline::default();
    let users = from_vec(
        &p,
        vec![
            User {
                id: 1,
                email: "alice@example.com".into(),
                age: 30,
            },
            User {
                id: 2,
                email: "invalid".into(),
                age: 25,
            },
            User {
                id: 3,
                email: "bob@example.com".into(),
                age: -5,
            },
            User {
                id: 4,
                email: "charlie@example.com".into(),
                age: 40,
            },
        ],
    );

    let valid = users.validate_skip_invalid();
    let results = valid.collect_seq().unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].id, 1);
    assert_eq!(results[1].id, 4);
}

#[test]
fn test_validate_with_error_collector() {
    use std::sync::{Arc, Mutex};

    let p = Pipeline::default();
    let users = from_vec(
        &p,
        vec![
            User {
                id: 1,
                email: "alice@example.com".into(),
                age: 30,
            },
            User {
                id: 2,
                email: "invalid".into(),
                age: 25,
            },
            User {
                id: 3,
                email: "bob@example.com".into(),
                age: -5,
            },
        ],
    );

    let collector = Arc::new(Mutex::new(ErrorCollector::new()));
    let valid =
        users.validate_with_mode(ValidationMode::LogAndContinue, Some(Arc::clone(&collector)));
    let results = valid.collect_seq().unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, 1);
    assert_eq!(collector.lock().unwrap().error_count(), 2);
}

#[test]
#[should_panic(expected = "Validation failed")]
fn test_validate_fail_fast() {
    let p = Pipeline::default();
    let users = from_vec(
        &p,
        vec![
            User {
                id: 1,
                email: "alice@example.com".into(),
                age: 30,
            },
            User {
                id: 2,
                email: "invalid".into(),
                age: 25,
            },
        ],
    );

    let valid = users.validate_fail_fast();
    let _results = valid.collect_seq().unwrap();
}

#[test]
fn test_validate_with_custom_function() {
    let p = Pipeline::default();
    let numbers = from_vec(&p, vec![1, -5, 10, -3, 7, 0]);

    let positive = numbers.filter(|n: &i32| *n > 0);
    let results = positive.collect_seq().unwrap();

    assert_eq!(results, vec![1, 10, 7]);
}

#[test]
fn test_error_collector_json_export() {
    let mut collector = ErrorCollector::new();
    collector.add_error(
        Some("record_1".into()),
        vec![ValidationError::field("email", "Invalid email")],
    );
    collector.add_error(
        Some("record_2".into()),
        vec![
            ValidationError::field("age", "Age out of range"),
            ValidationError::field("email", "Missing @"),
        ],
    );

    let json = collector.to_json().unwrap();
    assert!(json.contains("record_1"));
    assert!(json.contains("Invalid email"));
    assert!(json.contains("record_2"));
    assert!(json.contains("Age out of range"));
}

#[test]
fn test_validation_helpers() {
    use validators::*;

    // Test not_empty
    assert!(not_empty("name", "Alice").is_ok());
    assert!(not_empty("name", "").is_err());

    // Test contains
    assert!(contains("email", "alice@example.com", "@").is_ok());
    assert!(contains("email", "invalid", "@").is_err());

    // Test in_range
    assert!(in_range("age", &25, &0, &150).is_ok());
    assert!(in_range("age", &-5, &0, &150).is_err());
    assert!(in_range("age", &200,&0, &150).is_err());

    // Test is_email
    assert!(is_email("email", "alice@example.com").is_ok());
    assert!(is_email("email", "invalid").is_err());

    // Test min_length
    assert!(min_length("password", "secret123", 8).is_ok());
    assert!(min_length("password", "short", 8).is_err());

    // Test max_length
    assert!(max_length("username", "alice", 10).is_ok());
    assert!(max_length("username", "verylongusername", 10).is_err());
}

#[test]
fn test_combine_validations() {
    let result1 = Ok(());
    let result2 = Err(vec![ValidationError::new("error 1")]);
    let result3 = Err(vec![ValidationError::new("error 2")]);

    let combined = combine_validations(vec![result1, result2, result3]);
    assert!(combined.is_err());

    let errors = combined.unwrap_err();
    assert_eq!(errors.len(), 2);
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Order {
    order_id: String,
    amount: f64,
}

impl Validate for Order {
    fn validate(&self) -> ValidationResult {
        if self.amount >= 0.0 {
            Ok(())
        } else {
            Err(vec![ValidationError::field(
                "amount",
                "Amount must be non-negative",
            )])
        }
    }
}

#[test]
fn test_validate_values_skip_invalid() {
    let p = Pipeline::default();
    let orders = from_vec(
        &p,
        vec![
            (
                "customer_1".to_string(),
                Order {
                    order_id: "order_1".into(),
                    amount: 100.0,
                },
            ),
            (
                "customer_2".to_string(),
                Order {
                    order_id: "order_2".into(),
                    amount: -50.0,
                },
            ),
            (
                "customer_3".to_string(),
                Order {
                    order_id: "order_3".into(),
                    amount: 200.0,
                },
            ),
        ],
    );

    let valid = orders.validate_values_skip_invalid();
    let results = valid.collect_seq().unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, "customer_1");
    assert_eq!(results[1].0, "customer_3");
}

#[test]
fn test_validate_values_with_error_collector() {
    use std::sync::{Arc, Mutex};

    let p = Pipeline::default();
    let orders = from_vec(
        &p,
        vec![
            (
                "customer_1".to_string(),
                Order {
                    order_id: "order_1".into(),
                    amount: 100.0,
                },
            ),
            (
                "customer_2".to_string(),
                Order {
                    order_id: "order_2".into(),
                    amount: -50.0,
                },
            ),
        ],
    );

    let collector = Arc::new(Mutex::new(ErrorCollector::new()));
    let valid = orders
        .validate_values_with_mode(ValidationMode::LogAndContinue, Some(Arc::clone(&collector)));
    let results = valid.collect_seq().unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(collector.lock().unwrap().error_count(), 1);
}

#[test]
fn test_parallel_validation() {
    let p = Pipeline::default();
    let users = from_vec(
        &p,
        vec![
            User {
                id: 1,
                email: "alice@example.com".into(),
                age: 30,
            },
            User {
                id: 2,
                email: "invalid".into(),
                age: 25,
            },
            User {
                id: 3,
                email: "bob@example.com".into(),
                age: 40,
            },
        ],
    );

    let valid = users.validate_skip_invalid();
    let results = valid.collect_par(None, None).unwrap();

    assert_eq!(results.len(), 2);
}

#[test]
fn test_validation_with_grouping() {
    let p = Pipeline::default();
    let users = from_vec(
        &p,
        vec![
            User {
                id: 1,
                email: "alice@example.com".into(),
                age: 30,
            },
            User {
                id: 2,
                email: "invalid".into(),
                age: 25,
            },
            User {
                id: 3,
                email: "bob@example.com".into(),
                age: 30,
            },
        ],
    );

    let valid = users.validate_skip_invalid();
    let by_age = valid.key_by(|u: &User| u.age).group_by_key();

    let results = by_age.collect_seq().unwrap();
    assert_eq!(results.len(), 1); // Only age 30 (with 2 valid users)
    assert_eq!(results[0].1.len(), 2);
}

#[test]
fn test_validation_error_with_code() {
    let err = ValidationError::new("Test error").with_code("ERR001");
    assert_eq!(err.message, "Test error");
    assert_eq!(err.code, Some("ERR001".to_string()));
    assert!(err.to_string().contains("ERR001"));
}

#[test]
fn test_validation_error_display() {
    let err1 = ValidationError::new("Simple error");
    assert_eq!(err1.to_string(), "Simple error");

    let err2 = ValidationError::field("email", "Invalid format");
    assert!(err2.to_string().contains("[email]"));
    assert!(err2.to_string().contains("Invalid format"));

    let err3 = ValidationError::field("age", "Out of range").with_code("VAL_001");
    let s = err3.to_string();
    assert!(s.contains("[age]"));
    assert!(s.contains("Out of range"));
    assert!(s.contains("VAL_001"));
}

#[test]
fn test_error_collector_clear() {
    let mut collector = ErrorCollector::new();
    collector.add_error(Some("rec1".into()), vec![ValidationError::new("error1")]);
    collector.add_error(Some("rec2".into()), vec![ValidationError::new("error2")]);

    assert_eq!(collector.error_count(), 2);

    collector.clear();
    assert_eq!(collector.error_count(), 0);
    assert!(collector.errors().is_empty());
}

#[test]
fn test_error_collector_errors() {
    let mut collector = ErrorCollector::new();
    collector.add_error(
        Some("test_record".into()),
        vec![ValidationError::field("field1", "Error message")],
    );

    let errors = collector.errors();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].record_id, Some("test_record".into()));
    assert_eq!(errors[0].errors.len(), 1);
}

#[test]
fn test_error_collector_print() {
    let mut collector = ErrorCollector::new();
    collector.add_error(None, vec![ValidationError::new("Test error")]);

    // Just ensure it doesn't panic
    collector.print_errors();
}

#[test]
fn test_error_collector_display() {
    let mut collector = ErrorCollector::new();
    collector.add_error(None, vec![ValidationError::new("e1")]);
    collector.add_error(None, vec![ValidationError::new("e2")]);

    let display = collector.to_string();
    assert!(display.contains("ErrorCollector"));
    assert!(display.contains("2 errors"));
}

#[test]
fn test_error_collector_write_to_file() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("errors.json");

    let mut collector = ErrorCollector::new();
    collector.add_error(
        Some("rec1".into()),
        vec![ValidationError::field("email", "Invalid")],
    );

    collector.write_to_file(&file_path).unwrap();

    let contents = std::fs::read_to_string(&file_path).unwrap();
    assert!(contents.contains("rec1"));
    assert!(contents.contains("email"));
}

#[test]
fn test_validators_email_edge_cases() {
    use validators::*;

    // Valid emails
    assert!(is_email("email", "test@test.com").is_ok());
    assert!(is_email("email", "a@b.c").is_ok());

    // Invalid emails
    assert!(is_email("email", "@test.com").is_err());
    assert!(is_email("email", "test@").is_err());
    assert!(is_email("email", "test").is_err());
    assert!(is_email("email", "a@b").is_err()); // No dot after @
}

#[test]
fn test_validators_length_edge_cases() {
    use validators::*;

    // min_length
    assert!(min_length("field", "", 0).is_ok());
    assert!(min_length("field", "a", 1).is_ok());
    assert!(min_length("field", "", 1).is_err());

    // max_length
    assert!(max_length("field", "", 0).is_ok());
    assert!(max_length("field", "", 10).is_ok());
    assert!(max_length("field", "a", 0).is_err());
}

#[test]
fn test_validators_range_boundaries() {
    use validators::*;

    // Test exact boundaries
    assert!(in_range("val", &0, &0, &10).is_ok());
    assert!(in_range("val", &10, &0, &10).is_ok());
    assert!(in_range("val", &5, &0, &10).is_ok());
    assert!(in_range("val", &-1, &0, &10).is_err());
    assert!(in_range("val", &11, &0, &10).is_err());

    // Test with floats
    assert!(in_range("price", &5.5, &0.0, &10.0).is_ok());
    assert!(in_range("price", &-0.1, &0.0, &10.0).is_err());
}

#[test]
fn test_combine_validations_all_ok() {
    let results = vec![Ok(()), Ok(()), Ok(())];
    let combined = combine_validations(results);
    assert!(combined.is_ok());
}

#[test]
fn test_combine_validations_empty() {
    let results: Vec<ValidationResult> = vec![];
    let combined = combine_validations(results);
    assert!(combined.is_ok());
}

#[test]
fn test_validation_mode_equality() {
    assert_eq!(ValidationMode::SkipInvalid, ValidationMode::SkipInvalid);
    assert_ne!(ValidationMode::SkipInvalid, ValidationMode::FailFast);
    assert_ne!(ValidationMode::LogAndContinue, ValidationMode::FailFast);
}

#[test]
fn test_error_collector_default() {
    let collector = ErrorCollector::default();
    assert_eq!(collector.error_count(), 0);
}

#[test]
fn test_validation_error_std_error_trait() {
    use std::error::Error;

    let err = ValidationError::new("Test error");
    let _: &dyn Error = &err; // Should compile as it implements Error trait
}

#[test]
fn test_record_error_structure() {
    let record_err = RecordError {
        record_id: Some("test_id".to_string()),
        errors: vec![
            ValidationError::field("field1", "error1"),
            ValidationError::field("field2", "error2"),
        ],
    };

    assert_eq!(record_err.record_id, Some("test_id".to_string()));
    assert_eq!(record_err.errors.len(), 2);
}

#[test]
fn test_multiple_field_errors() {
    let mut errors = Vec::new();
    errors.push(ValidationError::field("email", "Invalid format"));
    errors.push(ValidationError::field("age", "Out of range"));
    errors.push(ValidationError::field("name", "Too short"));

    assert_eq!(errors.len(), 3);
    assert!(errors.iter().any(|e| e.field == Some("email".to_string())));
    assert!(errors.iter().any(|e| e.field == Some("age".to_string())));
    assert!(errors.iter().any(|e| e.field == Some("name".to_string())));
}
