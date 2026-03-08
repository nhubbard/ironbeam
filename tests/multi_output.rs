//! Comprehensive tests for multi-output (side outputs) pattern using enums and partition! macro.

#![allow(clippy::match_wildcard_for_single_variants)]
#![allow(clippy::option_if_let_else)]

use anyhow::Result;
use ironbeam::*;

// Test fixtures and helper types

#[derive(Clone, Debug, PartialEq)]
enum SimpleOutput {
    Good(String),
    Bad(String),
}

#[derive(Clone, Debug, PartialEq)]
enum TripleOutput {
    High(i32),
    Medium(i32),
    Low(i32),
}

#[derive(Clone, Debug, PartialEq)]
enum MixedTypeOutput {
    Integer(i64),
    Float(f64),
    Text(String),
}

#[derive(Clone, Debug, PartialEq)]
struct Record {
    id: u32,
    value: i32,
}

#[derive(Clone, Debug, PartialEq)]
enum ValidationResult {
    Valid(Record),
    InvalidId(Record),
    InvalidValue(Record),
    MultipleErrors(Record),
}

// Tests for filter_map functionality

#[test]
fn test_filter_map_basic() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec!["1", "2", "not_a_number", "3", "invalid", "4"]);

    let parsed = data.filter_map(|s: &&str| s.parse::<i32>().ok());

    let result = parsed.collect_seq()?;
    assert_eq!(result, vec![1, 2, 3, 4]);
    Ok(())
}

#[test]
fn test_filter_map_empty_input() -> Result<()> {
    let p = Pipeline::default();
    let data: Vec<&str> = vec![];
    let collection = from_vec(&p, data);

    let parsed = collection.filter_map(|s: &&str| s.parse::<i32>().ok());

    let result = parsed.collect_seq()?;
    assert_eq!(result, Vec::<i32>::new());
    Ok(())
}

#[test]
fn test_filter_map_all_filtered() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec!["not", "a", "number"]);

    let parsed = data.filter_map(|s: &&str| s.parse::<i32>().ok());

    let result = parsed.collect_seq()?;
    assert_eq!(result, Vec::<i32>::new());
    Ok(())
}

#[test]
fn test_filter_map_none_filtered() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec!["1", "2", "3"]);

    let parsed = data.filter_map(|s: &&str| s.parse::<i32>().ok());

    let result = parsed.collect_seq()?;
    assert_eq!(result, vec![1, 2, 3]);
    Ok(())
}

#[test]
fn test_filter_map_transform() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![1, 2, 3, 4, 5, 6]);

    // Keep only even numbers and double them
    let result_collection = data.filter_map(|n: &i32| {
        if n % 2 == 0 {
            Some(n * 2)
        } else {
            None
        }
    });

    let result = result_collection.collect_seq()?;
    assert_eq!(result, vec![4, 8, 12]);
    Ok(())
}

// Tests for manual partition pattern (without macro)

#[test]
fn test_manual_partition_two_outputs() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec!["valid", "invalid", "ok", "bad", "good"]);

    let classified = data.flat_map(|s: &&str| {
        if s.len() > 3 {
            vec![SimpleOutput::Good(s.to_string())]
        } else {
            vec![SimpleOutput::Bad(s.to_string())]
        }
    });

    let good = classified.filter_map(|out: &SimpleOutput| match out {
        SimpleOutput::Good(s) => Some(s.clone()),
        _ => None,
    });

    let bad = classified.filter_map(|out: &SimpleOutput| match out {
        SimpleOutput::Bad(s) => Some(s.clone()),
        _ => None,
    });

    assert_eq!(
        good.collect_seq()?,
        vec!["valid".to_string(), "invalid".to_string(), "good".to_string()]
    );
    assert_eq!(
        bad.collect_seq()?,
        vec!["ok".to_string(), "bad".to_string()]
    );
    Ok(())
}

#[test]
fn test_manual_partition_three_outputs() -> Result<()> {
    let p = Pipeline::default();
    let numbers = from_vec(&p, vec![5, 15, 25, 35, 45, 55, 65, 75, 85, 95]);

    let classified = numbers.flat_map(|n: &i32| {
        if *n < 33 {
            vec![TripleOutput::Low(*n)]
        } else if *n < 66 {
            vec![TripleOutput::Medium(*n)]
        } else {
            vec![TripleOutput::High(*n)]
        }
    });

    let low = classified.filter_map(|out: &TripleOutput| match out {
        TripleOutput::Low(n) => Some(*n),
        _ => None,
    });

    let medium = classified.filter_map(|out: &TripleOutput| match out {
        TripleOutput::Medium(n) => Some(*n),
        _ => None,
    });

    let high = classified.filter_map(|out: &TripleOutput| match out {
        TripleOutput::High(n) => Some(*n),
        _ => None,
    });

    assert_eq!(low.collect_seq()?, vec![5, 15, 25]);
    assert_eq!(medium.collect_seq()?, vec![35, 45, 55, 65]);
    assert_eq!(high.collect_seq()?, vec![75, 85, 95]);
    Ok(())
}

// Tests for partition! macro

#[test]
fn test_partition_macro_two_outputs() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec!["valid", "invalid", "ok", "bad", "good"]);

    let classified = data.flat_map(|s: &&str| {
        if s.len() > 3 {
            vec![SimpleOutput::Good(s.to_string())]
        } else {
            vec![SimpleOutput::Bad(s.to_string())]
        }
    });

    partition!(classified, SimpleOutput, {
        Good => good,
        Bad => bad
    });

    assert_eq!(
        good.collect_seq()?,
        vec!["valid".to_string(), "invalid".to_string(), "good".to_string()]
    );
    assert_eq!(
        bad.collect_seq()?,
        vec!["ok".to_string(), "bad".to_string()]
    );
    Ok(())
}

#[test]
fn test_partition_macro_three_outputs() -> Result<()> {
    let p = Pipeline::default();
    let numbers = from_vec(&p, vec![5, 15, 25, 35, 45, 55, 65, 75, 85, 95]);

    let classified = numbers.flat_map(|n: &i32| {
        if *n < 33 {
            vec![TripleOutput::Low(*n)]
        } else if *n < 66 {
            vec![TripleOutput::Medium(*n)]
        } else {
            vec![TripleOutput::High(*n)]
        }
    });

    partition!(classified, TripleOutput, {
        High => high,
        Medium => medium,
        Low => low
    });

    assert_eq!(low.collect_seq()?, vec![5, 15, 25]);
    assert_eq!(medium.collect_seq()?, vec![35, 45, 55, 65]);
    assert_eq!(high.collect_seq()?, vec![75, 85, 95]);
    Ok(())
}

#[test]
fn test_partition_macro_with_trailing_comma() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec!["a", "bbb"]);

    let classified = data.flat_map(|s: &&str| {
        if s.len() > 2 {
            vec![SimpleOutput::Good(s.to_string())]
        } else {
            vec![SimpleOutput::Bad(s.to_string())]
        }
    });

    partition!(classified, SimpleOutput, {
        Good => good,
        Bad => bad,
    });

    assert_eq!(good.collect_seq()?, vec!["bbb".to_string()]);
    assert_eq!(bad.collect_seq()?, vec!["a".to_string()]);
    Ok(())
}

// Tests with different output types per variant

#[test]
fn test_partition_mixed_types() -> Result<()> {
    let p = Pipeline::default();
    let inputs = from_vec(&p, vec!["42", "3.15", "not_a_number", "100", "2.72"]);

    let parsed = inputs.flat_map(|s: &&str| {
        if let Ok(i) = s.parse::<i64>() {
            vec![MixedTypeOutput::Integer(i)]
        } else if let Ok(f) = s.parse::<f64>() {
            vec![MixedTypeOutput::Float(f)]
        } else {
            vec![MixedTypeOutput::Text(s.to_string())]
        }
    });

    partition!(parsed, MixedTypeOutput, {
        Integer => integers,
        Float => floats,
        Text => texts
    });

    assert_eq!(integers.collect_seq()?, vec![42, 100]);

    let float_results = floats.collect_seq()?;
    assert_eq!(float_results.len(), 2);
    assert!((float_results[0] - 3.15).abs() < 0.01);
    assert!((float_results[1] - 2.72).abs() < 0.01);

    assert_eq!(texts.collect_seq()?, vec!["not_a_number".to_string()]);
    Ok(())
}

// Tests with complex data structures

#[test]
fn test_partition_complex_validation() -> Result<()> {
    let p = Pipeline::default();
    let records = from_vec(
        &p,
        vec![
            Record { id: 1, value: 100 },
            Record { id: 0, value: 50 },  // Invalid ID
            Record { id: 2, value: -10 }, // Invalid value
            Record { id: 0, value: -5 },  // Both invalid
            Record { id: 3, value: 200 },
        ],
    );

    let validated = records.flat_map(|rec: &Record| {
        let valid_id = rec.id > 0;
        let valid_value = rec.value > 0;

        match (valid_id, valid_value) {
            (true, true) => vec![ValidationResult::Valid(rec.clone())],
            (false, true) => vec![ValidationResult::InvalidId(rec.clone())],
            (true, false) => vec![ValidationResult::InvalidValue(rec.clone())],
            (false, false) => vec![ValidationResult::MultipleErrors(rec.clone())],
        }
    });

    partition!(validated, ValidationResult, {
        Valid => valid,
        InvalidId => invalid_id,
        InvalidValue => invalid_value,
        MultipleErrors => multiple_errors
    });

    let valid_results = valid.collect_seq()?;
    assert_eq!(valid_results.len(), 2);
    assert_eq!(valid_results[0].id, 1);
    assert_eq!(valid_results[1].id, 3);

    let invalid_id_results = invalid_id.collect_seq()?;
    assert_eq!(invalid_id_results.len(), 1);
    assert_eq!(invalid_id_results[0].value, 50);

    let invalid_value_results = invalid_value.collect_seq()?;
    assert_eq!(invalid_value_results.len(), 1);
    assert_eq!(invalid_value_results[0].id, 2);

    let multiple_errors_results = multiple_errors.collect_seq()?;
    assert_eq!(multiple_errors_results.len(), 1);
    assert_eq!(multiple_errors_results[0].id, 0);
    assert_eq!(multiple_errors_results[0].value, -5);

    Ok(())
}

// Tests with empty outputs

#[test]
fn test_partition_empty_variant() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec!["valid", "good", "excellent"]);

    let classified = data.flat_map(|s: &&str| vec![SimpleOutput::Good(s.to_string())]);

    partition!(classified, SimpleOutput, {
        Good => good,
        Bad => bad
    });

    assert_eq!(
        good.collect_seq()?,
        vec!["valid".to_string(), "good".to_string(), "excellent".to_string()]
    );
    assert_eq!(bad.collect_seq()?, Vec::<String>::new());
    Ok(())
}

#[test]
fn test_partition_all_empty() -> Result<()> {
    let p = Pipeline::default();
    let data: Vec<String> = vec![];
    let collection = from_vec(&p, data);

    let classified =
        collection.flat_map(|s: &String| vec![SimpleOutput::Good(s.clone())]);

    partition!(classified, SimpleOutput, {
        Good => good,
        Bad => bad
    });

    assert_eq!(good.collect_seq()?, Vec::<String>::new());
    assert_eq!(bad.collect_seq()?, Vec::<String>::new());
    Ok(())
}

// Tests demonstrating real-world use cases

#[test]
fn test_data_quality_pipeline() -> Result<()> {
    #[derive(Clone, Debug, PartialEq)]
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
    }

    let p = Pipeline::default();
    let users = from_vec(
        &p,
        vec![
            User {
                id: 1,
                email: "user@example.com".into(),
                age: 25,
            },
            User {
                id: 2,
                email: "invalid_email".into(),
                age: 30,
            },
            User {
                id: 3,
                email: "user2@example.com".into(),
                age: -5,
            },
            User {
                id: 4,
                email: "user3@example.com".into(),
                age: 40,
            },
        ],
    );

    let validated = users.flat_map(|user: &User| {
        let has_valid_email = user.email.contains('@');
        let has_valid_age = user.age > 0 && user.age < 150;

        if has_valid_email && has_valid_age {
            vec![UserValidation::Valid(user.clone())]
        } else if !has_valid_email {
            vec![UserValidation::InvalidEmail(user.clone())]
        } else {
            vec![UserValidation::InvalidAge(user.clone())]
        }
    });

    partition!(validated, UserValidation, {
        Valid => valid_users,
        InvalidEmail => email_errors,
        InvalidAge => age_errors
    });

    let valid = valid_users.collect_seq()?;
    assert_eq!(valid.len(), 2);
    assert_eq!(valid[0].id, 1);
    assert_eq!(valid[1].id, 4);

    let email_err = email_errors.collect_seq()?;
    assert_eq!(email_err.len(), 1);
    assert_eq!(email_err[0].id, 2);

    let age_err = age_errors.collect_seq()?;
    assert_eq!(age_err.len(), 1);
    assert_eq!(age_err[0].id, 3);

    Ok(())
}

#[test]
fn test_log_processing_pipeline() -> Result<()> {
    #[derive(Clone, Debug, PartialEq)]
    enum LogLevel {
        Info(String),
        Warning(String),
        Error(String),
    }

    let p = Pipeline::default();
    let logs = from_vec(
        &p,
        vec![
            "INFO: Server started",
            "ERROR: Connection failed",
            "INFO: Request received",
            "WARNING: High memory usage",
            "ERROR: Database timeout",
            "INFO: Request completed",
        ],
    );

    let classified = logs.flat_map(|log: &&str| {
        if log.starts_with("ERROR") {
            vec![LogLevel::Error(log.to_string())]
        } else if log.starts_with("WARNING") {
            vec![LogLevel::Warning(log.to_string())]
        } else {
            vec![LogLevel::Info(log.to_string())]
        }
    });

    partition!(classified, LogLevel, {
        Info => info_logs,
        Warning => warning_logs,
        Error => error_logs
    });

    assert_eq!(info_logs.collect_seq()?.len(), 3);
    assert_eq!(warning_logs.collect_seq()?.len(), 1);
    assert_eq!(error_logs.collect_seq()?.len(), 2);

    Ok(())
}

// Tests for chaining operations after partition

#[test]
fn test_partition_then_transform() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    let classified = data.flat_map(|n: &i32| {
        if n % 2 == 0 {
            vec![SimpleOutput::Good(n.to_string())]
        } else {
            vec![SimpleOutput::Bad(n.to_string())]
        }
    });

    partition!(classified, SimpleOutput, {
        Good => evens,
        Bad => odds
    });

    // Transform each partition independently
    let even_doubled = evens.map(|s: &String| {
        let n: i32 = s.parse().unwrap();
        n * 2
    });

    let odd_squared = odds.map(|s: &String| {
        let n: i32 = s.parse().unwrap();
        n * n
    });

    assert_eq!(even_doubled.collect_seq()?, vec![4, 8, 12, 16, 20]);
    assert_eq!(odd_squared.collect_seq()?, vec![1, 9, 25, 49, 81]);

    Ok(())
}

// Tests for performance considerations

#[test]
fn test_partition_large_dataset() -> Result<()> {
    let p = Pipeline::default();
    let data: Vec<i32> = (1..=10000).collect();
    let collection = from_vec(&p, data);

    let classified = collection.flat_map(|n: &i32| {
        if n % 2 == 0 {
            vec![SimpleOutput::Good(n.to_string())]
        } else {
            vec![SimpleOutput::Bad(n.to_string())]
        }
    });

    partition!(classified, SimpleOutput, {
        Good => evens,
        Bad => odds
    });

    assert_eq!(evens.collect_seq()?.len(), 5000);
    assert_eq!(odds.collect_seq()?.len(), 5000);

    Ok(())
}

// Test for parallel execution

#[test]
fn test_partition_parallel_execution() -> Result<()> {
    let p = Pipeline::default();
    let data: Vec<i32> = (1..=1000).collect();
    let collection = from_vec(&p, data);

    let classified = collection.flat_map(|n: &i32| {
        if n % 2 == 0 {
            vec![SimpleOutput::Good(n.to_string())]
        } else {
            vec![SimpleOutput::Bad(n.to_string())]
        }
    });

    partition!(classified, SimpleOutput, {
        Good => evens,
        Bad => odds
    });

    // Use parallel execution
    assert_eq!(evens.collect_par(None, None)?.len(), 500);
    assert_eq!(odds.collect_par(None, None)?.len(), 500);

    Ok(())
}
