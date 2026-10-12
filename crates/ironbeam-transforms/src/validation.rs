//! Validation and error collection for local bounded execution.
use ironbeam_core::{Element, Error, PCollection, Version};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

pub type ValidationResult = Result<(), Vec<ValidationError>>;

/// Combine validation results, retaining every error in check order.
pub fn combine_validations(results: Vec<ValidationResult>) -> ValidationResult {
    let errors: Vec<_> = results
        .into_iter()
        .filter_map(Result::err)
        .flatten()
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Built-in validators with the same acceptance rules as Ironbeam v4.
pub mod validators {
    use super::{ValidationError, ValidationResult};
    use std::fmt::Display;

    pub fn not_empty(field: &str, value: &str) -> ValidationResult {
        if value.is_empty() {
            Err(vec![ValidationError::field(field, "must not be empty")])
        } else {
            Ok(())
        }
    }

    pub fn contains(field: &str, value: &str, substring: &str) -> ValidationResult {
        if value.contains(substring) {
            Ok(())
        } else {
            Err(vec![ValidationError::field(
                field,
                format!("must contain '{substring}'"),
            )])
        }
    }

    pub fn in_range<T: PartialOrd + Display>(
        field: &str,
        value: &T,
        min: &T,
        max: &T,
    ) -> ValidationResult {
        if value >= min && value <= max {
            Ok(())
        } else {
            Err(vec![ValidationError::field(
                field,
                format!("must be between {min} and {max}"),
            )])
        }
    }

    pub fn is_email(field: &str, value: &str) -> ValidationResult {
        if let Some(at_pos) = value.find('@') {
            let local = &value[..at_pos];
            let domain = &value[at_pos + 1..];
            if !local.is_empty()
                && !domain.is_empty()
                && domain.contains('.')
                && let Some(dot_pos) = domain.rfind('.')
                && dot_pos < domain.len() - 1
            {
                return Ok(());
            }
        }
        Err(vec![ValidationError::field(field, "invalid email format")])
    }

    pub fn min_length(field: &str, value: &str, min: usize) -> ValidationResult {
        if value.len() >= min {
            Ok(())
        } else {
            Err(vec![ValidationError::field(
                field,
                format!("must have at least {min} characters"),
            )])
        }
    }

    pub fn max_length(field: &str, value: &str, max: usize) -> ValidationResult {
        if value.len() <= max {
            Ok(())
        } else {
            Err(vec![ValidationError::field(
                field,
                format!("must have at most {max} characters"),
            )])
        }
    }
}

pub trait Validate {
    fn validate(&self) -> ValidationResult;
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ValidationError {
    pub field: Option<String>,
    pub message: String,
    pub code: Option<String>,
}
impl ValidationError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            field: None,
            message: message.into(),
            code: None,
        }
    }
    pub fn field(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: Some(field.into()),
            message: message.into(),
            code: None,
        }
    }
    #[must_use]
    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }
}
impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(field) = &self.field {
            write!(f, "[{field}] ")?;
        }
        write!(f, "{}", self.message)?;
        if let Some(code) = &self.code {
            write!(f, " (code: {code})")?;
        }
        Ok(())
    }
}
impl std::error::Error for ValidationError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValidationMode {
    SkipInvalid,
    LogAndContinue,
    FailFast,
}

fn validation_semantic_id(mode: ValidationMode, keyed: bool) -> &'static str {
    match (mode, keyed) {
        (ValidationMode::SkipInvalid, false) => "ironbeam-transforms/validation-skip/v1",
        (ValidationMode::LogAndContinue, false) => "ironbeam-transforms/validation-log/v1",
        (ValidationMode::FailFast, false) => "ironbeam-transforms/validation-fail/v1",
        (ValidationMode::SkipInvalid, true) => "ironbeam-transforms/keyed-validation-skip/v1",
        (ValidationMode::LogAndContinue, true) => "ironbeam-transforms/keyed-validation-log/v1",
        (ValidationMode::FailFast, true) => "ironbeam-transforms/keyed-validation-fail/v1",
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RecordError {
    pub record_id: Option<String>,
    pub errors: Vec<ValidationError>,
}

#[derive(Clone, Debug, Default)]
pub struct ErrorCollector {
    errors: Vec<RecordError>,
}
impl ErrorCollector {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    pub fn add_error(&mut self, record_id: Option<String>, errors: Vec<ValidationError>) {
        self.errors.push(RecordError { record_id, errors });
    }
    #[must_use]
    pub const fn error_count(&self) -> usize {
        self.errors.len()
    }
    #[must_use]
    pub fn errors(&self) -> &[RecordError] {
        &self.errors
    }
    pub fn clear(&mut self) {
        self.errors.clear();
    }
    pub fn print_errors(&self) {
        for (index, record) in self.errors.iter().enumerate() {
            let id = record
                .record_id
                .as_deref()
                .map_or_else(|| format!("#{index}"), str::to_owned);
            eprintln!(
                "Record {}: {}",
                id,
                record
                    .errors
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }

    /// Export collected errors using the same JSON shape as Ironbeam v4.
    #[cfg(feature = "validation-json")]
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(&self.errors)
    }

    /// Write the JSON error report to a file.
    #[cfg(feature = "validation-json")]
    pub fn write_to_file(&self, path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        let json = self.to_json().map_err(std::io::Error::other)?;
        std::fs::write(path, json)
    }
}

impl fmt::Display for ErrorCollector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ErrorCollector({} errors)", self.error_count())
    }
}

fn accepted(
    value: &impl Validate,
    mode: ValidationMode,
    collector: Option<&Arc<Mutex<ErrorCollector>>>,
    ordinal: usize,
) -> bool {
    match value.validate() {
        Ok(()) => true,
        Err(errors) => match mode {
            ValidationMode::SkipInvalid => false,
            ValidationMode::LogAndContinue => {
                if let Some(collector) = collector {
                    collector
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .add_error(Some(format!("record_{ordinal}")), errors);
                }
                false
            }
            ValidationMode::FailFast => panic!(
                "Validation failed at record {ordinal}: {}",
                errors
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        },
    }
}

pub trait ValidationExt<T: Element + Validate> {
    fn validate_with_mode_named(
        &self,
        key: &str,
        mode: ValidationMode,
        collector: Option<Arc<Mutex<ErrorCollector>>>,
    ) -> Result<PCollection<T>, Error>;
    fn validate_skip_invalid_named(&self, key: &str) -> Result<PCollection<T>, Error>;
    fn validate_fail_fast_named(&self, key: &str) -> Result<PCollection<T>, Error>;
}
impl<T: Element + Validate> ValidationExt<T> for PCollection<T> {
    fn validate_with_mode_named(
        &self,
        key: &str,
        mode: ValidationMode,
        collector: Option<Arc<Mutex<ErrorCollector>>>,
    ) -> Result<Self, Error> {
        let ordinal = AtomicUsize::new(0);
        self.filter_with_semantic_named(
            key,
            validation_semantic_id(mode, false),
            Version { major: 1, minor: 0 },
            move |value| {
                accepted(
                    value,
                    mode,
                    collector.as_ref(),
                    ordinal.fetch_add(1, Ordering::Relaxed),
                )
            },
        )
    }
    fn validate_skip_invalid_named(&self, key: &str) -> Result<Self, Error> {
        self.validate_with_mode_named(key, ValidationMode::SkipInvalid, None)
    }
    fn validate_fail_fast_named(&self, key: &str) -> Result<Self, Error> {
        self.validate_with_mode_named(key, ValidationMode::FailFast, None)
    }
}

pub trait KeyedValidationExt<K: Element, V: Element + Validate> {
    fn validate_values_with_mode_named(
        &self,
        key: &str,
        mode: ValidationMode,
        collector: Option<Arc<Mutex<ErrorCollector>>>,
    ) -> Result<PCollection<(K, V)>, Error>;
    fn validate_values_skip_invalid_named(&self, key: &str) -> Result<PCollection<(K, V)>, Error>;
}
impl<K: Element, V: Element + Validate> KeyedValidationExt<K, V> for PCollection<(K, V)> {
    fn validate_values_with_mode_named(
        &self,
        key: &str,
        mode: ValidationMode,
        collector: Option<Arc<Mutex<ErrorCollector>>>,
    ) -> Result<Self, Error> {
        let ordinal = AtomicUsize::new(0);
        self.filter_with_semantic_named(
            key,
            validation_semantic_id(mode, true),
            Version { major: 1, minor: 0 },
            move |(_, value)| {
                accepted(
                    value,
                    mode,
                    collector.as_ref(),
                    ordinal.fetch_add(1, Ordering::Relaxed),
                )
            },
        )
    }
    fn validate_values_skip_invalid_named(&self, key: &str) -> Result<Self, Error> {
        self.validate_values_with_mode_named(key, ValidationMode::SkipInvalid, None)
    }
}
