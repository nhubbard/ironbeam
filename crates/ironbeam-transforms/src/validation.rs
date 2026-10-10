//! Validation and error collection for local bounded execution.
use ironbeam_core::{Element, Error, PCollection};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

pub type ValidationResult = Result<(), Vec<ValidationError>>;

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
    pub fn new() -> Self {
        Self::default()
    }
    pub fn add_error(&mut self, record_id: Option<String>, errors: Vec<ValidationError>) {
        self.errors.push(RecordError { record_id, errors });
    }
    #[must_use]
    pub fn error_count(&self) -> usize {
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
        for record in &self.errors {
            eprintln!(
                "{:?}: {}",
                record.record_id,
                record
                    .errors
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }
}

fn accepted(
    value: &impl Validate,
    mode: ValidationMode,
    collector: &Option<Arc<Mutex<ErrorCollector>>>,
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
        self.filter_named(key, move |value| {
            accepted(
                value,
                mode,
                &collector,
                ordinal.fetch_add(1, Ordering::Relaxed),
            )
        })
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
        self.filter_named(key, move |(_, value)| {
            accepted(
                value,
                mode,
                &collector,
                ordinal.fetch_add(1, Ordering::Relaxed),
            )
        })
    }
    fn validate_values_skip_invalid_named(&self, key: &str) -> Result<Self, Error> {
        self.validate_values_with_mode_named(key, ValidationMode::SkipInvalid, None)
    }
}
