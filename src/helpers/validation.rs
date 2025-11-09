//! Validation helper functions for `PCollection`.
//!
//! These helpers add validation capabilities to pipelines, allowing you to
//! handle bad data gracefully with configurable error handling modes.

use crate::collection::{PCollection, RFBound};
use crate::node::DynOp;
use crate::type_token::Partition;
use crate::validation::{ErrorCollector, Validate, ValidationError, ValidationMode};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

impl<T: RFBound + Validate> PCollection<T> {
    /// Validate elements in the collection using the provided validation mode.
    ///
    /// This method applies validation rules defined by the [`Validate`](crate::validation::Validate) trait
    /// implementation for type `T`. Depending on the mode, invalid records are either:
    /// - Skipped silently ([`ValidationMode::SkipInvalid`])
    /// - Logged to the error collector and skipped ([`ValidationMode::LogAndContinue`])
    /// - Cause immediate pipeline failure ([`ValidationMode::FailFast`])
    ///
    /// # Arguments
    /// * `mode` - How to handle validation failures
    /// * `collector` - Optional error collector for accumulating validation errors
    ///
    /// # Example
    /// ```no_run
    /// use rustflow::*;
    /// use rustflow::validation::*;
    /// use serde::{Deserialize, Serialize};
    /// use std::sync::{Arc, Mutex};
    ///
    /// #[derive(Clone, Serialize, Deserialize)]
    /// struct User {
    ///     id: u32,
    ///     email: String,
    /// }
    ///
    /// impl Validate for User {
    ///     fn validate(&self) -> ValidationResult {
    ///         if self.email.contains('@') {
    ///             Ok(())
    ///         } else {
    ///             Err(vec![ValidationError::field("email", "Invalid email")])
    ///         }
    ///     }
    /// }
    ///
    /// # fn main() -> anyhow::Result<()> {
    /// let p = Pipeline::default();
    /// let users = from_vec(&p, vec![
    ///     User { id: 1, email: "alice@example.com".into() },
    ///     User { id: 2, email: "invalid".into() },
    /// ]);
    ///
    /// let errors = Arc::new(Mutex::new(ErrorCollector::new()));
    /// let valid = users.validate_with_mode(ValidationMode::LogAndContinue, Some(Arc::clone(&errors)));
    /// let results = valid.collect_seq()?;
    /// let error_count = errors.lock().unwrap().error_count();
    /// assert_eq!(results.len(), 1); // Only valid record
    /// assert_eq!(error_count, 1); // One invalid record logged
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn validate_with_mode(
        &self,
        mode: ValidationMode,
        collector: Option<Arc<Mutex<ErrorCollector>>>,
    ) -> Self {
        self.apply_transform(Arc::new(ValidateOp {
            mode,
            collector,
            _phantom: PhantomData::<T>,
        }))
    }

    /// Validate elements and skip invalid records silently.
    ///
    /// This is a convenience method equivalent to calling
    /// `validate_with_mode(ValidationMode::SkipInvalid, None)`.
    ///
    /// # Example
    /// ```no_run
    /// use rustflow::*;
    /// use rustflow::validation::*;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Clone, Serialize, Deserialize)]
    /// struct User { id: u32, email: String }
    /// impl Validate for User {
    ///     fn validate(&self) -> ValidationResult { Ok(()) }
    /// }
    /// # fn main() -> anyhow::Result<()> {
    /// let p = Pipeline::default();
    /// let users = from_vec(&p, vec![User { id: 1, email: "alice@example.com".into() }]);
    /// let valid = users.validate_skip_invalid();
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn validate_skip_invalid(&self) -> Self {
        self.apply_transform(Arc::new(ValidateOp::<T> {
            mode: ValidationMode::SkipInvalid,
            collector: None,
            _phantom: PhantomData::<T>,
        }))
    }

    /// Validate elements and fail fast on the first error.
    ///
    /// This is a convenience method equivalent to calling
    /// `validate_with_mode(ValidationMode::FailFast, None)`.
    ///
    /// # Example
    /// ```no_run
    /// use rustflow::*;
    /// use rustflow::validation::*;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Clone, Serialize, Deserialize)]
    /// struct User { id: u32, email: String }
    /// impl Validate for User {
    ///     fn validate(&self) -> ValidationResult { Ok(()) }
    /// }
    /// # fn main() -> anyhow::Result<()> {
    /// let p = Pipeline::default();
    /// let users = from_vec(&p, vec![User { id: 1, email: "alice@example.com".into() }]);
    /// let valid = users.validate_fail_fast();
    /// // Pipeline will fail if any record is invalid
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn validate_fail_fast(&self) -> Self {
        self.apply_transform(Arc::new(ValidateOp::<T> {
            mode: ValidationMode::FailFast,
            collector: None,
            _phantom: PhantomData::<T>,
        }))
    }
}

/// Internal operator for validation.
struct ValidateOp<T: RFBound + Validate> {
    mode: ValidationMode,
    collector: Option<Arc<Mutex<ErrorCollector>>>,
    _phantom: PhantomData<T>,
}

impl<T: RFBound + Validate> DynOp for ValidateOp<T> {
    fn apply(&self, input: Partition) -> Partition {
        let elements = *input
            .downcast::<Vec<T>>()
            .expect("ValidateOp: expected Vec<T>");

        let mut valid = Vec::new();

        for (idx, elem) in elements.into_iter().enumerate() {
            match elem.validate() {
                Ok(()) => {
                    valid.push(elem);
                }
                Err(errors) => {
                    match self.mode {
                        ValidationMode::SkipInvalid => {
                            // Silently skip
                        }
                        ValidationMode::LogAndContinue => {
                            if let Some(ref collector) = self.collector {
                                collector
                                    .lock()
                                    .unwrap()
                                    .add_error(Some(format!("record_{idx}")), errors);
                            }
                        }
                        ValidationMode::FailFast => {
                            // Convert to runtime panic (will be caught by runner)
                            panic!(
                                "Validation failed at record {}: {}",
                                idx,
                                errors
                                    .iter()
                                    .map(ValidationError::to_string)
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            );
                        }
                    }
                }
            }
        }

        Box::new(valid)
    }
}

/// Helper to validate keyed collections.
impl<K, V> PCollection<(K, V)>
where
    K: RFBound,
    V: RFBound + Validate,
{
    /// Validate the values in a keyed collection, preserving keys for valid records.
    ///
    /// This is useful when working with `PCollection<(K, V)>` and you want to validate
    /// only the values while keeping the keys intact.
    ///
    /// # Example
    /// ```no_run
    /// use rustflow::*;
    /// use rustflow::validation::*;
    /// # use serde::{Deserialize, Serialize};
    ///
    /// # #[derive(Clone, Serialize, Deserialize)]
    /// # struct Order { amount: f64 }
    /// # impl Validate for Order {
    /// #     fn validate(&self) -> ValidationResult {
    /// #         if self.amount >= 0.0 { Ok(()) } else { Err(vec![]) }
    /// #     }
    /// # }
    /// # fn main() -> anyhow::Result<()> {
    /// let p = Pipeline::default();
    /// let orders = from_vec(&p, vec![
    ///     ("order_1".to_string(), Order { amount: 100.0 }),
    ///     ("order_2".to_string(), Order { amount: -50.0 }), // Invalid
    /// ]);
    ///
    /// let valid_orders = orders.validate_values_skip_invalid();
    /// let results = valid_orders.collect_seq()?;
    /// assert_eq!(results.len(), 1); // Only valid order
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn validate_values_skip_invalid(&self) -> Self {
        self.apply_transform(Arc::new(ValidateValuesOp::<K, V> {
            mode: ValidationMode::SkipInvalid,
            collector: None,
            _phantom: PhantomData::<(K, V)>,
        }))
    }

    /// Validate values with a specified mode and error collector.
    #[must_use]
    pub fn validate_values_with_mode(
        &self,
        mode: ValidationMode,
        collector: Option<Arc<Mutex<ErrorCollector>>>,
    ) -> Self {
        self.apply_transform(Arc::new(ValidateValuesOp::<K, V> {
            mode,
            collector,
            _phantom: PhantomData::<(K, V)>,
        }))
    }
}

/// Internal operator for validating values in keyed collections.
struct ValidateValuesOp<K: RFBound, V: RFBound + Validate> {
    mode: ValidationMode,
    collector: Option<Arc<Mutex<ErrorCollector>>>,
    _phantom: PhantomData<(K, V)>,
}

impl<K: RFBound, V: RFBound + Validate> DynOp for ValidateValuesOp<K, V> {
    fn apply(&self, input: Partition) -> Partition {
        let pairs = *input
            .downcast::<Vec<(K, V)>>()
            .expect("ValidateValuesOp: expected Vec<(K, V)>");

        let mut valid = Vec::new();

        for (idx, (key, value)) in pairs.into_iter().enumerate() {
            match value.validate() {
                Ok(()) => {
                    valid.push((key, value));
                }
                Err(errors) => {
                    match self.mode {
                        ValidationMode::SkipInvalid => {
                            // Silently skip
                        }
                        ValidationMode::LogAndContinue => {
                            if let Some(ref collector) = self.collector {
                                collector
                                    .lock()
                                    .unwrap()
                                    .add_error(Some(format!("pair_{idx}")), errors);
                            }
                        }
                        ValidationMode::FailFast => {
                            panic!(
                                "Validation failed at pair {}: {}",
                                idx,
                                errors
                                    .iter()
                                    .map(ValidationError::to_string)
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            );
                        }
                    }
                }
            }
        }

        Box::new(valid)
    }

    fn key_preserving(&self) -> bool {
        true
    }

    fn value_only(&self) -> bool {
        true
    }
}
