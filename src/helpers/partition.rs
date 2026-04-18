//! Multi-output (side outputs) pattern using enums and the `partition!` macro.
//!
//! This module implements the **side outputs** / **multi-output** pattern described in
//! Apache Beam, using an idiomatic Rust approach with enums and pattern matching.
//!
//! ## Overview
//!
//! In data processing pipelines, you often need to split a single stream into multiple
//! outputs based on some condition (e.g., good vs. bad records, different categories,
//! quality tiers). Rather than implementing a complex multi-output transform, Ironbeam
//! uses a simpler, more type-safe approach:
//!
//! 1. Define an enum with variants for each output type
//! 2. Use [`flat_map`](crate::PCollection::flat_map) to produce the enum
//! 3. Use [`filter_map`](crate::PCollection::filter_map) to extract each variant
//! 4. Or use the [`partition!`](macro@crate::partition) macro for convenience
//!
//! This approach is:
//! - **Type-safe**: Enum variants are checked at compile time
//! - **Explicit**: No runtime tag registration needed
//! - **Idiomatic**: Uses standard Rust pattern matching
//! - **Efficient**: Single pass through the data to create the enum, then efficient filters
//!
//! ## Usage Patterns
//!
//! ### Manual Pattern (Most Explicit)
//!
//! ```no_run
//! use ironbeam::*;
//! # use anyhow::Result;
//!
//! # fn main() -> Result<()> {
//! #[derive(Clone)]
//! struct Record {
//!     id: u32,
//!     value: i32,
//! }
//!
//! #[derive(Clone)]
//! enum RecordOutput {
//!     Valid(String),
//!     Invalid(String),
//!     NeedsReview(String),
//! }
//!
//! let p = Pipeline::default();
//! let records = from_vec(&p, vec![
//!     Record { id: 1, value: 100 },
//!     Record { id: 2, value: -50 },
//!     Record { id: 3, value: 0 },
//! ]);
//!
//! // Step 1: Classify records into enum variants
//! let classified = records.flat_map(|rec: &Record| {
//!     let msg = format!("Record {}: {}", rec.id, rec.value);
//!     if rec.value > 0 {
//!         vec![RecordOutput::Valid(msg)]
//!     } else if rec.value < 0 {
//!         vec![RecordOutput::Invalid(msg)]
//!     } else {
//!         vec![RecordOutput::NeedsReview(msg)]
//!     }
//! });
//!
//! // Step 2: Extract each variant into separate collections
//! let valid = classified.filter_map(|out: &RecordOutput| match out {
//!     RecordOutput::Valid(s) => Some(s.clone()),
//!     _ => None,
//! });
//!
//! let invalid = classified.filter_map(|out: &RecordOutput| match out {
//!     RecordOutput::Invalid(s) => Some(s.clone()),
//!     _ => None,
//! });
//!
//! let needs_review = classified.filter_map(|out: &RecordOutput| match out {
//!     RecordOutput::NeedsReview(s) => Some(s.clone()),
//!     _ => None,
//! });
//!
//! // Now you have three independent streams
//! assert_eq!(valid.collect_seq()?.len(), 1);
//! assert_eq!(invalid.collect_seq()?.len(), 1);
//! assert_eq!(needs_review.collect_seq()?.len(), 1);
//! # Ok(())
//! # }
//! ```
//!
//! ### Using the `partition!` Macro (Most Convenient)
//!
//! The [`partition!`](macro@crate::partition) macro reduces boilerplate by automatically generating the
//! `filter_map` calls for each variant:
//!
//! ```no_run
//! use ironbeam::*;
//! # use anyhow::Result;
//!
//! # fn main() -> Result<()> {
//! #[derive(Clone)]
//! enum Output {
//!     Good(String),
//!     Bad(String),
//! }
//!
//! let p = Pipeline::default();
//! let data = from_vec(&p, vec!["valid", "invalid", "ok"]);
//!
//! let classified = data.flat_map(|s: &&str| {
//!     if s.len() > 4 {
//!         vec![Output::Good(s.to_string())]
//!     } else {
//!         vec![Output::Bad(s.to_string())]
//!     }
//! });
//!
//! // Partition into separate collections automatically
//! partition!(classified, Output, {
//!     Good => good,
//!     Bad => bad
//! });
//!
//! assert_eq!(good.collect_seq()?, vec!["valid".to_string(), "invalid".to_string()]);
//! assert_eq!(bad.collect_seq()?, vec!["ok".to_string()]);
//! # Ok(())
//! # }
//! ```
//!
//! ## Design Rationale
//!
//! ### Why Not True Multi-Output?
//!
//! A true multi-output transform (like Apache Beam's `ParDo` with `MultiOutputReceiver`)
//! would require:
//! - Runtime type erasure and downcasting of multiple output types
//! - Complex graph modifications to support multiple output edges from one node
//! - Loss of type safety at compile time
//! - More complex implementation and maintenance
//!
//! ### Benefits of the Enum Approach
//!
//! - **Type Safety**: All output types are known at compile time
//! - **Simplicity**: Uses existing `flat_map` and `filter_map` primitives
//! - **Clarity**: Explicit enum variants make the code self-documenting
//! - **Flexibility**: Easy to add new output types by adding enum variants
//! - **Performance**: Efficient filtering via pattern matching
//!
//! ## Performance Considerations
//!
//! The enum approach creates intermediate collections, but this is efficient because:
//! - Enums are stack-allocated and have minimal overhead
//! - Pattern matching is optimized by the compiler
//! - The planner can fuse operations where possible
//! - Memory usage is proportional to the largest output, not the sum
//!
//! For most use cases, the performance difference compared to a true multi-output
//! transform is negligible, while the code clarity and type safety benefits are significant.

/// Partition a `PCollection` of enum values into separate collections by variant.
///
/// This macro simplifies the common pattern of extracting specific enum variants
/// into independent `PCollection`s. It's syntactic sugar around multiple
/// [`filter_map`](crate::PCollection::filter_map) calls.
///
/// # Syntax
///
/// ```text
/// partition!(source_collection, EnumType, {
///     Variant1 => output1,
///     Variant2 => output2,
///     ...
/// });
/// ```
///
/// Where:
/// - `source_collection` is a `PCollection<EnumType>`
/// - `EnumType` is the enum type (must be in scope)
/// - Each `Variant => output` line extracts that variant's data into a new variable
///
/// # Generated Code
///
/// The macro expands to:
/// ```ignore
/// let output1 = source_collection.filter_map(|item| match item {
///     EnumType::Variant1(value) => Some(value.clone()),
///     _ => None,
/// });
/// ```
///
/// # Examples
///
/// ## Basic Two-Output Split
///
/// ```no_run
/// use ironbeam::*;
/// # use anyhow::Result;
///
/// # fn main() -> Result<()> {
/// #[derive(Clone)]
/// enum Quality {
///     High(String),
///     Low(String),
/// }
///
/// let p = Pipeline::default();
/// let data = from_vec(&p, vec!["a", "bbb", "cc"]);
///
/// let classified = data.flat_map(|s: &&str| {
///     if s.len() > 2 {
///         vec![Quality::High(s.to_string())]
///     } else {
///         vec![Quality::Low(s.to_string())]
///     }
/// });
///
/// partition!(classified, Quality, {
///     High => high_quality,
///     Low => low_quality
/// });
///
/// assert_eq!(high_quality.collect_seq()?, vec!["bbb".to_string()]);
/// assert_eq!(low_quality.collect_seq()?, vec!["a".to_string(), "cc".to_string()]);
/// # Ok(())
/// # }
/// ```
///
/// ## Three-Output Split with Different Types
///
/// ```no_run
/// use ironbeam::*;
/// # use anyhow::Result;
///
/// # fn main() -> Result<()> {
/// #[derive(Clone)]
/// enum ParseResult {
///     Integer(i64),
///     Float(f64),
///     Invalid(String),
/// }
///
/// let p = Pipeline::default();
/// let inputs = from_vec(&p, vec!["42", "3.14", "not_a_number", "100"]);
///
/// let parsed = inputs.flat_map(|s: &&str| {
///     if let Ok(i) = s.parse::<i64>() {
///         vec![ParseResult::Integer(i)]
///     } else if let Ok(f) = s.parse::<f64>() {
///         vec![ParseResult::Float(f)]
///     } else {
///         vec![ParseResult::Invalid(s.to_string())]
///     }
/// });
///
/// partition!(parsed, ParseResult, {
///     Integer => integers,
///     Float => floats,
///     Invalid => errors
/// });
///
/// assert_eq!(integers.collect_seq()?, vec![42, 100]);
/// assert_eq!(floats.collect_seq()?, vec![3.14]);
/// assert_eq!(errors.collect_seq()?, vec!["not_a_number".to_string()]);
/// # Ok(())
/// # }
/// ```
///
/// ## Data Quality Pipeline
///
/// ```no_run
/// use ironbeam::*;
/// # use anyhow::Result;
///
/// # fn main() -> Result<()> {
/// #[derive(Clone)]
/// struct User {
///     id: u32,
///     email: String,
///     age: i32,
/// }
///
/// #[derive(Clone)]
/// enum ValidationResult {
///     Valid(User),
///     InvalidEmail(User),
///     InvalidAge(User),
///     MultipleErrors(User),
/// }
///
/// let p = Pipeline::default();
/// let users = from_vec(&p, vec![
///     User { id: 1, email: "user@example.com".into(), age: 25 },
///     User { id: 2, email: "invalid".into(), age: 30 },
///     User { id: 3, email: "user2@example.com".into(), age: -5 },
/// ]);
///
/// let validated = users.flat_map(|user: &User| {
///     let has_valid_email = user.email.contains('@');
///     let has_valid_age = user.age > 0 && user.age < 150;
///
///     match (has_valid_email, has_valid_age) {
///         (true, true) => vec![ValidationResult::Valid(user.clone())],
///         (false, true) => vec![ValidationResult::InvalidEmail(user.clone())],
///         (true, false) => vec![ValidationResult::InvalidAge(user.clone())],
///         (false, false) => vec![ValidationResult::MultipleErrors(user.clone())],
///     }
/// });
///
/// partition!(validated, ValidationResult, {
///     Valid => valid_users,
///     InvalidEmail => email_errors,
///     InvalidAge => age_errors,
///     MultipleErrors => multiple_errors
/// });
///
/// // Process each stream independently
/// // valid_users can be written to main database
/// // email_errors can be sent to data quality team
/// // age_errors can be flagged for review
/// // multiple_errors can be logged for investigation
/// # let _ = (valid_users, email_errors, age_errors, multiple_errors);
/// # Ok(())
/// # }
/// ```
///
/// # Type Requirements
///
/// - The enum must implement `Clone` (required by [`RFBound`](crate::RFBound))
/// - Each variant must be a tuple variant with exactly one field
/// - The inner type of each variant must also implement `Clone`
///
/// # Limitations
///
/// - The macro only works with tuple variants containing a single field
/// - Unit variants and struct variants are not supported
/// - For more complex extraction logic, use manual [`filter_map`](crate::PCollection::filter_map) calls
#[macro_export]
macro_rules! partition {
    // Main pattern: partition!(collection, EnumType, { Variant1 => name1, Variant2 => name2, ... })
    ($source:expr, $enum_type:ident, { $( $variant:ident => $output:ident ),* $(,)? }) => {
        $(
            let $output = $source.filter_map(|item: &$enum_type| match item {
                $enum_type::$variant(value) => Some(value.clone()),
                _ => None,
            });
        )*
    };
}
