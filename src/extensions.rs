//! Extension points for custom pipeline operations.
//!
//! This module provides traits and utilities for extending Ironbeam with custom
//! functionality:
//!
//! - [`CompositeTransform`]: Package multiple transforms into a reusable component
//!
//! These extension points allow you to build higher-level abstractions on top of
//! the core pipeline API without modifying the framework itself.

use crate::{PCollection, RFBound};

/// A reusable, packaged sequence of transformations.
///
/// Implement this trait to create composite transforms that bundle multiple
/// operations into a single, named component. This is useful for:
/// - Encapsulating common transformation patterns
/// - Building domain-specific pipeline stages
/// - Creating reusable data processing modules
/// - Improving code organization and readability
///
/// # Type Parameters
/// - `I`: Input element type
/// - `O`: Output element type
///
/// # Example: Email Normalization
/// ```
/// use ironbeam::*;
/// use ironbeam::extensions::CompositeTransform;
///
/// struct NormalizeEmails;
///
/// impl CompositeTransform<String, String> for NormalizeEmails {
///     fn expand(&self, input: PCollection<String>) -> PCollection<String> {
///         input
///             .map(|email: &String| email.trim().to_lowercase())
///             .filter(|email: &String| email.contains('@'))
///             .filter(|email: &String| !email.is_empty())
///     }
/// }
///
/// # fn main() -> anyhow::Result<()> {
/// let p = Pipeline::default();
/// let emails = from_vec(&p, vec![
///     "  Alice@EXAMPLE.com  ".to_string(),
///     "invalid".to_string(),
///     "bob@test.com".to_string(),
/// ]);
///
/// let normalized = emails.apply_composite(&NormalizeEmails);
/// let result = normalized.collect_seq()?;
/// assert_eq!(result, vec!["alice@example.com", "bob@test.com"]);
/// # Ok(())
/// # }
/// ```
///
/// # Example: Word Length Analysis
/// ```
/// use ironbeam::*;
/// use ironbeam::extensions::CompositeTransform;
///
/// struct WordLengths;
///
/// impl CompositeTransform<String, (String, usize)> for WordLengths {
///     fn expand(&self, input: PCollection<String>) -> PCollection<(String, usize)> {
///         input
///             .filter(|s: &String| !s.is_empty())
///             .map(|s: &String| s.trim().to_string())
///             .key_by(|s: &String| s.clone())
///             .map_values(|s: &String| s.len())
///     }
/// }
///
/// # fn main() -> anyhow::Result<()> {
/// let p = Pipeline::default();
/// let words = from_vec(&p, vec![
///     "hello".to_string(),
///     "world".to_string(),
///     " test ".to_string(),
/// ]);
///
/// let lengths = words.apply_composite(&WordLengths);
/// let result = lengths.collect_seq()?;
/// assert_eq!(result.len(), 3);
/// # Ok(())
/// # }
/// ```
pub trait CompositeTransform<I: RFBound, O: RFBound>: Send + Sync {
    /// Expand this composite transform into a sequence of operations.
    ///
    /// This method receives the input collection and must return the transformed
    /// output collection. You can use any combination of built-in transformations
    /// (`map`, `filter`, `group_by_key`, `combine_values`, joins, etc.) to implement
    /// your logic.
    ///
    /// # Arguments
    /// - `input`: The input collection to transform
    ///
    /// # Returns
    /// The transformed output collection
    fn expand(&self, input: PCollection<I>) -> PCollection<O>;
}

impl<T: RFBound> PCollection<T> {
    /// Apply a composite transform to this collection.
    ///
    /// Composite transforms are packaged sequences of operations that can be
    /// reused across pipelines. This method provides a clean way to apply them.
    ///
    /// # Type Parameters
    /// - `O`: The output element type after transformation
    /// - `CT`: The composite transform type (usually inferred)
    ///
    /// # Arguments
    /// - `transform`: The composite transform to apply
    ///
    /// # Returns
    /// A new collection with the composite transform applied
    ///
    /// # Example
    /// ```
    /// use ironbeam::*;
    /// use ironbeam::extensions::CompositeTransform;
    ///
    /// struct TrimAndFilter;
    ///
    /// impl CompositeTransform<String, String> for TrimAndFilter {
    ///     fn expand(&self, input: PCollection<String>) -> PCollection<String> {
    ///         input
    ///             .map(|s: &String| s.trim().to_string())
    ///             .filter(|s: &String| !s.is_empty())
    ///     }
    /// }
    ///
    /// # fn main() -> anyhow::Result<()> {
    /// let p = Pipeline::default();
    /// let data = from_vec(&p, vec!["  hello  ".into(), "".into(), "world".into()]);
    /// let cleaned = data.apply_composite(&TrimAndFilter);
    /// let result = cleaned.collect_seq()?;
    /// assert_eq!(result, vec!["hello", "world"]);
    /// # Ok(())
    /// # }
    /// ```
    pub fn apply_composite<O: RFBound, CT>(&self, transform: &CT) -> PCollection<O>
    where
        CT: CompositeTransform<T, O>,
    {
        transform.expand(self.clone())
    }
}
