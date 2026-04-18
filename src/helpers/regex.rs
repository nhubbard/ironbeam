//! Regex-based transform helpers for [`PCollection<String>`].
//!
//! This module adds six convenience methods to any `PCollection<String>`, covering the
//! most common regex operations: matching, capture-group extraction, find, replace, and
//! split. All methods compile the pattern at call time with
//! [`Regex::new`](regex::Regex::new). An invalid pattern causes an immediate panic with a
//! descriptive message.
//!
//! ## Methods
//!
//! | Method | Description |
//! |---|---|
//! | [`regex_matches`](PCollection::regex_matches) | Keep only lines where the entire string matches |
//! | [`regex_extract`](PCollection::regex_extract) | Extract a single capture group; non-matching lines are dropped |
//! | [`regex_extract_kv`](PCollection::regex_extract_kv) | Extract two capture groups as `(key, value)` pairs |
//! | [`regex_find`](PCollection::regex_find) | Return the first match substring; non-matching lines are dropped |
//! | [`regex_replace_all`](PCollection::regex_replace_all) | Replace every non-overlapping match |
//! | [`regex_split`](PCollection::regex_split) | Split each line on the pattern → `Vec<String>` |
//!
//! ## Examples
//!
//! ```no_run
//! use ironbeam::*;
//! use anyhow::Result;
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//! let lines = from_vec(&p, vec![
//!     "abc 123".to_string(),
//!     "xyz 456".to_string(),
//!     "no-digits-here".to_string(),
//! ]);
//!
//! // Keep only lines that contain a digit sequence
//! let with_digits = lines.regex_matches(r"\d+");
//!
//! // Extract the first digit sequence from each line
//! let digits = from_vec(&p, vec![
//!     "abc 123".to_string(),
//!     "xyz 456".to_string(),
//!     "no-digits-here".to_string(),
//! ]).regex_find(r"\d+");
//!
//! let result = digits.collect_seq()?;
//! assert_eq!(result, vec!["123".to_string(), "456".to_string()]);
//! # Ok(())
//! # }
//! ```

use crate::{PCollection, RFBound};
use regex::Regex;
use std::sync::Arc;

impl PCollection<String> {
    /// Keep only lines where the pattern is found anywhere in the string.
    ///
    /// Uses [`Regex::is_match`](regex::Regex::is_match) to test each element.
    /// Lines that do not match are discarded.
    ///
    /// # Panics
    ///
    /// Panics if `pattern` is not a valid regular expression.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let lines = from_vec(&p, vec![
    ///     "error: disk full".to_string(),
    ///     "info: all good".to_string(),
    ///     "error: timeout".to_string(),
    /// ]);
    ///
    /// let errors = lines.regex_matches(r"^error:");
    /// let result = errors.collect_seq()?;
    /// assert_eq!(result, vec![
    ///     "error: disk full".to_string(),
    ///     "error: timeout".to_string(),
    /// ]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn regex_matches(self, pattern: &str) -> Self {
        let re = Arc::new(Regex::new(pattern).expect("regex_matches: invalid pattern"));
        self.filter(move |line: &String| re.is_match(line))
    }

    /// Extract a single capture group from each line; lines with no match are dropped.
    ///
    /// `group` is 0-indexed in the standard `regex` crate sense: group `0` is the entire
    /// match, group `1` is the first explicit capture group, and so on.
    ///
    /// Lines where the pattern does not match, or where the requested capture group is not
    /// present (e.g., inside a branch that did not participate), are silently dropped.
    ///
    /// # Panics
    ///
    /// Panics if `pattern` is not a valid regular expression.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let lines = from_vec(&p, vec![
    ///     "name: Alice".to_string(),
    ///     "name: Bob".to_string(),
    ///     "unrelated line".to_string(),
    /// ]);
    ///
    /// // Capture group 1 extracts the value after "name: "
    /// let names = lines.regex_extract(r"name: (\w+)", 1);
    /// let result = names.collect_seq()?;
    /// assert_eq!(result, vec!["Alice".to_string(), "Bob".to_string()]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn regex_extract(self, pattern: &str, group: usize) -> Self {
        let re = Arc::new(Regex::new(pattern).expect("regex_extract: invalid pattern"));
        self.filter_map(move |line: &String| {
            re.captures(line)
                .and_then(|caps| caps.get(group))
                .map(|m| m.as_str().to_string())
        })
    }

    /// Extract two capture groups from each line as `(key, value)` string pairs.
    ///
    /// Lines where the pattern does not match, or where either requested group is absent,
    /// are dropped. The returned collection has element type `(String, String)`.
    ///
    /// # Panics
    ///
    /// Panics if `pattern` is not a valid regular expression.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let lines = from_vec(&p, vec![
    ///     "Alice 42".to_string(),
    ///     "Bob 37".to_string(),
    ///     "not matching".to_string(),
    /// ]);
    ///
    /// // Group 1 = word (name), group 2 = digits (age)
    /// let kv = lines.regex_extract_kv(r"(\w+)\s+(\d+)", 1, 2);
    /// let mut result = kv.collect_seq()?;
    /// result.sort();
    /// assert_eq!(result, vec![
    ///     ("Alice".to_string(), "42".to_string()),
    ///     ("Bob".to_string(), "37".to_string()),
    /// ]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn regex_extract_kv(
        self,
        pattern: &str,
        key_group: usize,
        val_group: usize,
    ) -> PCollection<(String, String)>
    where
        (String, String): RFBound,
    {
        let re = Arc::new(Regex::new(pattern).expect("regex_extract_kv: invalid pattern"));
        self.filter_map(move |line: &String| {
            let caps = re.captures(line)?;
            let key = caps.get(key_group)?.as_str().to_string();
            let val = caps.get(val_group)?.as_str().to_string();
            Some((key, val))
        })
    }

    /// Return the substring of the first match for each line; non-matching lines are dropped.
    ///
    /// Uses [`Regex::find`](regex::Regex::find) which returns the leftmost-first match.
    /// When a line matches, the matched substring is returned as a new `String`. When it
    /// does not match, the element is silently dropped.
    ///
    /// # Panics
    ///
    /// Panics if `pattern` is not a valid regular expression.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let lines = from_vec(&p, vec![
    ///     "price: $12.99".to_string(),
    ///     "total: $5.00".to_string(),
    ///     "free item".to_string(),
    /// ]);
    ///
    /// // Extract the first dollar amount from each line
    /// let prices = lines.regex_find(r"\$[\d.]+");
    /// let result = prices.collect_seq()?;
    /// assert_eq!(result, vec!["$12.99".to_string(), "$5.00".to_string()]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn regex_find(self, pattern: &str) -> Self {
        let re = Arc::new(Regex::new(pattern).expect("regex_find: invalid pattern"));
        self.filter_map(move |line: &String| re.find(line).map(|m| m.as_str().to_string()))
    }

    /// Replace every non-overlapping match of `pattern` with `replacement`.
    ///
    /// Equivalent to [`Regex::replace_all`](regex::Regex::replace_all). All elements are
    /// kept; elements with no match are passed through unchanged.
    ///
    /// # Panics
    ///
    /// Panics if `pattern` is not a valid regular expression.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let lines = from_vec(&p, vec![
    ///     "hello   world".to_string(),
    ///     "foo  bar   baz".to_string(),
    ///     "already fine".to_string(),
    /// ]);
    ///
    /// // Collapse runs of whitespace to a single space
    /// let normalised = lines.regex_replace_all(r"\s+", " ");
    /// let result = normalised.collect_seq()?;
    /// assert_eq!(result, vec![
    ///     "hello world".to_string(),
    ///     "foo bar baz".to_string(),
    ///     "already fine".to_string(),
    /// ]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn regex_replace_all(self, pattern: &str, replacement: &str) -> Self {
        let re = Arc::new(Regex::new(pattern).expect("regex_replace_all: invalid pattern"));
        let rep = replacement.to_string();
        self.map(move |line: &String| re.replace_all(line, rep.as_str()).into_owned())
    }

    /// Split each line on the pattern, producing a `Vec<String>` of parts.
    ///
    /// Equivalent to calling [`Regex::split`](regex::Regex::split) on each element and
    /// collecting the resulting pieces. An element with no matches produces a single-element
    /// `Vec` containing the original string.
    ///
    /// # Panics
    ///
    /// Panics if `pattern` is not a valid regular expression.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let lines = from_vec(&p, vec![
    ///     "a, b, c".to_string(),
    ///     "x,y,z".to_string(),
    ///     "no-comma".to_string(),
    /// ]);
    ///
    /// let parts = lines.regex_split(r",\s*");
    /// let result = parts.collect_seq()?;
    /// assert_eq!(result, vec![
    ///     vec!["a".to_string(), "b".to_string(), "c".to_string()],
    ///     vec!["x".to_string(), "y".to_string(), "z".to_string()],
    ///     vec!["no-comma".to_string()],
    /// ]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn regex_split(self, pattern: &str) -> PCollection<Vec<String>>
    where
        Vec<String>: RFBound,
    {
        let re = Arc::new(Regex::new(pattern).expect("regex_split: invalid pattern"));
        self.map(move |line: &String| re.split(line).map(String::from).collect())
    }
}
