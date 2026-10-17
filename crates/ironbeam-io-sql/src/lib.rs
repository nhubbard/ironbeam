#![allow(clippy::missing_errors_doc)]
//! SQL adapters. A source opens only after runner validation; a sink
//! issues one bulk insert at commit. Queries and insertion prefixes are trusted
//! application SQL, as in v4.

mod implementation;
pub use implementation::*;
