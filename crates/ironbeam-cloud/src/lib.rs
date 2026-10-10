#![allow(clippy::missing_errors_doc)]
//! Provider-neutral cloud contracts, fake services, and operation utilities.
pub mod fake;
pub mod helpers;
pub mod traits;
pub mod utils;
pub use fake::*;
pub use traits::*;
