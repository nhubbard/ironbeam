//! Runner-neutral Ironbeam 5 graph descriptors and typed construction.
//! Compiled Rust functions live in a separate local registry, never in the descriptor.
#![allow(
    clippy::missing_errors_doc,
    clippy::too_many_lines,
    clippy::struct_excessive_bools
)]

mod implementation;
pub use implementation::*;
