//! Test-only fixtures for comparing backend behavior during the v5 migration.
//!
//! This package does not define Ironbeam's production graph or `Runner` API.
//! Backends implement [`BackendUnderTest`] here to exercise the same cases while
//! the core and runner interfaces evolve.

mod contract;
pub use contract::{
    BackendUnderTest, ContractCase, FixtureCapabilities, TimeDeclaration, Unsupported,
    assert_batch_rejects_unbounded, assert_bounded_case, validate_fixture,
};
