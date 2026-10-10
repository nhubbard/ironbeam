//! Test-only fixtures for backend adapters during the v5 migration.
//!
//! This package does not define Ironbeam's production graph or `Runner` API.
//! Backends implement [`BackendUnderTest`] here to exercise the same cases while
//! the core and runner interfaces evolve in Phase 2.

use std::fmt;

/// A small graph shared by all backend adapters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContractCase {
    /// Bounded `[1, 2, 3, 4] -> map(double) -> filter(even) -> output`.
    BoundedMapFilter,
    /// Unbounded source -> timestamp-preserving map -> fixed window -> append sink.
    UnboundedWindowedSink,
}

/// Event-time declarations are separate from user element values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimeDeclaration {
    pub initial_timestamp_ns: i64,
    pub fixed_window_ns: Option<i64>,
}

impl ContractCase {
    #[must_use]
    pub const fn time(self) -> TimeDeclaration {
        match self {
            Self::BoundedMapFilter => TimeDeclaration {
                initial_timestamp_ns: 0,
                fixed_window_ns: None,
            },
            Self::UnboundedWindowedSink => TimeDeclaration {
                // A pre-epoch timestamp verifies signed time can be described.
                initial_timestamp_ns: -1,
                fixed_window_ns: Some(300_000_000_000),
            },
        }
    }

    #[must_use]
    pub const fn is_unbounded(self) -> bool {
        matches!(self, Self::UnboundedWindowedSink)
    }
}

/// Capability rejection recorded before a backend starts work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Unsupported {
    pub node: &'static str,
    pub capability: &'static str,
}

impl fmt::Display for Unsupported {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} requires {}", self.node, self.capability)
    }
}

impl std::error::Error for Unsupported {}

/// Capabilities used by a test adapter; production capabilities live in core.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FixtureCapabilities {
    pub unbounded_source: bool,
}

/// Check the fixture's source capability before invoking an adapter.
///
/// # Errors
///
/// Returns [`Unsupported`] when the fixture needs an unbounded source and the
/// adapter does not declare that capability.
pub const fn validate_fixture(
    case: ContractCase,
    capabilities: FixtureCapabilities,
) -> Result<(), Unsupported> {
    if case.is_unbounded() && !capabilities.unbounded_source {
        return Err(Unsupported {
            node: "events",
            capability: "unbounded-source",
        });
    }
    Ok(())
}

/// Adapter surface for tests only; it is deliberately smaller than a job API.
pub trait BackendUnderTest {
    /// Validate the case without starting source or sink work.
    ///
    /// # Errors
    ///
    /// Returns [`Unsupported`] for a capability the adapter lacks.
    fn validate(&self, case: ContractCase) -> Result<(), Unsupported>;
    fn run_bounded(&self) -> Vec<i32>;
    fn starts(&self) -> usize;
}

/// Runs the common bounded case and checks the specified result multiset.
///
/// Direct sequential execution can assert its order separately. A distributed
/// backend is only required to return the same values, not the same order.
///
/// # Panics
///
/// Panics if validation, job start, or output values violate the fixture.
pub fn assert_bounded_case(backend: &impl BackendUnderTest) {
    backend.validate(ContractCase::BoundedMapFilter).unwrap();
    let before = backend.starts();
    let mut output = backend.run_bounded();
    assert!(backend.starts() > before, "backend did not start a job");
    output.sort_unstable();
    assert_eq!(output, [2, 4, 6, 8]);
}

/// A batch-only backend must reject the unbounded fixture before side effects.
///
/// # Panics
///
/// Panics if rejection or preserved time metadata differs from the fixture.
pub fn assert_batch_rejects_unbounded(backend: &impl BackendUnderTest) {
    let before = backend.starts();
    assert_eq!(
        backend.validate(ContractCase::UnboundedWindowedSink),
        Err(Unsupported {
            node: "events",
            capability: "unbounded-source",
        })
    );
    assert_eq!(backend.starts(), before);
    assert_eq!(
        ContractCase::UnboundedWindowedSink.time(),
        TimeDeclaration {
            initial_timestamp_ns: -1,
            fixed_window_ns: Some(300_000_000_000),
        }
    );
}
