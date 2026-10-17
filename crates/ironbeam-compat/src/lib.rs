//! Bounded, in-process Ironbeam 4 and 5 migration helpers.
//!
//! Bridges run the source graph locally and insert its materialized values into
//! a new graph. They do not transfer graph nodes, execution state, or metadata.
/// Container images used by the optional published-v4 database references.
#[cfg(feature = "v4-service-tests")]
#[doc(hidden)]
pub use testcontainers_modules as service_testcontainers_modules;

/// Imports needed for the v5 convenience and local terminal methods.
pub mod prelude {
    pub use ironbeam_direct::DirectCollectExt;
    pub use ironbeam_transform_approx::{ApproxDistinctExt, KeyedApproxDistinctExt};
    pub use ironbeam_transform_regex::RegexExt;
    pub use ironbeam_transforms::{
        CollectionExt, ConstantSideInputExt, DeadLetterExt, DynamicSideInputExt, ElementwiseExt,
        KeyedCollectionExt, KeyedExt, KeyedSamplingExt, KeyedSelectionExt, KeyedStatisticalExt,
        KeyedValidationExt, KeyedWindowedExt, LogElementsExt, PredicateExt, SamplingExt,
        SelectionExt, StatisticalExt, TeeExt, TryProcessExt, ValidationExt, WindowedExt,
    };
}

mod bridge;
pub use bridge::{BridgeError, from_vec, v4_to_v5, v5_to_v4};
