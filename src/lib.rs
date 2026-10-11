#![cfg_attr(docsrs, feature(doc_cfg))]

#![cfg_attr(feature = "legacy-v4", doc = include_str!("legacy_docs.md"))]
#![cfg_attr(all(not(feature = "legacy-v4"), feature = "v5-preview"), doc = include_str!("v5_docs.md"))]
#![cfg_attr(all(not(feature = "legacy-v4"), not(feature = "v5-preview")), doc = "Enable `v5-preview` to build the isolated Ironbeam 5 facade.")]

/// Experimental Ironbeam 5 graph and direct runner.
#[cfg(feature = "v5-preview")]
pub mod v5 {
    /// Imports for v5 convenience transforms and Direct execution terminals.
    /// This prelude depends only on v5 packages; migration bridges are optional.
    pub mod prelude {
        pub use ironbeam_direct::{
            DirectCollectExt, DirectFallibleExt, DirectFlatPartitionExt, DirectKeySortedExt,
            DirectKeyedCollectExt, DirectPartitionExt, DirectSortedExt,
        };
        #[cfg(feature = "v5-approx")]
        pub use ironbeam_transform_approx::{ApproxDistinctExt, KeyedApproxDistinctExt};
        #[cfg(feature = "v5-regex")]
        pub use ironbeam_transform_regex::RegexExt;
        pub use ironbeam_transforms::{
            CollectionExt, ConstantSideInputExt, DeadLetterExt,
            DynamicSideInputExt, ElementwiseExt, KeyedCollectionExt,
            KeyedExt, KeyedSamplingExt, KeyedSelectionExt, KeyedStatisticalExt, KeyedValidationExt,
            KeyedWindowedExt, LogElementsExt, PredicateExt, SamplingExt, SelectionExt,
            StatisticalExt, TeeExt, TryProcessExt, ValidationExt, WindowedExt,
        };
    }
    pub use ironbeam_core::*;
    pub use ironbeam_direct::{
        CheckpointConfig, DirectCollectExt, DirectRunner, JobConfig, JobHandle, JobMetrics,
        JobStatus, PlanSummary, SpillConfig,
    };
    pub use ironbeam_io::*;
    pub use ironbeam_io_jsonl::*;
    #[cfg(feature = "v5-approx")]
    pub use ironbeam_transform_approx::*;
    #[cfg(feature = "v5-regex")]
    pub use ironbeam_transform_regex::*;
    #[cfg(feature = "v5-io-protobuf")]
    pub use ironbeam_io_protobuf::*;
    #[cfg(feature = "v5-io-tfrecord")]
    pub use ironbeam_io_tfrecord::*;
    #[cfg(feature = "v5-io-arrow")]
    pub use ironbeam_io_arrow::*;
    #[cfg(feature = "v5-io-avro")]
    pub use ironbeam_io_avro::*;
    #[cfg(feature = "v5-io-csv")]
    pub use ironbeam_io_csv::*;
    #[cfg(feature = "v5-io-mongodb")]
    pub use ironbeam_io_mongodb::*;
    #[cfg(feature = "v5-io-parquet")]
    pub use ironbeam_io_parquet::*;
    #[cfg(feature = "v5-io-sql")]
    pub use ironbeam_io_sql::*;
    #[cfg(feature = "v5-io-xml")]
    pub use ironbeam_io_xml::*;
    #[cfg(feature = "v5-io-msgpack")]
    pub use ironbeam_io_msgpack::*;
    #[cfg(feature = "v5-io-cbor")]
    pub use ironbeam_io_cbor::*;
    pub use ironbeam_transforms::*;
}

#[cfg(feature = "legacy-v4")]
include!("legacy_exports.rs");
