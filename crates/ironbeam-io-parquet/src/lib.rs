//! Bounded Parquet file sources and deferred sinks for Ironbeam 5.
#![allow(clippy::missing_errors_doc)]

mod implementation;
#[cfg(feature = "parallel-io")]
pub use implementation::parquet_parallel_sink;
pub use implementation::{
    ParquetFragmentStore, parquet_fragment_sink, parquet_fragment_store, parquet_glob_source,
    parquet_row_group_sharded_source, parquet_sharded_source, parquet_sink, parquet_source,
    parquet_streaming_source, publish_parquet_fragments,
};

#[cfg(feature = "cloud")]
mod cloud_io;
#[cfg(feature = "cloud")]
pub use cloud_io::{cloud_parquet_uri_prefix_source, cloud_parquet_uri_source};
