include!("row_sources.rs");
include!("row_fragments_and_batches.rs");
include!("batch_sources_and_fragments.rs");
#[cfg(feature = "cloud")]
mod cloud;
#[cfg(feature = "cloud")]
pub use cloud::{
    cloud_arrow_ipc_batches_streaming_uri_prefix_source,
    cloud_arrow_ipc_batches_streaming_uri_source, cloud_arrow_ipc_streaming_uri_prefix_source,
    cloud_arrow_ipc_streaming_uri_source,
};
