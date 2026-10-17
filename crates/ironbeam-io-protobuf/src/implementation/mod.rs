include!("sources_and_fragments.rs");
include!("append_and_parallel.rs");

#[cfg(feature = "cloud")]
mod cloud;
#[cfg(feature = "cloud")]
pub use cloud::{cloud_protobuf_streaming_uri_prefix_source, cloud_protobuf_streaming_uri_source};
