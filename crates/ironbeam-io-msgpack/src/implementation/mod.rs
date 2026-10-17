include!("format.rs");
#[cfg(feature = "cloud")]
mod cloud;
#[cfg(feature = "cloud")]
pub use cloud::{cloud_msgpack_streaming_uri_prefix_source, cloud_msgpack_streaming_uri_source};
