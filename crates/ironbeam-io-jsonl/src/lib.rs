//! JSON Lines file and object sources and deferred sinks for Ironbeam 5.
#![allow(clippy::missing_errors_doc)]

#[cfg(feature = "cloud")]
mod cloud_final;
#[cfg(feature = "cloud")]
mod cloud_io;
#[cfg(feature = "cloud")]
pub use cloud_final::{
    cloud_jsonl_fragment_sink, cloud_jsonl_fragment_store, publish_cloud_jsonl_fragments,
};
#[cfg(feature = "cloud")]
pub use cloud_io::{
    cloud_jsonl_glob_source, cloud_jsonl_sink, cloud_jsonl_source,
    cloud_jsonl_streaming_uri_glob_source, cloud_jsonl_streaming_uri_source,
    cloud_jsonl_uri_glob_source, cloud_jsonl_uri_sink, cloud_jsonl_uri_source, expand_cloud_glob,
    expand_cloud_glob_required, read_cloud_jsonl_glob, read_cloud_jsonl_vec, write_cloud_jsonl_vec,
};

mod implementation;
pub use implementation::*;
