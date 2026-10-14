//! Native Avro containers as bounded Ironbeam 5 sources and deferred sinks.
#![allow(clippy::missing_errors_doc)]
use ironbeam_io::io;
#[cfg(feature = "cloud")]
mod cloud_io;
#[cfg(feature = "cloud")]
pub use cloud_io::{cloud_avro_uri_source, cloud_avro_uri_source_with_reader_schema};
mod fragment;
pub use fragment::{
    AvroFragmentStore, avro_fragment_sink, avro_fragment_store, publish_avro_fragments,
};

mod implementation;
pub use implementation::*;
