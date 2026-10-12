//! Native Avro containers as bounded Ironbeam 5 sources and deferred sinks.
#![allow(clippy::missing_errors_doc)]
use ironbeam_io::io;
mod fragment;
pub use fragment::{
    AvroFragmentStore, avro_fragment_sink, avro_fragment_store, publish_avro_fragments,
};

mod implementation;
pub use implementation::*;
