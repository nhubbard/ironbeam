//! `TFRecord` file sources, deferred sinks, and native TensorFlow Example schema for Ironbeam 5.
#![allow(clippy::missing_errors_doc)]

pub mod tfrecord_proto;
pub use tfrecord_proto::{BytesList, Example, Feature, Features, FloatList, Int64List};

mod implementation;
pub use implementation::*;
