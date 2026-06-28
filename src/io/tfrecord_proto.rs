//! Hand-written `prost` structs equivalent to TensorFlow's `tf.Example` proto.
//!
//! These types match the wire format of TensorFlow's `example.proto` and
//! `feature.proto` exactly, so files produced by TensorFlow or `tf.data` can
//! be decoded here without running `protoc` or importing any TensorFlow crate.
//!
//! Compiled only when **both** `io-tfrecord` and `io-protobuf` are enabled.

#![cfg(all(feature = "io-tfrecord", feature = "io-protobuf"))]

use serde::{Deserialize, Serialize};

/// A list of bytes values (corresponds to `BytesList` in `feature.proto`).
#[derive(Clone, PartialEq, Eq, prost::Message, Serialize, Deserialize)]
pub struct BytesList {
    /// The raw byte strings.
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub value: Vec<Vec<u8>>,
}

/// A list of 32-bit float values (corresponds to `FloatList` in `feature.proto`).
#[derive(Clone, PartialEq, prost::Message, Serialize, Deserialize)]
pub struct FloatList {
    /// The float values.
    #[prost(float, repeated, tag = "1")]
    pub value: Vec<f32>,
}

/// A list of 64-bit integer values (corresponds to `Int64List` in `feature.proto`).
#[derive(Clone, PartialEq, Eq, prost::Message, Serialize, Deserialize)]
pub struct Int64List {
    /// The integer values.
    #[prost(int64, repeated, tag = "1")]
    pub value: Vec<i64>,
}

/// The oneof discriminant for [`Feature::kind`].
pub mod feature {
    use super::{BytesList, FloatList, Int64List};
    use serde::{Deserialize, Serialize};

    /// One of the three possible feature value kinds.
    #[derive(Clone, PartialEq, prost::Oneof, Serialize, Deserialize)]
    pub enum Kind {
        /// A bytes-valued feature.
        #[prost(message, tag = "1")]
        BytesList(BytesList),
        /// A float-valued feature.
        #[prost(message, tag = "2")]
        FloatList(FloatList),
        /// An int64-valued feature.
        #[prost(message, tag = "3")]
        Int64List(Int64List),
    }
}

/// A single named feature (corresponds to `Feature` in `feature.proto`).
///
/// Exactly one of `bytes_list`, `float_list`, or `int64_list` is set.
#[derive(Clone, PartialEq, prost::Message, Serialize, Deserialize)]
pub struct Feature {
    /// The value held by this feature.
    #[prost(oneof = "feature::Kind", tags = "1, 2, 3")]
    pub kind: Option<feature::Kind>,
}

/// A map of feature names to [`Feature`] values (corresponds to `Features`).
#[derive(Clone, PartialEq, prost::Message, Serialize, Deserialize)]
pub struct Features {
    /// The named features.
    #[prost(btree_map = "string, message", tag = "1")]
    pub feature: std::collections::BTreeMap<String, Feature>,
}

/// A single training example (corresponds to `Example` in `example.proto`).
///
/// An `Example` is the primary record type in `TFRecord` files produced by
/// TensorFlow. It holds a [`Features`] map keyed by feature name.
#[derive(Clone, PartialEq, prost::Message, Serialize, Deserialize)]
pub struct Example {
    /// The features in this example.
    #[prost(message, optional, tag = "1")]
    pub features: Option<Features>,
}
