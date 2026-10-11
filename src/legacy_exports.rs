pub mod bloom_filter;
#[cfg(feature = "coders")]
pub mod coders;
pub mod collection;
pub mod combiners;
pub mod extensions;
pub mod helpers;
pub mod io;
pub mod node;
pub mod node_id;
pub mod pipeline;
pub mod planner;
pub mod runner;
pub mod testing;
pub mod type_token;
pub mod utils;
pub mod validation;
pub mod window;

#[cfg(feature = "metrics")]
pub mod metrics;

#[cfg(feature = "checkpointing")]
pub mod checkpoint;

#[cfg(feature = "spilling")]
pub mod spill;

pub mod spill_integration;

// General re-exports
pub use collection::{
    CombineFn, Count, Element, PCollection, SideInput, SideMap, SideMultimap, SideSingleton,
};
pub use combiners::{AverageF64, BottomK, DistinctCount, Max, Min, Sum, TopK};
pub use helpers::*;
pub use node_id::NodeId;
pub use pipeline::Pipeline;
pub use planner::{
    CostEstimate, ExecutionExplanation, ExplainStep, OptimizationDecision, Plan, build_plan,
};
pub use runner::{ExecMode, Runner, SharedCSECache};
pub use type_token::Partition;
pub use utils::OrdF64;
pub use window::{TimestampMs, Timestamped, Window};

// Extension point exports
pub use extensions::CompositeTransform;
pub use node::DynOp;
pub use type_token::{TypeTag, VecOps};

#[cfg(feature = "coders")]
pub use coders::{ElementCoder, PostcardCoder, PostcardGroupedKvCoder, PostcardKvCoder};

// I/O re-exports. The API surface is always present (the modules compile
// unconditionally and stub at runtime when their feature is disabled); only the
// `*_par` writers stay behind `parallel-io`, which remains a compile gate.
pub use io::jsonl::{read_jsonl_range, read_jsonl_vec};

pub use helpers::jsonl::read_jsonl_streaming;

#[cfg(feature = "parallel-io")]
pub use io::jsonl::write_jsonl_par;

pub use io::csv::{read_csv_vec, write_csv, write_csv_vec};

#[cfg(feature = "parallel-io")]
pub use io::csv::write_csv_par;

pub use io::parquet::{read_parquet_vec, write_parquet_vec};

#[cfg(feature = "parallel-io")]
pub use io::parquet::write_parquet_par;

pub use helpers::csv::read_csv;
pub use helpers::csv::read_csv_streaming;
pub use helpers::jsonl::read_jsonl;
pub use helpers::parquet::{read_parquet, read_parquet_streaming};

pub use io::avro::{read_avro_vec, write_avro_vec};

pub use helpers::avro::{read_avro, read_avro_streaming};

#[cfg(feature = "parallel-io")]
pub use io::avro::write_avro_par;

pub use io::xml::{read_xml_vec, write_xml_vec};

pub use helpers::xml::{read_xml, read_xml_streaming};

#[cfg(feature = "parallel-io")]
pub use io::xml::write_xml_par;

pub use io::msgpack::{read_msgpack_vec, write_msgpack_vec};

pub use helpers::msgpack::{read_msgpack, read_msgpack_streaming};

#[cfg(feature = "parallel-io")]
pub use io::msgpack::write_msgpack_par;

pub use io::cbor::{read_cbor_vec, write_cbor_vec};

pub use helpers::cbor::{read_cbor, read_cbor_streaming};

#[cfg(feature = "parallel-io")]
pub use io::cbor::write_cbor_par;

pub use helpers::protobuf::{ProtoShards, ProtoVecOps, build_proto_shards};

#[cfg(feature = "io-protobuf")]
pub use io::protobuf::{read_proto_vec, write_proto_vec};

#[cfg(feature = "io-protobuf")]
pub use helpers::protobuf::{read_proto, read_proto_streaming};

#[cfg(all(feature = "parallel-io", feature = "io-protobuf"))]
pub use io::protobuf::write_proto_par;

pub use io::sql::{SqlShards, SqlVecOps, build_sql_shards};

#[cfg(feature = "io-sql")]
pub use io::sql::{read_sql_vec, write_sql_with};

#[cfg(feature = "io-sql")]
pub use helpers::sql::{read_sql, read_sql_streaming};

#[cfg(all(feature = "io-sql", feature = "parallel-io"))]
pub use io::sql::write_sql_par_with;

pub use io::mongodb::{MongoShards, MongoVecOps};

#[cfg(feature = "io-mongodb")]
pub use io::mongodb::{
    build_mongodb_shards, read_mongodb_range, read_mongodb_vec, write_mongodb_vec,
};

#[cfg(feature = "io-mongodb")]
pub use helpers::mongodb::{read_mongodb, read_mongodb_streaming};

#[cfg(all(feature = "io-mongodb", feature = "parallel-io"))]
pub use io::mongodb::write_mongodb_par;

pub use io::tfrecord::{TFRecordShards, TFRecordVecOps, build_tfrecord_shards};

pub use io::tfrecord::{read_tfrecord_vec, write_tfrecord_vec};

pub use helpers::tfrecord::{read_tfrecord, read_tfrecord_streaming};

#[cfg(feature = "parallel-io")]
pub use io::tfrecord::write_tfrecord_par;

#[cfg(all(feature = "io-tfrecord", feature = "io-protobuf"))]
pub use helpers::tfrecord::{
    BytesList, Example, Feature, Features, FloatList, Int64List, read_tfrecord_examples,
};

#[cfg(all(feature = "io-tfrecord", feature = "io-protobuf"))]
pub use io::tfrecord::read_tfrecord_examples_vec;

#[cfg(feature = "io-arrow")]
pub use helpers::arrow_ipc::{read_arrow_ipc, read_arrow_ipc_batches, read_arrow_ipc_streaming};
pub use io::arrow_ipc::ArrowShards;
#[cfg(feature = "io-arrow")]
pub use io::arrow_ipc::{
    ArrowBatch, ArrowBatchVecOps, build_arrow_shards, read_arrow_ipc_range,
    read_arrow_ipc_rows_range, read_arrow_ipc_rows_vec, read_arrow_ipc_vec,
    write_arrow_ipc_rows_vec, write_arrow_ipc_vec,
};
#[cfg(all(feature = "io-arrow", feature = "parallel-io"))]
pub use io::arrow_ipc::{write_arrow_ipc_par, write_arrow_ipc_rows_par};
