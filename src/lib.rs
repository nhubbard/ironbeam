pub mod collection;
pub mod collection_helpers;
pub mod combiners;
pub mod io;
pub mod node;
pub mod node_id;
pub mod pipeline;
pub mod planner;
pub mod runner;
pub mod type_token;
pub mod window;

// General re-exports
pub use collection::{CombineFn, Count, PCollection, RFBound};
pub use collection_helpers::{from_iter, from_vec, side_hashmap, side_vec};
pub use combiners::{AverageF64, DistinctCount, Max, Min, Sum, TopK};
pub use node_id::NodeId;
pub use pipeline::Pipeline;
pub use runner::{ExecMode, Runner};
pub use type_token::Partition;
pub use window::{Timestamped, Window, TimestampMs};

// Gated re-exports
#[cfg(feature = "io-jsonl")]
pub use io::jsonl::{read_jsonl_range, read_jsonl_vec};

#[cfg(feature = "io-jsonl")]
pub use collection_helpers::{read_jsonl, read_jsonl_streaming};

#[cfg(all(feature = "io-jsonl", feature = "parallel-io"))]
pub use io::jsonl::write_jsonl_par;

#[cfg(feature = "io-csv")]
pub use io::csv::{read_csv_vec, write_csv, write_csv_vec};

#[cfg(all(feature = "io-csv", feature = "parallel-io"))]
pub use io::csv::write_csv_par;

#[cfg(feature = "io-csv")]
pub use collection_helpers::{read_csv, read_csv_streaming};

#[cfg(feature = "io-parquet")]
pub use io::parquet::{read_parquet_vec, write_parquet_vec};

#[cfg(feature = "io-parquet")]
pub use collection_helpers::read_parquet_streaming;
