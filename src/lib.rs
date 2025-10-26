pub mod node_id;
pub mod node;
pub mod pipeline;
pub mod collection;
pub mod runner;
pub mod type_token;
pub mod io;
pub mod collection_helpers;

// General re-exports
pub use node_id::NodeId;
pub use pipeline::Pipeline;
pub use collection::{PCollection, RFBound, CombineFn, Count};
pub use collection_helpers::{from_vec, from_iter, side_hashmap, side_vec};
pub use runner::{Runner, ExecMode};
pub use type_token::Partition;

// Gated re-exports
#[cfg(feature = "io-jsonl")]
pub use io::jsonl::{read_jsonl_vec, read_jsonl_range};

#[cfg(feature = "io-jsonl")]
pub use collection_helpers::{read_jsonl, read_jsonl_streaming};

#[cfg(all(feature = "io-jsonl", feature = "parallel-io"))]
pub use io::jsonl::{write_jsonl_par};

#[cfg(feature = "io-csv")]
pub use io::csv::{read_csv_vec, write_csv_vec, write_csv};

#[cfg(all(feature = "io-csv", feature = "parallel-io"))]
pub use io::csv::{write_csv_par};

#[cfg(feature = "io-csv")]
pub use collection_helpers::{read_csv, read_csv_streaming};

#[cfg(feature = "io-parquet")]
pub use io::parquet::{read_parquet_vec, write_parquet_vec};

#[cfg(feature = "io-parquet")]
pub use collection_helpers::{read_parquet_streaming};