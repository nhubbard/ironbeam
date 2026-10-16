//! Beam Fn API bundle execution for bounded v5 portable graphs.
//! The runner supplies windowed, length-prefixed element bytes; the worker
//! rebuilds the graph and uses its registered `Element` coders by stable key.

include!("v5_executor/wire.rs");
include!("v5_executor/group_boundaries.rs");
include!("v5_executor/fold_boundaries.rs");
include!("v5_executor/v5_executor_setup.rs");
include!("v5_executor/v5_executor_map_windows.rs");
include!("v5_executor/v5_executor_merge_windows.rs");
include!("v5_executor/v5_executor_sdf_process.rs");
include!("v5_executor/v5_executor_offset_sdf.rs");
include!("v5_executor/v5_executor_validate.rs");
include!("v5_executor/v5_executor_apply.rs");
include!("v5_executor/stateful_cell.rs");
include!("v5_executor/timed_cell.rs");
include!("v5_executor/v5_executor_records.rs");
include!("v5_executor/bundle_runner.rs");
