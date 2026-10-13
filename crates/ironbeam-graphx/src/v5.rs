//! Bounded v5 graph lowering for the first Dataflow contract slice.
//! Bounded source-rooted Map/Filter/FlatMap/Flatten/GroupByKey graphs with
//! registered element coders are accepted. Validation happens before
//! any source is opened, and the v4 bincode wire contract is never reused.

include!("v5/descriptors.rs");
include!("v5/contract.rs");
include!("v5/ids.rs");
include!("v5/lowering.rs");
