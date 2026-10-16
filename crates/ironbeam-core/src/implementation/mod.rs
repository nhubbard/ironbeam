use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::any::{Any, TypeId};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt;
use std::fmt::Write as _;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, OnceLock};

mod ordered_f64;
pub use ordered_f64::OrdF64;
mod restriction;
pub use restriction::{
    OffsetRange, OffsetRangeChunks, OffsetRangeTracker, RestrictionSplit, RestrictionTracker,
};
mod splittable;
pub use splittable::{OffsetProcessContext, OffsetWatermarkEstimator};
mod timer_queue;
pub use timer_queue::{TimerFiring, TimerIdentity, TimerMutation, TimerQueue, TimerSchedule};
mod sharded_key;
pub use sharded_key::{BeamPortableKey, BeamShardedBatch, BeamShardedKey};

include!("element.rs");
include!("descriptors.rs");
include!("graph.rs");
include!("runtime.rs");
include!("stateful_timer_runtime.rs");
include!("registry.rs");
include!("pipeline.rs");
include!("collection.rs");
include!("splittable_collection.rs");
include!("side_views.rs");
include!("grouping.rs");
include!("session_join.rs");
include!("stateful.rs");
include!("stateful_collection.rs");
include!("stateful_timer_collection.rs");
include!("stateful_bag_collection.rs");
include!("stateful_set_collection.rs");
include!("stateful_map_collection.rs");
include!("stateful_multimap_collection.rs");
include!("stateful_ordered_list_collection.rs");
include!("stateful_combining_collection.rs");
include!("keyed.rs");
include!("keyed_stream_byte_batches.rs");
include!("keyed_stream_timed_byte_batches.rs");
include!("co_group_homogeneous.rs");
include!("output.rs");

#[cfg(test)]
#[path = "../../tests/unit/lib__internal_tests.rs"]
mod internal_tests;
