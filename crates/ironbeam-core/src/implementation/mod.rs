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

include!("element.rs");
include!("descriptors.rs");
include!("graph.rs");
include!("runtime.rs");
include!("registry.rs");
include!("pipeline.rs");
include!("collection.rs");
include!("side_views.rs");
include!("grouping.rs");
include!("session_join.rs");
include!("keyed.rs");
include!("co_group_homogeneous.rs");
include!("output.rs");

#[cfg(test)]
#[path = "../../tests/unit/lib__internal_tests.rs"]
mod internal_tests;
