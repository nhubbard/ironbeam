#![allow(clippy::missing_errors_doc)]
//! Composed convenience transforms over runner-neutral Ironbeam graph primitives.
mod basic_fold;
mod predicates;
mod representative;
pub use predicates::PredicateExt;
mod selection;
pub use selection::{KeyedSelectionExt, SelectionExt};
mod collections;
pub use collections::{CollectionExt, KeyedCollectionExt};
mod fallible;
pub use fallible::TryProcessExt;
mod sampling;
pub use sampling::{KeyedSamplingExt, SamplingExt};
mod statistical;
pub use statistical::{KeyedStatisticalExt, StatisticalExt, TDigest};
mod side_inputs;
pub use side_inputs::{
    ConstantSideInputExt, DynamicSideInputExt, SideMap, SideMultimap, SideSingleton, SideVec,
    side_hashmap, side_multimap, side_singleton, side_vec,
};
mod log_elements;
pub use log_elements::LogElementsExt;
mod validation;
pub use validation::{
    ErrorCollector, KeyedValidationExt, RecordError, Validate, ValidationError, ValidationExt,
    ValidationMode, ValidationResult,
};
pub use validation::{combine_validations, validators};
mod dead_letter;
pub use dead_letter::{Classified, DeadLetter, DeadLetterExt};
mod tee;
pub use tee::TeeExt;
mod partition;
pub use partition::PartitionExt;
mod windowed;
pub use windowed::{KeyedWindowedExt, WindowedExt};

mod implementation;
pub use implementation::*;
