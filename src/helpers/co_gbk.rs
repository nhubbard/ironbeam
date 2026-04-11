//! Multi-way ``CoGroupByKey`` operations supporting up to 10-way joins.
//!
//! This module provides tagged enums and macros to perform N-way full outer joins
//! on keyed collections. Unlike the binary joins in [`crate::helpers::joins`],
//! ``CoGroupByKey`` returns all values grouped by key from all input collections.
//!
//! ## How it works
//!
//! 1. Each input `PCollection<(K, V_i)>` is tagged with a unique type-level marker
//! 2. All tagged collections are flattened into a single `PCollection<(K, Tagged)>`
//! 3. `GroupByKey` groups all values by their common key
//! 4. The tag partitions values into separate vectors
//! 5. Result is `PCollection<(K, (Vec<V1>, Vec<V2>, ...))>`
//!
//! ## Examples
//!
//! ### 3-way ``CoGroupByKey``
//! ```no_run
//! use ironbeam::*;
//! use anyhow::Result;
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//! let users: PCollection<(String, String)> = from_vec(&p, vec![
//!     ("user1".to_string(), "Alice".to_string()),
//! ]);
//! let orders: PCollection<(String, u32)> = from_vec(&p, vec![
//!     ("user1".to_string(), 100),
//!     ("user1".to_string(), 200),
//! ]);
//! let addresses: PCollection<(String, String)> = from_vec(&p, vec![
//!     ("user1".to_string(), "123 Main St".to_string()),
//! ]);
//!
//! // Co-group returns (K, (Vec<V1>, Vec<V2>, Vec<V3>))
//! let cogroup = cogroup_by_key!(users, orders, addresses);
//!
//! // Process with flat_map or map
//! let results = cogroup.flat_map(|(user_id, (users, orders, addrs))| {
//!     // Custom join logic
//!     // users: Vec<String>
//!     // orders: Vec<u32>
//!     // addrs: Vec<String>
//!     vec![format!("{}: {} users, {} orders, {} addrs",
//!                  user_id, users.len(), orders.len(), addrs.len())]
//! });
//! # Ok(()) }
//! ```

#![allow(clippy::similar_names)]
#![allow(clippy::type_complexity)]

use crate::{PCollection, RFBound};
use std::hash::Hash;

// Macro to generate a single Tagged enum with N type parameters
// Usage: generate_tagged_enum!(2) produces Tagged2<V1, V2> with variants V1(V1), V2(V2)
macro_rules! generate_tagged_enum {
    // Generate Tagged2
    (2) => {
        /// Tagged enum for `2`-way `CoGroupByKey`
        #[derive(Clone, Debug)]
        pub enum Tagged2<V1, V2> {
            /// Value from input V1
            V1(V1),
            /// Value from input V2
            V2(V2),
        }
    };
    // Generate Tagged3
    (3) => {
        /// Tagged enum for `3`-way `CoGroupByKey`
        #[derive(Clone, Debug)]
        pub enum Tagged3<V1, V2, V3> {
            /// Value from input V1
            V1(V1),
            /// Value from input V2
            V2(V2),
            /// Value from input V3
            V3(V3),
        }
    };
    // Generate Tagged4
    (4) => {
        /// Tagged enum for `4`-way `CoGroupByKey`
        #[derive(Clone, Debug)]
        pub enum Tagged4<V1, V2, V3, V4> {
            /// Value from input V1
            V1(V1),
            /// Value from input V2
            V2(V2),
            /// Value from input V3
            V3(V3),
            /// Value from input V4
            V4(V4),
        }
    };
    // Generate Tagged5
    (5) => {
        /// Tagged enum for `5`-way `CoGroupByKey`
        #[derive(Clone, Debug)]
        pub enum Tagged5<V1, V2, V3, V4, V5> {
            /// Value from input V1
            V1(V1),
            /// Value from input V2
            V2(V2),
            /// Value from input V3
            V3(V3),
            /// Value from input V4
            V4(V4),
            /// Value from input V5
            V5(V5),
        }
    };
    // Generate Tagged6
    (6) => {
        /// Tagged enum for `6`-way `CoGroupByKey`
        #[derive(Clone, Debug)]
        pub enum Tagged6<V1, V2, V3, V4, V5, V6> {
            /// Value from input V1
            V1(V1),
            /// Value from input V2
            V2(V2),
            /// Value from input V3
            V3(V3),
            /// Value from input V4
            V4(V4),
            /// Value from input V5
            V5(V5),
            /// Value from input V6
            V6(V6),
        }
    };
    // Generate Tagged7
    (7) => {
        /// Tagged enum for `7`-way `CoGroupByKey`
        #[derive(Clone, Debug)]
        pub enum Tagged7<V1, V2, V3, V4, V5, V6, V7> {
            /// Value from input V1
            V1(V1),
            /// Value from input V2
            V2(V2),
            /// Value from input V3
            V3(V3),
            /// Value from input V4
            V4(V4),
            /// Value from input V5
            V5(V5),
            /// Value from input V6
            V6(V6),
            /// Value from input V7
            V7(V7),
        }
    };
    // Generate Tagged8
    (8) => {
        /// Tagged enum for `8`-way `CoGroupByKey`
        #[derive(Clone, Debug)]
        pub enum Tagged8<V1, V2, V3, V4, V5, V6, V7, V8> {
            /// Value from input V1
            V1(V1),
            /// Value from input V2
            V2(V2),
            /// Value from input V3
            V3(V3),
            /// Value from input V4
            V4(V4),
            /// Value from input V5
            V5(V5),
            /// Value from input V6
            V6(V6),
            /// Value from input V7
            V7(V7),
            /// Value from input V8
            V8(V8),
        }
    };
    // Generate Tagged9
    (9) => {
        /// Tagged enum for `9`-way `CoGroupByKey`
        #[derive(Clone, Debug)]
        pub enum Tagged9<V1, V2, V3, V4, V5, V6, V7, V8, V9> {
            /// Value from input V1
            V1(V1),
            /// Value from input V2
            V2(V2),
            /// Value from input V3
            V3(V3),
            /// Value from input V4
            V4(V4),
            /// Value from input V5
            V5(V5),
            /// Value from input V6
            V6(V6),
            /// Value from input V7
            V7(V7),
            /// Value from input V8
            V8(V8),
            /// Value from input V9
            V9(V9),
        }
    };
    // Generate Tagged10
    (10) => {
        /// Tagged enum for `10`-way `CoGroupByKey`
        #[derive(Clone, Debug)]
        pub enum Tagged10<V1, V2, V3, V4, V5, V6, V7, V8, V9, V10> {
            /// Value from input V1
            V1(V1),
            /// Value from input V2
            V2(V2),
            /// Value from input V3
            V3(V3),
            /// Value from input V4
            V4(V4),
            /// Value from input V5
            V5(V5),
            /// Value from input V6
            V6(V6),
            /// Value from input V7
            V7(V7),
            /// Value from input V8
            V8(V8),
            /// Value from input V9
            V9(V9),
            /// Value from input V10
            V10(V10),
        }
    };
}

// Generate Tagged2 through Tagged10 - just pass the numbers!
generate_tagged_enum!(2);
generate_tagged_enum!(3);
generate_tagged_enum!(4);
generate_tagged_enum!(5);
generate_tagged_enum!(6);
generate_tagged_enum!(7);
generate_tagged_enum!(8);
generate_tagged_enum!(9);
generate_tagged_enum!(10);

/// Macro for performing N-way ``CoGroupByKey`` operations.
///
/// Supports 2 to 10 input collections. All collections must have the same key type `K`.
///
/// # Examples
///
/// ## 2-way ``CoGroupByKey``
/// ```no_run
/// # use ironbeam::*;
/// # let p = Pipeline::default();
/// # let c1: PCollection<(String, u32)> = from_vec(&p, vec![]);
/// # let c2: PCollection<(String, String)> = from_vec(&p, vec![]);
/// let result = cogroup_by_key!(c1, c2);
/// // Type: PCollection<(String, (Vec<u32>, Vec<String>))>
/// ```
///
/// ## 3-way ``CoGroupByKey``
/// ```no_run
/// # use ironbeam::*;
/// # let p = Pipeline::default();
/// # let c1: PCollection<(String, u32)> = from_vec(&p, vec![]);
/// # let c2: PCollection<(String, String)> = from_vec(&p, vec![]);
/// # let c3: PCollection<(String, bool)> = from_vec(&p, vec![]);
/// let result = cogroup_by_key!(c1, c2, c3);
/// // Type: PCollection<(String, (Vec<u32>, Vec<String>, Vec<bool>))>
/// ```
#[macro_export]
macro_rules! cogroup_by_key {
    ($c1:expr, $c2:expr) => {{ $crate::helpers::co_gbk::cogroup_by_key_2(&$c1, &$c2) }};
    ($c1:expr, $c2:expr, $c3:expr) => {{ $crate::helpers::co_gbk::cogroup_by_key_3(&$c1, &$c2, &$c3) }};
    ($c1:expr, $c2:expr, $c3:expr, $c4:expr) => {{ $crate::helpers::co_gbk::cogroup_by_key_4(&$c1, &$c2, &$c3, &$c4) }};
    ($c1:expr, $c2:expr, $c3:expr, $c4:expr, $c5:expr) => {{ $crate::helpers::co_gbk::cogroup_by_key_5(&$c1, &$c2, &$c3, &$c4, &$c5) }};
    ($c1:expr, $c2:expr, $c3:expr, $c4:expr, $c5:expr, $c6:expr) => {{ $crate::helpers::co_gbk::cogroup_by_key_6(&$c1, &$c2, &$c3, &$c4, &$c5, &$c6) }};
    ($c1:expr, $c2:expr, $c3:expr, $c4:expr, $c5:expr, $c6:expr, $c7:expr) => {{ $crate::helpers::co_gbk::cogroup_by_key_7(&$c1, &$c2, &$c3, &$c4, &$c5, &$c6, &$c7) }};
    ($c1:expr, $c2:expr, $c3:expr, $c4:expr, $c5:expr, $c6:expr, $c7:expr, $c8:expr) => {{ $crate::helpers::co_gbk::cogroup_by_key_8(&$c1, &$c2, &$c3, &$c4, &$c5, &$c6, &$c7, &$c8) }};
    ($c1:expr, $c2:expr, $c3:expr, $c4:expr, $c5:expr, $c6:expr, $c7:expr, $c8:expr, $c9:expr) => {{
        $crate::helpers::co_gbk::cogroup_by_key_9(
            &$c1, &$c2, &$c3, &$c4, &$c5, &$c6, &$c7, &$c8, &$c9,
        )
    }};
    ($c1:expr, $c2:expr, $c3:expr, $c4:expr, $c5:expr, $c6:expr, $c7:expr, $c8:expr, $c9:expr, $c10:expr) => {{
        $crate::helpers::co_gbk::cogroup_by_key_10(
            &$c1, &$c2, &$c3, &$c4, &$c5, &$c6, &$c7, &$c8, &$c9, &$c10,
        )
    }};
}

// Macro to generate all cogroup_by_key_N implementation functions
macro_rules! generate_cogroup_impls {
    // Generate implementation for each N-way cogroup
    ($(
        $n:tt => {
            fn_name: $fn_name:ident,
            tagged: $tagged:ident,
            params: [$(($idx:tt, $param:ident, $variant:ident)),+ $(,)?],
        }
    ),+ $(,)?) => {
        $(
            #[doc = concat!(" Performs a ", stringify!($n), "-way `CoGroupByKey` operation.")]
            #[doc = " "]
            #[doc = " Returns `PCollection<(K, (Vec<V1>, Vec<V2>, ...))>` with all values grouped by key."]
            #[must_use]
            #[allow(clippy::too_many_arguments)]
            pub fn $fn_name<K, $($variant),+>(
                $($param: &PCollection<(K, $variant)>),+
            ) -> PCollection<(K, ($(Vec<$variant>),+))>
            where
                K: RFBound + Hash + Eq,
                $($variant: RFBound),+
            {
                use crate::helpers::flatten::flatten;

                // Tag each collection with its variant type
                $(
                    paste::paste! {
                        let [<tagged $idx>] = $param.clone().map(|(k, v)| (k.clone(), $tagged::$variant(v.clone())));
                    }
                )+

                // Flatten all tagged collections and group by key
                paste::paste! {
                    let flattened = flatten(&[$(&[<tagged $idx>]),+]);
                }
                let grouped = flattened.group_by_key();

                // Partition values by their tags and return grouped result
                grouped.map(|(k, values)| {
                    let k = k.clone();

                    // Initialize result vectors for each input
                    $(
                        paste::paste! {
                            let mut [<v $idx s>] = Vec::new();
                        }
                    )+

                    // Partition values by their tags
                    for tagged in values {
                        match tagged {
                            $(
                                $tagged::$variant(v) => {
                                    paste::paste! {
                                        [<v $idx s>].push(v.clone());
                                    }
                                }
                            )+
                        }
                    }

                    // Return tuple of (key, (vec1, vec2, ...))
                    paste::paste! {
                        (k, ($([<v $idx s>]),+))
                    }
                })
            }
        )+
    };
}

// Generate all implementations from 2-way to 10-way
generate_cogroup_impls!(
    2 => {
        fn_name: cogroup_by_key_2,
        tagged: Tagged2,
        params: [(1, c1, V1), (2, c2, V2)],
    },
    3 => {
        fn_name: cogroup_by_key_3,
        tagged: Tagged3,
        params: [(1, c1, V1), (2, c2, V2), (3, c3, V3)],
    },
    4 => {
        fn_name: cogroup_by_key_4,
        tagged: Tagged4,
        params: [(1, c1, V1), (2, c2, V2), (3, c3, V3), (4, c4, V4)],
    },
    5 => {
        fn_name: cogroup_by_key_5,
        tagged: Tagged5,
        params: [(1, c1, V1), (2, c2, V2), (3, c3, V3), (4, c4, V4), (5, c5, V5)],
    },
    6 => {
        fn_name: cogroup_by_key_6,
        tagged: Tagged6,
        params: [(1, c1, V1), (2, c2, V2), (3, c3, V3), (4, c4, V4), (5, c5, V5), (6, c6, V6)],
    },
    7 => {
        fn_name: cogroup_by_key_7,
        tagged: Tagged7,
        params: [(1, c1, V1), (2, c2, V2), (3, c3, V3), (4, c4, V4), (5, c5, V5), (6, c6, V6), (7, c7, V7)],
    },
    8 => {
        fn_name: cogroup_by_key_8,
        tagged: Tagged8,
        params: [(1, c1, V1), (2, c2, V2), (3, c3, V3), (4, c4, V4), (5, c5, V5), (6, c6, V6), (7, c7, V7), (8, c8, V8)],
    },
    9 => {
        fn_name: cogroup_by_key_9,
        tagged: Tagged9,
        params: [(1, c1, V1), (2, c2, V2), (3, c3, V3), (4, c4, V4), (5, c5, V5), (6, c6, V6), (7, c7, V7), (8, c8, V8), (9, c9, V9)],
    },
    10 => {
        fn_name: cogroup_by_key_10,
        tagged: Tagged10,
        params: [(1, c1, V1), (2, c2, V2), (3, c3, V3), (4, c4, V4), (5, c5, V5), (6, c6, V6), (7, c7, V7), (8, c8, V8), (9, c9, V9), (10, c10, V10)],
    },
);
