//! Opt-in Postcard coding for user-defined v5 elements.
//!
//! A stable type ID and coder ID are part of graph and checkpoint identity.
//! Change the coder ID when a type's wire representation changes.
//!
//! ```
//! use ironbeam_coder_postcard::impl_postcard_element;
//!
//! #[derive(Clone, serde::Serialize, serde::Deserialize)]
//! struct Row { id: i32, label: String }
//! impl_postcard_element!(Row, "example/row/v1");
//! ```

#[doc(hidden)]
pub use ironbeam_core;
#[doc(hidden)]
pub use postcard;

/// Implement [`ironbeam_core::Element`] for a Serde record using Postcard.
///
/// The two-argument form uses `ironbeam/postcard-v1/<type ID>` as the coder ID.
/// Supply a third string literal when an explicit coder identity is needed.
/// The type must implement `Clone`, `Send`, `Sync`, `Serialize`, and
/// `DeserializeOwned`. Both the driver and a portable worker must compile the
/// same type and macro invocation.
#[macro_export]
macro_rules! impl_postcard_element {
    ($type:ty, $type_id:literal) => {
        $crate::impl_postcard_element!($type, $type_id, concat!("ironbeam/postcard-v1/", $type_id));
    };
    ($type:ty, $type_id:literal, $coder_id:expr) => {
        impl $crate::ironbeam_core::Element for $type {
            const TYPE_ID: &'static str = $type_id;
            const CODER_ID: &'static str = $coder_id;

            fn supports_coder() -> bool {
                true
            }

            fn encode(&self) -> Result<Vec<u8>, $crate::ironbeam_core::Error> {
                $crate::postcard::to_allocvec(self).map_err(|error| {
                    $crate::ironbeam_core::Error::incompatible(format!(
                        "Postcard encode {}: {error}",
                        Self::TYPE_ID
                    ))
                })
            }

            fn decode(bytes: &[u8]) -> Result<Self, $crate::ironbeam_core::Error> {
                $crate::postcard::from_bytes(bytes).map_err(|error| {
                    $crate::ironbeam_core::Error::incompatible(format!(
                        "Postcard decode {}: {error}",
                        Self::TYPE_ID
                    ))
                })
            }
        }
    };
}
