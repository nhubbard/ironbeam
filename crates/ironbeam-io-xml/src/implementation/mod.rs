include!("format.rs");
#[cfg(feature = "cloud")]
mod cloud;
#[cfg(feature = "cloud")]
pub use cloud::{cloud_xml_uri_prefix_source, cloud_xml_uri_source};
