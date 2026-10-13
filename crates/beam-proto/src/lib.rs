//! Prost/tonic-generated Rust types for Apache Beam's portability protobufs.
//!
//! The `.proto` files are vendored from <https://github.com/apache/beam> under
//! `crates/beam-proto/proto/` and regenerated on each build by `build.rs`.
//!
//! Inter-package references in the generated code use `super::super::...`
//! paths that assume the module tree mirrors the proto package tree. The
//! nested `mod org { mod apache { ... } }` below is deliberately shaped to
//! match that assumption — do not flatten it.

#![allow(clippy::all)]
#![allow(clippy::pedantic, clippy::nursery)]
#![allow(non_snake_case)]
#![allow(dead_code)]
#![allow(rustdoc::broken_intra_doc_links, rustdoc::invalid_html_tags)]

pub mod org {
    pub mod apache {
        pub mod beam {
            pub mod model {
                pub mod pipeline {
                    pub mod v1 {
                        include!(concat!(
                            env!("OUT_DIR"),
                            "/org.apache.beam.model.pipeline.v1.rs"
                        ));
                    }
                }
                pub mod fn_execution {
                    pub mod v1 {
                        include!(concat!(
                            env!("OUT_DIR"),
                            "/org.apache.beam.model.fn_execution.v1.rs"
                        ));
                    }
                }
                pub mod job_management {
                    pub mod v1 {
                        include!(concat!(
                            env!("OUT_DIR"),
                            "/org.apache.beam.model.job_management.v1.rs"
                        ));
                    }
                }
            }
        }
    }
}

// Short aliases for consumers.
pub use org::apache::beam::model::fn_execution::v1 as fn_execution_v1;
pub use org::apache::beam::model::job_management::v1 as job_management_v1;
pub use org::apache::beam::model::pipeline::v1 as pipeline_v1;
