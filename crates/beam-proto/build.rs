//! Compiles vendored Apache Beam `.proto` files into Rust types.
//!
//! The vendored tree is expected to mirror Beam's repo layout so the
//! per-file `import "org/apache/beam/model/..."` statements resolve.

use std::path::PathBuf;

fn main() {
    let proto_root: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto");

    let protos = [
        // pipeline v1
        "org/apache/beam/model/pipeline/v1/beam_runner_api.proto",
        "org/apache/beam/model/pipeline/v1/endpoints.proto",
        "org/apache/beam/model/pipeline/v1/metrics.proto",
        "org/apache/beam/model/pipeline/v1/schema.proto",
        "org/apache/beam/model/pipeline/v1/standard_window_fns.proto",
        "org/apache/beam/model/pipeline/v1/external_transforms.proto",
        // fn-execution v1
        "org/apache/beam/model/fn_execution/v1/beam_fn_api.proto",
        "org/apache/beam/model/fn_execution/v1/beam_provision_api.proto",
        // job-management v1
        "org/apache/beam/model/job_management/v1/beam_artifact_api.proto",
        "org/apache/beam/model/job_management/v1/beam_expansion_api.proto",
        "org/apache/beam/model/job_management/v1/beam_job_api.proto",
    ];

    let proto_paths: Vec<PathBuf> = protos.iter().map(|p| proto_root.join(p)).collect();

    // Re-run if any vendored proto changes.
    for p in &proto_paths {
        println!("cargo:rerun-if-changed={}", p.display());
    }
    println!("cargo:rerun-if-changed=build.rs");

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_well_known_types(false)
        // Beam's fn-execution protos use `proto3 optional`, which protoc < 3.15
        // gates behind this experimental flag. Accepted (as a no-op) by newer
        // protoc too, so safe across host / Docker builder toolchains.
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(
            &proto_paths.iter().map(PathBuf::as_path).collect::<Vec<_>>(),
            &[proto_root.as_path()],
        )
        .expect("beam-proto: compile_protos failed");
}
