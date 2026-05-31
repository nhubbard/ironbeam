//! Runtime-stub contract for feature-gated I/O connectors.
//!
//! Ironbeam keeps the full I/O API surface present in every build: when an
//! `io-*` feature is disabled, its functions are compiled as stubs that return a
//! runtime error rather than being removed (which would break compilation of any
//! dependent code). These assertions verify that contract.
//!
//! Each assertion is gated on its feature being **off**, so under the normal
//! `--all-features` test run they compile to nothing. They are exercised by the
//! feature-matrix CI lane via `cargo test --no-default-features`.

/// With `io-msgpack` disabled, the reader stub must surface a clear runtime error
/// instead of failing to compile or silently succeeding.
#[cfg(not(feature = "io-msgpack"))]
#[test]
fn msgpack_disabled_returns_runtime_error() {
    let result = ironbeam::read_msgpack_vec::<u8>("does-not-matter.msgpack");
    let err = result.expect_err("disabled `io-msgpack` must return an error");
    assert!(
        format!("{err}").contains("`io-msgpack` feature is not enabled"),
        "unexpected error message: {err}"
    );
}
