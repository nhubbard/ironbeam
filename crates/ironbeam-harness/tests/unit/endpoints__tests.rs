// Unit tests for src/endpoints.rs.
use super::*;

#[test]
fn parses_equals_form() {
    let e = Endpoints::from_argv([
        "--id=worker0",
        "--control_endpoint=localhost:12345",
        "--logging_endpoint=localhost:12346",
        "--provision_endpoint=localhost:12347",
    ])
    .unwrap();
    assert_eq!(e.id, "worker0");
    assert_eq!(e.control_endpoint, "localhost:12345");
    assert_eq!(e.provision_endpoint.as_deref(), Some("localhost:12347"));
    assert!(e.artifact_endpoint.is_none());
}

#[test]
fn parses_space_form() {
    let e = Endpoints::from_argv([
        "--id",
        "w",
        "--control_endpoint",
        "c:1",
        "--logging_endpoint",
        "l:1",
    ])
    .unwrap();
    assert_eq!(e.id, "w");
    assert_eq!(e.logging_endpoint, "l:1");
}

#[test]
fn missing_required_errors() {
    let err = Endpoints::from_argv(["--id=x"]).unwrap_err();
    assert!(format!("{err}").contains("--control_endpoint"));
}

#[test]
fn normalizes_bare_host_port() {
    assert_eq!(
        normalize_endpoint("localhost:1234").unwrap(),
        "http://localhost:1234"
    );
    assert_eq!(
        normalize_endpoint("https://foo:1234").unwrap(),
        "https://foo:1234"
    );
    assert!(normalize_endpoint("localhost").is_err());
}
