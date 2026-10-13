// Unit tests for src/mode.rs.
use super::*;

#[test]
fn driver_without_control_endpoint() {
    assert_eq!(Mode::detect::<_, &str>([]), Mode::Driver);
    assert_eq!(Mode::detect(["--other=1", "--id=w"]), Mode::Driver);
}

#[test]
fn harness_with_space_form() {
    assert_eq!(
        Mode::detect(["--id", "w", "--control_endpoint", "localhost:1"]),
        Mode::Harness
    );
}

#[test]
fn harness_with_equals_form() {
    assert_eq!(
        Mode::detect(["--control_endpoint=localhost:1", "--id=w"]),
        Mode::Harness
    );
}

#[test]
fn portable_worker_detects_provision_only_launch() {
    assert_eq!(
        Mode::detect_portable(["--id=w", "--provision_endpoint=localhost:1"]),
        Mode::Harness
    );
    assert_eq!(
        Mode::detect_portable(["--id", "w", "--provision_endpoint", "localhost:1"]),
        Mode::Harness
    );
    assert_eq!(Mode::detect_portable::<_, &str>([]), Mode::Driver);
}
