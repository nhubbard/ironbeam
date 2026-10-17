//! Common test macros.

/// Macro to check if a floating-point value is within an acceptable tolerance.
///
/// # Usage
/// ```
/// assert_approx_eq!(actual, expected);
/// assert_approx_eq!(actual, expected, epsilon);
/// ```
#[macro_export]
macro_rules! assert_approx_eq {
    ($actual:expr, $expected:expr) => {
        assert_approx_eq!($actual, $expected, 1e-10)
    };
    ($actual:expr, $expected:expr, $epsilon:expr) => {
        let actual = $actual;
        let expected = $expected;
        let epsilon = $epsilon;
        let diff = (actual - expected).abs();
        assert!(
            diff <= epsilon,
            "assertion failed: `(left ≈ right)`\n  left: `{:?}`,\n right: `{:?}`,\n  diff: `{:?}`,\n   eps: `{:?}`",
            actual,
            expected,
            diff,
            epsilon
        );
    };
}
