//! Comprehensive tests for feature 4.8: `to_display_string`.
//!
//! Validates that each element's [`Display`] representation is emitted as a
//! `String` and that the transform composes correctly with other operators.

use ironbeam::*;
use serde::{Deserialize, Serialize};
use std::fmt;

// ── Basic correctness across `Display`-implementing types ────────────────────

/// Integer elements format as their decimal `Display`.
#[test]
fn test_to_display_string_integers() {
    let p = Pipeline::default();
    let strings = from_vec(&p, vec![1u32, 2, 3, 100, 1_000_000])
        .to_display_string()
        .collect_seq()
        .unwrap();
    assert_eq!(
        strings,
        vec![
            "1".to_string(),
            "2".into(),
            "3".into(),
            "100".into(),
            "1000000".into(),
        ]
    );
}

/// Signed integers including negatives.
#[test]
fn test_to_display_string_signed_integers() {
    let p = Pipeline::default();
    let strings = from_vec(&p, vec![-5i32, -1, 0, 1, 42])
        .to_display_string()
        .collect_seq()
        .unwrap();
    assert_eq!(
        strings,
        vec![
            "-5".to_string(),
            "-1".into(),
            "0".into(),
            "1".into(),
            "42".into(),
        ]
    );
}

/// Floating-point elements format via `Display`. Note: `Display` for floats
/// uses the shortest unambiguous representation by default.
#[test]
fn test_to_display_string_floats() {
    let p = Pipeline::default();
    let strings = from_vec(&p, vec![1.0_f64, 2.5, -3.25, 0.0])
        .to_display_string()
        .collect_seq()
        .unwrap();
    assert_eq!(
        strings,
        vec!["1".to_string(), "2.5".into(), "-3.25".into(), "0".into(),]
    );
}

/// `bool` formats as `"true"` / `"false"`.
#[test]
fn test_to_display_string_bool() {
    let p = Pipeline::default();
    let strings = from_vec(&p, vec![true, false, true, true, false])
        .to_display_string()
        .collect_seq()
        .unwrap();
    assert_eq!(
        strings,
        vec![
            "true".to_string(),
            "false".into(),
            "true".into(),
            "true".into(),
            "false".into(),
        ]
    );
}

/// `char` formats as the single character.
#[test]
fn test_to_display_string_char() {
    let p = Pipeline::default();
    let strings = from_vec(&p, vec!['a', 'b', 'Ω', '🦀'])
        .to_display_string()
        .collect_seq()
        .unwrap();
    assert_eq!(
        strings,
        vec!["a".to_string(), "b".into(), "Ω".into(), "🦀".into(),]
    );
}

/// `String` input passes through (its `Display` is the string itself).
#[test]
fn test_to_display_string_string_passthrough() {
    let p = Pipeline::default();
    let strings = from_vec(&p, vec!["alpha".to_string(), "beta".into(), "gamma".into()])
        .to_display_string()
        .collect_seq()
        .unwrap();
    assert_eq!(
        strings,
        vec!["alpha".to_string(), "beta".into(), "gamma".into()]
    );
}

// ── Edge cases ──────────────────────────────────────────────────────────────

/// Empty input → empty output.
#[test]
fn test_to_display_string_empty() {
    let p = Pipeline::default();
    let input: Vec<u32> = vec![];
    let strings = from_vec(&p, input)
        .to_display_string()
        .collect_seq()
        .unwrap();
    assert!(strings.is_empty());
}

/// Single element.
#[test]
fn test_to_display_string_single_element() {
    let p = Pipeline::default();
    let strings = from_vec(&p, vec![42u64])
        .to_display_string()
        .collect_seq()
        .unwrap();
    assert_eq!(strings, vec!["42".to_string()]);
}

// ── Custom Display impl ─────────────────────────────────────────────────────

/// Custom struct with a manual `Display` impl.
#[test]
fn test_to_display_string_custom_display_struct() {
    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Point {
        x: i32,
        y: i32,
    }
    impl fmt::Display for Point {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "({}, {})", self.x, self.y)
        }
    }

    let p = Pipeline::default();
    let strings = from_vec(
        &p,
        vec![
            Point { x: 1, y: 2 },
            Point { x: -3, y: 0 },
            Point { x: 100, y: 200 },
        ],
    )
    .to_display_string()
    .collect_seq()
    .unwrap();

    assert_eq!(
        strings,
        vec!["(1, 2)".to_string(), "(-3, 0)".into(), "(100, 200)".into(),]
    );
}

/// Custom enum with a manual `Display` impl.
#[test]
fn test_to_display_string_custom_display_enum() {
    #[derive(Clone, Debug, Serialize, Deserialize)]
    enum Color {
        Red,
        Green,
        Rgb(u8, u8, u8),
    }
    impl fmt::Display for Color {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Red => write!(f, "red"),
                Self::Green => write!(f, "green"),
                Self::Rgb(r, g, b) => write!(f, "#{r:02x}{g:02x}{b:02x}"),
            }
        }
    }

    let p = Pipeline::default();
    let strings = from_vec(
        &p,
        vec![Color::Red, Color::Green, Color::Rgb(0xff, 0x80, 0x00)],
    )
    .to_display_string()
    .collect_seq()
    .unwrap();

    assert_eq!(
        strings,
        vec!["red".to_string(), "green".into(), "#ff8000".into()]
    );
}

// ── Parallel execution / preservation ───────────────────────────────────────

/// Parallel execution preserves element count and content.
#[test]
fn test_to_display_string_parallel() {
    let p = Pipeline::default();
    let n = 200u32;
    let mut strings = from_vec(&p, (0..n).collect::<Vec<_>>())
        .to_display_string()
        .collect_par(Some(4), Some(4))
        .unwrap();
    strings.sort();

    let mut expected: Vec<String> = (0..n).map(|i| i.to_string()).collect();
    expected.sort();
    assert_eq!(strings, expected);
}

// ── Composition ─────────────────────────────────────────────────────────────

/// Chained with `map` to transform after stringification.
#[test]
fn test_to_display_string_then_map_length() {
    let p = Pipeline::default();
    let lens = from_vec(&p, vec![1u32, 12, 123, 1234, 12345])
        .to_display_string()
        .map(String::len)
        .collect_seq()
        .unwrap();
    assert_eq!(lens, vec![1usize, 2, 3, 4, 5]);
}

/// Chained with `filter` on the resulting strings.
#[test]
fn test_to_display_string_then_filter_length() {
    let p = Pipeline::default();
    let strings = from_vec(&p, vec![1u32, 22, 333, 4444, 55555])
        .to_display_string()
        .filter(|s: &String| s.len() >= 3)
        .collect_seq()
        .unwrap();
    assert_eq!(
        strings,
        vec!["333".to_string(), "4444".into(), "55555".into()]
    );
}

/// Stringification is identity-preserving on the `String` itself.
#[test]
fn test_to_display_string_identity_for_strings() {
    let p = Pipeline::default();
    let input = vec!["a".to_string(), "bb".into(), "ccc".into()];
    let strings = from_vec(&p, input.clone())
        .to_display_string()
        .collect_seq()
        .unwrap();
    assert_eq!(strings, input);
}

/// Large input: element count is preserved.
#[test]
fn test_to_display_string_large() {
    const N: u32 = 1_000;
    let p = Pipeline::default();
    let strings = from_vec(&p, (0..N).collect::<Vec<_>>())
        .to_display_string()
        .collect_seq()
        .unwrap();
    assert_eq!(strings.len(), N as usize);
    assert_eq!(strings[0], "0");
    assert_eq!(strings[(N - 1) as usize], (N - 1).to_string());
}
