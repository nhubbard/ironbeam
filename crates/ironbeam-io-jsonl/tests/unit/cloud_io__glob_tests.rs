// Unit tests for src/cloud_io.rs.
use super::glob_matches;

#[test]
fn wildcard_rules_match_the_existing_cloud_glob_alphabet() {
    let cases = [
        ("data/*.jsonl", "data/a.jsonl", true),
        ("data/*.jsonl", "data/nested/a.jsonl", false),
        ("data/**/*.jsonl", "data/nested/a.jsonl", true),
        ("data/**/*.jsonl", "data/a.jsonl", false),
        ("data/?.jsonl", "data/é.jsonl", true),
        ("data/?.jsonl", "data/ab.jsonl", false),
        ("data/a[1]+(x).jsonl", "data/a[1]+(x).jsonl", true),
        ("data/a[1]+(x).jsonl", "data/a11x.jsonl", false),
        ("***", "a/b", true),
        ("**", "a\nb", false),
        ("?", "\n", false),
        ("*", "\n", true),
        ("", "", true),
        ("", "a", false),
    ];
    for (pattern, key, expected) in cases {
        assert_eq!(glob_matches(pattern, key), expected, "{pattern:?} {key:?}");
    }
}
