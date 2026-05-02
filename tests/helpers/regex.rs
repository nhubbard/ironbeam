use anyhow::Result;
use ironbeam::*;

// ─────────────────────────────────── regex_matches ───────────────────────────────────

#[test]
fn regex_matches_keeps_matching_lines() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(
        &p,
        vec![
            "error: disk full".to_string(),
            "info: all good".to_string(),
            "error: timeout".to_string(),
        ],
    )
    .regex_matches(r"^error:")
    .collect_seq()?;

    assert_eq!(
        result,
        vec!["error: disk full".to_string(), "error: timeout".to_string()]
    );
    Ok(())
}

#[test]
fn regex_matches_no_match_produces_empty() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec!["hello".to_string(), "world".to_string()])
        .regex_matches(r"\d+")
        .collect_seq()?;

    assert!(result.is_empty());
    Ok(())
}

#[test]
fn regex_matches_all_match() -> Result<()> {
    let p = Pipeline::default();
    let input = vec!["abc123".to_string(), "def456".to_string()];
    let result = from_vec(&p, input.clone())
        .regex_matches(r"\d+")
        .collect_seq()?;

    assert_eq!(result, input);
    Ok(())
}

#[test]
fn regex_matches_empty_collection() -> Result<()> {
    let p = Pipeline::default();
    let result: Vec<String> = from_vec(&p, Vec::<String>::new())
        .regex_matches(r"\d+")
        .collect_seq()?;

    assert!(result.is_empty());
    Ok(())
}

#[test]
fn regex_matches_anchored_full_match() -> Result<()> {
    let p = Pipeline::default();
    // Pattern anchored to match the whole line (three digits, dash, four digits)
    let result = from_vec(
        &p,
        vec![
            "123-4567".to_string(),
            "not-a-phone".to_string(),
            "000-1234".to_string(),
        ],
    )
    .regex_matches(r"^\d{3}-\d{4}$")
    .collect_seq()?;

    assert_eq!(result, vec!["123-4567".to_string(), "000-1234".to_string()]);
    Ok(())
}

#[test]
fn regex_matches_single_element_match() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec!["42".to_string()])
        .regex_matches(r"\d+")
        .collect_seq()?;
    assert_eq!(result, vec!["42".to_string()]);
    Ok(())
}

#[test]
fn regex_matches_single_element_no_match() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec!["hello".to_string()])
        .regex_matches(r"\d+")
        .collect_seq()?;
    assert!(result.is_empty());
    Ok(())
}

// ─────────────────────────────────── regex_extract ───────────────────────────────────

#[test]
fn regex_extract_group_1() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec!["name: Alice".to_string(), "name: Bob".to_string()])
        .regex_extract(r"name: (\w+)", 1)
        .collect_seq()?;

    assert_eq!(result, vec!["Alice".to_string(), "Bob".to_string()]);
    Ok(())
}

#[test]
fn regex_extract_group_0_is_full_match() -> Result<()> {
    let p = Pipeline::default();
    // Group 0 is the entire match
    let result = from_vec(
        &p,
        vec![
            "price $12.99 here".to_string(),
            "free item".to_string(),
            "cost $5.00 today".to_string(),
        ],
    )
    .regex_extract(r"\$[\d.]+", 0)
    .collect_seq()?;

    assert_eq!(result, vec!["$12.99".to_string(), "$5.00".to_string()]);
    Ok(())
}

#[test]
fn regex_extract_no_match_drops_elements() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec!["hello world".to_string(), "foo bar".to_string()])
        .regex_extract(r"(\d+)", 1)
        .collect_seq()?;

    assert!(result.is_empty());
    Ok(())
}

#[test]
fn regex_extract_partial_match() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(
        &p,
        vec![
            "value: 42".to_string(),
            "no number here".to_string(),
            "count: 7".to_string(),
        ],
    )
    .regex_extract(r"(\d+)", 1)
    .collect_seq()?;

    assert_eq!(result, vec!["42".to_string(), "7".to_string()]);
    Ok(())
}

#[test]
fn regex_extract_empty_collection() -> Result<()> {
    let p = Pipeline::default();
    let result: Vec<String> = from_vec(&p, Vec::<String>::new())
        .regex_extract(r"(\w+)", 1)
        .collect_seq()?;

    assert!(result.is_empty());
    Ok(())
}

#[test]
fn regex_extract_second_group() -> Result<()> {
    let p = Pipeline::default();
    // Pattern with two groups; extract group 2
    let result = from_vec(&p, vec!["Alice 42".to_string(), "Bob 37".to_string()])
        .regex_extract(r"(\w+)\s+(\d+)", 2)
        .collect_seq()?;

    assert_eq!(result, vec!["42".to_string(), "37".to_string()]);
    Ok(())
}

// ─────────────────────────────────── regex_extract_kv ────────────────────────────────

#[test]
fn regex_extract_kv_basic() -> Result<()> {
    let p = Pipeline::default();
    let mut result = from_vec(
        &p,
        vec![
            "Alice 42".to_string(),
            "Bob 37".to_string(),
            "not matching".to_string(),
        ],
    )
    .regex_extract_kv(r"(\w+)\s+(\d+)", 1, 2)
    .collect_seq()?;

    result.sort();
    assert_eq!(
        result,
        vec![
            ("Alice".to_string(), "42".to_string()),
            ("Bob".to_string(), "37".to_string()),
        ]
    );
    Ok(())
}

#[test]
fn regex_extract_kv_no_match_produces_empty() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec!["hello world".to_string(), "foo bar".to_string()])
        .regex_extract_kv(r"(\d+)\s+(\d+)", 1, 2)
        .collect_seq()?;

    assert!(result.is_empty());
    Ok(())
}

#[test]
fn regex_extract_kv_key_val_reversed() -> Result<()> {
    let p = Pipeline::default();
    // Extract groups in reverse order: key=group2, val=group1
    let mut result = from_vec(&p, vec!["Alice 42".to_string(), "Bob 37".to_string()])
        .regex_extract_kv(r"(\w+)\s+(\d+)", 2, 1)
        .collect_seq()?;

    result.sort();
    assert_eq!(
        result,
        vec![
            ("37".to_string(), "Bob".to_string()),
            ("42".to_string(), "Alice".to_string()),
        ]
    );
    Ok(())
}

#[test]
fn regex_extract_kv_empty_collection() -> Result<()> {
    let p = Pipeline::default();
    let result: Vec<(String, String)> = from_vec(&p, Vec::<String>::new())
        .regex_extract_kv(r"(\w+)\s+(\d+)", 1, 2)
        .collect_seq()?;

    assert!(result.is_empty());
    Ok(())
}

#[test]
fn regex_extract_kv_single_match() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec!["key=value".to_string()])
        .regex_extract_kv(r"(\w+)=(\w+)", 1, 2)
        .collect_seq()?;

    assert_eq!(result, vec![("key".to_string(), "value".to_string())]);
    Ok(())
}

// ─────────────────────────────────── regex_find ──────────────────────────────────────

#[test]
fn regex_find_returns_first_match() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(
        &p,
        vec![
            "price: $12.99".to_string(),
            "total: $5.00".to_string(),
            "free item".to_string(),
        ],
    )
    .regex_find(r"\$[\d.]+")
    .collect_seq()?;

    assert_eq!(result, vec!["$12.99".to_string(), "$5.00".to_string()]);
    Ok(())
}

#[test]
fn regex_find_only_first_occurrence_per_line() -> Result<()> {
    let p = Pipeline::default();
    // Line has two digit sequences; find should return only the first
    let result = from_vec(&p, vec!["abc 123 def 456".to_string()])
        .regex_find(r"\d+")
        .collect_seq()?;

    assert_eq!(result, vec!["123".to_string()]);
    Ok(())
}

#[test]
fn regex_find_no_match_drops_element() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec!["hello world".to_string()])
        .regex_find(r"\d+")
        .collect_seq()?;

    assert!(result.is_empty());
    Ok(())
}

#[test]
fn regex_find_empty_collection() -> Result<()> {
    let p = Pipeline::default();
    let result: Vec<String> = from_vec(&p, Vec::<String>::new())
        .regex_find(r"\d+")
        .collect_seq()?;

    assert!(result.is_empty());
    Ok(())
}

#[test]
fn regex_find_all_match() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(
        &p,
        vec![
            "abc 1".to_string(),
            "def 2".to_string(),
            "ghi 3".to_string(),
        ],
    )
    .regex_find(r"\d+")
    .collect_seq()?;

    assert_eq!(
        result,
        vec!["1".to_string(), "2".to_string(), "3".to_string()]
    );
    Ok(())
}

#[test]
fn regex_find_none_match() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec!["abc".to_string(), "def".to_string()])
        .regex_find(r"\d+")
        .collect_seq()?;

    assert!(result.is_empty());
    Ok(())
}

// ─────────────────────────────────── regex_replace_all ───────────────────────────────

#[test]
fn regex_replace_all_basic() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(
        &p,
        vec!["hello   world".to_string(), "foo  bar   baz".to_string()],
    )
    .regex_replace_all(r"\s+", " ")
    .collect_seq()?;

    assert_eq!(
        result,
        vec!["hello world".to_string(), "foo bar baz".to_string()]
    );
    Ok(())
}

#[test]
fn regex_replace_all_no_match_passes_through() -> Result<()> {
    let p = Pipeline::default();
    let input = vec!["hello world".to_string(), "foo bar".to_string()];
    let result = from_vec(&p, input.clone())
        .regex_replace_all(r"\d+", "N")
        .collect_seq()?;

    assert_eq!(result, input);
    Ok(())
}

//noinspection SpellCheckingInspection
#[test]
fn regex_replace_all_multiple_per_line() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec!["a1b2c3".to_string()])
        .regex_replace_all(r"\d", "X")
        .collect_seq()?;

    assert_eq!(result, vec!["aXbXcX".to_string()]);
    Ok(())
}

#[test]
fn regex_replace_all_empty_collection() -> Result<()> {
    let p = Pipeline::default();
    let result: Vec<String> = from_vec(&p, Vec::<String>::new())
        .regex_replace_all(r"\s+", " ")
        .collect_seq()?;

    assert!(result.is_empty());
    Ok(())
}

#[test]
fn regex_replace_all_empty_replacement() -> Result<()> {
    let p = Pipeline::default();
    // Replace all digits with empty string (deletion)
    let result = from_vec(&p, vec!["a1b2c3d".to_string()])
        .regex_replace_all(r"\d", "")
        .collect_seq()?;

    assert_eq!(result, vec!["abcd".to_string()]);
    Ok(())
}

#[test]
fn regex_replace_all_entire_string() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec!["old".to_string(), "also old".to_string()])
        .regex_replace_all(r"old", "new")
        .collect_seq()?;

    assert_eq!(result, vec!["new".to_string(), "also new".to_string()]);
    Ok(())
}

// ─────────────────────────────────── regex_split ─────────────────────────────────────

#[test]
fn regex_split_basic_comma() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec!["a, b, c".to_string(), "x,y,z".to_string()])
        .regex_split(r",\s*")
        .collect_seq()?;

    assert_eq!(
        result,
        vec![
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            vec!["x".to_string(), "y".to_string(), "z".to_string()],
        ]
    );
    Ok(())
}

#[test]
fn regex_split_no_match_returns_original() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec!["no-comma".to_string()])
        .regex_split(r",\s*")
        .collect_seq()?;

    assert_eq!(result, vec![vec!["no-comma".to_string()]]);
    Ok(())
}

#[test]
fn regex_split_empty_string() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![String::new()])
        .regex_split(r",")
        .collect_seq()?;

    assert_eq!(result, vec![vec![String::new()]]);
    Ok(())
}

#[test]
fn regex_split_multiple_separators() -> Result<()> {
    let p = Pipeline::default();
    // Split on one or more whitespace characters
    let result = from_vec(&p, vec!["one  two   three".to_string()])
        .regex_split(r"\s+")
        .collect_seq()?;

    assert_eq!(
        result,
        vec![vec![
            "one".to_string(),
            "two".to_string(),
            "three".to_string()
        ]]
    );
    Ok(())
}

#[test]
fn regex_split_empty_collection() -> Result<()> {
    let p = Pipeline::default();
    let result: Vec<Vec<String>> = from_vec(&p, Vec::<String>::new())
        .regex_split(r",")
        .collect_seq()?;

    assert!(result.is_empty());
    Ok(())
}

#[test]
fn regex_split_preserves_all_elements() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(
        &p,
        vec!["a,b".to_string(), "c,d,e".to_string(), "f".to_string()],
    )
    .regex_split(r",")
    .collect_seq()?;

    assert_eq!(result.len(), 3);
    assert_eq!(result[0], vec!["a".to_string(), "b".to_string()]);
    assert_eq!(
        result[1],
        vec!["c".to_string(), "d".to_string(), "e".to_string()]
    );
    assert_eq!(result[2], vec!["f".to_string()]);
    Ok(())
}

// ─────────────────────────── chaining / integration ──────────────────────────────────

#[test]
fn regex_pipeline_chain_extract_then_replace() -> Result<()> {
    let p = Pipeline::default();
    // Extract group 1 (the word) from lines matching "key: value", then uppercase
    let result = from_vec(
        &p,
        vec![
            "status: ok".to_string(),
            "status: error".to_string(),
            "irrelevant line".to_string(),
        ],
    )
    .regex_extract(r"status: (\w+)", 1)
    .regex_replace_all(r"[aeiou]", "*")
    .collect_seq()?;

    let mut sorted = result;
    sorted.sort();
    assert_eq!(sorted, vec!["*k".to_string(), "*rr*r".to_string()]);
    Ok(())
}

#[test]
fn regex_pipeline_find_then_filter() -> Result<()> {
    let p = Pipeline::default();
    // Find first digit sequence, then keep only those over 2 characters long
    let result = from_vec(
        &p,
        vec![
            "x 1234".to_string(),
            "y 5".to_string(),
            "z 678".to_string(),
            "no digits".to_string(),
        ],
    )
    .regex_find(r"\d+")
    .filter(|s: &String| s.len() > 2)
    .collect_seq()?;

    let mut sorted = result;
    sorted.sort();
    assert_eq!(sorted, vec!["1234".to_string(), "678".to_string()]);
    Ok(())
}

#[test]
fn regex_extract_kv_then_combine() -> Result<()> {
    let p = Pipeline::default();
    // Extract name/score pairs, then sum scores per name
    let result = from_vec(
        &p,
        vec![
            "Alice 10".to_string(),
            "Bob 20".to_string(),
            "Alice 5".to_string(),
            "invalid line".to_string(),
        ],
    )
    .regex_extract_kv(r"(\w+)\s+(\d+)", 1, 2)
    .map(|(name, score_str): &(String, String)| {
        let score: u32 = score_str.parse().unwrap_or(0);
        (name.clone(), score)
    })
    .combine_values(Sum::<u32>::new())
    .collect_seq_sorted()?;

    assert_eq!(
        result,
        vec![("Alice".to_string(), 15u32), ("Bob".to_string(), 20u32)]
    );
    Ok(())
}

#[test]
fn regex_split_then_flat_map() -> Result<()> {
    let p = Pipeline::default();
    // Split CSV lines and flat_map to individual fields
    let result = from_vec(&p, vec!["a,b,c".to_string(), "d,e".to_string()])
        .regex_split(r",")
        .flat_map(|parts: &Vec<String>| parts.clone())
        .collect_seq()?;

    let mut sorted = result;
    sorted.sort();
    assert_eq!(
        sorted,
        vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string()
        ]
    );
    Ok(())
}
