//! Comprehensive tests for feature 4.11: named operations.
//!
//! Exercises:
//! - `Pipeline::set_node_name`, `Pipeline::node_name`, and
//!   `Pipeline::node_names_snapshot` (direct accessors).
//! - `PCollection::with_name` (fluent setter), including last-write-wins,
//!   source labelling, barrier-output labelling, and chaining across many
//!   transforms.
//! - Behavioural invariant: naming a pipeline does not change its results.
//! - Planner integration: `Plan::node_names` is populated at plan-build time;
//!   `ExecutionExplanation::node_names` propagates the snapshot.
//! - `Display` integration: the "NAMED OPERATIONS" footer appears iff any
//!   node is named, entries are sorted deterministically, and unnamed
//!   pipelines produce identical explain output to before the feature.

use ironbeam::*;
use std::collections::HashMap;

// ── Pipeline-level direct API ────────────────────────────────────────────────

/// `node_name` returns `None` for a node that has never been named.
#[test]
fn test_pipeline_node_name_default_is_none() {
    let p = Pipeline::default();
    let coll = from_vec(&p, vec![1u32, 2, 3]);
    assert!(p.node_name(coll.node_id()).is_none());
}

/// `set_node_name` records the name; `node_name` reads it back.
#[test]
fn test_pipeline_set_and_get_node_name() {
    let p = Pipeline::default();
    let coll = from_vec(&p, vec![1u32, 2, 3]);
    p.set_node_name(coll.node_id(), "Source");
    assert_eq!(p.node_name(coll.node_id()).as_deref(), Some("Source"));
}

/// `set_node_name` is last-write-wins for the same node id.
#[test]
fn test_pipeline_set_node_name_last_write_wins() {
    let p = Pipeline::default();
    let coll = from_vec(&p, vec![1u32, 2, 3]);
    p.set_node_name(coll.node_id(), "First");
    p.set_node_name(coll.node_id(), "Second");
    p.set_node_name(coll.node_id(), "Final");
    assert_eq!(p.node_name(coll.node_id()).as_deref(), Some("Final"));
}

/// `node_names_snapshot` returns a deep clone of every named node.
#[test]
fn test_pipeline_node_names_snapshot() {
    let p = Pipeline::default();
    let a = from_vec(&p, vec![1u32, 2, 3]);
    let b = a.clone().map(|x| x + 1);

    p.set_node_name(a.node_id(), "Source");
    p.set_node_name(b.node_id(), "AddOne");

    let snap = p.node_names_snapshot();
    assert_eq!(snap.len(), 2);
    assert_eq!(snap.get(&a.node_id()).map(String::as_str), Some("Source"));
    assert_eq!(snap.get(&b.node_id()).map(String::as_str), Some("AddOne"));
}

/// `node_names_snapshot` is independent — mutating the returned map does not
/// affect the pipeline's internal state.
#[test]
fn test_pipeline_node_names_snapshot_is_independent() {
    let p = Pipeline::default();
    let coll = from_vec(&p, vec![1u32]);
    p.set_node_name(coll.node_id(), "Source");

    let mut snap = p.node_names_snapshot();
    snap.insert(coll.node_id(), "Mutated".to_string());

    // The local snapshot saw the mutation, …
    assert_eq!(
        snap.get(&coll.node_id()).map(String::as_str),
        Some("Mutated")
    );
    // … but the pipeline retains the original name despite it.
    assert_eq!(p.node_name(coll.node_id()).as_deref(), Some("Source"));
}

/// Accepts both `&str` and `String` via `impl Into<String>`.
#[test]
fn test_pipeline_set_node_name_accepts_str_and_string() {
    let p = Pipeline::default();
    let a = from_vec(&p, vec![0u8]);
    let b = a.clone().map(|x| *x);

    p.set_node_name(a.node_id(), "StaticStr");
    p.set_node_name(b.node_id(), String::from("Owned"));

    assert_eq!(p.node_name(a.node_id()).as_deref(), Some("StaticStr"));
    assert_eq!(p.node_name(b.node_id()).as_deref(), Some("Owned"));
}

// ── PCollection::with_name fluent API ────────────────────────────────────────

/// `with_name` labels the current node and returns self for chaining.
#[test]
fn test_with_name_labels_current_node() {
    let p = Pipeline::default();
    let evens = from_vec(&p, vec![1u32, 2, 3, 4, 5])
        .filter(|x| x % 2 == 0)
        .with_name("KeepEven");

    assert_eq!(p.node_name(evens.node_id()).as_deref(), Some("KeepEven"));
}

/// `with_name` can be chained between transforms without changing semantics.
#[test]
fn test_with_name_preserves_results() {
    let p = Pipeline::default();
    let unnamed = from_vec(&p, vec![1u32, 2, 3, 4, 5])
        .filter(|x| x % 2 == 0)
        .map(|x| x * 10)
        .collect_seq()
        .unwrap();

    let q = Pipeline::default();
    let named = from_vec(&q, vec![1u32, 2, 3, 4, 5])
        .with_name("Source")
        .filter(|x| x % 2 == 0)
        .with_name("KeepEven")
        .map(|x| x * 10)
        .with_name("MultiplyByTen")
        .collect_seq()
        .unwrap();

    assert_eq!(unnamed, named);
}

/// `with_name` labels the source node when applied immediately after `from_vec`.
#[test]
fn test_with_name_on_source() {
    let p = Pipeline::default();
    let src = from_vec(&p, vec![1u32]).with_name("OriginalSource");
    assert_eq!(
        p.node_name(src.node_id()).as_deref(),
        Some("OriginalSource")
    );
}

/// `with_name` is last-write-wins when reapplied to the same node id via two
/// fluent chains that share an upstream collection.
#[test]
fn test_with_name_last_write_wins_via_fluent() {
    let p = Pipeline::default();
    let coll = from_vec(&p, vec![1u32]);
    let first = coll.clone().with_name("First");
    let second = coll.with_name("Second");

    // Both handles point at the same node id.
    assert_eq!(first.node_id(), second.node_id());
    assert_eq!(p.node_name(first.node_id()).as_deref(), Some("Second"));
}

/// Naming a node after a barrier (e.g. `group_by_key`) works just like any
/// other transform.
#[test]
fn test_with_name_after_barrier() {
    let p = Pipeline::default();
    let grouped = from_vec(&p, vec![("a", 1u32), ("a", 2), ("b", 3)])
        .group_by_key()
        .with_name("GroupedByKey");

    assert_eq!(
        p.node_name(grouped.node_id()).as_deref(),
        Some("GroupedByKey")
    );
}

/// `with_name` accepts owned `String`s as well as borrowed `&str`s.
#[test]
fn test_with_name_accepts_owned_string() {
    let p = Pipeline::default();
    let label = String::from("Owned");
    let coll = from_vec(&p, vec![1u32]).with_name(label);
    assert_eq!(p.node_name(coll.node_id()).as_deref(), Some("Owned"));
}

/// Long fluent chain — each `with_name` lands on its own distinct node id.
#[test]
fn test_with_name_long_chain_distinct_nodes() {
    let p = Pipeline::default();
    let source = from_vec(&p, vec![1u32, 2, 3]).with_name("Source");
    let filtered = source.clone().filter(|x| *x > 0).with_name("Positive");
    let mapped = filtered.clone().map(|x| x + 1).with_name("Increment");

    let names = p.node_names_snapshot();
    assert_eq!(names.len(), 3);
    assert_eq!(
        names.get(&source.node_id()).map(String::as_str),
        Some("Source")
    );
    assert_eq!(
        names.get(&filtered.node_id()).map(String::as_str),
        Some("Positive")
    );
    assert_eq!(
        names.get(&mapped.node_id()).map(String::as_str),
        Some("Increment")
    );
}

// ── Planner integration ─────────────────────────────────────────────────────

/// `build_plan` snapshots node names from the pipeline at plan-build time.
#[test]
fn test_build_plan_snapshots_node_names() {
    let p = Pipeline::default();
    let coll = from_vec(&p, vec![1u32, 2, 3])
        .filter(|x| *x > 0)
        .with_name("FilteredPositives");

    let plan = build_plan(&p, coll.node_id()).unwrap();
    assert_eq!(
        plan.node_names.get(&coll.node_id()).map(String::as_str),
        Some("FilteredPositives")
    );
}

/// Names set on the pipeline *after* `build_plan` do not appear in the
/// already-built plan (snapshot semantics).
#[test]
fn test_build_plan_snapshot_is_frozen() {
    let p = Pipeline::default();
    let coll = from_vec(&p, vec![1u32, 2, 3]);

    let plan = build_plan(&p, coll.node_id()).unwrap();
    // Name set after plan construction.
    p.set_node_name(coll.node_id(), "Late");

    assert!(plan.node_names.is_empty());
    // The live pipeline still sees the late name.
    assert_eq!(p.node_name(coll.node_id()).as_deref(), Some("Late"));
}

/// An empty `node_names` map on a Plan yields an empty map on the explanation
/// too — and the rendered Display contains no "NAMED OPERATIONS" block.
#[test]
fn test_explanation_no_names_renders_no_section() {
    let p = Pipeline::default();
    let coll = from_vec(&p, vec![1u32, 2, 3]).filter(|x| *x > 0);

    let plan = build_plan(&p, coll.node_id()).unwrap();
    let expl = plan.explain();

    assert!(expl.node_names.is_empty());
    let s = format!("{expl}");
    assert!(!s.contains("NAMED OPERATIONS"), "{s}");
}

/// When at least one node is named, the explanation surfaces the map and the
/// Display impl renders a "NAMED OPERATIONS" block containing each entry.
#[test]
fn test_explanation_renders_named_section() {
    let p = Pipeline::default();
    let coll = from_vec(&p, vec![1u32, 2, 3])
        .with_name("MySource")
        .filter(|x| *x > 0)
        .with_name("MyFilter");

    let plan = build_plan(&p, coll.node_id()).unwrap();
    let expl = plan.explain();

    assert_eq!(expl.node_names.len(), 2);
    let s = format!("{expl}");
    assert!(s.contains("NAMED OPERATIONS"), "{s}");
    assert!(s.contains("MySource"), "{s}");
    assert!(s.contains("MyFilter"), "{s}");
}

/// Entries in the rendered "NAMED OPERATIONS" block are sorted by `NodeId` —
/// so output is deterministic regardless of `HashMap` iteration order.
#[test]
fn test_explanation_named_section_is_sorted_deterministic() {
    let p = Pipeline::default();
    let coll = from_vec(&p, vec![1u32, 2, 3])
        .with_name("First")
        .filter(|x| *x > 0)
        .with_name("Second")
        .map(|x| x + 1)
        .with_name("Third");

    let plan = build_plan(&p, coll.node_id()).unwrap();
    let expl = plan.explain();
    let s = format!("{expl}");

    let pos_first = s.find("First").expect("First label rendered");
    let pos_second = s.find("Second").expect("Second label rendered");
    let pos_third = s.find("Third").expect("Third label rendered");

    // The source was created first (lowest NodeId) and gets rendered first, etc.
    assert!(
        pos_first < pos_second && pos_second < pos_third,
        "names should render in NodeId order:\n{s}"
    );
}

/// `ExecutionExplanation::node_names` is a clone of `Plan::node_names`.
#[test]
fn test_explanation_node_names_matches_plan() {
    let p = Pipeline::default();
    let coll = from_vec(&p, vec![1u32, 2, 3])
        .with_name("Source")
        .map(|x| x * 2)
        .with_name("Double");

    let plan = build_plan(&p, coll.node_id()).unwrap();
    let expl = plan.explain();

    let plan_map: HashMap<_, _> = plan.node_names.iter().collect();
    let expl_map: HashMap<_, _> = expl.node_names.iter().collect();
    assert_eq!(plan_map, expl_map);
}

/// Existing pipelines that never call `with_name` continue to render exactly
/// as before — no `NAMED OPERATIONS` header anywhere in the output.
#[test]
fn test_no_named_section_when_pipeline_unused() {
    let p = Pipeline::default();
    let coll = from_vec(&p, vec![1u32, 2, 3, 4, 5])
        .filter(|x| x % 2 == 0)
        .map(|x| x * 10);

    let plan = build_plan(&p, coll.node_id()).unwrap();
    let expl = plan.explain();
    let s = format!("{expl}");

    assert!(expl.node_names.is_empty(), "{s}");
    assert!(!s.contains("NAMED OPERATIONS"), "{s}");
}
