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
//! - `Display` integration: per-step name annotation appears alongside the
//!   op category when at least one origin node was named, fused chains
//!   show the joined names of their contributing origins, and unnamed
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
    let grouped = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("a".to_string(), 2),
            ("b".to_string(), 3),
        ],
    )
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
/// too — and no step renders a `[name]` annotation.
#[test]
fn test_explanation_no_names_renders_no_step_annotation() {
    let p = Pipeline::default();
    let coll = from_vec(&p, vec![1u32, 2, 3]).filter(|x| *x > 0);

    let plan = build_plan(&p, coll.node_id()).unwrap();
    let expl = plan.explain();

    assert!(expl.node_names.is_empty());
    for step in &expl.steps {
        assert!(step.name.is_none(), "unexpected step name: {step:?}");
    }
}

/// When at least one node is named, the matching step carries the label and
/// the rendered `Display` shows `Step N: NodeType [Name]` inline.
#[test]
fn test_explanation_renders_inline_step_names() {
    let p = Pipeline::default();
    let coll = from_vec(&p, vec![1u32, 2, 3])
        .with_name("MySource")
        .filter(|x| *x > 0)
        .with_name("MyFilter");

    let plan = build_plan(&p, coll.node_id()).unwrap();
    let expl = plan.explain();

    let names: Vec<Option<&str>> = expl.steps.iter().map(|s| s.name.as_deref()).collect();
    assert!(
        names.contains(&Some("MySource")),
        "expected MySource in step names: {names:?}"
    );
    // After fusion, the Source and the filter may share a step (the filter
    // could be pulled into the source's Stateless chain) — either way the
    // filter's label appears in some step.
    assert!(
        expl.steps
            .iter()
            .any(|s| s.name.as_deref().is_some_and(|n| n.contains("MyFilter"))),
        "expected a step labelled with MyFilter: {names:?}"
    );

    let s = format!("{expl}");
    assert!(s.contains("[MySource]"), "{s}");
    assert!(s.contains("MyFilter"), "{s}");
}

/// Steps render in chain order — so labels appear in the source-to-terminal
/// sequence the user wrote, not in `HashMap` iteration order.
#[test]
fn test_explanation_step_names_render_in_chain_order() {
    let p = Pipeline::default();
    let coll = from_vec(&p, vec![("a".to_string(), 1u32), ("b".to_string(), 2)])
        .with_name("First") // Source
        .group_by_key() // anonymous barrier blocks downstream fusion
        .with_name("Second")
        .map_values(|vs: &Vec<u32>| vs.iter().sum::<u32>())
        .with_name("Third");

    let plan = build_plan(&p, coll.node_id()).unwrap();
    let s = format!("{}", plan.explain());

    let pos_first = s.find("[First]").expect("First label rendered");
    let pos_second = s.find("[Second]").expect("Second label rendered");
    let pos_third = s.find("Third").expect("Third label rendered");

    assert!(
        pos_first < pos_second && pos_second < pos_third,
        "labels should appear in chain order:\n{s}"
    );
}

/// When the optimizer fuses multiple named nodes into a single chain entry,
/// the rendered name is the chain-ordered join of their labels with " + ".
#[test]
fn test_explanation_fused_step_joins_origin_names() {
    let p = Pipeline::default();
    // Two named Stateless transforms in a row — fusion merges them.
    let coll = from_vec(&p, vec![1u32, 2, 3])
        .filter(|x| *x > 0)
        .with_name("Positive")
        .map(|x| x * 2)
        .with_name("Double");

    let plan = build_plan(&p, coll.node_id()).unwrap();
    let expl = plan.explain();

    let joined_present = expl.steps.iter().any(|s| {
        s.name
            .as_deref()
            .is_some_and(|n| n.contains("Positive") && n.contains("Double") && n.contains(" + "))
    });
    assert!(
        joined_present,
        "expected a fused step joining Positive + Double: {:?}",
        expl.steps.iter().map(|s| &s.name).collect::<Vec<_>>()
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
/// as before — no `[…]` step annotations anywhere in the output.
#[test]
fn test_no_step_annotations_when_pipeline_unused() {
    let p = Pipeline::default();
    let coll = from_vec(&p, vec![1u32, 2, 3, 4, 5])
        .filter(|x| x % 2 == 0)
        .map(|x| x * 10);

    let plan = build_plan(&p, coll.node_id()).unwrap();
    let expl = plan.explain();
    let s = format!("{expl}");

    assert!(expl.node_names.is_empty(), "{s}");
    for step in &expl.steps {
        assert!(step.name.is_none(), "unexpected step name: {step:?}");
    }
}

// ── named_scope — composite labelling ────────────────────────────────────────

/// Nodes created inside a `named_scope` are auto-labelled `"<path>/<counter>"`,
/// with the counter starting at `0` per scope frame.
#[test]
fn test_named_scope_auto_labels_inserted_nodes() {
    let p = Pipeline::default();
    let _coll = p.named_scope("WordCount", |p| {
        let a = from_vec(p, vec![1u32, 2, 3]); // → "WordCount/0"
        let b = a.map(|x| x + 1); // → "WordCount/1"
        b.filter(|x| *x > 0) //          → "WordCount/2"
    });

    let snap = p.node_names_snapshot();
    let mut names: Vec<&String> = snap.values().collect();
    names.sort();
    assert_eq!(
        names,
        vec![
            &String::from("WordCount/0"),
            &String::from("WordCount/1"),
            &String::from("WordCount/2"),
        ]
    );
}

/// `with_name` inside a scope replaces the auto-generated label with
/// `"<path>/<user-supplied>"`.
#[test]
fn test_named_scope_with_name_qualifies_user_label() {
    let p = Pipeline::default();
    let coll = p.named_scope("WordCount", |p| {
        from_vec(p, vec![1u32, 2, 3])
            .with_name("Source")
            .filter(|x| *x > 0)
            .with_name("KeepPositive")
    });

    let snap = p.node_names_snapshot();
    let names: std::collections::HashSet<&String> = snap.values().collect();
    assert!(names.contains(&String::from("WordCount/Source")));
    assert!(names.contains(&String::from("WordCount/KeepPositive")));
    assert_eq!(
        p.node_name(coll.node_id()).as_deref(),
        Some("WordCount/KeepPositive")
    );
}

/// `with_name` outside any scope behaves as before — no prefix is added.
#[test]
fn test_with_name_outside_scope_is_raw() {
    let p = Pipeline::default();
    let coll = from_vec(&p, vec![1u32]).with_name("PlainLabel");
    assert_eq!(p.node_name(coll.node_id()).as_deref(), Some("PlainLabel"));
}

/// Nested scopes compose into a "/"-joined path.
#[test]
fn test_named_scope_nested() {
    let p = Pipeline::default();
    let coll = p.named_scope("WordCount", |p| {
        p.named_scope("Split", |p| {
            from_vec(p, vec!["hi there".to_string()])
                .flat_map(|s: &String| s.split_whitespace().map(String::from).collect())
                .with_name("Words")
        })
    });
    assert_eq!(
        p.node_name(coll.node_id()).as_deref(),
        Some("WordCount/Split/Words")
    );
}

/// Per-frame counters: the outer frame's counter is not advanced by
/// inner-scope inserts.
#[test]
fn test_named_scope_per_frame_counters() {
    let p = Pipeline::default();
    p.named_scope("Outer", |p| {
        let _a = from_vec(p, vec![1u32]); // → "Outer/0"
        p.named_scope("Inner", |p| {
            let _b = from_vec(p, vec![2u32]); // → "Outer/Inner/0"
            let _c = from_vec(p, vec![3u32]); // → "Outer/Inner/1"
        });
        let _d = from_vec(p, vec![4u32]); // → "Outer/1" (outer counter resumes from 1)
    });

    let snap = p.node_names_snapshot();
    let mut values: Vec<&String> = snap.values().collect();
    values.sort();
    assert_eq!(
        values,
        vec![
            &String::from("Outer/0"),
            &String::from("Outer/1"),
            &String::from("Outer/Inner/0"),
            &String::from("Outer/Inner/1"),
        ]
    );
}

/// After the scope returns, the scope stack is empty so `with_name` on a
/// later collection is no longer prefixed.
#[test]
fn test_named_scope_pops_on_return() {
    let p = Pipeline::default();
    let inside = p.named_scope("MyScope", |p| from_vec(p, vec![1u32]).with_name("Inside"));
    let outside = from_vec(&p, vec![2u32]).with_name("Outside");

    assert_eq!(
        p.node_name(inside.node_id()).as_deref(),
        Some("MyScope/Inside")
    );
    assert_eq!(p.node_name(outside.node_id()).as_deref(), Some("Outside"));
}

/// `named_scope` forwards the closure's return value verbatim.
#[test]
fn test_named_scope_forwards_closure_return() {
    let p = Pipeline::default();
    let value: u32 = p.named_scope("Whatever", |_| 42);
    assert_eq!(value, 42);
}

/// An empty closure is a no-op: no nodes get named, and the scope stack
/// returns to empty.
#[test]
fn test_named_scope_empty_closure_is_noop() {
    let p = Pipeline::default();
    p.named_scope("Nope", |_| {});
    let after = from_vec(&p, vec![1u32]).with_name("AfterEmpty");
    assert_eq!(p.node_name(after.node_id()).as_deref(), Some("AfterEmpty"));
}

/// If the closure panics, the scope is still popped before the panic
/// propagates — proven by checking that a subsequent `with_name` call
/// outside the scope is not prefixed.
#[test]
fn test_named_scope_panic_safe() {
    use std::panic::AssertUnwindSafe;

    let p = Pipeline::default();
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        p.named_scope("Panicky", |_| panic!("boom"));
    }));
    assert!(result.is_err());

    // Scope stack popped — subsequent with_name has no prefix.
    let coll = from_vec(&p, vec![1u32]).with_name("AfterPanic");
    assert_eq!(p.node_name(coll.node_id()).as_deref(), Some("AfterPanic"));
}

/// `set_node_name` is a raw setter: it does not honour the active scope.
/// (This preserves the "explicit advanced API" contract for backends.)
#[test]
fn test_set_node_name_is_raw_inside_scope() {
    let p = Pipeline::default();
    let coll = p.named_scope("Outer", |p| {
        let c = from_vec(p, vec![1u32]);
        p.set_node_name(c.node_id(), "RawLabel");
        c
    });
    assert_eq!(p.node_name(coll.node_id()).as_deref(), Some("RawLabel"));
}

/// Names produced inside a scope flow into the per-step annotations on the
/// explain output, with the scope path preserved as a `"<scope>/<label>"`
/// prefix.
#[test]
fn test_named_scope_visible_in_explanation() {
    let p = Pipeline::default();
    let coll = p.named_scope("Pipeline", |p| {
        from_vec(p, vec![1u32, 2, 3])
            .with_name("Source")
            .filter(|x| *x > 0)
            .with_name("Filter")
    });

    let plan = build_plan(&p, coll.node_id()).unwrap();
    let expl = plan.explain();
    let s = format!("{expl}");

    assert!(s.contains("Pipeline/Source"), "{s}");
    assert!(s.contains("Pipeline/Filter"), "{s}");
    // At least one step carries a scope-qualified name.
    assert!(
        expl.steps.iter().any(|step| step
            .name
            .as_deref()
            .is_some_and(|n| n.starts_with("Pipeline/"))),
        "expected at least one step name to start with 'Pipeline/': {:?}",
        expl.steps.iter().map(|s| &s.name).collect::<Vec<_>>()
    );
}
