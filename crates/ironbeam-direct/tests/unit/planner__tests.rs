// Unit tests for src/planner.rs.
use super::*;
use ironbeam_core::{ErrorKind, Pipeline};

#[test]
fn stream_planner_keeps_ordinary_reshuffles_as_materialization_boundaries() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![1_i32])
        .unwrap()
        .reshuffle_named("ordinary-boundary")
        .unwrap()
        .output("out")
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    assert!(
        lower_stream(&graph, &registry, "out", 1)
            .unwrap()
            .materialized
    );
}

#[test]
fn lowering_checks_terminal_and_chain_shape() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![1_i32])
        .unwrap()
        .map_named("map", |x| x + 1)
        .unwrap()
        .output("out")
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    assert_eq!(
        lower(&graph, &registry, "missing", 1).err().unwrap().kind,
        ErrorKind::Graph
    );
    assert_eq!(
        lower(&graph, &registry, "source", 1).err().unwrap().kind,
        ErrorKind::Graph
    );
    let mut broken = graph.clone();
    broken
        .nodes
        .iter_mut()
        .find(|n| n.key == "out")
        .unwrap()
        .input = None;
    assert_eq!(
        lower(&broken, &registry, "out", 1).err().unwrap().kind,
        ErrorKind::Graph
    );
    let mut broken = graph.clone();
    broken
        .nodes
        .iter_mut()
        .find(|n| n.key == "out")
        .unwrap()
        .input = Some("absent".into());
    assert_eq!(
        lower(&broken, &registry, "out", 1).err().unwrap().kind,
        ErrorKind::Graph
    );
    let mut broken = graph;
    broken
        .nodes
        .iter_mut()
        .find(|n| n.key == "map")
        .unwrap()
        .operation = Operation::GlobalCombine;
    assert!(lower(&broken, &registry, "out", 1).unwrap().materialized);
}

#[test]
fn explain_reports_only_decisions_the_selected_plan_makes() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32, 2]).unwrap();
    source.map_named("unused", |value| value + 10).unwrap();
    source
        .map_named("double", |value| value * 2)
        .unwrap()
        .filter_named("even", |value| value % 2 == 0)
        .unwrap()
        .output("out")
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let summary = lower(&graph, &registry, "out", 1).unwrap().summary;
    assert!(!summary.materialized);
    assert_eq!(
        summary.decisions,
        vec![
            PlanDecision::PrunedNodes {
                keys: vec!["unused".into()],
            },
            PlanDecision::FusedStatelessRoute {
                source_key: "source".into(),
                operations: vec!["double".into(), "even".into()],
            },
        ]
    );
    let explanation = summary.explain();
    assert!(explanation.contains("Pruned unselected nodes: unused"));
    assert!(explanation.contains("Fused stateless route from source: double -> even"));
    assert!(!explanation.contains("Materialization barrier"));
}

#[test]
fn source_only_plan_reports_no_physical_decisions() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![1_i32, 2])
        .unwrap()
        .output("out")
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let summary = lower(&graph, &registry, "out", 2).unwrap().summary;
    assert_eq!(summary.decisions.len(), 0);
    assert!(summary.explain().contains("Decisions: none"));
}

#[test]
fn explain_identifies_the_materializing_operation() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![(1_i32, 2_i32)])
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .output("out")
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let summary = lower(&graph, &registry, "out", 2).unwrap().summary;
    assert!(summary.materialized);
    assert!(summary.decisions.iter().any(|decision| {
        matches!(decision, PlanDecision::MaterializationBarrier { key, .. } if key == "group")
    }));
    assert!(summary.explain().contains("Materialization barrier: group"));
}
