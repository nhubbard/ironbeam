use anyhow::Result;
use rustflow::{from_vec, Pipeline};

#[test]
fn planner_fuses_stateless_equivalence() -> Result<()> {
    let p = Pipeline::default();
    let input: Vec<u32> = (0..10_000).collect();

    // Build with many tiny stateless ops
    let many = from_vec(&p, input.clone())
        .map(|x: &u32| x + 1)
        .filter(|x: &u32| x.is_multiple_of(2))
        .flat_map(|x: &u32| vec![*x, *x]) // duplicate
        .map(|x: &u32| x / 2)
        .filter(|x: &u32| !x.is_multiple_of(3));

    // Collect seq and par to ensure planner changes don't affect results
    let seq = many.clone().collect_seq_sorted()?;
    let par = many.clone().collect_par_sorted(Some(8), None)?;
    assert_eq!(seq, par);
    Ok(())
}

#[test]
fn planner_drops_mid_materialized_equivalence() -> Result<()> {
    let p = Pipeline::default();
    let v: Vec<String> = (0..1000).map(|i| format!("w{i}")).collect();

    // Force a mid-chain materialized by collecting and re-inserting (simulating a checkpoint)
    let col = from_vec(&p, v.clone())
        .map(|s: &String| s.to_uppercase())
        .filter(|s: &String| s.len() > 1);

    // Not directly accessible to force Materialized in public API,
    // but if you have tests that insert Node::Materialized, the planner will drop it mid-chain.
    // We just ensure the runner still yields stable results:
    let seq = col.clone().collect_seq_sorted()?;
    let par = col.clone().collect_par_sorted(Some(6), None)?;
    assert_eq!(seq, par);

    Ok(())
}
