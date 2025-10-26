use rustflow::{Pipeline, from_vec};

#[test]
fn par_equals_seq_after_sort() -> anyhow::Result<()> {
    let p = Pipeline::default();
    let col = from_vec(&p, (0..10_000).map(|i| format!("w{}", i % 257)).collect::<Vec<_>>())
        .flat_map(|w: &String| vec![w.clone(), w.clone()]);
    let seq = col.clone().collect_seq_sorted()?;
    let par = col.collect_par_sorted(Some(8), None)?;
    assert_eq!(seq, par);
    Ok(())
}