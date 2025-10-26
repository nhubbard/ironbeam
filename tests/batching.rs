use rustflow::*;
use anyhow::Result;

#[test]
fn map_batches_matches_elementwise_seq() -> Result<()> {
    let p = Pipeline::default();
    let input: Vec<u32> = (0..103).collect();

    // element-wise baseline
    let baseline = from_vec(&p, input.clone())
        .map(|x| x * 2)
        .collect_seq()?;

    // batched version (different batch sizes)
    for bs in [1usize, 3, 8, 16, 64, 128] {
        let got = from_vec(&p, input.clone())
            .map_batches(bs, |chunk: &[u32]| {
                // example heavy transform: double
                chunk.iter().map(|x| x * 2).collect::<Vec<u32>>()
            })
            .collect_seq()?;

        assert_eq!(got, baseline, "batch_size={}", bs);
    }
    Ok(())
}

#[test]
fn map_batches_tail_handling() -> Result<()> {
    let p = Pipeline::default();
    let input: Vec<usize> = (0..10).collect();

    let got = from_vec(&p, input.clone())
        .map_batches(4, |chunk: &[usize]| {
            // emit chunk length (so the tail is visible as 2)
            vec![chunk.len()]
        })
        .collect_seq()?;

    // 10 with chunk size 4 -> chunks of [4,4,2]
    assert_eq!(got, vec![4, 4, 2]);
    Ok(())
}

#[test]
fn map_batches_par_equivalence_after_sort() -> Result<()> {
    let p = Pipeline::default();
    let input: Vec<u32> = (0..10_000).collect();

    // elementwise baseline (seq)
    let mut baseline = from_vec(&p, input.clone())
        .map(|x| x + 1)
        .collect_seq()?;
    baseline.sort();

    // batched (par). Note: collect_par may interleave partitions, so sort before comparing.
    let mut got = from_vec(&p, input.clone())
        .map_batches(256, |chunk: &[u32]| chunk.iter().map(|x| x + 1).collect::<Vec<u32>>())
        .collect_par(None, None)?;

    got.sort();
    assert_eq!(got, baseline);
    Ok(())
}

#[test]
fn map_values_batches_matches_elementwise_seq() -> Result<()> {
    let p = Pipeline::default();
    let input: Vec<(String, u32)> =
        (0..103).map(|i| (format!("k{}", i % 5), i)).collect();

    // Baseline elementwise
    let baseline = from_vec(&p, input.clone())
        .map_values(|v| v * 3)
        .collect_seq()?;

    // Batched values
    for bs in [1usize, 3, 8, 16, 64, 128] {
        let got = from_vec(&p, input.clone())
            .map_values_batches(bs, |chunk: &[u32]| chunk.iter().map(|x| x * 3).collect::<Vec<u32>>())
            .collect_seq()?;
        assert_eq!(got, baseline, "batch_size={}", bs);
    }
    Ok(())
}

#[test]
fn map_values_batches_tail_handling() -> Result<()> {
    let p = Pipeline::default();
    let input: Vec<(u32, usize)> = (0..10).map(|i| ((i % 2) as u32, i as usize)).collect();

    // Return the size of each processed chunk (visible in values).
    let got = from_vec(&p, input.clone())
        .map_values_batches(4, |chunk: &[usize]| vec![chunk.len(); chunk.len()])
        .collect_seq()?;

    // 10 with chunk size 4 -> chunks [4,4,2], so values become [4,4,4,4, 4,4,4,4, 2,2] under existing keys
    let expected: Vec<(u32, usize)> = input
        .iter()
        .enumerate()
        .map(|(i, (k, _))| {
            let len = if i < 8 { 4 } else { 2 };
            (*k, len)
        })
        .collect();

    assert_eq!(got, expected);
    Ok(())
}

#[test]
fn map_values_batches_par_equivalence_after_sort() -> Result<()> {
    let p = Pipeline::default();
    let input: Vec<(u32, u32)> = (0..10_000).map(|i| (i % 17, i)).collect();

    // Elementwise (seq) baseline
    let mut baseline = from_vec(&p, input.clone())
        .map_values(|v| v + 1)
        .collect_seq()?;
    baseline.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    // Batched (par)
    let mut got = from_vec(&p, input.clone())
        .map_values_batches(256, |chunk: &[u32]| chunk.iter().map(|x| x + 1).collect::<Vec<u32>>())
        .collect_par(None, None)?;
    got.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    assert_eq!(got, baseline);
    Ok(())
}