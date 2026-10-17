use super::*;
use ironbeam_prism_proof::two_source_count_boundary_fixture;

fn read_rows(path: &Path) -> Vec<String> {
    let mut rows = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

fn assert_two_count_panes(rows: &[String]) {
    assert_eq!(
        rows.len(),
        2,
        "expected exactly two count-triggered panes: {rows:?}"
    );
    let mut left_total = 0;
    let mut right_total = 0;
    let mut indices = Vec::new();
    for row in rows {
        let parts = row.split(':').collect::<Vec<_>>();
        assert_eq!(parts.len(), 5);
        assert_eq!(parts[0], "1");
        let left = parts[1].parse::<usize>().unwrap();
        let right = parts[2].parse::<usize>().unwrap();
        assert_eq!(left + right, 2, "count pane overshot its threshold: {row}");
        assert_eq!(parts[3], "Early");
        left_total += left;
        right_total += right;
        indices.push(parts[4].parse::<i64>().unwrap());
    }
    indices.sort();
    assert_eq!(indices, [0, 1]);
    assert_eq!((left_total, right_total), (3, 1));
}

#[test]
fn direct_multi_source_count_panes_fire_at_element_boundaries() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("direct-count-boundary");
    let pipeline = Pipeline::default();
    let sink = two_source_count_boundary_fixture(&pipeline, path.clone()).unwrap();
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_two_count_panes(&read_rows(&path));
}

#[tokio::test]
async fn prism_multi_source_count_panes_fire_at_element_boundaries() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("portable-count-boundary");
    let pipeline = Pipeline::default();
    two_source_count_boundary_fixture(&pipeline, path.clone()).unwrap();
    let (state, messages) = run_shared_fixture(
        &endpoint,
        &pipeline,
        "two-source-count-boundary",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_TWO_SOURCE_COUNT_BOUNDARY".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(state, 3, "Prism count boundary failed: {messages:?}");
    assert_two_count_panes(&read_rows(&path));
}
