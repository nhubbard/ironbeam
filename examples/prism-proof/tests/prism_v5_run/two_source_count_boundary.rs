use super::*;
use ironbeam_prism_proof::{
    three_source_count_boundary_fixture, two_source_count_and_time_boundary_fixture,
    two_source_count_boundary_fixture, two_source_count_or_time_boundary_fixture,
    two_source_count_then_time_boundary_fixture,
};

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
    let mut timestamps = Vec::new();
    for row in rows {
        let parts = row.split(':').collect::<Vec<_>>();
        assert_eq!(parts.len(), 6);
        assert_eq!(parts[0], "1");
        let left = parts[1].parse::<usize>().unwrap();
        let right = parts[2].parse::<usize>().unwrap();
        assert_eq!(left + right, 2, "count pane overshot its threshold: {row}");
        assert_eq!(parts[3], "Early");
        left_total += left;
        right_total += right;
        indices.push(parts[4].parse::<i64>().unwrap());
        timestamps.push((
            parts[4].parse::<i64>().unwrap(),
            parts[5].parse::<i64>().unwrap(),
        ));
    }
    indices.sort();
    timestamps.sort();
    assert_eq!(indices, [0, 1]);
    assert_eq!(timestamps, [(0, 1_000_000), (1, 3_000_000)]);
    assert_eq!((left_total, right_total), (3, 1));
}

fn assert_three_source_count_panes(rows: &[String]) {
    assert_eq!(rows.len(), 3, "expected three early panes: {rows:?}");
    let mut totals = [0_usize; 3];
    let mut indices = Vec::new();
    for row in rows {
        let parts = row.split(':').collect::<Vec<_>>();
        assert_eq!(parts.len(), 6);
        assert_eq!(parts[0], "1");
        let sizes = [
            parts[1].parse::<usize>().unwrap(),
            parts[2].parse::<usize>().unwrap(),
            parts[3].parse::<usize>().unwrap(),
        ];
        assert_eq!(sizes.iter().sum::<usize>(), 2, "pane overshot: {row}");
        for (total, size) in totals.iter_mut().zip(sizes) {
            *total += size;
        }
        assert_eq!(parts[4], "Early");
        indices.push(parts[5].parse::<i64>().unwrap());
    }
    indices.sort();
    assert_eq!(indices, [0, 1, 2]);
    assert_eq!(totals, [3, 1, 2]);
}

#[test]
fn direct_three_source_count_panes_fire_at_element_boundaries() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("direct-three-source-count-boundary");
    let pipeline = Pipeline::default();
    let sink = three_source_count_boundary_fixture(&pipeline, path.clone()).unwrap();
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_three_source_count_panes(&read_rows(&path));
}

#[tokio::test]
async fn prism_three_source_count_panes_fire_at_element_boundaries() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory
        .path()
        .join("portable-three-source-count-boundary");
    let pipeline = Pipeline::default();
    three_source_count_boundary_fixture(&pipeline, path.clone()).unwrap();
    let (state, messages) = run_shared_fixture(
        &endpoint,
        &pipeline,
        "three-source-count-boundary",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_THREE_SOURCE_COUNT_BOUNDARY".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(state, 3, "Prism three-source count failed: {messages:?}");
    assert_three_source_count_panes(&read_rows(&path));
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

#[test]
fn direct_multi_source_count_or_time_panes_fire_at_element_boundaries() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("direct-count-or-time-boundary");
    let pipeline = Pipeline::default();
    let sink = two_source_count_or_time_boundary_fixture(&pipeline, path.clone()).unwrap();
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_two_count_panes(&read_rows(&path));
}

#[tokio::test]
#[ignore = "pinned Prism panics in processing-time timer evaluation for this schedule"]
async fn prism_multi_source_count_or_time_panes_fire_at_element_boundaries() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("portable-count-or-time-boundary");
    let pipeline = Pipeline::default();
    two_source_count_or_time_boundary_fixture(&pipeline, path.clone()).unwrap();
    let (state, messages) = run_shared_fixture(
        &endpoint,
        &pipeline,
        "two-source-count-or-time-boundary",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_TWO_SOURCE_COUNT_OR_TIME_BOUNDARY".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        state, 3,
        "Prism count-or-time boundary failed: {messages:?}"
    );
    assert_two_count_panes(&read_rows(&path));
}

fn assert_count_then_time_boundary(rows: &[String]) {
    assert_eq!(
        rows.len(),
        2,
        "expected first count and final panes: {rows:?}"
    );
    let mut total = 0;
    let mut early_size = None;
    for row in rows {
        let parts = row.split(':').collect::<Vec<_>>();
        assert_eq!(parts.len(), 6);
        assert_eq!(parts[0], "1");
        let size = parts[1].parse::<usize>().unwrap() + parts[2].parse::<usize>().unwrap();
        total += size;
        if parts[3] == "Early" && parts[4] == "0" {
            early_size = Some(size);
        }
    }
    assert_eq!(early_size, Some(2), "first count pane overshot: {rows:?}");
    assert_eq!(total, 4);
}

#[test]
fn direct_multi_source_count_then_time_fires_first_pane_at_element_boundary() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("direct-count-then-time-boundary");
    let pipeline = Pipeline::default();
    let sink = two_source_count_then_time_boundary_fixture(&pipeline, path.clone()).unwrap();
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_count_then_time_boundary(&read_rows(&path));
}

#[tokio::test]
#[ignore = "pinned Prism panics in processing-time timer evaluation for this schedule"]
async fn prism_multi_source_count_then_time_fires_first_pane_at_element_boundary() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("portable-count-then-time-boundary");
    let pipeline = Pipeline::default();
    two_source_count_then_time_boundary_fixture(&pipeline, path.clone()).unwrap();
    let (state, messages) = run_shared_fixture(
        &endpoint,
        &pipeline,
        "two-source-count-then-time-boundary",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_TWO_SOURCE_COUNT_THEN_TIME_BOUNDARY".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        state, 3,
        "Prism count-then-time boundary failed: {messages:?}"
    );
    assert_count_then_time_boundary(&read_rows(&path));
}

fn assert_count_and_time_boundary(rows: &[String]) {
    assert_eq!(rows, ["1:[[2, 3], []]:Early:0", "1:[[4, 5], []]:OnTime:1",]);
}

#[test]
fn direct_multi_source_count_and_time_fires_at_ready_boundary() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("direct-count-and-time-boundary");
    let pipeline = Pipeline::default();
    let sink = two_source_count_and_time_boundary_fixture(&pipeline, path.clone()).unwrap();
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_count_and_time_boundary(&read_rows(&path));
}

#[tokio::test]
#[ignore = "pinned Prism panics in processing-time timer evaluation for this schedule"]
async fn prism_multi_source_count_and_time_fires_at_ready_boundary() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("portable-count-and-time-boundary");
    let pipeline = Pipeline::default();
    two_source_count_and_time_boundary_fixture(&pipeline, path.clone()).unwrap();
    let (state, messages) = run_shared_fixture(
        &endpoint,
        &pipeline,
        "two-source-count-and-time-boundary",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_TWO_SOURCE_COUNT_AND_TIME_BOUNDARY".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        state, 3,
        "Prism count-and-time boundary failed: {messages:?}"
    );
    assert_count_and_time_boundary(&read_rows(&path));
}
