/// Twelve bounded sources exercise co-group beyond the fixed typed arity.
pub fn homogeneous_twelve_way_cogroup_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    let inputs = (0..12)
        .map(|index| {
            let mut rows = if index == 4 {
                Vec::new()
            } else {
                vec![(1_000_000_i64, ("a".to_owned(), index))]
            };
            if index == 0 {
                rows.push((2_000_000, ("a".to_owned(), 100)));
            }
            if index == 11 {
                rows.push((0, ("z".to_owned(), 99)));
            }
            pipeline
                .from_vec(&format!("homogeneous-input-{index}"), rows)?
                .timestamps_from_pairs_named(&format!("homogeneous-time-{index}"))?
                .fixed_windows(&format!("homogeneous-window-{index}"), 10_000_000)
        })
        .collect::<Result<Vec<_>, Error>>()?;
    inputs[0]
        .co_group_homogeneous_named("homogeneous-group", &inputs[1..])?
        .reify_timestamps_named("homogeneous-result-time")?
        .with_window_named("homogeneous-result-window")?
        .map_named("homogeneous-result-row", |(window, (time, (key, mut buckets)))| {
            for bucket in &mut buckets {
                bucket.sort_unstable();
            }
            format!(
                "{}..{}:{time}:{key}:{buckets:?}",
                window.start_ns, window.end_ns
            )
        })
}

pub fn homogeneous_twelve_way_cogroup_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    homogeneous_twelve_way_cogroup_collection(pipeline)?.sink_named_with_config(
        "homogeneous-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("homogeneous-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}
