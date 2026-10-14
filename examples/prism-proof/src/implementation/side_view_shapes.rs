/// Exercise singleton, map, and multimap views through one portable graph.
pub fn side_view_shapes_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<String>, Error> {
    let main = pipeline.from_vec(
        "side-shapes-main",
        vec!["a".to_owned(), "b".to_owned(), "missing".to_owned()],
    )?;
    let singleton = pipeline.from_vec("side-shapes-singleton", vec![100_i32])?;
    let map = pipeline.from_vec(
        "side-shapes-map",
        vec![("a".to_owned(), 10_i32), ("b".to_owned(), 20)],
    )?;
    let multimap = pipeline.from_vec(
        "side-shapes-multimap",
        vec![
            ("a".to_owned(), 1_i32),
            ("a".to_owned(), 2),
            ("b".to_owned(), 3),
        ],
    )?;
    let single_rows =
        main.map_with_singleton_side_named("side-shapes-single", &singleton, |key, value| {
            format!("single:{key}:{value}")
        })?;
    let map_rows = main.map_with_map_side_named("side-shapes-lookup", &map, |key, values| {
        format!(
            "map:{key}:{}",
            values.get(&key).copied().unwrap_or_default()
        )
    })?;
    let multimap_rows =
        main.map_with_multimap_side_named("side-shapes-many", &multimap, |key, values| {
            let mut values = values.get(&key).cloned().unwrap_or_default();
            values.sort_unstable();
            format!("multi:{key}:{values:?}")
        })?;
    let sink_path = path.clone();
    single_rows
        .flatten_named("side-shapes-merge", &[map_rows, multimap_rows])?
        .sink_named_with_config(
            "side-shapes-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("side-shapes-result:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}
