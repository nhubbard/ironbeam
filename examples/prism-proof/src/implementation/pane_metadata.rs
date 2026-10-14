struct PaneCheckingSink(GlobalFoldSink<String>);

impl TypedSink<String> for PaneCheckingSink {
    fn write(&mut self, value: String) -> Result<(), Error> {
        self.0.write(value)
    }

    fn write_with_metadata(
        &mut self,
        value: String,
        metadata: ElementMetadata,
    ) -> Result<(), Error> {
        let pane = metadata.pane;
        let suffix = format!(
            ":{:?}:{}:{}:{}:{}",
            pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index
        );
        if !value.ends_with(&suffix) {
            return Err(Error::execution(
                "pane-result",
                "runner",
                format!(
                    "sink pane metadata differs from reified element pane: value={value}, actual={pane:?}"
                ),
            ));
        }
        self.0.write(value)
    }

    fn commit(&mut self) -> Result<(), Error> {
        self.0.commit()
    }

    fn abort(&mut self) -> Result<(), Error> {
        self.0.abort()
    }
}

/// Compare source and grouped pane metadata through Direct and native Beam GBK.
pub fn pane_metadata_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<String>, Error> {
    pane_metadata_fixture_for(pipeline, path, PaneFixtureWindow::Fixed)
}

#[derive(Clone, Copy, Debug)]
pub enum PaneFixtureWindow {
    Fixed,
    Sliding,
    Session,
}

pub fn pane_metadata_fixture_for(
    pipeline: &Pipeline,
    path: PathBuf,
    window: PaneFixtureWindow,
) -> Result<Sink<String>, Error> {
    pane_metadata_fixture_with_trigger(pipeline, path, window, false)
}

pub fn pane_metadata_fixture_with_trigger(
    pipeline: &Pipeline,
    path: PathBuf,
    window: PaneFixtureWindow,
    early_count_one: bool,
) -> Result<Sink<String>, Error> {
    let values = pipeline
        .from_vec(
            "pane-events",
            vec![
                (1_000_000_i64, ("a".to_owned(), 2_i32)),
                (2_000_000, ("a".to_owned(), 3)),
            ],
        )?
        .timestamps_from_pairs_named("pane-event-time")?;
    let values = match window {
        PaneFixtureWindow::Fixed => values.fixed_windows("pane-windows", 10_000_000)?,
        PaneFixtureWindow::Sliding => {
            values.sliding_windows("pane-windows", 10_000_000, 5_000_000, 0)?
        }
        PaneFixtureWindow::Session => values.session_windows_named("pane-windows", 10_000_000)?,
    };
    let values = if early_count_one {
        values.early_count_trigger_named("pane-count-trigger", 1)?
    } else {
        values
    };
    let raw = values.reify_panes_named("source-pane")?.map_named(
        "source-pane-row",
        |(pane, (key, value))| {
            format!(
                "source:{key}:{value}:{:?}:{}:{}:{}:{}",
                pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index
            )
        },
    )?;
    let grouped = values
        .group_by_key_named("pane-group")?
        .reify_panes_named("group-pane")?
        .map_named("group-pane-row", |(pane, (key, mut values))| {
            values.sort_unstable();
            format!(
                "group:{key}:{values:?}:{:?}:{}:{}:{}:{}",
                pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index
            )
        })?;
    let sink_path = path.clone();
    raw.flatten_named("pane-merge", &[grouped])?
        .sink_named_with_config(
            "pane-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("pane-result:{}", path.display()),
            move || {
                Ok(Box::new(PaneCheckingSink(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                })))
            },
        )
}

/// Characterize repeated count firings across separate TestStream advances.
pub fn count_trigger_stream_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let values = pipeline
        .test_stream_source(
            "count-events",
            vec![
                TestStreamEvent::Elements(vec![(('a'.to_string(), 2_i32), 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(2_000_000),
                TestStreamEvent::Elements(vec![(('a'.to_string(), 3_i32), 3_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )?
        .fixed_windows("count-windows", 10_000_000)?
        .early_count_trigger_named("count-trigger", 1)?;
    let rows = values
        .group_by_key_named("count-group")?
        .reify_panes_named("count-pane")?
        .map_named("count-row", |(pane, (key, mut values))| {
            values.sort_unstable();
            format!(
                "group:{key}:{values:?}:{:?}:{}:{}:{}:{}",
                pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index
            )
        })?;
    let sink_path = path.clone();
    rows.sink_named_with_config(
        "count-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("count-result:{}", path.display()),
        move || {
            Ok(Box::new(PaneCheckingSink(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            })))
        },
    )
}
