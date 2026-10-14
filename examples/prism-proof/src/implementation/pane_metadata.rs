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
        let expected = if value.starts_with("group:") {
            ironbeam_core::PaneInfo::FIRST_LAST_ON_TIME
        } else {
            ironbeam_core::PaneInfo::NO_FIRING
        };
        if metadata.pane != expected {
            return Err(Error::execution(
                "pane-result",
                "runner",
                "sink pane metadata differs from reified element pane",
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
    let values = pipeline
        .from_vec(
            "pane-events",
            vec![
                (1_000_000_i64, ("a".to_owned(), 2_i32)),
                (2_000_000, ("a".to_owned(), 3)),
            ],
        )?
        .timestamps_from_pairs_named("pane-event-time")?
        .fixed_windows("pane-windows", 10_000_000)?;
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
