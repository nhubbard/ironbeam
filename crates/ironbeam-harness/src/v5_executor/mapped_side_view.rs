fn mapped_side_window_for(main: WindowInterval, side: &Window) -> Result<WindowInterval> {
    if main.end_ns % 1_000_000 != 0 {
        bail!("mapped main window needs whole-millisecond end");
    }
    let mapping = match side {
        Window::Fixed { size_ns } => WindowMapping::Fixed {
            size_millis: size_ns / 1_000_000,
            offset_millis: 0,
        },
        Window::FixedOffset { size_ns, offset_ns } => WindowMapping::Fixed {
            size_millis: size_ns / 1_000_000,
            offset_millis: offset_ns / 1_000_000,
        },
        Window::Sliding { size_ns, period_ns, offset_ns } => WindowMapping::Sliding {
            size_millis: size_ns / 1_000_000,
            period_millis: period_ns / 1_000_000,
            offset_millis: offset_ns / 1_000_000,
        },
        _ => bail!("mapped side window must be fixed or sliding"),
    };
    let (start, end) = mapped_target_window(main.end_ns / 1_000_000, mapping)?;
    Ok(WindowInterval {
        start_ns: start.checked_mul(1_000_000)
            .context("mapped side window start exceeds signed nanoseconds")?,
        end_ns: end.checked_mul(1_000_000)
            .context("mapped side window end exceeds signed nanoseconds")?,
    })
}

impl V5BundleExecutor {
    async fn ensure_mapped_side_view(
        &self,
        instruction_id: &str,
        transform_id: &str,
        node: &Node,
        main_window: WindowInterval,
        state: &StateClient,
        views: &mut HashMap<(String, Option<WindowInterval>), Box<dyn Any + Send>>,
    ) -> Result<WindowInterval> {
        let side_key = &node.additional_inputs.first()
            .ok_or_else(|| anyhow!("mapped side view has no side input"))?.key;
        let side = self.graph.node(side_key)
            .ok_or_else(|| anyhow!("mapped side view input is missing"))?;
        let target = mapped_side_window_for(main_window, &side.time.window)?;
        let cache_key = (node.key.clone(), Some(target));
        if views.contains_key(&cache_key) {
            return Ok(target);
        }
        let mut encoded_window = Vec::new();
        encode_interval_window(
            target.start_ns / 1_000_000,
            target.end_ns / 1_000_000,
            &mut encoded_window,
        )?;
        let encoded = state.get_all(instruction_id, StateKey {
            r#type: Some(state_key::Type::IterableSideInput(
                state_key::IterableSideInput {
                    transform_id: transform_id.into(),
                    side_input_id: "side".into(),
                    window: encoded_window,
                },
            )),
        }).await?;
        let mut values = Vec::new();
        let mut consumed = 0;
        while consumed < encoded.len() {
            let (value, used) = decode_length_prefix(&encoded[consumed..])
                .context("decode mapped side input element")?;
            values.push(self.registry.decode_element(side_key, value)?);
            consumed += used;
        }
        let view = catch_unwind(AssertUnwindSafe(|| {
            self.registry.materialize_side_input(&node.key, values)
        }))
        .map_err(|panic| user_panic_error(&node.key, "aggregate", panic))??;
        views.insert(cache_key, view);
        Ok(target)
    }
}
