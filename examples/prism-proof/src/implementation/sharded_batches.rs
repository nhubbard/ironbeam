use super::*;
use ironbeam_core::Version;

pub fn beam_sharded_batches_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec(
            "sharded-batch-input",
            vec![
                ("hot".to_owned(), (1_u8, 10_i32)),
                ("hot".to_owned(), (2, 20)),
                ("hot".to_owned(), (1, 30)),
                ("hot".to_owned(), (2, 40)),
            ],
        )?
        .group_into_batches_with_sharded_key_named(
            "sharded-batches",
            2,
            "proof/select-shard",
            Version { major: 1, minor: 0 },
            |_, (shard, _)| vec![*shard],
        )?
        .map_named("sharded-batch-output", |(key, values)| {
            let mut values = values
                .into_iter()
                .map(|(_, value)| value)
                .collect::<Vec<_>>();
            values.sort_unstable();
            format!("{}:{:?}:{values:?}", key.key, key.shard_id)
        })?
        .sink_named_with_config(
            "sharded-batch-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("sharded-batches:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

pub fn beam_sharded_batches_stream_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    pipeline
        .test_stream_source(
            "sharded-stream-input",
            vec![
                TestStreamEvent::Elements(vec![
                    (("hot".to_owned(), (1_u8, 10_i32)), 1_000_000),
                    (("hot".to_owned(), (2, 20)), 2_000_000),
                ]),
                TestStreamEvent::Elements(vec![
                    (("hot".to_owned(), (1, 30)), 3_000_000),
                    (("hot".to_owned(), (2, 40)), 4_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )?
        .fixed_windows("sharded-stream-window", 10_000_000)?
        .group_into_batches_with_sharded_key_named(
            "sharded-stream-batches",
            2,
            "proof/select-stream-shard",
            Version { major: 1, minor: 0 },
            |_, (shard, _)| vec![*shard],
        )?
        .map_named("sharded-stream-output", |(key, values)| {
            let mut values = values
                .into_iter()
                .map(|(_, value)| value)
                .collect::<Vec<_>>();
            values.sort_unstable();
            format!("{}:{:?}:{values:?}", key.key, key.shard_id)
        })?
        .sink_named_with_config(
            "sharded-stream-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("sharded-stream:{}", path.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_path.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}
