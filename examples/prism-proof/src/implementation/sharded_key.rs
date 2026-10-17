use super::*;
use ironbeam_core::BeamShardedKey;

/// Send a standard Beam sharded-key value across a runner reshuffle.
pub fn beam_sharded_key_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec("sharded-key-input", vec![1_u8, 2])?
        .map_named("sharded-key-build", |shard| {
            BeamShardedKey::new("account".to_owned(), vec![0, shard])
        })?
        .reshuffle_named("sharded-key-reshuffle")?
        .map_named("sharded-key-read", |value| {
            format!("{}:{:?}", value.key, value.shard_id)
        })?
        .sink_named_with_config(
            "sharded-key-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("sharded-key:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// Send the bytes-key form of Beam's sharded-key coder across reshuffle.
pub fn beam_binary_sharded_key_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec("binary-sharded-key-input", vec![0_u8, 1])?
        .map_named("binary-sharded-key-build", |index| {
            BeamShardedKey::new(vec![index, 255], vec![9, index])
        })?
        .reshuffle_named("binary-sharded-key-reshuffle")?
        .map_named("binary-sharded-key-read", |value| {
            format!("{:?}:{:?}", value.key, value.shard_id)
        })?
        .sink_named_with_config(
            "binary-sharded-key-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("binary-sharded-key:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}
