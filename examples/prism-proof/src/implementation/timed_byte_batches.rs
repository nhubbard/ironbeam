use super::IdempotentStateLabelSink;
use ironbeam_core::{
    Error, Pipeline, ProgressSignal, Replay, Sink, SinkMode, SinkSpec, SourceProgress, Timestamped,
    TypedUnboundedSourceReader, UnboundedBatch, UnboundedPoll, UnboundedSource,
};
use std::path::PathBuf;
use std::time::{Duration, Instant};

struct DelayedByteSource {
    cursor: Option<u8>,
    next_ready: Instant,
}

impl UnboundedSource<Timestamped<(i32, String)>> for DelayedByteSource {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<(i32, String)>>, Error> {
        let (values, cursor, watermark, completed) = match self.cursor {
            None => (
                vec![("a", 1_000_000), ("b", 2_000_000)],
                1,
                2_000_000,
                false,
            ),
            Some(1) if Instant::now() < self.next_ready => return Ok(UnboundedPoll::Pending),
            Some(1) => (vec![("c", 3_000_000)], 2, 10_000_000, true),
            _ => return Ok(UnboundedPoll::Pending),
        };
        self.cursor = Some(cursor);
        self.next_ready = Instant::now() + Duration::from_millis(200);
        Ok(UnboundedPoll::Ready(UnboundedBatch {
            elements: values
                .into_iter()
                .map(|(value, timestamp_ns)| Timestamped {
                    value: (1_i32, value.to_owned()),
                    timestamp_ns,
                })
                .collect(),
            progress: ProgressSignal {
                source_key: "timed-byte-input".into(),
                split_id: "default".into(),
                checkpoint_position: Some(vec![cursor]),
                event_time_ns: Some(watermark),
                completed,
            },
        }))
    }
}

/// A replayable production source leaves enough real processing time between
/// its two batches for the first partial byte batch to expire.
pub fn streaming_timed_byte_batches_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .unbounded_source_with_factory(
            "timed-byte-input",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "prism-proof/timed-byte-source-v1".into(),
            0,
            move |position| {
                let cursor = match position.as_deref() {
                    None => None,
                    Some([1]) => Some(1),
                    Some([2]) => Some(2),
                    _ => return Err(Error::incompatible("unexpected timed byte source cursor")),
                };
                Ok(Box::new(DelayedByteSource {
                    cursor,
                    next_ready: Instant::now() + Duration::from_millis(200),
                })
                    as TypedUnboundedSourceReader<(i32, String)>)
            },
        )?
        .fixed_windows("timed-byte-window", 10_000_000)?
        .group_into_batches_with_limits_and_max_buffering_duration_named(
            "timed-byte-batches",
            10,
            1_000,
            20_000_000,
        )?
        .reify_timestamps_named("timed-byte-time")?
        .map_named("timed-byte-label", |(timestamp, (key, mut values))| {
            values.sort();
            format!("{key}:{timestamp}:{values:?}")
        })?
        .sink_named_with_config(
            "timed-byte-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("timed-byte-result:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentStateLabelSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}
