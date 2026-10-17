use super::IdempotentStateLabelSink;
use ironbeam_core::{
    Error, Pipeline, ProgressSignal, Replay, Sink, SinkMode, SinkSpec, SourceProgress, Timestamped,
    TypedUnboundedSourceReader, UnboundedBatch, UnboundedPoll, UnboundedSource,
};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

struct DelayedByteSource {
    cursor: Option<u8>,
    next_ready_millis: u64,
    global: bool,
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
            Some(1) if source_clock_millis()? < self.next_ready_millis => {
                return Ok(UnboundedPoll::Pending);
            }
            Some(1) => (
                vec![("c", 3_000_000)],
                2,
                if self.global { 3_000_000 } else { 10_000_000 },
                !self.global,
            ),
            Some(2) if self.global && source_clock_millis()? < self.next_ready_millis => {
                return Ok(UnboundedPoll::Pending);
            }
            Some(2) if self.global => (Vec::new(), 3, 3_000_000, true),
            _ => return Ok(UnboundedPoll::Pending),
        };
        self.cursor = Some(cursor);
        let checkpoint_position = if completed {
            vec![cursor]
        } else {
            self.next_ready_millis = source_clock_millis()?.saturating_add(200);
            let mut position = vec![cursor];
            position.extend_from_slice(&self.next_ready_millis.to_be_bytes());
            position
        };
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
                checkpoint_position: Some(checkpoint_position),
                event_time_ns: Some(watermark),
                completed,
            },
        }))
    }
}

fn source_clock_millis() -> Result<u64, Error> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Error::incompatible(error.to_string()))?;
    u64::try_from(elapsed.as_millis())
        .map_err(|_| Error::incompatible("timed byte source clock exceeds u64 milliseconds"))
}

/// A replayable production source leaves enough real processing time between
/// its two batches for the first partial byte batch to expire.
pub fn streaming_timed_byte_batches_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    timed_byte_batches_fixture(pipeline, directory, false)
}

/// A global stream waits for both processing timers before completing.
pub fn streaming_global_timed_byte_batches_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    timed_byte_batches_fixture(pipeline, directory, true)
}

fn timed_byte_batches_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
    global: bool,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    let source = pipeline.unbounded_source_with_factory(
        "timed-byte-input",
        SourceProgress {
            stable_split_id: true,
            replay: Replay::AtLeastOnce,
            reports_event_time: true,
        },
        format!("prism-proof/timed-byte-source-v3/global={global}"),
        0,
        move |position| {
            if std::env::var_os("IRONBEAM_V5_PRISM_KILL_TIMED_BYTE_BATCH").is_some()
                && position
                    .as_deref()
                    .is_some_and(|bytes| bytes.first() == Some(&1))
            {
                super::await_supervised_source_replacement(Some(&[1]))?;
            }
            let (cursor, next_ready_millis) = match position.as_deref() {
                None => (None, 0),
                Some([stage @ (1 | 2), rest @ ..])
                    if rest.len() == 8 && (*stage == 1 || global) =>
                {
                    (
                        Some(*stage),
                        u64::from_be_bytes(rest.try_into().map_err(|_| {
                            Error::incompatible("invalid timed byte source deadline")
                        })?),
                    )
                }
                Some([2]) => (Some(2), 0),
                _ => return Err(Error::incompatible("unexpected timed byte source cursor")),
            };
            Ok(Box::new(DelayedByteSource {
                cursor,
                next_ready_millis,
                global,
            }) as TypedUnboundedSourceReader<(i32, String)>)
        },
    )?;
    let source = if global {
        source
    } else {
        source.fixed_windows("timed-byte-window", 10_000_000)?
    };
    source
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
