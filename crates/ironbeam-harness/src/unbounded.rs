//! Worker-side polling boundary for a one-split source restriction.

use ironbeam_core::{
    Boundedness, Error, Graph, Operation, Registry, Replay, UnboundedPoll, UnboundedSourceReader,
    Window,
};
use ironbeam_graphx::sdf::{RestrictionError, SourceRestriction};
use std::panic::{catch_unwind, AssertUnwindSafe};

#[derive(Debug, thiserror::Error)]
/// Failure while opening or polling a restricted source.
pub enum SourcePollError {
    #[error(transparent)]
    Core(#[from] Error),
    #[error(transparent)]
    Restriction(#[from] RestrictionError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// One coded source element with its event timestamp.
pub struct EncodedSourceElement {
    pub bytes: Vec<u8>,
    pub timestamp_ns: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Pending or ready result of one unbounded-source poll.
pub enum SourcePoll {
    Pending,
    Ready {
        elements: Vec<EncodedSourceElement>,
        /// Residual work for a runner-owned split; absent on completion.
        residual: Option<SourceRestriction>,
        watermark_ns: Option<i64>,
    },
}

/// Reads a replayable source from a validated restriction cursor.
pub struct SourcePoller<'a> {
    key: String,
    registry: &'a Registry,
    reader: UnboundedSourceReader,
    accepted: SourceRestriction,
}

impl<'a> SourcePoller<'a> {
    /// Open a registered unbounded source at a validated restriction cursor.
    pub fn open(
        graph: &Graph,
        registry: &'a Registry,
        key: &str,
        restriction: SourceRestriction,
    ) -> Result<Self, SourcePollError> {
        restriction.encode()?;
        if restriction.source_key != key {
            return Err(RestrictionError::Identity.into());
        }
        let node = graph
            .node(key)
            .ok_or_else(|| Error::graph(key, "source node missing"))?;
        let Operation::Source { progress } = &node.operation else {
            return Err(Error::graph(key, "restriction does not name a source").into());
        };
        if node.boundedness != Boundedness::Unbounded
            || !progress.stable_split_id
            || progress.replay == Replay::None
            || !registry.has_unbounded_source(key)
            || !registry.has_coder(key)
        {
            return Err(Error::unsupported(key, "dataflow-v5", "resumable-coded-source").into());
        }
        if registry.source_requires_finalization(key) {
            return Err(
                Error::unsupported(key, "dataflow-v5", "source-bundle-finalization").into(),
            );
        }
        let reader = catch_unwind(AssertUnwindSafe(|| {
            registry.open_unbounded_source(key, restriction.progress.clone())
        }))
        .unwrap_or_else(|_| {
            Err(Error::execution(
                key,
                "dataflow-v5",
                "source factory panicked",
            ))
        })?;
        Ok(Self {
            key: key.into(),
            registry,
            reader,
            accepted: restriction,
        })
    }

    /// Poll once without dropping the reader on `Pending`. The Fn API process
    /// stage decides when to split and return `residual` to the runner.
    pub fn poll(&mut self, max_elements: usize) -> Result<SourcePoll, SourcePollError> {
        let result = catch_unwind(AssertUnwindSafe(|| self.reader.poll(max_elements)))
            .unwrap_or_else(|_| {
                Err(Error::execution(
                    &self.key,
                    "dataflow-v5",
                    "source poll panicked",
                ))
            })?;
        let UnboundedPoll::Ready(batch) = result else {
            return Ok(SourcePoll::Pending);
        };
        if !batch.elements.is_empty()
            && self.accepted.progress.as_ref().is_some_and(|previous| {
                previous.checkpoint_position == batch.progress.checkpoint_position
            })
        {
            return Err(Error::execution(
                &self.key,
                "dataflow-v5",
                "data batch did not advance its cursor",
            )
            .into());
        }
        // A watermark-only poll without a cursor cannot be restored by the
        // factory, so retain the live reader and wait for a durable position.
        if batch.progress.checkpoint_position.is_none() && !batch.progress.completed {
            return Ok(SourcePoll::Pending);
        }
        let mut elements = Vec::with_capacity(batch.elements.len());
        for element in batch.elements {
            if element.window != Window::Global || element.assigned_window.is_some() {
                return Err(Error::execution(
                    &self.key,
                    "dataflow-v5",
                    "source emitted unsupported window metadata",
                )
                .into());
            }
            let bytes = catch_unwind(AssertUnwindSafe(|| {
                self.registry
                    .encode_element(&self.key, element.value.as_ref())
            }))
            .unwrap_or_else(|_| {
                Err(Error::execution(
                    &self.key,
                    "dataflow-v5",
                    "source coder panicked",
                ))
            })?;
            elements.push(EncodedSourceElement {
                bytes,
                timestamp_ns: element.timestamp_ns,
            });
        }
        let residual = if batch.progress.completed {
            None
        } else {
            Some(SourceRestriction::resume(batch.progress.clone())?)
        };
        if let Some(next) = &residual {
            self.accepted = next.clone();
        }
        Ok(SourcePoll::Ready {
            elements,
            residual,
            watermark_ns: batch.progress.event_time_ns,
        })
    }
}

#[cfg(test)]
#[path = "../tests/unit/unbounded__tests.rs"]
mod tests;
