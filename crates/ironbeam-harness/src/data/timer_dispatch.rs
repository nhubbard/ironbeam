//! Deliver one declared Beam timer family from Fn Data to user callbacks.

use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};

use anyhow::{bail, Result};
use futures_util::FutureExt;
use ironbeam_core::StatefulParDoContract;

use super::DataClient;
use crate::timer_coder::{try_decode_timer_record, TimerRecord};

const MAX_TIMER_INPUT_BYTES: usize = 64 * 1024 * 1024;

/// One declared timer family in an active Beam bundle.
pub struct TimerFamilyScope<'a> {
    client: &'a DataClient,
    instruction_id: &'a str,
    transform_id: &'a str,
    family_id: &'a str,
}

impl<'a> TimerFamilyScope<'a> {
    pub fn new(
        client: &'a DataClient,
        contract: &StatefulParDoContract,
        instruction_id: &'a str,
        transform_id: &'a str,
        family_id: &'a str,
    ) -> Result<Self> {
        contract.validate()?;
        if instruction_id.is_empty() || transform_id.is_empty() {
            bail!("Beam timer dispatch requires instruction and transform IDs");
        }
        if !contract.timers.contains_key(family_id) {
            bail!("undeclared Beam timer family {family_id}");
        }
        Ok(Self {
            client,
            instruction_id,
            transform_id,
            family_id,
        })
    }

    /// Queue one timer mutation for this declared family. The runner commits
    /// it only if the enclosing bundle succeeds. A user-facing callback layer
    /// must bind the record's encoded key and window to its current element.
    pub fn emit(&self, record: &TimerRecord) -> Result<()> {
        self.client.write_timer_record(
            self.instruction_id,
            self.transform_id,
            self.family_id,
            record,
        )
    }

    /// Close this family's outbound timer stream after all callbacks finish.
    pub fn finish_output(&self) -> Result<()> {
        self.client
            .close_timer_write(self.instruction_id, self.transform_id, self.family_id)
    }

    /// Drain the timer input stream and invoke a callback for each complete
    /// record. Chunks may split records at any byte boundary. The executor
    /// supplies the declared key and window coder boundary decoders.
    /// A callback failure fails the bundle before runner acknowledgement.
    pub async fn dispatch<K, W, F, Fut>(
        &self,
        decode_key: K,
        decode_window: W,
        mut callback: F,
    ) -> Result<usize>
    where
        K: Fn(&[u8]) -> Result<Option<usize>>,
        W: Fn(&[u8]) -> Result<Option<usize>>,
        F: FnMut(TimerRecord) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let mut reader =
            self.client
                .open_timer_read(self.instruction_id, self.transform_id, self.family_id);
        let mut encoded = Vec::new();
        let mut callbacks = 0;
        while let Some(chunk) = reader.recv().await {
            if encoded.len().saturating_add(chunk.len()) > MAX_TIMER_INPUT_BYTES {
                bail!("Beam timer-family input exceeds 64 MiB");
            }
            encoded.extend_from_slice(&chunk);
            let mut consumed = 0;
            while let Some((record, used)) =
                try_decode_timer_record(&encoded[consumed..], &decode_key, &decode_window)?
            {
                if used == 0 {
                    bail!("Beam timer coder consumed no bytes");
                }
                consumed += used;
                let future = match catch_unwind(AssertUnwindSafe(|| callback(record))) {
                    Ok(future) => future,
                    Err(_) => bail!("Beam timer callback panicked"),
                };
                match AssertUnwindSafe(future).catch_unwind().await {
                    Ok(result) => result?,
                    Err(_) => bail!("Beam timer callback panicked"),
                }
                callbacks += 1;
            }
            if consumed != 0 {
                encoded.drain(..consumed);
            }
        }
        if !encoded.is_empty() {
            bail!("Beam timer-family input ended with a truncated record");
        }
        Ok(callbacks)
    }
}
