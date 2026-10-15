fn assemble_fold_boundary_row(group: GroupBoundary) -> Result<WireValue> {
    let GroupBoundary {
        key,
        values,
        timestamp_ns,
        assigned_window,
        ..
    } = group;
    if values.is_empty() {
        bail!("v5 keyed fold group has no values");
    }
    let mut bytes = Vec::new();
    encode_length_prefix(&key, &mut bytes)?;
    for value in values {
        encode_length_prefix(&value, &mut bytes)?;
    }
    Ok(WireValue {
        bytes,
        timestamp_ns,
        window: assigned_window,
        pane: group.pane,
        element_metadata: group.element_metadata,
    })
}

fn append_state_backed_values(group: &mut GroupBoundary, encoded: &[u8]) -> Result<()> {
    let mut offset = 0;
    while offset < encoded.len() {
        let (value, used) = decode_length_prefix(&encoded[offset..])
            .context("decode v5 state-backed group value")?;
        group.values.push(value.to_vec());
        offset += used;
    }
    Ok(())
}

struct SinkGuard {
    session: Box<dyn SinkSession>,
    committed: bool,
}

impl SinkGuard {
    fn begin_bundle(&mut self, instruction_id: &str) -> Result<()> {
        catch_unwind(AssertUnwindSafe(|| {
            self.session.begin_bundle(instruction_id)
        }))
        .map_err(|_| anyhow!("v5 sink bundle initialization panicked"))??;
        Ok(())
    }
    fn write(&mut self, value: Envelope) -> Result<()> {
        self.session.write(value)?;
        Ok(())
    }
    fn commit(&mut self) -> Result<()> {
        self.session.commit()?;
        self.committed = true;
        Ok(())
    }
    fn requires_finalization(&self) -> Result<bool> {
        catch_unwind(AssertUnwindSafe(|| {
            self.session.requires_bundle_finalization()
        }))
        .map_err(|_| anyhow!("v5 sink finalization declaration panicked"))
    }
    fn finalize(&mut self) -> Result<()> {
        catch_unwind(AssertUnwindSafe(|| self.session.finalize_bundle()))
            .map_err(|_| anyhow!("v5 sink bundle finalizer panicked"))??;
        Ok(())
    }
    fn recover(&mut self, instruction_id: &str) -> Result<()> {
        catch_unwind(AssertUnwindSafe(|| {
            self.session.recover_bundle_finalization(instruction_id)
        }))
        .map_err(|_| anyhow!("v5 sink recovery finalizer panicked"))??;
        Ok(())
    }
}

impl Drop for SinkGuard {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.session.abort();
        }
    }
}

#[derive(Default)]
struct BundleFinalizations {
    entries: Mutex<HashMap<String, Arc<Mutex<FinalizationEntry>>>>,
    journal: Option<Arc<dyn FinalizationJournal>>,
}

struct FinalizationEntry {
    sinks: Vec<(String, SinkGuard)>,
    recovered: bool,
    completed: bool,
}

impl BundleFinalizations {
    fn with_journal(journal: Arc<dyn FinalizationJournal>) -> Self {
        Self {
            entries: Mutex::default(),
            journal: Some(journal),
        }
    }

    fn register(&self, instruction_id: &str, mut sinks: Vec<(String, SinkGuard)>) -> Result<()> {
        if instruction_id.is_empty() || sinks.is_empty() {
            bail!("v5 bundle finalization needs an instruction id and sinks");
        }
        sinks.sort_by(|left, right| left.0.cmp(&right.0));
        if sinks.iter().any(|(key, _)| key.is_empty())
            || sinks.windows(2).any(|pair| pair[0].0 == pair[1].0)
        {
            bail!("v5 bundle finalization sink keys must be distinct");
        }
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| anyhow!("v5 finalization registry poisoned"))?;
        if entries.contains_key(instruction_id) {
            bail!("v5 bundle finalization already registered for {instruction_id}");
        }
        if let Some(journal) = &self.journal {
            let keys = sinks.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
            if let Err(error) = journal.record(instruction_id, &keys) {
                for (_, sink) in &mut sinks {
                    let _ = sink.session.abort();
                }
                return Err(error);
            }
        }
        // Pop successful sinks from the end without replaying them if a later
        // finalizer fails. Reversing preserves sorted-key finalization order.
        sinks.reverse();
        entries.insert(
            instruction_id.into(),
            Arc::new(Mutex::new(FinalizationEntry {
                sinks,
                recovered: false,
                completed: false,
            })),
        );
        Ok(())
    }

    fn finalize(&self, instruction_id: &str, registry: &Registry) -> Result<()> {
        let entry = {
            let mut entries = self
                .entries
                .lock()
                .map_err(|_| anyhow!("v5 finalization registry poisoned"))?;
            if let Some(entry) = entries.get(instruction_id) {
                Arc::clone(entry)
            } else {
                let journal = self.journal.as_ref().ok_or_else(|| {
                    anyhow!("v5 bundle {instruction_id} has no registered finalizer")
                })?;
                let record = journal.load(instruction_id)?.ok_or_else(|| {
                    anyhow!("v5 bundle {instruction_id} has no durable finalizer")
                })?;
                if record.completed {
                    // The marker is already durable; duplicate callbacks do not
                    // need to retain another entry in a long-running worker.
                    return Ok(());
                }
                let mut sinks = Vec::new();
                for key in record.sinks {
                    let session = registry.open_sink(&key)?;
                    if !session.requires_bundle_finalization() {
                        bail!("recovered v5 sink {key} does not request finalization");
                    }
                    sinks.push((
                        key,
                        SinkGuard {
                            session,
                            committed: true,
                        },
                    ));
                }
                sinks.reverse();
                let entry = Arc::new(Mutex::new(FinalizationEntry {
                    sinks,
                    recovered: true,
                    completed: false,
                }));
                entries.insert(instruction_id.into(), Arc::clone(&entry));
                entry
            }
        };
        let mut state = entry
            .lock()
            .map_err(|_| anyhow!("v5 finalization entry poisoned"))?;
        if state.completed {
            return Ok(());
        }
        let recovered = state.recovered;
        while let Some((_, sink)) = state.sinks.last_mut() {
            if recovered {
                sink.recover(instruction_id)?;
            } else {
                sink.finalize()?;
            }
            state.sinks.pop();
        }
        if let Some(journal) = &self.journal {
            journal.complete(instruction_id)?;
        }
        state.completed = true;
        drop(state);
        if self.journal.is_some() {
            // The durable completion marker now handles duplicate callbacks.
            // Release the per-bundle sessions for long-running workers.
            let mut entries = self
                .entries
                .lock()
                .map_err(|_| anyhow!("v5 finalization registry poisoned"))?;
            if entries
                .get(instruction_id)
                .is_some_and(|current| Arc::ptr_eq(current, &entry))
            {
                entries.remove(instruction_id);
            }
        }
        Ok(())
    }
}

struct BundleOutputs<'a> {
    user_sinks: &'a mut HashMap<String, SinkGuard>,
    user_sink_counts: &'a mut HashMap<String, u64>,
    sink_clients: &'a HashMap<String, DataClient>,
    output_counts: &'a mut HashMap<String, u64>,
}

pub struct V5BundleExecutor {
    graph: Graph,
    registry: Registry,
    identity: ironbeam_core::ExecutionIdentity,
    worker_id: Option<String>,
    data_clients: AsyncMutex<HashMap<String, DataClient>>,
    active_sdf: Mutex<HashMap<String, ActiveSdfBundle>>,
    finalizations: BundleFinalizations,
}
