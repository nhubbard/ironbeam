struct FinalizationProbe<T> {
    store: LocalFinalResult,
    count: u64,
    payload: Vec<u8>,
    bundle_id: Option<String>,
    staged: bool,
    fail_after_stage: bool,
    marker: std::marker::PhantomData<T>,
}

impl<T: Element> TypedSink<T> for FinalizationProbe<T> {
    fn begin_bundle(&mut self, instruction_id: &str) -> Result<(), Error> {
        if instruction_id.is_empty() || self.bundle_id.is_some() {
            return Err(Error::execution(
                "finalization-probe",
                "prism",
                "invalid bundle attempt identity",
            ));
        }
        self.bundle_id = Some(instruction_id.to_owned());
        Ok(())
    }

    fn write(&mut self, value: T) -> Result<(), Error> {
        let encoded = value.encode()?;
        let len = u64::try_from(encoded.len())
            .map_err(|_| Error::incompatible("finalization probe record is too large"))?;
        self.payload.extend_from_slice(&len.to_le_bytes());
        self.payload.extend_from_slice(&encoded);
        self.count = self
            .count
            .checked_add(1)
            .ok_or_else(|| Error::incompatible("finalization probe row count overflow"))?;
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        let bundle_id = self.bundle_id.as_ref().ok_or_else(|| {
            Error::execution(
                "finalization-probe",
                "prism",
                "bundle attempt identity is missing",
            )
        })?;
        self.store.stage(bundle_id, self.count, &self.payload)?;
        self.staged = true;
        if self.fail_after_stage {
            return Err(Error::execution(
                "finalization-probe",
                "prism",
                "injected failure after staging",
            ));
        }
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        if self.staged {
            if let Some(bundle_id) = self.bundle_id.as_deref() {
                self.store.abandon(bundle_id)?;
                self.staged = false;
            }
        }
        Ok(())
    }

    fn requires_bundle_finalization(&self) -> bool {
        true
    }

    fn finalize_bundle(&mut self) -> Result<(), Error> {
        let bundle_id = self.bundle_id.as_deref().ok_or_else(|| {
            Error::execution("finalization-probe", "prism", "bundle was not committed")
        })?;
        self.store.accept(bundle_id)
    }

    fn recover_bundle_finalization(&mut self, instruction_id: &str) -> Result<(), Error> {
        self.store.accept(instruction_id)
    }
}

/// Shared local staging scope for the Prism driver and worker processes.
pub fn finalization_probe_store(
    directory: impl AsRef<Path>,
    key: &str,
) -> Result<LocalFinalResult, Error> {
    let directory = directory.as_ref();
    LocalFinalResult::new(
        directory,
        &directory.to_string_lossy(),
        "prism-run-fixture",
        key,
    )
}

/// Local probe for Prism's postcommit `FinalizeBundle` callback.
pub fn finalization_probe_sink<T: Element>(
    input: &PCollection<T>,
    key: &str,
    directory: impl AsRef<Path>,
) -> Result<Sink<T>, Error> {
    finalization_probe_sink_with_failure(input, key, directory, false)
}

fn finalization_probe_sink_with_failure<T: Element>(
    input: &PCollection<T>,
    key: &str,
    directory: impl AsRef<Path>,
    fail_after_stage: bool,
) -> Result<Sink<T>, Error> {
    let directory = directory.as_ref().to_path_buf();
    let digest = format!(
        "prism-finalization-probe:{}:{fail_after_stage}",
        directory.display()
    );
    let store = finalization_probe_store(&directory, key)?;
    input.sink_named_with_config_and_finalization(
        key,
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        digest,
        move || {
            Ok(Box::new(FinalizationProbe::<T> {
                store: store.clone(),
                count: 0,
                payload: Vec::new(),
                bundle_id: None,
                staged: false,
                fail_after_stage,
                marker: std::marker::PhantomData,
            }))
        },
    )
}

/// A bounded Prism job whose bundle stages output and then fails before it
/// can request finalization. The worker must abandon that attempt.
pub fn finalization_failure_fixture(
    pipeline: &Pipeline,
    directory: impl AsRef<Path>,
) -> Result<(), Error> {
    let input = pipeline.from_vec("failure-source", vec![7_i32])?;
    let staged = input.reshuffle_named("failure-boundary")?;
    finalization_probe_sink_with_failure(&staged, "failure-finalization", directory, true)?;
    Ok(())
}

pub fn large_group_rows() -> Vec<(String, String)> {
    let payload = "x".repeat(4091);
    (0..512)
        .map(|index| ("large".to_owned(), format!("{index:04}-{payload}")))
        .collect()
}

/// Summarize every value byte without depending on Beam's group iteration order.
pub fn large_group_summary(mut values: Vec<String>) -> (i32, usize, String) {
    values.sort_unstable();
    let mut digest = Sha256::new();
    let mut total_len = 0;
    for value in &values {
        let bytes = value.as_bytes();
        total_len += bytes.len();
        digest.update((bytes.len() as u64).to_le_bytes());
        digest.update(bytes);
    }
    (
        values.len() as i32,
        total_len,
        hex::encode(digest.finalize()),
    )
}

/// A bounded signal that records when its iterator reaches completion.
pub fn completion_signal(pipeline: &Pipeline, marker: PathBuf) -> Result<PCollection<i32>, Error> {
    completion_signal_named(pipeline, "wait-signal", marker)
}

/// A separately named bounded signal that marks its completed source read.
pub fn completion_signal_named(
    pipeline: &Pipeline,
    key: &str,
    marker: PathBuf,
) -> Result<PCollection<i32>, Error> {
    let digest = format!("wait-signal-v1:{}", marker.display());
    pipeline.bounded_source_with_semantic(
        key,
        "tests/prism-completion-signal",
        SourceProgress {
            stable_split_id: true,
            replay: Replay::AtLeastOnce,
            reports_event_time: false,
        },
        digest,
        Some(1),
        move || {
            let marker = marker.clone();
            let mut emitted = false;
            Ok(Box::new(std::iter::from_fn(move || {
                if !emitted {
                    emitted = true;
                    Some(Ok(1_i32))
                } else {
                    std::fs::write(&marker, b"complete")
                        .expect("write local Prism completion marker");
                    None
                }
            })))
        },
    )
}

#[cfg(test)]
#[path = "../../tests/unit/lib__finalization_probe_tests.rs"]
mod finalization_probe_tests;
