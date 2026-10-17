/// Append complete `TFRecord` frames at each bundle commit.
///
/// Replays may duplicate records. Use `tfrecord_sink` for whole-job replacement.
/// This local filesystem sink requires a path shared by all workers.
pub fn tfrecord_append_sink(
    input: &PCollection<Vec<u8>>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<Vec<u8>>, Error> {
    let path = path.as_ref().to_path_buf();
    if has_compression_extension(&path) {
        return Err(Error::graph(
            key,
            "append sink does not support compressed output",
        ));
    }
    let name = key.to_owned();
    input.sink_named_with_config(
        key,
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("tfrecord-append:{}", path.display()),
        move || {
            Ok(Box::new(TfrecordAppendSink {
                path: path.clone(),
                key: name.clone(),
                values: Vec::new(),
            }))
        },
    )
}

fn has_compression_extension(path: &Path) -> bool {
    path.extension()
        .and_then(|part| part.to_str())
        .is_some_and(|part| {
            matches!(
                part.to_ascii_lowercase().as_str(),
                "gz" | "gzip" | "zst" | "zstd" | "bz2" | "xz"
            )
        })
}

struct TfrecordFragmentSink {
    store: LocalFinalResult,
    bundle_id: Option<String>,
    spool: Option<tempfile::NamedTempFile>,
    rows: u64,
    staged: bool,
}

impl TypedSink<Vec<u8>> for TfrecordFragmentSink {
    fn begin_bundle(&mut self, instruction_id: &str) -> Result<(), Error> {
        if instruction_id.is_empty() || self.bundle_id.is_some() {
            return Err(Error::incompatible(
                "TFRecord fragment needs one bundle attempt identity",
            ));
        }
        self.bundle_id = Some(instruction_id.to_owned());
        Ok(())
    }

    fn write(&mut self, value: Vec<u8>) -> Result<(), Error> {
        if self.bundle_id.is_none() {
            return Err(Error::incompatible(
                "TFRecord fragment has no bundle attempt identity",
            ));
        }
        let spool = match self.spool.as_mut() {
            Some(spool) => spool,
            None => self.spool.insert(
                tempfile::NamedTempFile::new().map_err(|error| io("tfrecord-fragment", error))?,
            ),
        };
        write_frames(spool, std::slice::from_ref(&value))?;
        self.rows = self
            .rows
            .checked_add(1)
            .ok_or_else(|| Error::incompatible("TFRecord fragment row count overflow"))?;
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        let bundle_id = self.bundle_id.as_deref().ok_or_else(|| {
            Error::incompatible("TFRecord fragment has no bundle attempt identity")
        })?;
        let spool = match self.spool.as_mut() {
            Some(spool) => spool,
            None => self.spool.insert(
                tempfile::NamedTempFile::new().map_err(|error| io("tfrecord-fragment", error))?,
            ),
        };
        spool
            .as_file_mut()
            .seek(SeekFrom::Start(0))
            .map_err(|error| io("tfrecord-fragment", error))?;
        self.store
            .stage_reader(bundle_id, self.rows, spool.as_file_mut())?;
        self.staged = true;
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        if self.staged {
            if let Some(bundle_id) = self.bundle_id.as_deref() {
                self.store.abandon(bundle_id)?;
            }
            self.staged = false;
        }
        self.spool = None;
        Ok(())
    }

    fn requires_bundle_finalization(&self) -> bool {
        true
    }

    fn finalize_bundle(&mut self) -> Result<(), Error> {
        let bundle_id = self.bundle_id.as_deref().ok_or_else(|| {
            Error::incompatible("TFRecord fragment has no bundle attempt identity")
        })?;
        if !self.staged {
            return Err(Error::incompatible("TFRecord fragment has not committed"));
        }
        self.store.accept(bundle_id)
    }

    fn recover_bundle_finalization(&mut self, instruction_id: &str) -> Result<(), Error> {
        self.store.accept(instruction_id)
    }
}

/// Reopen the same local staging scope on a worker or job coordinator.
/// The staging path must be absolute and visible to both processes.
pub fn tfrecord_fragment_store(
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
    key: &str,
) -> Result<LocalFinalResult, Error> {
    let staging = staging.as_ref();
    if !staging.is_absolute() {
        return Err(Error::graph(
            key,
            "TFRecord fragment staging path must be absolute",
        ));
    }
    LocalFinalResult::new(staging, job, graph, key)
}

/// Register a bundle-finalized `TFRecord` fragment sink for portable jobs.
///
/// The caller must publish after the runner reports DONE. This remains an
/// append-mode graph sink until the whole-job result contract is complete.
pub fn tfrecord_fragment_sink(
    input: &PCollection<Vec<u8>>,
    key: &str,
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
) -> Result<Sink<Vec<u8>>, Error> {
    let staging = staging.as_ref();
    let store = tfrecord_fragment_store(staging, job, graph, key)?;
    let config = serde_json::to_string(&("tfrecord-fragments-v1", staging, job, graph, key))
        .map_err(|error| Error::incompatible(error.to_string()))?;
    input.sink_named_with_config_and_finalization(
        key,
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        config,
        move || {
            Ok(Box::new(TfrecordFragmentSink {
                store: store.clone(),
                bundle_id: None,
                spool: None,
                rows: 0,
                staged: false,
            }))
        },
    )
}

/// Publish accepted `TFRecord` fragments after a successful portable job.
/// Every frame and CRC is checked before the final path becomes visible.
pub fn publish_tfrecord_fragments(
    store: &LocalFinalResult,
    expected_rows: u64,
    target: impl AsRef<Path>,
) -> Result<PublishedResult, Error> {
    let target = target.as_ref();
    if has_compression_extension(target) {
        return Err(Error::graph(
            "tfrecord-fragment",
            "compressed final result needs a format merger",
        ));
    }
    store.publish_validated_concatenated(expected_rows, target, |path| {
        let mut count = 0_u64;
        for row in read_tfrecord_lazy(path.to_path_buf())? {
            row?;
            count = count
                .checked_add(1)
                .ok_or_else(|| Error::incompatible("TFRecord result row count overflow"))?;
        }
        if count != expected_rows {
            return Err(Error::incompatible(
                "TFRecord result row count does not match runner",
            ));
        }
        Ok(())
    })
}
