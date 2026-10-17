struct JsonlAppendSink<T> {
    path: PathBuf,
    key: String,
    values: Vec<T>,
}

impl<T: Element + Serialize> TypedSink<T> for JsonlAppendSink<T> {
    fn write(&mut self, value: T) -> Result<(), Error> {
        self.values.push(value);
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        use std::io::Write;
        let parent = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent).map_err(|error| ironbeam_io::io(&self.key, error))?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| ironbeam_io::io(&self.key, error))?;
        let mut file = std::io::BufWriter::new(file);
        for value in self.values.drain(..) {
            serde_json::to_writer(&mut file, &value)
                .map_err(|error| ironbeam_io::io(&self.key, error))?;
            file.write_all(b"\n")
                .map_err(|error| ironbeam_io::io(&self.key, error))?;
        }
        file.flush()
            .map_err(|error| ironbeam_io::io(&self.key, error))
    }

    fn abort(&mut self) -> Result<(), Error> {
        self.values.clear();
        Ok(())
    }
}

/// Append JSON Lines at each bundle commit.
///
/// Replays may duplicate records; use `jsonl_sink` when whole-job replacement
/// and final-result semantics are needed.
/// This local filesystem sink requires a path shared by all workers.
pub fn jsonl_append_sink<T: Element + Serialize>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<T>, Error> {
    let path = path.as_ref().to_path_buf();
    if path
        .extension()
        .and_then(|part| part.to_str())
        .is_some_and(|part| {
            matches!(
                part.to_ascii_lowercase().as_str(),
                "gz" | "gzip" | "zst" | "zstd" | "bz2" | "xz"
            )
        })
    {
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
        format!("jsonl-append:{}", path.display()),
        move || {
            Ok(Box::new(JsonlAppendSink::<T> {
                path: path.clone(),
                key: name.clone(),
                values: Vec::new(),
            }))
        },
    )
}

#[cfg(feature = "parallel-io")]
/// Serialize contiguous shards concurrently and publish their ordered JSONL output at job commit.
pub fn jsonl_parallel_sink<T: Element + Serialize + Sync>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
    shards: usize,
) -> Result<Sink<T>, Error> {
    if shards == 0 {
        return Err(Error::graph(
            key,
            "parallel writer needs at least one shard",
        ));
    }
    ironbeam_io::sink(input, key, path, move |path, values| {
        ironbeam_io::parallel_file_writer(path, values, shards, |part, chunk, _| {
            write_jsonl(part, chunk)
        })
    })
}

/// Expand a JSON Lines file glob into stable bounded source nodes.
pub fn jsonl_glob_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    pattern: &str,
) -> Result<PCollection<T>, Error> {
    ironbeam_io::file_glob_source(pipeline, key, pattern, |pipeline, key, path| {
        jsonl_source(pipeline, key, path)
    })
}
