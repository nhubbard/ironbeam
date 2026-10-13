//! Durable bundle-to-sink mapping for portable finalization callbacks.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use ironbeam_cloud_objects::{ErrorKind as ObjectErrorKind, ObjectLocation, ObjectStores};
use ironbeam_core::ExecutionIdentity;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalEntry {
    pub sinks: Vec<String>,
    pub completed: bool,
}

/// Validated bundle attempts for every declared final-result sink in a job.
/// Construct this only after the runner reports successful job completion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedFinalizationManifest {
    attempts: BTreeMap<String, Vec<String>>,
}

impl CompletedFinalizationManifest {
    pub fn attempts_for_sink(&self, sink: &str) -> Result<&[String]> {
        self.attempts
            .get(sink)
            .map(Vec::as_slice)
            .with_context(|| format!("sink {sink:?} is absent from the finalization manifest"))
    }

    pub fn sinks(&self) -> impl Iterator<Item = &str> {
        self.attempts.keys().map(String::as_str)
    }
}

/// Storage for the exact sink set committed by one ProcessBundle response.
/// A callback must never infer this set from fragments found on a worker.
/// Implementations must durably bind each record to the execution identity,
/// reject conflicting replays, and make completion idempotent across workers.
pub trait FinalizationJournal: Send + Sync {
    fn record(&self, bundle_id: &str, sinks: &[String]) -> Result<()>;
    fn load(&self, bundle_id: &str) -> Result<Option<JournalEntry>>;
    fn complete(&self, bundle_id: &str) -> Result<()>;
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
struct Payload {
    version: u32,
    identity: ExecutionIdentity,
    bundle_id: String,
    sinks: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct Record {
    payload: Payload,
    checksum: String,
}

fn digest(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        write!(result, "{byte:02x}").expect("writing to String cannot fail");
    }
    result
}

fn encode_record(
    identity: &ExecutionIdentity,
    bundle_id: &str,
    sinks: &[String],
) -> Result<Vec<u8>> {
    if bundle_id.is_empty()
        || sinks.is_empty()
        || sinks.iter().any(String::is_empty)
        || sinks.windows(2).any(|pair| pair[0] >= pair[1])
    {
        bail!("finalization journal requires an ID and sorted, distinct sink keys");
    }
    let payload = Payload {
        version: 1,
        identity: identity.clone(),
        bundle_id: bundle_id.into(),
        sinks: sinks.to_vec(),
    };
    let checksum = digest(&serde_json::to_vec(&payload)?);
    Ok(serde_json::to_vec(&Record { payload, checksum })?)
}

fn decode_record(identity: &ExecutionIdentity, bundle_id: &str, bytes: &[u8]) -> Result<Record> {
    let record: Record = serde_json::from_slice(bytes).context("decode finalization journal")?;
    let encoded = serde_json::to_vec(&record.payload)?;
    if record.checksum != digest(&encoded)
        || record.payload.version != 1
        || record.payload.identity != *identity
        || record.payload.bundle_id != bundle_id
        || record.payload.sinks.is_empty()
        || record.payload.sinks.iter().any(String::is_empty)
        || record
            .payload
            .sinks
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
    {
        bail!("finalization journal identity, sink set, or checksum differs");
    }
    Ok(record)
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
fn crash_after(stage: &str) {
    if std::env::var("IRONBEAM_FINALIZATION_EXIT_STAGE").as_deref() == Ok(stage) {
        std::process::exit(86);
    }
}

fn create_dir_all_durable(path: &Path) -> Result<()> {
    let mut missing = Vec::new();
    let mut current = path;
    while !current.exists() {
        missing.push(current.to_path_buf());
        current = current
            .parent()
            .context("journal directory has no parent")?;
    }
    fs::create_dir_all(path)?;
    for directory in missing.into_iter().rev() {
        sync_directory(
            directory
                .parent()
                .context("journal directory has no parent")?,
        )?;
    }
    Ok(())
}

/// Local shared-filesystem journal used by the Prism worker fixture.
/// The same directory and execution identity must be supplied on restart.
pub struct FileFinalizationJournal {
    root: PathBuf,
    identity: ExecutionIdentity,
}

impl FileFinalizationJournal {
    pub fn new(root: impl AsRef<Path>, identity: ExecutionIdentity) -> Result<Self> {
        let root = root.as_ref();
        if !root.is_absolute() {
            bail!("finalization journal directory must be absolute");
        }
        Ok(Self {
            root: root.to_path_buf(),
            identity,
        })
    }

    fn paths(&self, bundle_id: &str) -> Result<(PathBuf, PathBuf)> {
        if bundle_id.is_empty() {
            bail!("finalization journal needs a bundle instruction ID");
        }
        let stem = format!("bundle-{}", digest(bundle_id.as_bytes()));
        Ok((
            self.root.join(format!("{stem}.json")),
            self.root.join(format!("{stem}.complete")),
        ))
    }

    fn read_record(&self, bundle_id: &str, path: &Path) -> Result<Record> {
        let bytes = fs::read(path)
            .with_context(|| format!("read finalization journal {}", path.display()))?;
        decode_record(&self.identity, bundle_id, &bytes)
    }

    /// Validate every record and completion marker after a job reaches a
    /// terminal state. Unknown files, orphan markers, and mixed identities
    /// make the snapshot unusable for publication decisions.
    pub fn snapshot(&self) -> Result<BTreeMap<String, JournalEntry>> {
        let entries = match fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(BTreeMap::new());
            }
            Err(error) => return Err(error.into()),
        };
        let mut records = BTreeMap::new();
        let mut markers = Vec::new();
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                bail!("finalization journal contains a non-file entry");
            }
            let path = entry.path();
            match path.extension().and_then(|extension| extension.to_str()) {
                Some("json") => {
                    let bytes = fs::read(&path)?;
                    let record: Record = serde_json::from_slice(&bytes)
                        .context("decode finalization journal snapshot")?;
                    let bundle_id = record.payload.bundle_id.clone();
                    decode_record(&self.identity, &bundle_id, &bytes)?;
                    if self.paths(&bundle_id)?.0 != path {
                        bail!("finalization journal filename differs from bundle identity");
                    }
                    let loaded = self
                        .load(&bundle_id)?
                        .context("finalization journal record disappeared")?;
                    if records.insert(bundle_id, loaded).is_some() {
                        bail!("duplicate finalization journal bundle identity");
                    }
                }
                Some("complete") => markers.push(path),
                _ => bail!("finalization journal contains an incomplete entry"),
            }
        }
        for marker in markers {
            let record = marker.with_extension("json");
            if !record.is_file() {
                bail!("completed finalization has no journal record");
            }
        }
        Ok(records)
    }

    /// Return the completed bundle IDs expected to have fragments for one sink.
    /// Publication is refused while any bundle in this job lacks finalization.
    pub fn completed_attempts_for_sink(&self, sink: &str) -> Result<Vec<String>> {
        if sink.is_empty() {
            bail!("finalization sink key is required");
        }
        let entries = self.snapshot()?;
        if entries.values().any(|entry| !entry.completed) {
            bail!("finalization journal has incomplete bundle callbacks");
        }
        Ok(entries
            .into_iter()
            .filter_map(|(bundle, entry)| {
                entry.sinks.iter().any(|key| key == sink).then_some(bundle)
            })
            .collect())
    }

    /// Validate the entire journal against the job's declared final-result
    /// sinks. Every callback must be complete, and no unknown sink can hide
    /// behind a per-sink lookup. A declared sink may have zero bundle attempts.
    /// The caller must first verify that the runner reports `DONE`.
    pub fn completed_manifest<I, S>(&self, sinks: I) -> Result<CompletedFinalizationManifest>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut declared = BTreeSet::new();
        for sink in sinks {
            let sink = sink.as_ref();
            if sink.is_empty() || !declared.insert(sink.to_owned()) {
                bail!("finalization manifest requires distinct, nonempty sink keys");
            }
        }
        if declared.is_empty() {
            bail!("finalization manifest needs at least one declared sink");
        }
        let mut attempts = declared
            .into_iter()
            .map(|sink| (sink, Vec::new()))
            .collect::<BTreeMap<_, _>>();
        for (bundle_id, entry) in self.snapshot()? {
            if !entry.completed {
                bail!("finalization journal has incomplete bundle callbacks");
            }
            for sink in entry.sinks {
                attempts
                    .get_mut(&sink)
                    .with_context(|| format!("journal contains undeclared sink {sink:?}"))?
                    .push(bundle_id.clone());
            }
        }
        Ok(CompletedFinalizationManifest { attempts })
    }
}

impl FinalizationJournal for FileFinalizationJournal {
    fn record(&self, bundle_id: &str, sinks: &[String]) -> Result<()> {
        let (path, _) = self.paths(bundle_id)?;
        let bytes = encode_record(&self.identity, bundle_id, sinks)?;
        create_dir_all_durable(&self.root)?;
        let mut temp = tempfile::NamedTempFile::new_in(&self.root)?;
        temp.write_all(&bytes)?;
        temp.as_file().sync_all()?;
        match temp.persist_noclobber(&path) {
            Ok(_) => {
                #[cfg(test)]
                crash_after("journal_record_named");
                sync_directory(&self.root)?;
            }
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                self.read_record(bundle_id, &path)?;
                if fs::read(&path)? != bytes {
                    bail!("finalization journal conflicts with an existing bundle");
                }
                sync_directory(&self.root)?;
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    fn load(&self, bundle_id: &str) -> Result<Option<JournalEntry>> {
        let (path, completed_path) = self.paths(bundle_id)?;
        if !path.exists() {
            if completed_path.exists() {
                bail!("completed finalization has no journal record");
            }
            return Ok(None);
        }
        let record = self.read_record(bundle_id, &path)?;
        let completed = completed_path.exists();
        if completed && fs::read(&path)? != fs::read(&completed_path)? {
            bail!("completed finalization marker differs from journal record");
        }
        Ok(Some(JournalEntry {
            sinks: record.payload.sinks,
            completed,
        }))
    }

    fn complete(&self, bundle_id: &str) -> Result<()> {
        let (path, completed_path) = self.paths(bundle_id)?;
        self.read_record(bundle_id, &path)?;
        match fs::hard_link(&path, &completed_path) {
            Ok(()) => {
                #[cfg(test)]
                crash_after("journal_complete_linked");
                sync_directory(&self.root)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                self.load(bundle_id)?
                    .context("finalization journal disappeared")?;
                sync_directory(&self.root)
            }
            Err(error) => Err(error.into()),
        }
    }
}

/// Provider-neutral object journal for workers that do not share a filesystem.
/// The registered object provider must implement atomic conditional creation
/// and read-after-write visibility for new objects.
pub struct ObjectFinalizationJournal {
    stores: Arc<ObjectStores>,
    base_uri: String,
    identity: ExecutionIdentity,
}

impl ObjectFinalizationJournal {
    pub fn new(
        stores: Arc<ObjectStores>,
        base_uri: impl Into<String>,
        identity: ExecutionIdentity,
    ) -> Result<Self> {
        let base_uri = base_uri.into();
        if !base_uri.ends_with('/') {
            bail!("finalization object journal URI must end with /");
        }
        ObjectLocation::parse(&base_uri)?;
        Ok(Self {
            stores,
            base_uri,
            identity,
        })
    }

    fn uris(&self, bundle_id: &str) -> Result<(String, String)> {
        if bundle_id.is_empty() {
            bail!("finalization journal needs a bundle instruction ID");
        }
        let stem = format!("{}bundle-{}", self.base_uri, digest(bundle_id.as_bytes()));
        Ok((format!("{stem}.json"), format!("{stem}.complete")))
    }
}

impl FinalizationJournal for ObjectFinalizationJournal {
    fn record(&self, bundle_id: &str, sinks: &[String]) -> Result<()> {
        let (uri, _) = self.uris(bundle_id)?;
        let bytes = encode_record(&self.identity, bundle_id, sinks)?;
        match self.stores.create(&uri, &bytes) {
            Ok(()) => Ok(()),
            Err(error) if error.kind == ObjectErrorKind::AlreadyExists => {
                let existing = self.stores.read(&uri)?;
                decode_record(&self.identity, bundle_id, &existing)?;
                if existing != bytes {
                    bail!("finalization object journal conflicts with an existing bundle");
                }
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    fn load(&self, bundle_id: &str) -> Result<Option<JournalEntry>> {
        let (uri, completed_uri) = self.uris(bundle_id)?;
        if !self.stores.exists(&uri)? {
            if self.stores.exists(&completed_uri)? {
                bail!("completed object finalization has no journal record");
            }
            return Ok(None);
        }
        let bytes = self.stores.read(&uri)?;
        let record = decode_record(&self.identity, bundle_id, &bytes)?;
        let completed = self.stores.exists(&completed_uri)?;
        if completed && self.stores.read(&completed_uri)? != bytes {
            bail!("completed object finalization marker differs from journal record");
        }
        Ok(Some(JournalEntry {
            sinks: record.payload.sinks,
            completed,
        }))
    }

    fn complete(&self, bundle_id: &str) -> Result<()> {
        let (uri, completed_uri) = self.uris(bundle_id)?;
        let entry = self
            .load(bundle_id)?
            .context("finalization object journal is missing")?;
        if entry.completed {
            return Ok(());
        }
        let bytes = self.stores.read(&uri)?;
        match self.stores.create(&completed_uri, &bytes) {
            Ok(()) => Ok(()),
            Err(error) if error.kind == ObjectErrorKind::AlreadyExists => {
                self.load(bundle_id)?
                    .context("finalization object journal disappeared")?;
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/finalization_journal__tests.rs"]
mod tests;
