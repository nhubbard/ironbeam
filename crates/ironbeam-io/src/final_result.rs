//! Durable local fragments for a portable whole-job result.
//! After the runner reports DONE, the driver binds the completed bundle-attempt
//! manifest with `require_bundle_attempts` before publishing.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use ironbeam_core::Error;
use sha2::{Digest, Sha256};

const MAGIC: &[u8; 8] = b"IBFRAG01";
const HEADER_SIZE: usize = 8 + 32 + 32 + 8 + 8 + 32;
const HEADER_LEN: u64 = HEADER_SIZE as u64;

fn io(error: impl std::fmt::Display) -> Error {
    Error::io("final-result", "file", error.to_string())
}

fn invalid(message: impl Into<String>) -> Error {
    Error::incompatible(message)
}

fn hex(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut value, "{byte:02x}").expect("writing to String cannot fail");
    }
    value
}

fn scope_digest(job: &str, graph: &str, sink: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for component in [job, graph, sink] {
        hasher.update((component.len() as u64).to_le_bytes());
        hasher.update(component.as_bytes());
    }
    hasher.finalize().into()
}

fn sync_directory(path: &Path) -> Result<(), Error> {
    super::sync_directory(path, "final-result")
}

#[cfg(test)]
fn crash_after(stage: &str) {
    if std::env::var("IRONBEAM_FINAL_RESULT_EXIT_STAGE").as_deref() == Ok(stage) {
        std::process::exit(86);
    }
}

/// Local fragment journal and atomic final-result publisher for one sink.
#[derive(Debug, Clone)]
pub struct LocalFinalResult {
    staging: PathBuf,
    scope: [u8; 32],
    expected_attempts: Option<BTreeSet<String>>,
}

/// Verified staged fragment accepted for a bundle attempt.
#[derive(Debug, Clone)]
pub struct AcceptedFragment {
    path: PathBuf,
    bundle_digest: [u8; 32],
    pub rows: u64,
    pub payload_len: u64,
}

impl AcceptedFragment {
    /// Stable SHA-256 identity of the bundle attempt that produced this fragment.
    #[must_use]
    pub fn bundle_digest_hex(&self) -> String {
        hex(&self.bundle_digest)
    }

    /// Open only this fragment's payload bytes after metadata and digest checks.
    pub fn open_payload(&self) -> Result<std::io::Take<File>, Error> {
        let mut file = File::open(&self.path).map_err(io)?;
        file.seek(SeekFrom::Start(HEADER_LEN)).map_err(io)?;
        Ok(file.take(self.payload_len))
    }

    /// Stream this accepted fragment's bytes to `output` and verify its length.
    pub fn copy_payload(&self, output: &mut impl Write) -> Result<(), Error> {
        let copied = std::io::copy(&mut self.open_payload()?, output).map_err(io)?;
        if copied != self.payload_len {
            return Err(invalid("accepted fragment payload was truncated"));
        }
        Ok(())
    }
}

/// Row, fragment, and byte counts in a published local result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishedResult {
    pub rows: u64,
    pub fragments: usize,
    pub bytes: u64,
}

impl LocalFinalResult {
    /// Bind a local staging directory to one job, graph, and sink identity.
    ///
    /// All three identities must be nonempty.
    pub fn new(
        staging: impl AsRef<Path>,
        job: &str,
        graph: &str,
        sink: &str,
    ) -> Result<Self, Error> {
        if job.is_empty() || graph.is_empty() || sink.is_empty() {
            return Err(invalid(
                "final-result job, graph, and sink identities are required",
            ));
        }
        Ok(Self {
            staging: staging.as_ref().to_path_buf(),
            scope: scope_digest(job, graph, sink),
            expected_attempts: None,
        })
    }

    /// Bind publication to the runner's completed bundle attempt IDs.
    /// Every accepted fragment must match exactly one ID in this manifest.
    ///
    /// # Errors
    ///
    /// Rejects empty or duplicate attempt IDs before any output is published.
    pub fn require_bundle_attempts<I, S>(&self, bundle_ids: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut expected = BTreeSet::new();
        for id in bundle_ids {
            let id = id.as_ref();
            if id.is_empty() || !expected.insert(hex(&Sha256::digest(id.as_bytes()))) {
                return Err(invalid(
                    "final-result manifest has an empty or duplicate attempt",
                ));
            }
        }
        let mut bound = self.clone();
        bound.expected_attempts = Some(expected);
        Ok(bound)
    }

    fn path(&self, bundle_id: &str, extension: &str) -> Result<PathBuf, Error> {
        if bundle_id.is_empty() {
            return Err(invalid("final-result bundle attempt identity is required"));
        }
        let digest = Sha256::digest(bundle_id.as_bytes());
        Ok(self
            .staging
            .join(format!("part-{}.{}", hex(&digest), extension)))
    }

    /// Stage one immutable fragment. A conflicting retry cannot overwrite it.
    pub fn stage(&self, bundle_id: &str, rows: u64, payload: &[u8]) -> Result<(), Error> {
        self.stage_reader(bundle_id, rows, std::io::Cursor::new(payload))
    }

    /// Stream an immutable fragment without holding its payload in memory.
    /// A read failure does not create a named pending fragment.
    pub fn stage_reader(
        &self,
        bundle_id: &str,
        rows: u64,
        mut payload: impl Read,
    ) -> Result<(), Error> {
        let pending = self.path(bundle_id, "pending")?;
        let accepted = self.path(bundle_id, "accepted")?;
        super::create_dir_all_durable(&self.staging, "final-result")?;
        if accepted.exists() {
            return Err(invalid("bundle fragment is already accepted"));
        }
        let mut temp = tempfile::NamedTempFile::new_in(&self.staging).map_err(io)?;
        temp.write_all(MAGIC).map_err(io)?;
        temp.write_all(&self.scope).map_err(io)?;
        temp.write_all(&Sha256::digest(bundle_id.as_bytes()))
            .map_err(io)?;
        temp.write_all(&rows.to_le_bytes()).map_err(io)?;
        temp.write_all(&[0_u8; 8 + 32]).map_err(io)?;
        let mut length = 0_u64;
        let mut digest = Sha256::new();
        let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
        loop {
            let size = payload.read(&mut buffer).map_err(io)?;
            if size == 0 {
                break;
            }
            length = length
                .checked_add(
                    u64::try_from(size).map_err(|_| invalid("fragment payload length overflow"))?,
                )
                .ok_or_else(|| invalid("fragment payload length overflow"))?;
            digest.update(&buffer[..size]);
            temp.write_all(&buffer[..size]).map_err(io)?;
        }
        temp.as_file_mut().seek(SeekFrom::Start(80)).map_err(io)?;
        temp.write_all(&length.to_le_bytes()).map_err(io)?;
        temp.write_all(&digest.finalize()).map_err(io)?;
        temp.as_file().sync_all().map_err(io)?;
        match temp.persist_noclobber(&pending) {
            Ok(_) => {}
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                self.read_fragment(&pending, Some(bundle_id))?;
                if !files_equal(error.file.path(), &pending)? {
                    return Err(invalid("pending fragment conflicts with retry"));
                }
            }
            Err(error) => return Err(io(error)),
        }
        #[cfg(test)]
        crash_after("fragment_staged");
        sync_directory(&self.staging)
    }

    /// Accept one committed attempt after the runner's bundle callback.
    /// A retry completes an interrupted hard-link-and-unlink only when both
    /// names still contain the same validated fragment.
    pub fn accept(&self, bundle_id: &str) -> Result<(), Error> {
        let pending = self.path(bundle_id, "pending")?;
        let accepted = self.path(bundle_id, "accepted")?;
        if accepted.exists() {
            self.read_fragment(&accepted, Some(bundle_id))?;
            if pending.exists() {
                self.read_fragment(&pending, Some(bundle_id))?;
                if !files_equal(&accepted, &pending)? {
                    return Err(invalid("accepted fragment conflicts with pending attempt"));
                }
                fs::remove_file(&pending).map_err(io)?;
            }
            return sync_directory(&self.staging);
        }
        self.read_fragment(&pending, Some(bundle_id))?;
        fs::hard_link(&pending, &accepted).map_err(io)?;
        #[cfg(test)]
        crash_after("fragment_linked");
        fs::remove_file(&pending).map_err(io)?;
        sync_directory(&self.staging)
    }

    /// Remove only this attempt's pending fragment. Accepted data stays durable.
    pub fn abandon(&self, bundle_id: &str) -> Result<(), Error> {
        let pending = self.path(bundle_id, "pending")?;
        match fs::remove_file(pending) {
            Ok(()) => sync_directory(&self.staging),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if self.staging.exists() {
                    sync_directory(&self.staging)
                } else {
                    Ok(())
                }
            }
            Err(error) => Err(io(error)),
        }
    }

    fn read_fragment(
        &self,
        path: &Path,
        bundle_id: Option<&str>,
    ) -> Result<AcceptedFragment, Error> {
        let mut file = File::open(path).map_err(io)?;
        let mut header = [0_u8; HEADER_SIZE];
        file.read_exact(&mut header).map_err(io)?;
        if &header[..8] != MAGIC || header[8..40] != self.scope {
            return Err(invalid(
                "fragment scope or format does not match this result",
            ));
        }
        let bundle_digest: [u8; 32] = header[40..72].try_into().expect("fixed header width");
        let extension = path
            .extension()
            .and_then(|extension| extension.to_str())
            .ok_or_else(|| invalid("fragment has no lifecycle extension"))?;
        let expected_name = format!("part-{}.{}", hex(&bundle_digest), extension);
        if path.file_name().and_then(|name| name.to_str()) != Some(expected_name.as_str()) {
            return Err(invalid("fragment filename does not match bundle identity"));
        }
        if bundle_id.is_some_and(|id| Sha256::digest(id.as_bytes()).as_slice() != bundle_digest) {
            return Err(invalid("fragment bundle identity does not match"));
        }
        let rows = u64::from_le_bytes(header[72..80].try_into().expect("fixed header width"));
        let payload_len =
            u64::from_le_bytes(header[80..88].try_into().expect("fixed header width"));
        let actual_len = file.metadata().map_err(io)?.len();
        if actual_len
            != HEADER_LEN
                .checked_add(payload_len)
                .ok_or_else(|| invalid("fragment length overflow"))?
        {
            return Err(invalid("fragment payload length does not match file"));
        }
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
        loop {
            let size = file.read(&mut buffer).map_err(io)?;
            if size == 0 {
                break;
            }
            hasher.update(&buffer[..size]);
        }
        if hasher.finalize().as_slice() != &header[88..120] {
            return Err(invalid("fragment payload digest does not match"));
        }
        Ok(AcceptedFragment {
            path: path.to_path_buf(),
            bundle_digest,
            rows,
            payload_len,
        })
    }

    /// Validate and enumerate accepted attempts, sorted by stable attempt ID.
    pub fn accepted_fragments(&self) -> Result<Vec<AcceptedFragment>, Error> {
        self.scan_fragments(false)
    }

    fn scan_fragments(&self, require_complete: bool) -> Result<Vec<AcceptedFragment>, Error> {
        let entries = match fs::read_dir(&self.staging) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(io(error)),
        };
        let mut paths = entries
            .map(|entry| entry.map(|entry| entry.path()).map_err(io))
            .collect::<Result<Vec<_>, _>>()?;
        if require_complete
            && paths.iter().any(|path| {
                path.extension()
                    .is_none_or(|extension| extension != "accepted")
            })
        {
            return Err(invalid(
                "unaccepted or incomplete fragment remains in staging",
            ));
        }
        paths.retain(|path| {
            path.extension()
                .is_some_and(|extension| extension == "accepted")
        });
        paths.sort();
        let fragments: Vec<_> = paths
            .into_iter()
            .map(|path| self.read_fragment(&path, None))
            .collect::<Result<_, _>>()?;
        if require_complete && let Some(expected) = &self.expected_attempts {
            let actual = fragments
                .iter()
                .map(AcceptedFragment::bundle_digest_hex)
                .collect::<BTreeSet<_>>();
            if &actual != expected {
                return Err(invalid(
                    "accepted fragment attempts differ from runner manifest",
                ));
            }
        }
        Ok(fragments)
    }

    /// Assemble accepted bytes after job success and exact committed-row accounting.
    /// Use only when the caller has independently validated a format whose
    /// bytes concatenate cleanly. Prefer `publish_validated_concatenated` for
    /// framed records. Schema-bearing formats need their own merger over
    /// `accepted_fragments`.
    pub fn publish_concatenated(
        &self,
        expected_rows: u64,
        target: impl AsRef<Path>,
    ) -> Result<PublishedResult, Error> {
        self.publish_validated_concatenated(expected_rows, target, |_| Ok(()))
    }

    /// Validate the complete temporary result before making the target visible.
    /// The validator must check the payload format and its actual record count.
    pub fn publish_validated_concatenated(
        &self,
        expected_rows: u64,
        target: impl AsRef<Path>,
        validate: impl FnOnce(&Path) -> Result<(), Error>,
    ) -> Result<PublishedResult, Error> {
        self.publish_merged(expected_rows, target, |fragments, path, output| {
            for fragment in fragments {
                fragment.copy_payload(output)?;
            }
            output.sync_all().map_err(io)?;
            validate(path)?;
            Ok(expected_rows)
        })
    }

    /// Merge accepted fragments into one temporary file, then atomically
    /// publish only when the merger confirms the decoded row count. The store
    /// must first be bound to the completed runner bundle-attempt manifest.
    pub fn publish_merged(
        &self,
        expected_rows: u64,
        target: impl AsRef<Path>,
        merge: impl FnOnce(&[AcceptedFragment], &Path, &mut File) -> Result<u64, Error>,
    ) -> Result<PublishedResult, Error> {
        if self.expected_attempts.is_none() {
            return Err(invalid(
                "final-result publication requires a completed bundle-attempt manifest",
            ));
        }
        let fragments = self.scan_fragments(true)?;
        let rows = fragments.iter().try_fold(0_u64, |sum, part| {
            sum.checked_add(part.rows)
                .ok_or_else(|| invalid("fragment row count overflow"))
        })?;
        if rows != expected_rows {
            return Err(invalid(format!(
                "accepted {rows} rows but runner committed {expected_rows}"
            )));
        }
        let target = target.as_ref();
        let parent = target
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        super::create_dir_all_durable(parent, "final-result")?;
        let mut temp = tempfile::NamedTempFile::new_in(parent).map_err(io)?;
        let temp_path = temp.path().to_path_buf();
        let merged_rows = merge(&fragments, &temp_path, temp.as_file_mut())?;
        if merged_rows != expected_rows {
            return Err(invalid("merged result row count does not match runner"));
        }
        temp.as_file().sync_all().map_err(io)?;
        let bytes = temp.as_file().metadata().map_err(io)?.len();
        match temp.persist_noclobber(target) {
            Ok(_) => {
                #[cfg(test)]
                crash_after("result_persisted");
                sync_directory(parent)?;
            }
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                if !files_equal(error.file.path(), target)? {
                    return Err(invalid("published result conflicts with this job output"));
                }
                sync_directory(parent)?;
            }
            Err(error) => return Err(io(error)),
        }
        Ok(PublishedResult {
            rows,
            fragments: fragments.len(),
            bytes,
        })
    }
}

fn files_equal(left: &Path, right: &Path) -> Result<bool, Error> {
    let mut left = File::open(left).map_err(io)?;
    let mut right = File::open(right).map_err(io)?;
    if left.metadata().map_err(io)?.len() != right.metadata().map_err(io)?.len() {
        return Ok(false);
    }
    let mut a = vec![0_u8; 64 * 1024].into_boxed_slice();
    let mut b = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        let size = left.read(&mut a).map_err(io)?;
        if size == 0 {
            return Ok(true);
        }
        right.read_exact(&mut b[..size]).map_err(io)?;
        if a[..size] != b[..size] {
            return Ok(false);
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/final_result__tests.rs"]
mod tests;
