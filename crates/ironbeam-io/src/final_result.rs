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
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(io)
}

#[derive(Debug, Clone)]
pub struct LocalFinalResult {
    staging: PathBuf,
    scope: [u8; 32],
    expected_attempts: Option<BTreeSet<String>>,
}

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

    pub fn copy_payload(&self, output: &mut impl Write) -> Result<(), Error> {
        let copied = std::io::copy(&mut self.open_payload()?, output).map_err(io)?;
        if copied != self.payload_len {
            return Err(invalid("accepted fragment payload was truncated"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishedResult {
    pub rows: u64,
    pub fragments: usize,
    pub bytes: u64,
}

impl LocalFinalResult {
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
        fs::create_dir_all(&self.staging).map_err(io)?;
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
        let parent = target.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(io)?;
        let mut temp = tempfile::NamedTempFile::new_in(parent).map_err(io)?;
        let temp_path = temp.path().to_path_buf();
        let merged_rows = merge(&fragments, &temp_path, temp.as_file_mut())?;
        if merged_rows != expected_rows {
            return Err(invalid("merged result row count does not match runner"));
        }
        temp.as_file().sync_all().map_err(io)?;
        let bytes = temp.as_file().metadata().map_err(io)?.len();
        match temp.persist_noclobber(target) {
            Ok(_) => sync_directory(parent)?,
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
mod tests {
    use super::*;

    #[test]
    fn accepted_fragments_publish_once_after_exact_row_accounting() {
        let temp = tempfile::tempdir().unwrap();
        let store =
            LocalFinalResult::new(temp.path().join("stage"), "job-1", "graph-1", "sink-1").unwrap();
        let output = temp.path().join("result.bin");
        store.stage("bundle-a", 1, b"alpha").unwrap();
        assert!(store.publish_concatenated(1, &output).is_err());
        assert!(!output.exists());
        store.stage("bundle-b", 2, b"beta").unwrap();
        store.accept("bundle-a").unwrap();
        store.accept("bundle-b").unwrap();
        store.accept("bundle-a").unwrap();
        assert!(store.publish_concatenated(3, &output).is_err());
        assert!(!output.exists());
        let bound = store
            .require_bundle_attempts(["bundle-a", "bundle-b"])
            .unwrap();
        assert!(bound.publish_concatenated(4, &output).is_err());
        assert!(!output.exists());
        let fragments = store.accepted_fragments().unwrap();
        assert_eq!(
            fragments
                .iter()
                .map(AcceptedFragment::bundle_digest_hex)
                .collect::<std::collections::BTreeSet<_>>(),
            ["bundle-a", "bundle-b"]
                .into_iter()
                .map(|id| hex(&Sha256::digest(id.as_bytes())))
                .collect()
        );
        let mut expected = Vec::new();
        for fragment in &fragments {
            fragment.copy_payload(&mut expected).unwrap();
        }
        let published = bound.publish_concatenated(3, &output).unwrap();
        assert_eq!(published.rows, 3);
        assert_eq!(published.fragments, 2);
        assert_eq!(published.bytes, expected.len() as u64);
        assert_eq!(fs::read(&output).unwrap(), expected);
        assert_eq!(bound.publish_concatenated(3, &output).unwrap(), published);
        fs::write(&output, b"short").unwrap();
        assert!(bound.publish_concatenated(3, &output).is_err());
        fs::write(&output, vec![b'X'; expected.len()]).unwrap();
        assert!(bound.publish_concatenated(3, &output).is_err());
    }

    #[test]
    fn attempts_are_immutable_and_scoped_to_one_job_graph_and_sink() {
        let temp = tempfile::tempdir().unwrap();
        let stage = temp.path().join("stage");
        let store = LocalFinalResult::new(&stage, "job-1", "graph-1", "sink-1").unwrap();
        store.stage("bundle-a", 1, b"payload").unwrap();
        assert!(store.stage("bundle-a", 1, b"other").is_err());
        let pending = store.path("bundle-a", "pending").unwrap();
        assert!(pending.exists());
        store.accept("bundle-a").unwrap();
        assert!(store.stage("bundle-a", 1, b"other").is_err());
        let other = LocalFinalResult::new(&stage, "job-2", "graph-1", "sink-1").unwrap();
        assert!(other.accepted_fragments().is_err());
        let another_graph = LocalFinalResult::new(&stage, "job-1", "graph-2", "sink-1").unwrap();
        assert!(another_graph.accepted_fragments().is_err());
        let another_sink = LocalFinalResult::new(&stage, "job-1", "graph-1", "sink-2").unwrap();
        assert!(another_sink.accepted_fragments().is_err());
    }

    #[test]
    fn identical_pending_attempt_can_be_staged_again_after_restart() {
        let temp = tempfile::tempdir().unwrap();
        let stage = temp.path().join("stage");
        let store = LocalFinalResult::new(&stage, "job", "graph", "sink").unwrap();
        store.stage("bundle", 1, b"payload").unwrap();
        let pending = store.path("bundle", "pending").unwrap();
        let original = fs::read(&pending).unwrap();

        let reopened = LocalFinalResult::new(&stage, "job", "graph", "sink").unwrap();
        reopened.stage("bundle", 1, b"payload").unwrap();
        assert_eq!(fs::read(&pending).unwrap(), original);
        assert!(reopened.stage("bundle", 2, b"payload").is_err());
        assert!(reopened.stage("bundle", 1, b"different").is_err());

        fs::write(&pending, b"corrupt").unwrap();
        assert!(reopened.stage("bundle", 1, b"payload").is_err());
        assert_eq!(fs::read(&pending).unwrap(), b"corrupt");
    }

    #[test]
    fn corrupt_or_abandoned_attempts_cannot_publish() {
        let temp = tempfile::tempdir().unwrap();
        let store =
            LocalFinalResult::new(temp.path().join("stage"), "job", "graph", "sink").unwrap();
        store.abandon("never-staged").unwrap();
        store.stage("abandoned", 1, b"x").unwrap();
        store.abandon("abandoned").unwrap();
        store.abandon("abandoned").unwrap();
        assert!(store.accepted_fragments().unwrap().is_empty());
        store.stage("accepted", 1, b"payload").unwrap();
        store.accept("accepted").unwrap();
        let path = store.path("accepted", "accepted").unwrap();
        let mut file = fs::OpenOptions::new().write(true).open(path).unwrap();
        file.seek(SeekFrom::Start(HEADER_LEN)).unwrap();
        file.write_all(b"X").unwrap();
        assert!(store.accepted_fragments().is_err());
        assert!(!temp.path().join("result.bin").exists());
    }

    #[test]
    fn invalid_identity_and_format_are_rejected_before_publication() {
        let temp = tempfile::tempdir().unwrap();
        assert!(LocalFinalResult::new(temp.path(), "", "graph", "sink").is_err());
        assert!(LocalFinalResult::new(temp.path(), "job", "", "sink").is_err());
        assert!(LocalFinalResult::new(temp.path(), "job", "graph", "").is_err());
        let store =
            LocalFinalResult::new(temp.path().join("stage"), "job", "graph", "sink").unwrap();
        assert!(store.stage("", 1, b"row").is_err());
        store.stage("bundle", 1, b"bad framing").unwrap();
        store.accept("bundle").unwrap();
        let output = temp.path().join("result.bin");
        assert!(
            store
                .publish_validated_concatenated(1, &output, |_| {
                    Err(invalid("invalid row framing"))
                })
                .is_err()
        );
        assert!(!output.exists());
        let accepted = store.path("bundle", "accepted").unwrap();
        let original = fs::read(&accepted).unwrap();
        assert!(
            store
                .read_fragment(&accepted, Some("another-bundle"))
                .is_err()
        );
        let mut wrong_magic = original.clone();
        wrong_magic[0] = b'X';
        fs::write(&accepted, &wrong_magic).unwrap();
        assert!(store.accepted_fragments().is_err());
        fs::write(&accepted, &original).unwrap();
        let mut wrong_size = original.clone();
        wrong_size[80..88].copy_from_slice(&1_u64.to_le_bytes());
        fs::write(&accepted, &wrong_size).unwrap();
        assert!(store.accepted_fragments().is_err());
        wrong_size[80..88].copy_from_slice(&u64::MAX.to_le_bytes());
        fs::write(&accepted, &wrong_size).unwrap();
        assert!(store.accepted_fragments().is_err());
        fs::write(&accepted, &original).unwrap();
        let fragment = store.accepted_fragments().unwrap().remove(0);
        fs::write(&accepted, &original[..original.len() - 1]).unwrap();
        assert!(fragment.copy_payload(&mut Vec::new()).is_err());
        fs::write(&accepted, &original).unwrap();
        fs::write(store.staging.join("unrelated"), b"ignored").unwrap();
        assert_eq!(store.accepted_fragments().unwrap().len(), 1);
        let renamed = accepted.with_file_name("incorrect.accepted");
        fs::rename(&accepted, &renamed).unwrap();
        assert!(store.accepted_fragments().is_err());
    }

    #[test]
    fn interrupted_acceptance_resumes_only_matching_attempt() {
        let temp = tempfile::tempdir().unwrap();
        let store =
            LocalFinalResult::new(temp.path().join("stage"), "job", "graph", "sink").unwrap();
        store.stage("bundle", 1, b"row").unwrap();
        let pending = store.path("bundle", "pending").unwrap();
        let accepted = store.path("bundle", "accepted").unwrap();
        fs::hard_link(&pending, &accepted).unwrap();
        let reopened = LocalFinalResult::new(&store.staging, "job", "graph", "sink").unwrap();
        reopened.accept("bundle").unwrap();
        assert!(!pending.exists());
        assert_eq!(reopened.accepted_fragments().unwrap().len(), 1);
        assert_eq!(
            reopened
                .require_bundle_attempts(["bundle"])
                .unwrap()
                .publish_concatenated(1, temp.path().join("result.bin"))
                .unwrap()
                .rows,
            1
        );
        reopened.accept("bundle").unwrap();

        let other =
            LocalFinalResult::new(temp.path().join("other"), "job", "graph", "sink").unwrap();
        other.stage("bundle", 1, b"different").unwrap();
        let different = other.path("bundle", "pending").unwrap();
        fs::copy(&different, &pending).unwrap();
        assert!(reopened.accept("bundle").is_err());
        assert!(pending.exists());
        assert!(accepted.exists());
        fs::remove_file(&pending).unwrap();
        let directory = store.path("directory-bundle", "pending").unwrap();
        fs::create_dir(&directory).unwrap();
        assert!(store.abandon("directory-bundle").is_err());
    }

    #[test]
    fn streamed_fragments_cross_buffer_boundaries_and_fail_atomically() {
        struct FailingReader {
            first: bool,
        }
        impl Read for FailingReader {
            fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
                if !self.first {
                    return Err(std::io::Error::other("source failed"));
                }
                self.first = false;
                buffer[0] = b'X';
                Ok(1)
            }
        }
        let temp = tempfile::tempdir().unwrap();
        let store =
            LocalFinalResult::new(temp.path().join("stage"), "job", "graph", "sink").unwrap();
        assert!(
            store
                .stage_reader("failed", 1, FailingReader { first: true })
                .is_err()
        );
        assert!(!store.path("failed", "pending").unwrap().exists());
        let payload = vec![b'A'; 150_000];
        store
            .stage_reader("large", 1, std::io::Cursor::new(&payload))
            .unwrap();
        store.accept("large").unwrap();
        let output = temp.path().join("large.bin");
        assert_eq!(
            store
                .require_bundle_attempts(["large"])
                .unwrap()
                .publish_concatenated(1, &output)
                .unwrap()
                .bytes,
            payload.len() as u64
        );
        assert_eq!(fs::read(output).unwrap(), payload);
    }

    #[test]
    fn matching_row_count_cannot_hide_pending_or_orphaned_fragments() {
        let temp = tempfile::tempdir().unwrap();
        let store =
            LocalFinalResult::new(temp.path().join("stage"), "job", "graph", "sink").unwrap();
        store.stage("accepted", 1, b"one").unwrap();
        store.accept("accepted").unwrap();
        store.stage("pending-empty", 0, b"").unwrap();
        let output = temp.path().join("result.bin");
        let bound = store.require_bundle_attempts(["accepted"]).unwrap();
        assert!(bound.publish_concatenated(1, &output).is_err());
        assert!(!output.exists());
        store.abandon("pending-empty").unwrap();
        let orphan = store.staging.join("orphaned-tempfile");
        fs::write(&orphan, b"incomplete").unwrap();
        assert!(bound.publish_concatenated(1, &output).is_err());
        fs::remove_file(orphan).unwrap();
        assert_eq!(bound.publish_concatenated(1, &output).unwrap().rows, 1);
    }

    #[test]
    fn runner_manifest_rejects_missing_extra_and_duplicate_attempts() {
        let temp = tempfile::tempdir().unwrap();
        let store =
            LocalFinalResult::new(temp.path().join("stage"), "job", "graph", "sink").unwrap();
        let output = temp.path().join("result.bin");
        store.stage("bundle-a", 1, b"a").unwrap();
        store.accept("bundle-a").unwrap();
        assert!(store.require_bundle_attempts(["", "bundle-a"]).is_err());
        assert!(
            store
                .require_bundle_attempts(["bundle-a", "bundle-a"])
                .is_err()
        );
        assert!(
            store
                .require_bundle_attempts(["bundle-b"])
                .unwrap()
                .publish_concatenated(1, &output)
                .is_err()
        );
        assert!(
            store
                .require_bundle_attempts(["bundle-a", "bundle-b"])
                .unwrap()
                .publish_concatenated(1, &output)
                .is_err()
        );
        assert!(!output.exists());
        let bound = store.require_bundle_attempts(["bundle-a"]).unwrap();
        assert_eq!(bound.publish_concatenated(1, &output).unwrap().rows, 1);
        store.stage("bundle-b", 0, b"").unwrap();
        store.accept("bundle-b").unwrap();
        assert!(bound.publish_concatenated(1, &output).is_err());
    }

    #[test]
    fn merged_publication_checks_decoded_rows_before_visibility() {
        let temp = tempfile::tempdir().unwrap();
        let store =
            LocalFinalResult::new(temp.path().join("stage"), "job", "graph", "sink").unwrap();
        store.stage("bundle", 2, b"one").unwrap();
        store.accept("bundle").unwrap();
        let store = store.require_bundle_attempts(["bundle"]).unwrap();
        let output = temp.path().join("merged.bin");
        assert!(
            store
                .publish_merged(2, &output, |parts, _, file| {
                    for part in parts {
                        part.copy_payload(file)?;
                    }
                    Ok(1)
                })
                .is_err()
        );
        assert!(!output.exists());
        assert_eq!(
            store
                .publish_merged(2, &output, |parts, _, file| {
                    for part in parts {
                        part.copy_payload(file)?;
                    }
                    Ok(2)
                })
                .unwrap()
                .bytes,
            3
        );
    }
}
