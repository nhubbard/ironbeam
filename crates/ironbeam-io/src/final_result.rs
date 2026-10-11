//! Durable local fragments for a portable whole-job result.
//! The driver calls `publish_concatenated` only after the runner reports DONE.

use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use ironbeam_core::Error;
use sha2::{Digest, Sha256};

const MAGIC: &[u8; 8] = b"IBFRAG01";
const HEADER_LEN: u64 = 8 + 32 + 32 + 8 + 8 + 32;

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
}

#[derive(Debug, Clone)]
pub struct AcceptedFragment {
    path: PathBuf,
    pub rows: u64,
    pub payload_len: u64,
}

impl AcceptedFragment {
    pub fn copy_payload(&self, output: &mut impl Write) -> Result<(), Error> {
        let mut file = File::open(&self.path).map_err(io)?;
        file.seek(SeekFrom::Start(HEADER_LEN)).map_err(io)?;
        let copied = std::io::copy(&mut file.take(self.payload_len), output).map_err(io)?;
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
        })
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
        let mut buffer = [0_u8; 64 * 1024];
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
        temp.persist_noclobber(&pending).map_err(io)?;
        sync_directory(&self.staging)
    }

    /// Accept one committed attempt after the runner's bundle callback.
    pub fn accept(&self, bundle_id: &str) -> Result<(), Error> {
        let pending = self.path(bundle_id, "pending")?;
        let accepted = self.path(bundle_id, "accepted")?;
        if accepted.exists() {
            self.read_fragment(&accepted, Some(bundle_id))?;
            if pending.exists() {
                return Err(invalid("accepted fragment conflicts with pending attempt"));
            }
            return Ok(());
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
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(io(error)),
        }
    }

    fn read_fragment(
        &self,
        path: &Path,
        bundle_id: Option<&str>,
    ) -> Result<AcceptedFragment, Error> {
        let mut file = File::open(path).map_err(io)?;
        let mut header = [0_u8; HEADER_LEN as usize];
        file.read_exact(&mut header).map_err(io)?;
        if &header[..8] != MAGIC || header[8..40] != self.scope {
            return Err(invalid(
                "fragment scope or format does not match this result",
            ));
        }
        let bundle_digest = &header[40..72];
        let extension = path
            .extension()
            .and_then(|extension| extension.to_str())
            .ok_or_else(|| invalid("fragment has no lifecycle extension"))?;
        let expected_name = format!("part-{}.{}", hex(bundle_digest), extension);
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
        let mut buffer = [0_u8; 64 * 1024];
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
                !path
                    .extension()
                    .is_some_and(|extension| extension == "accepted")
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
        paths
            .into_iter()
            .map(|path| self.read_fragment(&path, None))
            .collect()
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
        for fragment in &fragments {
            fragment.copy_payload(&mut temp)?;
        }
        temp.as_file().sync_all().map_err(io)?;
        validate(temp.path())?;
        let bytes = temp.as_file().metadata().map_err(io)?.len();
        match temp.persist_noclobber(target) {
            Ok(_) => sync_directory(parent)?,
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                if !files_equal(error.file.path(), target)? {
                    return Err(invalid("published result conflicts with this job output"));
                }
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
    let mut a = [0_u8; 64 * 1024];
    let mut b = [0_u8; 64 * 1024];
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
        assert!(store.publish_concatenated(4, &output).is_err());
        assert!(!output.exists());
        let fragments = store.accepted_fragments().unwrap();
        let mut expected = Vec::new();
        for fragment in &fragments {
            fragment.copy_payload(&mut expected).unwrap();
        }
        let published = store.publish_concatenated(3, &output).unwrap();
        assert_eq!(published.rows, 3);
        assert_eq!(published.fragments, 2);
        assert_eq!(published.bytes, expected.len() as u64);
        assert_eq!(fs::read(&output).unwrap(), expected);
        assert_eq!(store.publish_concatenated(3, &output).unwrap(), published);
        fs::write(&output, b"short").unwrap();
        assert!(store.publish_concatenated(3, &output).is_err());
        fs::write(&output, vec![b'X'; expected.len()]).unwrap();
        assert!(store.publish_concatenated(3, &output).is_err());
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
    fn corrupt_or_abandoned_attempts_cannot_publish() {
        let temp = tempfile::tempdir().unwrap();
        let store =
            LocalFinalResult::new(temp.path().join("stage"), "job", "graph", "sink").unwrap();
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
    fn accepted_and_pending_same_attempt_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalFinalResult::new(temp.path(), "job", "graph", "sink").unwrap();
        store.stage("bundle", 1, b"row").unwrap();
        let pending = store.path("bundle", "pending").unwrap();
        let accepted = store.path("bundle", "accepted").unwrap();
        fs::hard_link(&pending, &accepted).unwrap();
        assert!(store.accept("bundle").is_err());
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
            store.publish_concatenated(1, &output).unwrap().bytes,
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
        assert!(store.publish_concatenated(1, &output).is_err());
        assert!(!output.exists());
        store.abandon("pending-empty").unwrap();
        let orphan = store.staging.join("orphaned-tempfile");
        fs::write(&orphan, b"incomplete").unwrap();
        assert!(store.publish_concatenated(1, &output).is_err());
        fs::remove_file(orphan).unwrap();
        assert_eq!(store.publish_concatenated(1, &output).unwrap().rows, 1);
    }
}
