//! Provider-neutral immutable fragments for a portable whole-job result.
//! Publication requires the runner's completed bundle-attempt manifest.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::sync::Arc;

use ironbeam_cloud_objects::{ErrorKind as ObjectErrorKind, ObjectIO};
use ironbeam_core::Error;
use sha2::{Digest, Sha256};

const MAGIC: &[u8; 8] = b"IBOBJ001";
const HEADER_LEN: usize = 8 + 32 + 32 + 8 + 8 + 32;

fn invalid(message: impl Into<String>) -> Error {
    Error::incompatible(message)
}

fn object_error(error: impl std::fmt::Display) -> Error {
    Error::io("object-final", "cloud", error.to_string())
}

fn hex(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(result, "{byte:02x}").expect("writing to String cannot fail");
    }
    result
}

fn digest(parts: &[&str]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    hasher.finalize().into()
}

fn valid_object_component(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('/')
        && value
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != ".." && !part.contains('\\'))
}

#[derive(Clone)]
pub struct ObjectFinalResult {
    storage: Arc<dyn ObjectIO>,
    staging_bucket: String,
    staging_prefix: String,
    target_bucket: String,
    target_key: String,
    scope: [u8; 32],
    expected_attempts: Option<BTreeSet<String>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectPublishedResult {
    pub rows: u64,
    pub fragments: usize,
    pub bytes: u64,
}

struct Fragment {
    bundle_digest: [u8; 32],
    rows: u64,
    payload: Vec<u8>,
}

impl ObjectFinalResult {
    /// Bind one provider, staging prefix, final object, and execution scope.
    pub fn new(
        storage: Arc<dyn ObjectIO>,
        staging: (&str, &str),
        target: (&str, &str),
        job: &str,
        graph: &str,
        sink: &str,
    ) -> Result<Self, Error> {
        let (staging_bucket, staging_prefix) = staging;
        let (target_bucket, target_key) = target;
        if !valid_object_component(staging_bucket)
            || !valid_object_component(staging_prefix)
            || !valid_object_component(target_bucket)
            || !valid_object_component(target_key)
            || job.is_empty()
            || graph.is_empty()
            || sink.is_empty()
            || (staging_bucket == target_bucket
                && target_key.starts_with(&format!("{staging_prefix}/")))
        {
            return Err(invalid("invalid object final-result location or identity"));
        }
        Ok(Self {
            storage,
            staging_bucket: staging_bucket.to_owned(),
            staging_prefix: staging_prefix.to_owned(),
            target_bucket: target_bucket.to_owned(),
            target_key: target_key.to_owned(),
            scope: digest(&[
                job,
                graph,
                sink,
                staging_bucket,
                staging_prefix,
                target_bucket,
                target_key,
            ]),
            expected_attempts: None,
        })
    }

    /// Bind publication to exactly the completed runner attempt IDs.
    pub fn require_bundle_attempts<I, S>(&self, bundle_ids: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut expected = BTreeSet::new();
        for bundle_id in bundle_ids {
            let bundle_id = bundle_id.as_ref();
            if bundle_id.is_empty() || !expected.insert(hex(&digest(&[bundle_id]))) {
                return Err(invalid(
                    "object final-result manifest has duplicate or empty attempt",
                ));
            }
        }
        let mut bound = self.clone();
        bound.expected_attempts = Some(expected);
        Ok(bound)
    }

    fn key(&self, bundle_id: &str, suffix: &str) -> Result<String, Error> {
        if bundle_id.is_empty() {
            return Err(invalid("object fragment needs a bundle attempt identity"));
        }
        Ok(format!(
            "{}/part-{}.{}",
            self.staging_prefix,
            hex(&digest(&[bundle_id])),
            suffix
        ))
    }

    fn decode(&self, bytes: Vec<u8>, bundle_id: Option<&str>) -> Result<Fragment, Error> {
        if bytes.len() < HEADER_LEN || &bytes[..8] != MAGIC || bytes[8..40] != self.scope {
            return Err(invalid("object fragment header or scope differs"));
        }
        let bundle_digest: [u8; 32] = bytes[40..72]
            .try_into()
            .map_err(|_| invalid("object fragment has no bundle digest"))?;
        if bundle_id.is_some_and(|id| bundle_digest != digest(&[id])) {
            return Err(invalid("object fragment bundle identity differs"));
        }
        let rows = u64::from_le_bytes(
            bytes[72..80]
                .try_into()
                .map_err(|_| invalid("object fragment has no row count"))?,
        );
        let payload_len = u64::from_le_bytes(
            bytes[80..88]
                .try_into()
                .map_err(|_| invalid("object fragment has no payload length"))?,
        );
        let payload = &bytes[HEADER_LEN..];
        if usize::try_from(payload_len).ok() != Some(payload.len())
            || bytes[88..HEADER_LEN] != Sha256::digest(payload)[..]
        {
            return Err(invalid(
                "object fragment payload length or checksum differs",
            ));
        }
        Ok(Fragment {
            bundle_digest,
            rows,
            payload: payload.to_vec(),
        })
    }

    fn encode(&self, bundle_id: &str, rows: u64, payload: &[u8]) -> Result<Vec<u8>, Error> {
        let length = u64::try_from(payload.len())
            .map_err(|_| invalid("object fragment payload length overflow"))?;
        let mut bytes = Vec::with_capacity(HEADER_LEN.saturating_add(payload.len()));
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&self.scope);
        bytes.extend_from_slice(&digest(&[bundle_id]));
        bytes.extend_from_slice(&rows.to_le_bytes());
        bytes.extend_from_slice(&length.to_le_bytes());
        bytes.extend_from_slice(&Sha256::digest(payload));
        bytes.extend_from_slice(payload);
        Ok(bytes)
    }

    /// Conditionally create an immutable pending fragment. Identical retries succeed.
    pub fn stage(&self, bundle_id: &str, rows: u64, payload: &[u8]) -> Result<(), Error> {
        let pending = self.key(bundle_id, "pending")?;
        let accepted = self.key(bundle_id, "accepted")?;
        if self
            .storage
            .object_exists(&self.staging_bucket, &accepted)
            .map_err(object_error)?
        {
            return Err(invalid("object fragment is already accepted"));
        }
        let bytes = self.encode(bundle_id, rows, payload)?;
        match self
            .storage
            .put_object_if_absent(&self.staging_bucket, &pending, &bytes)
        {
            Ok(()) => Ok(()),
            Err(error) if error.kind == ObjectErrorKind::AlreadyExists => {
                let existing = self
                    .storage
                    .get_object(&self.staging_bucket, &pending)
                    .map_err(object_error)?;
                self.decode(existing.clone(), Some(bundle_id))?;
                if existing != bytes {
                    return Err(invalid("object pending fragment conflicts with retry"));
                }
                Ok(())
            }
            Err(error) => Err(object_error(error)),
        }
    }

    fn delete_pending(&self, key: &str) -> Result<(), Error> {
        match self.storage.delete_object(&self.staging_bucket, key) {
            Ok(()) => Ok(()),
            Err(error) if error.kind == ObjectErrorKind::NotFound => Ok(()),
            Err(error) => Err(object_error(error)),
        }
    }

    /// Accept a committed attempt only after its runner finalization callback.
    pub fn accept(&self, bundle_id: &str) -> Result<(), Error> {
        let pending = self.key(bundle_id, "pending")?;
        let accepted = self.key(bundle_id, "accepted")?;
        if self
            .storage
            .object_exists(&self.staging_bucket, &accepted)
            .map_err(object_error)?
        {
            let accepted_bytes = self
                .storage
                .get_object(&self.staging_bucket, &accepted)
                .map_err(object_error)?;
            self.decode(accepted_bytes.clone(), Some(bundle_id))?;
            if self
                .storage
                .object_exists(&self.staging_bucket, &pending)
                .map_err(object_error)?
            {
                let pending_bytes = self
                    .storage
                    .get_object(&self.staging_bucket, &pending)
                    .map_err(object_error)?;
                self.decode(pending_bytes.clone(), Some(bundle_id))?;
                if accepted_bytes != pending_bytes {
                    return Err(invalid(
                        "object accepted fragment conflicts with pending attempt",
                    ));
                }
                self.delete_pending(&pending)?;
            }
            return Ok(());
        }
        let bytes = self
            .storage
            .get_object(&self.staging_bucket, &pending)
            .map_err(object_error)?;
        self.decode(bytes.clone(), Some(bundle_id))?;
        match self
            .storage
            .put_object_if_absent(&self.staging_bucket, &accepted, &bytes)
        {
            Ok(()) => {}
            Err(error) if error.kind == ObjectErrorKind::AlreadyExists => {
                let existing = self
                    .storage
                    .get_object(&self.staging_bucket, &accepted)
                    .map_err(object_error)?;
                self.decode(existing.clone(), Some(bundle_id))?;
                if existing != bytes {
                    return Err(invalid(
                        "object accepted fragment conflicts with pending attempt",
                    ));
                }
            }
            Err(error) => return Err(object_error(error)),
        }
        self.delete_pending(&pending)
    }

    /// Remove an unaccepted attempt without touching committed fragments.
    pub fn abandon(&self, bundle_id: &str) -> Result<(), Error> {
        self.delete_pending(&self.key(bundle_id, "pending")?)
    }

    fn accepted_fragments(&self) -> Result<Vec<Fragment>, Error> {
        let prefix = format!("{}/", self.staging_prefix);
        let listed = match self
            .storage
            .list_objects(&self.staging_bucket, Some(&prefix))
        {
            Ok(objects) => objects,
            Err(error) if error.kind == ObjectErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(object_error(error)),
        };
        let mut fragments = Vec::new();
        let mut seen = BTreeSet::new();
        for object in listed {
            let Some(name) = object.key.strip_prefix(&prefix) else {
                return Err(invalid("object fragment listing escaped staging prefix"));
            };
            if name.ends_with(".pending") {
                return Err(invalid("object final result has a pending attempt"));
            }
            let bytes = self
                .storage
                .get_object(&self.staging_bucket, &object.key)
                .map_err(object_error)?;
            let fragment = self.decode(bytes, None)?;
            if name != format!("part-{}.accepted", hex(&fragment.bundle_digest))
                || !seen.insert(hex(&fragment.bundle_digest))
            {
                return Err(invalid(
                    "object final result has an unknown or duplicate fragment",
                ));
            }
            fragments.push(fragment);
        }
        if self.expected_attempts.as_ref() != Some(&seen) {
            return Err(invalid(
                "object accepted attempts differ from completed runner manifest",
            ));
        }
        fragments.sort_by_key(|fragment| fragment.bundle_digest);
        Ok(fragments)
    }

    /// Validate each complete fragment and the concatenation, then atomically
    /// create the final object. The format validator must reject partial records.
    /// An identical published object is an idempotent retry.
    pub fn publish_validated_concatenated(
        &self,
        expected_rows: u64,
        mut validate: impl FnMut(&[u8]) -> Result<u64, Error>,
    ) -> Result<ObjectPublishedResult, Error> {
        if self.expected_attempts.is_none() {
            return Err(invalid(
                "object publication requires a completed bundle manifest",
            ));
        }
        let fragments = self.accepted_fragments()?;
        for fragment in &fragments {
            if validate(&fragment.payload)? != fragment.rows {
                return Err(invalid(
                    "object fragment decoded row count differs from header",
                ));
            }
        }
        let rows = fragments.iter().try_fold(0_u64, |sum, fragment| {
            sum.checked_add(fragment.rows)
                .ok_or_else(|| invalid("object fragment row count overflow"))
        })?;
        if rows != expected_rows {
            return Err(invalid(
                "object fragment rows differ from runner commit count",
            ));
        }
        let bytes = fragments
            .iter()
            .try_fold(Vec::new(), |mut result, fragment| {
                result
                    .len()
                    .checked_add(fragment.payload.len())
                    .ok_or_else(|| invalid("object result payload length overflow"))?;
                result.extend_from_slice(&fragment.payload);
                Ok::<_, Error>(result)
            })?;
        if validate(&bytes)? != expected_rows {
            return Err(invalid(
                "object result decoded row count differs from runner",
            ));
        }
        match self
            .storage
            .put_object_if_absent(&self.target_bucket, &self.target_key, &bytes)
        {
            Ok(()) => {}
            Err(error) if error.kind == ObjectErrorKind::AlreadyExists => {
                if self
                    .storage
                    .get_object(&self.target_bucket, &self.target_key)
                    .map_err(object_error)?
                    != bytes
                {
                    return Err(invalid("published object conflicts with this job output"));
                }
            }
            Err(error) => return Err(object_error(error)),
        }
        Ok(ObjectPublishedResult {
            rows,
            fragments: fragments.len(),
            bytes: u64::try_from(bytes.len())
                .map_err(|_| invalid("object result payload length overflow"))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironbeam_cloud_objects::LocalObjectIO;

    fn store() -> (tempfile::TempDir, Arc<dyn ObjectIO>, ObjectFinalResult) {
        let directory = tempfile::tempdir().unwrap();
        let storage: Arc<dyn ObjectIO> = Arc::new(LocalObjectIO::new(directory.path()).unwrap());
        let result = ObjectFinalResult::new(
            Arc::clone(&storage),
            ("stage", "job/result"),
            ("output", "result.jsonl"),
            "job",
            "graph",
            "sink",
        )
        .unwrap();
        (directory, storage, result)
    }

    fn count_lines(bytes: &[u8]) -> Result<u64, Error> {
        Ok(bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .count() as u64)
    }

    #[test]
    fn accepted_attempts_publish_only_with_exact_manifest_and_are_idempotent() {
        let (_directory, storage, result) = store();
        result.stage("attempt-b", 1, b"2\n").unwrap();
        result.stage("attempt-a", 1, b"1\n").unwrap();
        result.stage("attempt-a", 1, b"1\n").unwrap();
        assert!(result.stage("attempt-a", 1, b"changed\n").is_err());
        result.accept("attempt-b").unwrap();
        result.accept("attempt-a").unwrap();
        result.accept("attempt-a").unwrap();
        assert!(
            result
                .publish_validated_concatenated(2, count_lines)
                .is_err()
        );
        assert!(
            result
                .require_bundle_attempts(["attempt-a"])
                .unwrap()
                .publish_validated_concatenated(2, count_lines)
                .is_err()
        );
        let bound = result
            .require_bundle_attempts(["attempt-a", "attempt-b"])
            .unwrap();
        let published = bound
            .publish_validated_concatenated(2, count_lines)
            .unwrap();
        assert_eq!(published.rows, 2);
        assert_eq!(published.fragments, 2);
        assert_eq!(
            bound
                .publish_validated_concatenated(2, count_lines)
                .unwrap(),
            published
        );
        let output = storage.get_object("output", "result.jsonl").unwrap();
        assert_eq!(count_lines(&output).unwrap(), 2);
    }

    #[test]
    fn pending_and_unknown_attempts_block_publication() {
        let (_directory, _storage, result) = store();
        result.stage("expected", 1, b"1\n").unwrap();
        let bound = result.require_bundle_attempts(["expected"]).unwrap();
        assert!(
            bound
                .publish_validated_concatenated(1, count_lines)
                .is_err()
        );
        result.abandon("expected").unwrap();
        result.stage("extra", 1, b"2\n").unwrap();
        result.accept("extra").unwrap();
        assert!(
            bound
                .publish_validated_concatenated(1, count_lines)
                .is_err()
        );
    }

    #[test]
    fn corrupt_fragment_and_conflicting_output_fail_closed() {
        let (_directory, storage, result) = store();
        result.stage("a", 1, b"1\n").unwrap();
        result.accept("a").unwrap();
        let accepted = result.key("a", "accepted").unwrap();
        storage.put_object("stage", &accepted, b"bad").unwrap();
        let bound = result.require_bundle_attempts(["a"]).unwrap();
        assert!(
            bound
                .publish_validated_concatenated(1, count_lines)
                .is_err()
        );
        storage.delete_object("stage", &accepted).unwrap();
        result.stage("a", 1, b"1\n").unwrap();
        result.accept("a").unwrap();
        storage
            .put_object("output", "result.jsonl", b"other\n")
            .unwrap();
        assert!(
            bound
                .publish_validated_concatenated(1, count_lines)
                .is_err()
        );
        assert_eq!(
            storage.get_object("output", "result.jsonl").unwrap(),
            b"other\n"
        );
    }

    #[test]
    fn empty_manifest_creates_empty_output_without_staging_bucket() {
        let (_directory, storage, result) = store();
        let bound = result.require_bundle_attempts([] as [&str; 0]).unwrap();
        assert_eq!(
            bound
                .publish_validated_concatenated(0, count_lines)
                .unwrap()
                .bytes,
            0
        );
        assert!(
            storage
                .get_object("output", "result.jsonl")
                .unwrap()
                .is_empty()
        );
    }
}
