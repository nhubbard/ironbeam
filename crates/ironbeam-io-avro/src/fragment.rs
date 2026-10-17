//! Native-schema Avro fragments for portable whole-job publication.

use crate::io;
use apache_avro::{reader::datum::GenericDatumReader, writer::datum::GenericDatumWriter};
use ironbeam_core::{Element, Error, PCollection, Replay, Sink, SinkMode, SinkSpec, TypedSink};
use ironbeam_io::final_result::{AcceptedFragment, LocalFinalResult, PublishedResult};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

/// Native-schema scope shared by the portable worker and job coordinator.
#[derive(Clone)]
pub struct AvroFragmentStore {
    local: LocalFinalResult,
    schema: Arc<apache_avro::Schema>,
    schema_identity: Arc<str>,
    marker: [u8; 16],
}

impl AvroFragmentStore {
    /// Require the publisher to match the runner's completed bundle attempts.
    ///
    /// # Errors
    ///
    /// Rejects empty or duplicate attempt IDs.
    pub fn require_bundle_attempts<I, S>(mut self, bundle_ids: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.local = self.local.require_bundle_attempts(bundle_ids)?;
        Ok(self)
    }

    /// Return accepted Avro fragments after runner finalization.
    pub fn accepted_fragments(&self) -> Result<Vec<AcceptedFragment>, Error> {
        self.local.accepted_fragments()
    }
}

/// Reopen a fragment scope with the same native Avro schema and identities.
pub fn avro_fragment_store(
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
    key: &str,
    schema: &str,
) -> Result<AvroFragmentStore, Error> {
    let staging = staging.as_ref();
    if !staging.is_absolute() || graph.is_empty() {
        return Err(Error::graph(
            key,
            "Avro fragment needs an absolute staging path and graph identity",
        ));
    }
    let schema = apache_avro::Schema::parse_str(schema).map_err(|error| io(key, error))?;
    let full_schema = serde_json::to_value(&schema)
        .map_err(|error| Error::incompatible(error.to_string()))?
        .to_string();
    let scope = format!("{graph}\0avro-datum-v1\0{full_schema}");
    let digest = Sha256::digest(format!("{job}\0{scope}\0{key}").as_bytes());
    let mut marker = [0_u8; 16];
    marker.copy_from_slice(&digest[..16]);
    Ok(AvroFragmentStore {
        local: LocalFinalResult::new(staging, job, &scope, key)?,
        schema: Arc::new(schema),
        schema_identity: Arc::from(full_schema),
        marker,
    })
}

struct AvroFragmentSink<T> {
    store: AvroFragmentStore,
    bundle_id: Option<String>,
    spool: Option<tempfile::NamedTempFile>,
    rows: u64,
    staged: bool,
    marker: std::marker::PhantomData<T>,
}

impl<T: Element + Serialize> TypedSink<T> for AvroFragmentSink<T> {
    fn begin_bundle(&mut self, instruction_id: &str) -> Result<(), Error> {
        if instruction_id.is_empty() || self.bundle_id.is_some() {
            return Err(Error::incompatible(
                "Avro fragment needs one bundle attempt identity",
            ));
        }
        self.bundle_id = Some(instruction_id.to_owned());
        Ok(())
    }

    fn write(&mut self, value: T) -> Result<(), Error> {
        if self.bundle_id.is_none() {
            return Err(Error::incompatible(
                "Avro fragment has no bundle attempt identity",
            ));
        }
        let datum_writer = GenericDatumWriter::builder(&self.store.schema)
            .build()
            .map_err(|error| io("avro-fragment", error))?;
        let datum = datum_writer
            .write_ser_to_vec(&value)
            .map_err(|error| io("avro-fragment", error))?;
        let length = u64::try_from(datum.len())
            .map_err(|_| Error::incompatible("Avro datum length overflow"))?;
        let spool = match self.spool.as_mut() {
            Some(spool) => spool,
            None => self.spool.insert(
                tempfile::NamedTempFile::new().map_err(|error| io("avro-fragment", error))?,
            ),
        };
        spool
            .write_all(&length.to_le_bytes())
            .map_err(|error| io("avro-fragment", error))?;
        spool
            .write_all(&datum)
            .map_err(|error| io("avro-fragment", error))?;
        self.rows = self
            .rows
            .checked_add(1)
            .ok_or_else(|| Error::incompatible("Avro fragment row count overflow"))?;
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        let bundle_id = self
            .bundle_id
            .as_deref()
            .ok_or_else(|| Error::incompatible("Avro fragment has no bundle attempt identity"))?;
        let spool = match self.spool.as_mut() {
            Some(spool) => spool,
            None => self.spool.insert(
                tempfile::NamedTempFile::new().map_err(|error| io("avro-fragment", error))?,
            ),
        };
        spool
            .as_file_mut()
            .seek(SeekFrom::Start(0))
            .map_err(|error| io("avro-fragment", error))?;
        self.store
            .local
            .stage_reader(bundle_id, self.rows, spool.as_file_mut())?;
        self.staged = true;
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        if self.staged {
            if let Some(bundle_id) = self.bundle_id.as_deref() {
                self.store.local.abandon(bundle_id)?;
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
        let bundle_id = self
            .bundle_id
            .as_deref()
            .ok_or_else(|| Error::incompatible("Avro fragment has no bundle attempt identity"))?;
        if !self.staged {
            return Err(Error::incompatible("Avro fragment has not committed"));
        }
        self.store.local.accept(bundle_id)
    }

    fn recover_bundle_finalization(&mut self, instruction_id: &str) -> Result<(), Error> {
        self.store.local.accept(instruction_id)
    }
}

/// Register a native-schema Avro fragment sink for portable jobs. Publication
/// must occur after DONE through `publish_avro_fragments`.
pub fn avro_fragment_sink<T: Element + Serialize>(
    input: &PCollection<T>,
    key: &str,
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
    schema: &str,
) -> Result<Sink<T>, Error> {
    let staging = staging.as_ref();
    let store = avro_fragment_store(staging, job, graph, key, schema)?;
    let config = serde_json::to_string(&(
        "avro-fragments-v1",
        staging,
        job,
        graph,
        key,
        store.schema_identity.as_ref(),
    ))
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
            Ok(Box::new(AvroFragmentSink::<T> {
                store: store.clone(),
                bundle_id: None,
                spool: None,
                rows: 0,
                staged: false,
                marker: std::marker::PhantomData,
            }))
        },
    )
}

/// Decode accepted datum fragments under the declared native schema and
/// atomically publish one Avro container after successful job completion.
pub fn publish_avro_fragments(
    store: &AvroFragmentStore,
    expected_rows: u64,
    target: impl AsRef<Path>,
) -> Result<PublishedResult, Error> {
    let target = target.as_ref();
    if target
        .extension()
        .and_then(|value| value.to_str())
        .is_some_and(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "gz" | "gzip" | "zst" | "zstd" | "bz2" | "xz"
            )
        })
    {
        return Err(Error::graph(
            "avro-fragment",
            "compressed final result needs a format merger",
        ));
    }
    store
        .local
        .publish_merged(expected_rows, target, |parts, _, output| {
            let datum_reader = GenericDatumReader::builder(&store.schema)
                .build()
                .map_err(|error| io("avro-fragment", error))?;
            let mut writer = apache_avro::Writer::builder()
                .schema(&store.schema)
                .writer(output)
                .marker(store.marker)
                .build()
                .map_err(|error| io("avro-fragment", error))?;
            let mut total = 0_u64;
            for part in parts {
                let mut payload = part.open_payload()?;
                let mut part_rows = 0_u64;
                while payload.limit() > 0 {
                    let mut length = [0_u8; 8];
                    payload
                        .read_exact(&mut length)
                        .map_err(|error| io("avro-fragment", error))?;
                    let length = u64::from_le_bytes(length);
                    if length > payload.limit() {
                        return Err(Error::incompatible(
                            "Avro fragment datum length exceeds payload",
                        ));
                    }
                    let mut datum = payload.by_ref().take(length);
                    let value = datum_reader
                        .read_value(&mut datum)
                        .map_err(|error| io("avro-fragment", error))?;
                    if datum.limit() != 0 {
                        return Err(Error::incompatible(
                            "Avro fragment datum has trailing bytes",
                        ));
                    }
                    writer
                        .append_value(value)
                        .map_err(|error| io("avro-fragment", error))?;
                    part_rows = part_rows
                        .checked_add(1)
                        .ok_or_else(|| Error::incompatible("Avro fragment row count overflow"))?;
                }
                if part_rows != part.rows {
                    return Err(Error::incompatible(
                        "Avro fragment decoded row count differs from metadata",
                    ));
                }
                total = total
                    .checked_add(part_rows)
                    .ok_or_else(|| Error::incompatible("Avro result row count overflow"))?;
            }
            writer.flush().map_err(|error| io("avro-fragment", error))?;
            Ok(total)
        })
}

#[cfg(test)]
#[path = "../tests/unit/fragment__fragment_tests.rs"]
mod fragment_tests;
