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
mod fragment_tests {
    use super::*;
    use ironbeam_core::Pipeline;

    const SCHEMA: &str = r#"{"type":"record","name":"Row","fields":[{"name":"id","type":"int"},{"name":"label","type":"string"}]}"#;

    #[derive(Clone, Debug, PartialEq, Serialize, serde::Deserialize)]
    struct Row {
        id: i32,
        label: String,
    }

    impl Element for Row {
        const TYPE_ID: &'static str = "test/avro-fragment-row";
        const CODER_ID: &'static str = "test/avro-fragment-row-v1";
        fn supports_coder() -> bool {
            true
        }
        fn encode(&self) -> Result<Vec<u8>, Error> {
            serde_json::to_vec(self).map_err(|error| Error::incompatible(error.to_string()))
        }
        fn decode(bytes: &[u8]) -> Result<Self, Error> {
            serde_json::from_slice(bytes).map_err(|error| Error::incompatible(error.to_string()))
        }
    }

    fn session(store: &AvroFragmentStore) -> AvroFragmentSink<Row> {
        AvroFragmentSink {
            store: store.clone(),
            bundle_id: None,
            spool: None,
            rows: 0,
            staged: false,
            marker: std::marker::PhantomData,
        }
    }

    #[test]
    fn two_native_schema_fragments_merge_into_one_container() {
        let temp = tempfile::tempdir().unwrap();
        let store =
            avro_fragment_store(temp.path().join("stage"), "job", "graph", "sink", SCHEMA).unwrap();
        let output = temp.path().join("final.avro");
        let mut first = session(&store);
        assert!(
            first
                .write(Row {
                    id: 0,
                    label: "early".into()
                })
                .is_err()
        );
        assert!(first.begin_bundle("").is_err());
        assert!(first.finalize_bundle().is_err());
        first.begin_bundle("bundle-1").unwrap();
        assert!(first.finalize_bundle().is_err());
        assert!(first.begin_bundle("duplicate").is_err());
        first
            .write(Row {
                id: 1,
                label: "a".into(),
            })
            .unwrap();
        first
            .write(Row {
                id: 2,
                label: "b".into(),
            })
            .unwrap();
        first.commit().unwrap();
        assert!(publish_avro_fragments(&store, 2, &output).is_err());
        assert!(!output.exists());
        let mut overlap = session(&store);
        overlap.begin_bundle("bundle-1").unwrap();
        overlap
            .write(Row {
                id: 9,
                label: "wrong".into(),
            })
            .unwrap();
        assert!(overlap.commit().is_err());
        overlap.abort().unwrap();
        first.finalize_bundle().unwrap();
        first.finalize_bundle().unwrap();
        let mut second = session(&store);
        second.begin_bundle("bundle-2").unwrap();
        second
            .write(Row {
                id: 3,
                label: "c".into(),
            })
            .unwrap();
        second.commit().unwrap();
        let mut restarted = session(&store);
        assert!(restarted.recover_bundle_finalization("missing").is_err());
        restarted.recover_bundle_finalization("bundle-2").unwrap();
        second.finalize_bundle().unwrap();
        let published = publish_avro_fragments(&store, 3, &output).unwrap();
        assert_eq!(published.rows, 3);
        assert_eq!(published.fragments, 2);
        assert_eq!(
            publish_avro_fragments(&store, 3, &output).unwrap(),
            published
        );
        let reader = apache_avro::Reader::new(std::fs::File::open(&output).unwrap()).unwrap();
        assert_eq!(
            reader.writer_schema().canonical_form(),
            apache_avro::Schema::parse_str(SCHEMA)
                .unwrap()
                .canonical_form()
        );
        let mut rows = reader
            .map(|value| apache_avro::from_value::<Row>(&value.unwrap()).unwrap())
            .collect::<Vec<_>>();
        rows.sort_by_key(|row| row.id);
        assert_eq!(
            rows,
            [
                Row {
                    id: 1,
                    label: "a".into()
                },
                Row {
                    id: 2,
                    label: "b".into()
                },
                Row {
                    id: 3,
                    label: "c".into()
                }
            ]
        );
    }

    #[test]
    fn builder_scope_abort_and_invalid_schema_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let pipeline = Pipeline::default();
        let input = pipeline
            .from_vec(
                "input",
                vec![Row {
                    id: 1,
                    label: "a".into(),
                }],
            )
            .unwrap();
        assert!(avro_fragment_sink(&input, "bad", "relative", "job", "graph", SCHEMA).is_err());
        assert!(avro_fragment_sink(&input, "bad", temp.path(), "job", "graph", "{").is_err());
        let staging = temp.path().join("stage");
        let sink = avro_fragment_sink(&input, "sink", &staging, "job", "graph", SCHEMA).unwrap();
        let (_, registry) = sink.snapshot().unwrap();
        assert!(registry.sink_requires_bundle_finalization("sink"));
        assert!(
            registry
                .sink_config_digest("sink")
                .unwrap()
                .contains("avro-fragments-v1")
        );
        let mut opened = registry.open_sink("sink").unwrap();
        assert!(opened.requires_bundle_finalization());
        opened.begin_bundle("aborted").unwrap();
        opened.commit().unwrap();
        opened.abort().unwrap();
        let store = avro_fragment_store(&staging, "job", "graph", "sink", SCHEMA).unwrap();
        assert!(store.accepted_fragments().unwrap().is_empty());
        assert!(std::fs::read_dir(&staging).unwrap().next().is_none());
        let different =
            r#"{"type":"record","name":"Other","fields":[{"name":"id","type":"long"}]}"#;
        let mut accepted = session(&store);
        accepted.begin_bundle("schema-bound").unwrap();
        accepted
            .write(Row {
                id: 1,
                label: "a".into(),
            })
            .unwrap();
        accepted.commit().unwrap();
        accepted.finalize_bundle().unwrap();
        let wrong = avro_fragment_store(&staging, "job", "graph", "sink", different).unwrap();
        assert!(wrong.accepted_fragments().is_err());
        let metadata_only = r#"{"type":"record","name":"Row","doc":"another contract","fields":[{"name":"id","type":"int"},{"name":"label","type":"string"}]}"#;
        let wrong_metadata =
            avro_fragment_store(&staging, "job", "graph", "sink", metadata_only).unwrap();
        assert!(wrong_metadata.accepted_fragments().is_err());
        assert!(avro_fragment_store(&staging, "job", "", "sink", SCHEMA).is_err());
    }

    #[test]
    fn malformed_datum_and_row_metadata_never_publish() {
        let temp = tempfile::tempdir().unwrap();
        let store =
            avro_fragment_store(temp.path().join("stage"), "job", "graph", "sink", SCHEMA).unwrap();
        store
            .local
            .stage("bad-length", 1, &100_u64.to_le_bytes())
            .unwrap();
        store.local.accept("bad-length").unwrap();
        let output = temp.path().join("result.avro");
        assert!(publish_avro_fragments(&store, 1, &output).is_err());
        assert!(!output.exists());
        let other =
            avro_fragment_store(temp.path().join("other"), "job", "graph", "sink", SCHEMA).unwrap();
        let value = Row {
            id: 1,
            label: "a".into(),
        };
        let datum = GenericDatumWriter::builder(&other.schema)
            .build()
            .unwrap()
            .write_ser_to_vec(&value)
            .unwrap();
        let mut framed = (datum.len() as u64).to_le_bytes().to_vec();
        framed.extend_from_slice(&datum);
        other.local.stage("wrong-count", 2, &framed).unwrap();
        other.local.accept("wrong-count").unwrap();
        assert!(publish_avro_fragments(&other, 2, &output).is_err());
        assert!(!output.exists());
        assert!(publish_avro_fragments(&other, 2, temp.path().join("result.avro.gz")).is_err());
        let truncated = avro_fragment_store(
            temp.path().join("truncated"),
            "job",
            "graph",
            "sink",
            SCHEMA,
        )
        .unwrap();
        truncated.local.stage("truncated", 1, &[1, 2, 3]).unwrap();
        truncated.local.accept("truncated").unwrap();
        assert!(publish_avro_fragments(&truncated, 1, temp.path().join("truncated.avro")).is_err());
        let trailing =
            avro_fragment_store(temp.path().join("trailing"), "job", "graph", "sink", SCHEMA)
                .unwrap();
        let mut framed = ((datum.len() + 1) as u64).to_le_bytes().to_vec();
        framed.extend_from_slice(&datum);
        framed.push(0);
        trailing.local.stage("trailing", 1, &framed).unwrap();
        trailing.local.accept("trailing").unwrap();
        assert!(publish_avro_fragments(&trailing, 1, temp.path().join("trailing.avro")).is_err());
        let invalid =
            avro_fragment_store(temp.path().join("invalid"), "job", "graph", "sink", SCHEMA)
                .unwrap();
        let mut framed = 1_u64.to_le_bytes().to_vec();
        framed.push(0xff);
        invalid.local.stage("invalid", 1, &framed).unwrap();
        invalid.local.accept("invalid").unwrap();
        assert!(publish_avro_fragments(&invalid, 1, temp.path().join("invalid.avro")).is_err());
    }
}
