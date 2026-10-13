use ironbeam_cloud_objects::{LocalObjectIO, ObjectIO};
use ironbeam_coder_postcard::impl_postcard_element;
use ironbeam_core::{
    CombineFn, DynOp, Element, ElementMetadata, Error, OrderedFoldFn, PCollection, Partition,
    Pipeline, ProgressSignal, Replay, Sink, SinkMode, SinkSpec, SourceProgress, TestStreamEvent,
    Timestamped, TypedSink, TypedUnboundedSourceReader, UnboundedBatch, UnboundedPoll,
    UnboundedSource, Version, Window, WindowInterval,
};
use ironbeam_io::final_result::LocalFinalResult;
#[cfg(feature = "database-proofs")]
use ironbeam_io_mongodb::{mongodb_idempotent_sink, mongodb_source};
#[cfg(feature = "database-proofs")]
use ironbeam_io_sql::{sql_idempotent_sink_with, sql_source};
use ironbeam_transforms::{
    DeadLetterExt, Validate, ValidationError, ValidationExt, ValidationResult,
};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone, PartialEq, prost_v5::Message)]
pub struct ProductionProtoRow {
    #[prost(int32, tag = "1")]
    pub value: i32,
}

impl Element for ProductionProtoRow {
    const TYPE_ID: &'static str = "tests/prism-production-protobuf-row";
    const CODER_ID: &'static str = "tests/prism-production-protobuf-row-prost14";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        Ok(prost_v5::Message::encode_to_vec(self))
    }
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        <Self as prost_v5::Message>::decode(bytes)
            .map_err(|error| Error::incompatible(error.to_string()))
    }
}

pub const PRODUCTION_AVRO_SCHEMA: &str = r#"{"type":"record","name":"ProductionAvroRow","fields":[{"name":"id","type":"int"},{"name":"label","type":"string"}]}"#;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ProductionAvroRow {
    pub id: i32,
    pub label: String,
}

impl_postcard_element!(ProductionAvroRow, "tests/prism-production-avro-row/v2");

#[cfg(feature = "database-proofs")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, sqlx::FromRow)]
pub struct SqliteRow {
    pub id: i64,
    pub name: String,
}

#[cfg(feature = "database-proofs")]
impl_postcard_element!(SqliteRow, "tests/prism-sqlite-row/v1");

#[cfg(feature = "database-proofs")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MongoRow {
    #[serde(rename = "_id")]
    pub id: i64,
    pub name: String,
}

#[cfg(feature = "database-proofs")]
impl_postcard_element!(MongoRow, "tests/prism-mongodb-row/v1");

/// Exercise a MongoDB source and stable-`_id` upsert sink on both runners.
#[cfg(feature = "database-proofs")]
pub fn mongodb_round_trip_fixture(
    pipeline: &Pipeline,
    uri: &str,
    database: &str,
    output_collection: &str,
) -> Result<Sink<MongoRow>, Error> {
    if !matches!(output_collection, "direct_out" | "portable_out") {
        return Err(Error::graph(
            "mongodb-sink",
            "unknown fixture output collection",
        ));
    }
    let transformed = mongodb_source::<MongoRow>(
        pipeline,
        "mongodb-source",
        uri,
        database,
        "source_rows",
        bson::doc! {},
    )?
    .map_named("mongodb-transform", |mut row| {
        row.id *= 10;
        row.name = row.name.to_uppercase();
        row
    })?;
    mongodb_idempotent_sink(
        &transformed,
        "mongodb-sink",
        uri,
        database,
        output_collection,
    )
}

/// Exercise a real database source and an idempotent database sink on each runner.
#[cfg(feature = "database-proofs")]
pub fn sqlite_round_trip_fixture(
    pipeline: &Pipeline,
    url: &str,
    output_table: &str,
) -> Result<Sink<SqliteRow>, Error> {
    if !matches!(output_table, "direct_out" | "portable_out") {
        return Err(Error::graph("sqlite-sink", "unknown fixture output table"));
    }
    let transformed = sql_source::<SqliteRow>(
        pipeline,
        "sqlite-source",
        url,
        "SELECT id, name FROM source_rows ORDER BY id",
    )?
    .map_named("sqlite-transform", |mut row| {
        row.id *= 10;
        row.name = row.name.to_uppercase();
        row
    })?;
    sql_idempotent_sink_with(
        &transformed,
        "sqlite-sink",
        url,
        &format!("INSERT OR IGNORE INTO {output_table} (id, name)"),
        "prism-proof/sqlite-bind",
        Version { major: 1, minor: 0 },
        |mut separated, row: &SqliteRow| {
            separated.push_bind(row.id).push_bind(row.name.clone());
        },
    )
}

impl Validate for ProductionAvroRow {
    fn validate(&self) -> ValidationResult {
        if self.id >= 0 {
            Ok(())
        } else {
            Err(vec![ValidationError::field("id", "must be nonnegative")])
        }
    }
}

struct ProductionSum;

impl CombineFn<i32, i32, i32> for ProductionSum {
    fn create(&self) -> i32 {
        0
    }
    fn add_input(&self, total: &mut i32, value: i32) {
        *total += value;
    }
    fn merge(&self, total: &mut i32, other: i32) {
        *total += other;
    }
    fn finish(&self, total: i32) -> i32 {
        total
    }
}

struct PanickingSum;

impl CombineFn<i32, i32, i32> for PanickingSum {
    fn create(&self) -> i32 {
        0
    }
    fn add_input(&self, total: &mut i32, value: i32) {
        assert_ne!(value, 2, "injected keyed fold failure");
        *total += value;
    }
    fn merge(&self, total: &mut i32, other: i32) {
        *total += other;
    }
    fn finish(&self, total: i32) -> i32 {
        total
    }
}

struct GlobalFoldSink<T> {
    path: PathBuf,
    values: Vec<T>,
}

impl<T: Element + std::fmt::Display> TypedSink<T> for GlobalFoldSink<T> {
    fn write(&mut self, value: T) -> Result<(), Error> {
        self.values.push(value);
        Ok(())
    }
    fn commit(&mut self) -> Result<(), Error> {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| Error::incompatible(error.to_string()))?;
        file.lock()
            .map_err(|error| Error::incompatible(error.to_string()))?;
        for value in self.values.drain(..) {
            writeln!(file, "{value}").map_err(|error| Error::incompatible(error.to_string()))?;
        }
        file.sync_data()
            .map_err(|error| Error::incompatible(error.to_string()))
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.values.clear();
        Ok(())
    }
}

/// All windowed join kinds and a two-input co-group share sources on both runners.
pub fn bounded_join_cogroup_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let left = pipeline
        .from_vec(
            "join-cogroup-left",
            vec![
                (1_000_000_i64, ("a".to_owned(), 1_i32)),
                (2_000_000, ("a".to_owned(), 2)),
                (5_000_000, ("b".to_owned(), 3)),
                (6_000_000, ("d".to_owned(), 4)),
            ],
        )?
        .timestamps_from_pairs_named("join-cogroup-left-time")?
        .fixed_windows("join-cogroup-left-window", 4_000_000)?;
    let right = pipeline
        .from_vec(
            "join-cogroup-right",
            vec![
                (0_i64, ("a".to_owned(), 10_i32)),
                (4_000_000, ("b".to_owned(), 20)),
                (7_000_000, ("c".to_owned(), 30)),
            ],
        )?
        .timestamps_from_pairs_named("join-cogroup-right-time")?
        .fixed_windows("join-cogroup-right-window", 4_000_000)?;
    let joined = left
        .join_full_named("join-cogroup-full-join", &right)?
        .reify_timestamps_named("join-cogroup-join-time")?
        .with_window_named("join-cogroup-join-window")?
        .map_named(
            "join-cogroup-join-row",
            |(window, (time, (key, (l, r))))| {
                format!(
                    "J|{key}|{l:?}|{r:?}|{time}|{}|{}",
                    window.start_ns, window.end_ns
                )
            },
        )?;
    let inner = left
        .join_inner_named("join-cogroup-inner-join", &right)?
        .reify_timestamps_named("join-cogroup-inner-time")?
        .with_window_named("join-cogroup-inner-window")?
        .map_named(
            "join-cogroup-inner-row",
            |(window, (time, (key, (l, r))))| {
                format!(
                    "I|{key}|{l}|{r}|{time}|{}|{}",
                    window.start_ns, window.end_ns
                )
            },
        )?;
    let left_joined = left
        .join_left_named("join-cogroup-left-join", &right)?
        .reify_timestamps_named("join-cogroup-left-join-time")?
        .with_window_named("join-cogroup-left-join-window")?
        .map_named(
            "join-cogroup-left-join-row",
            |(window, (time, (key, (l, r))))| {
                format!(
                    "L|{key}|{l}|{r:?}|{time}|{}|{}",
                    window.start_ns, window.end_ns
                )
            },
        )?;
    let right_joined = left
        .join_right_named("join-cogroup-right-join", &right)?
        .reify_timestamps_named("join-cogroup-right-join-time")?
        .with_window_named("join-cogroup-right-join-window")?
        .map_named(
            "join-cogroup-right-join-row",
            |(window, (time, (key, (l, r))))| {
                format!(
                    "R|{key}|{l:?}|{r}|{time}|{}|{}",
                    window.start_ns, window.end_ns
                )
            },
        )?;
    let grouped = ironbeam_core::cogroup_by_key_2_named("join-cogroup-group", &left, &right)?
        .reify_timestamps_named("join-cogroup-group-time")?
        .with_window_named("join-cogroup-group-window")?
        .map_named(
            "join-cogroup-group-row",
            |(window, (time, (key, (mut l, mut r))))| {
                l.sort_unstable();
                r.sort_unstable();
                format!(
                    "C|{key}|{l:?}|{r:?}|{time}|{}|{}",
                    window.start_ns, window.end_ns
                )
            },
        )?;
    let sink_path = path.clone();
    joined
        .flatten_named(
            "join-cogroup-merge",
            &[grouped, inner, left_joined, right_joined],
        )?
        .sink_named_with_config(
            "join-cogroup-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("join-cogroup:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}
