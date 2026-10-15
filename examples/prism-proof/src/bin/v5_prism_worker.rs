//! Local Prism fixture: a separately launched v5 Fn API worker.

extern crate prost_v5 as prost;

use ironbeam_cloud_objects::{LocalObjectIO, ObjectStores};
use ironbeam_core::{
    Element, Error, Replay, SinkMode, SinkSpec, SourceProgress, TestStreamEvent, TypedSink, Version,
};
use ironbeam_prism_proof::{
    accumulating_count_trigger_stream_fixture, accumulating_late_count_fixture,
    accumulating_late_join_fixture, accumulating_latest_count_trigger_stream_fixture,
    allowed_lateness_fixture, bounded_batch_helpers_fixture, bounded_join_cogroup_fixture,
    bounded_join_cogroup_output_time_fixture, bounded_map_filter_fixture,
    bounded_multi_source_fixture, bounded_multi_wait_fixture, bounded_session_cogroup_fixture,
    bounded_session_join_fixture, bounded_side_input_fixture, bounded_ten_way_cogroup_fixture,
    bounded_three_way_cogroup_fixture, bounded_wait_on_fixture, combine_output_time_fixture,
    combined_count_fixture, completion_signal, count_trigger_stream_fixture, dead_letter_fixture,
    dual_late_cogroup_fixture, dual_late_count_cogroup_fixture, dual_positive_late_cogroup_fixture,
    dual_positive_late_full_join_fixture, dual_positive_late_join_time_fixture,
    dual_stream_windowed_side_fixture, dual_stream_windowed_wait_fixture,
    dual_unbounded_cogroup_fixture, dual_unbounded_fanin_output_time_fixture,
    dual_unbounded_flatten_fixture, dual_unbounded_join_fixture, dual_unbounded_join_kind_fixture,
    dual_unbounded_late_join_fixture, dual_unbounded_map_side_fixture,
    dual_unbounded_session_cogroup_fixture, dual_unbounded_session_join_fixture,
    explicit_custom_batches_fixture, failing_wait_on_fixture, finalization_failure_fixture,
    finalization_probe_sink, global_accumulating_early_cogroup_fixture, global_count_fixture,
    global_fold_fixture, global_keyed_batches_fixture, global_keyed_combine_fixture,
    global_vector_fixture, group_end_time_fixture, group_latest_time_fixture, group_time_fixture,
    heterogeneous_late_count_cogroup_fixture, homogeneous_twelve_way_cogroup_fixture,
    homogeneous_twelve_way_session_fixture, invalid_dynop_fixture, invalid_keyed_batch_fixture,
    large_group_rows, large_group_summary, late_count_fixture, late_count_join_fixture,
    late_count_join_kind_fixture, latest_tie_duplicate_fixture, latest_tie_empty_fixture,
    latest_tie_fixture, mixed_boundedness_join_cogroup_fixture, mixed_flatten_fixture,
    mixed_homogeneous_cogroup_fixture, offset_window_group_fixture,
    ordered_batch_duplicate_fixture, ordered_global_duplicate_fixture, ordered_global_fixture,
    ordered_keyed_duplicate_fixture, ordered_keyed_fixture, pane_metadata_fixture_for,
    pane_metadata_fixture_with_accumulating_count, pane_metadata_fixture_with_trigger,
    panicking_global_combine_fixture, panicking_keyed_fold_fixture, panicking_map_fixture,
    panicking_side_input_fixture, partition_fixture, portable_object_avro_fixture,
    portable_object_parquet_fixture, production_join_cogroup_fixture, production_stream_fixture,
    session_group_fixture, shared_accumulating_early_cogroup_fixture,
    shared_accumulating_late_cogroup_fixture, shared_unbounded_homogeneous_cogroup_fixture,
    side_view_shapes_fixture, sliding_allowed_lateness_fixture, sliding_late_join_fixture,
    sliding_window_group_fixture, streaming_group_fixture, streaming_ordered_batch_fixture,
    streaming_ordered_keyed_fixture, streaming_partition_fixture, streaming_session_group_fixture,
    streaming_sliding_group_fixture, streaming_windowed_side_input_fixture,
    triple_unbounded_flatten_fixture, triple_unbounded_homogeneous_cogroup_fixture,
    triple_unbounded_session_homogeneous_fixture, validation_fail_fixture, validation_skip_fixture,
    windowed_duplicate_map_side_fixture, windowed_keyed_batches_fixture,
    windowed_side_input_fixture, windowed_side_view_shapes_fixture, ExpandSingleton,
    PaneFixtureWindow,
};
#[cfg(feature = "database-proofs")]
use ironbeam_prism_proof::{mongodb_round_trip_fixture, sqlite_round_trip_fixture};
use sha2::{Digest, Sha256};
use std::sync::Arc;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct UserRow {
    id: i32,
    label: String,
}

impl Element for UserRow {
    const TYPE_ID: &'static str = "tests/prism-run-user-row";
    const CODER_ID: &'static str = "tests/prism-run-user-row-v1";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = self.id.to_le_bytes().to_vec();
        bytes.extend_from_slice(self.label.as_bytes());
        Ok(bytes)
    }
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let id: [u8; 4] = bytes
            .get(..4)
            .ok_or_else(|| Error::incompatible("short row"))?
            .try_into()
            .map_err(|_| Error::incompatible("short row"))?;
        let label = String::from_utf8(bytes[4..].to_vec())
            .map_err(|_| Error::incompatible("invalid row label"))?;
        Ok(Self {
            id: i32::from_le_bytes(id),
            label,
        })
    }
}

#[derive(Clone, PartialEq, prost_v5::Message)]
struct ProtoRow {
    #[prost(int32, tag = "1")]
    id: i32,
    #[prost(string, tag = "2")]
    label: String,
}

impl Element for ProtoRow {
    const TYPE_ID: &'static str = "tests/prism-protobuf-row";
    const CODER_ID: &'static str = "tests/prism-protobuf-row-prost14";
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

struct CaptureSink {
    rows: Vec<UserRow>,
    path: String,
}
struct CompletionCheckedSink {
    inner: CaptureSink,
    marker: std::path::PathBuf,
}
impl TypedSink<UserRow> for CompletionCheckedSink {
    fn write(&mut self, value: UserRow) -> Result<(), Error> {
        if !self.marker.exists() {
            return Err(Error::execution(
                "capture-wait",
                "prism",
                "signal was not complete",
            ));
        }
        self.inner.write(value)
    }
    fn commit(&mut self) -> Result<(), Error> {
        self.inner.commit()
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.inner.abort()
    }
}
impl TypedSink<UserRow> for CaptureSink {
    fn write(&mut self, value: UserRow) -> Result<(), Error> {
        self.rows.push(value);
        Ok(())
    }
    fn commit(&mut self) -> Result<(), Error> {
        use std::io::Write;
        let mut payload = Vec::new();
        for value in &self.rows {
            writeln!(&mut payload, "{}:{}", value.id, value.label)
                .map_err(|error| Error::incompatible(error.to_string()))?;
        }
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| Error::incompatible(error.to_string()))?;
        file.lock()
            .map_err(|error| Error::incompatible(error.to_string()))?;
        file.write_all(&payload)
            .and_then(|()| file.sync_data())
            .map_err(|error| Error::incompatible(error.to_string()))?;
        self.rows.clear();
        Ok(())
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.rows.clear();
        Ok(())
    }
}

struct TimestampSink {
    rows: Vec<(i64, UserRow)>,
    path: String,
}
impl TypedSink<(i64, UserRow)> for TimestampSink {
    fn write(&mut self, value: (i64, UserRow)) -> Result<(), Error> {
        self.rows.push(value);
        Ok(())
    }
    fn commit(&mut self) -> Result<(), Error> {
        use std::io::Write;
        let mut payload = Vec::new();
        for (time, row) in &self.rows {
            writeln!(&mut payload, "{time}:{}:{}", row.id, row.label)
                .map_err(|error| Error::incompatible(error.to_string()))?;
        }
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| Error::incompatible(error.to_string()))?;
        file.lock()
            .map_err(|error| Error::incompatible(error.to_string()))?;
        file.write_all(&payload)
            .and_then(|()| file.sync_data())
            .map_err(|error| Error::incompatible(error.to_string()))?;
        self.rows.clear();
        Ok(())
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.rows.clear();
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    ironbeam_dataflow::run_v5_from_env(|pipeline| {
    #[cfg(feature = "database-proofs")]
    if let (Ok(uri), Ok(database), Ok(collection)) = (
        std::env::var("IRONBEAM_V5_PRISM_MONGODB_URI"),
        std::env::var("IRONBEAM_V5_PRISM_MONGODB_DATABASE"),
        std::env::var("IRONBEAM_V5_PRISM_MONGODB_OUTPUT"),
    ) {
        return mongodb_round_trip_fixture(pipeline, &uri, &database, &collection)
            .map(|_| ())
            .map_err(Into::into);
    }
    #[cfg(feature = "database-proofs")]
    if let (Ok(url), Ok(table)) = (
        std::env::var("IRONBEAM_V5_PRISM_SQLITE_URL"),
        std::env::var("IRONBEAM_V5_PRISM_SQLITE_OUTPUT"),
    ) {
        return sqlite_round_trip_fixture(pipeline, &url, &table)
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_BOUNDED_CONTRACT") {
        return bounded_map_filter_fixture(pipeline, path.into()).map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_CUSTOM_BATCHES") {
        return explicit_custom_batches_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_THREE_COGROUP") {
        return bounded_three_way_cogroup_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_TEN_COGROUP") {
        return bounded_ten_way_cogroup_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_HOMOGENEOUS_COGROUP") {
        return homogeneous_twelve_way_cogroup_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_HOMOGENEOUS_SESSION") {
        return homogeneous_twelve_way_session_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_SHARED_HOMOGENEOUS") {
        return shared_unbounded_homogeneous_cogroup_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_SHARED_ACCUMULATING_LATE") {
        return shared_accumulating_late_cogroup_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_SHARED_ACCUMULATING_EARLY") {
        return shared_accumulating_early_cogroup_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_GLOBAL_ACCUMULATING_EARLY") {
        return global_accumulating_early_cogroup_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let (Ok(root), Ok(path)) = (
        std::env::var("IRONBEAM_V5_PRISM_OBJECT_AVRO_ROOT"),
        std::env::var("IRONBEAM_V5_PRISM_OBJECT_AVRO_RESULT"),
    ) {
        return portable_object_avro_fixture(pipeline, std::path::Path::new(&root), path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let (Ok(root), Ok(path)) = (
        std::env::var("IRONBEAM_V5_PRISM_OBJECT_PARQUET_ROOT"),
        std::env::var("IRONBEAM_V5_PRISM_OBJECT_PARQUET_RESULT"),
    ) {
        return portable_object_parquet_fixture(pipeline, std::path::Path::new(&root), path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_DEAD_LETTER") {
        return dead_letter_fixture(pipeline, path.into()).map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_VALIDATION_SKIP") {
        return validation_skip_fixture(pipeline, path.into()).map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_VALIDATION_FAIL") {
        return validation_fail_fixture(pipeline, path.into()).map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_SIDE_INPUT_CONTRACT") {
        return bounded_side_input_fixture(pipeline, path.into()).map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_WINDOWED_SIDE_CONTRACT") {
        return windowed_side_input_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_STREAM_WINDOWED_SIDE") {
        return streaming_windowed_side_input_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_DUAL_WINDOWED_SIDE") {
        return dual_stream_windowed_side_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_DUAL_WINDOWED_WAIT") {
        return dual_stream_windowed_wait_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_DUAL_TYPED_MAP_SIDE") {
        return dual_unbounded_map_side_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_SIDE_VIEW_SHAPES") {
        return side_view_shapes_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_WINDOWED_SIDE_VIEW_SHAPES") {
        return windowed_side_view_shapes_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_WINDOWED_DUPLICATE_MAP") {
        return windowed_duplicate_map_side_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_DUAL_JOIN") {
        return dual_unbounded_join_fixture(pipeline, path.into()).map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_DUAL_FANIN_END_TIME") {
        return dual_unbounded_fanin_output_time_fixture(
            pipeline, path.into(), ironbeam_core::WindowOutputTime::EndOfWindow,
        ).map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_DUAL_FANIN_LATEST_TIME") {
        return dual_unbounded_fanin_output_time_fixture(
            pipeline, path.into(), ironbeam_core::WindowOutputTime::LatestInPane,
        ).map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_DUAL_SESSION_JOIN") {
        return dual_unbounded_session_join_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_DUAL_SESSION_COGROUP") {
        return dual_unbounded_session_cogroup_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_DUAL_LATE_JOIN") {
        return dual_unbounded_late_join_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_DUAL_POSITIVE_LATE_JOIN") {
        return dual_positive_late_full_join_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_DUAL_LATE_COUNT_JOIN") {
        return late_count_join_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_ACCUMULATING_LATE_JOIN") {
        return accumulating_late_join_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let (Ok(kind), Ok(path)) = (
        std::env::var("IRONBEAM_V5_PRISM_LATE_COUNT_JOIN_KIND"),
        std::env::var("IRONBEAM_V5_PRISM_LATE_COUNT_JOIN_KIND_PATH"),
    ) {
        return late_count_join_kind_fixture(pipeline, &kind, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_SLIDING_LATE_JOIN") {
        return sliding_late_join_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_DUAL_POSITIVE_LATE_END_TIME") {
        return dual_positive_late_join_time_fixture(
            pipeline,
            path.into(),
            ironbeam_core::WindowOutputTime::EndOfWindow,
        )
        .map(|_| ())
        .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_DUAL_POSITIVE_LATE_LATEST_TIME") {
        return dual_positive_late_join_time_fixture(
            pipeline,
            path.into(),
            ironbeam_core::WindowOutputTime::LatestInPane,
        )
        .map(|_| ())
        .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_DUAL_POSITIVE_LATE_COGROUP") {
        return dual_positive_late_cogroup_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_HETEROGENEOUS_LATE_COUNT_COGROUP") {
        return heterogeneous_late_count_cogroup_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let (Ok(kind), Ok(path)) = (
        std::env::var("IRONBEAM_V5_PRISM_DUAL_JOIN_KIND"),
        std::env::var("IRONBEAM_V5_PRISM_DUAL_JOIN_KIND_PATH"),
    ) {
        return dual_unbounded_join_kind_fixture(pipeline, &kind, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_DUAL_COGROUP") {
        return dual_unbounded_cogroup_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_DUAL_FLATTEN") {
        return dual_unbounded_flatten_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_TRIPLE_FLATTEN") {
        return triple_unbounded_flatten_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_TRIPLE_HOMOGENEOUS") {
        return triple_unbounded_homogeneous_cogroup_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_TRIPLE_SESSION") {
        return triple_unbounded_session_homogeneous_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let (Ok(path), Ok(marker)) = (
        std::env::var("IRONBEAM_V5_PRISM_WAIT_CONTRACT"),
        std::env::var("IRONBEAM_V5_PRISM_WAIT_MARKER"),
    ) {
        return bounded_wait_on_fixture(pipeline, path.into(), marker.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let (Ok(path), Ok(first), Ok(second)) = (
        std::env::var("IRONBEAM_V5_PRISM_MULTI_WAIT_CONTRACT"),
        std::env::var("IRONBEAM_V5_PRISM_MULTI_WAIT_FIRST"),
        std::env::var("IRONBEAM_V5_PRISM_MULTI_WAIT_SECOND"),
    ) {
        return bounded_multi_wait_fixture(pipeline, path.into(), first.into(), second.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_WAIT_FAILURE") {
        return failing_wait_on_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_BAD_DYNOP") {
        return invalid_dynop_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_PANICKING_MAP") {
        return panicking_map_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_PANICKING_SIDE") {
        return panicking_side_input_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_PANICKING_GLOBAL") {
        return panicking_global_combine_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_PANICKING_FOLD") {
        return panicking_keyed_fold_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_BATCH_HELPERS") {
        return bounded_batch_helpers_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_BAD_KEYED_BATCH") {
        return invalid_keyed_batch_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_MULTI_SOURCE_CONTRACT") {
        return bounded_multi_source_fixture(pipeline, path.into()).map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_JOIN_COGROUP_CONTRACT") {
        return bounded_join_cogroup_fixture(pipeline, path.into()).map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_COMBINE_END_TIME") {
        return combine_output_time_fixture(pipeline, path.into(), ironbeam_core::WindowOutputTime::EndOfWindow)
            .map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_COMBINE_LATEST_TIME") {
        return combine_output_time_fixture(pipeline, path.into(), ironbeam_core::WindowOutputTime::LatestInPane)
            .map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_GLOBAL_COUNT_DISCARDING") {
        return global_count_fixture(pipeline, path.into(), false)
            .map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_GLOBAL_KEYED_BATCHES") {
        return global_keyed_batches_fixture(pipeline, path.into())
            .map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_WINDOWED_KEYED_BATCHES") {
        return windowed_keyed_batches_fixture(pipeline, path.into())
            .map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_GLOBAL_COUNT_ACCUMULATING") {
        return global_count_fixture(pipeline, path.into(), true)
            .map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_GLOBAL_COMBINE_DISCARDING") {
        return global_keyed_combine_fixture(pipeline, path.into(), false)
            .map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_GLOBAL_COMBINE_ACCUMULATING") {
        return global_keyed_combine_fixture(pipeline, path.into(), true)
            .map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_DUAL_LATE_COUNT_COGROUP") {
        return dual_late_count_cogroup_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_DUAL_LATE_COGROUP") {
        return dual_late_cogroup_fixture(pipeline, path.into())
            .map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_JOIN_COGROUP_END_TIME") {
        return bounded_join_cogroup_output_time_fixture(
            pipeline, path.into(), Some(ironbeam_core::WindowOutputTime::EndOfWindow),
        ).map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_JOIN_COGROUP_LATEST_TIME") {
        return bounded_join_cogroup_output_time_fixture(
            pipeline, path.into(), Some(ironbeam_core::WindowOutputTime::LatestInPane),
        ).map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_OFFSET_WINDOW") {
        return offset_window_group_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_SLIDING_WINDOW") {
        return sliding_window_group_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_SESSION_WINDOW") {
        return session_group_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_SESSION_JOIN") {
        return bounded_session_join_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_SESSION_COGROUP") {
        return bounded_session_cogroup_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_PARTITION") {
        return partition_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_PANES") {
        let window = match std::env::var("IRONBEAM_V5_PRISM_PANES_WINDOW").as_deref() {
            Ok("sliding") => PaneFixtureWindow::Sliding,
            Ok("session") => PaneFixtureWindow::Session,
            _ => PaneFixtureWindow::Fixed,
        };
        return pane_metadata_fixture_for(pipeline, path.into(), window)
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_COUNT_STREAM") {
        return count_trigger_stream_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_COUNT_ACCUMULATING") {
        return accumulating_count_trigger_stream_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_COUNT_LATEST") {
        return accumulating_latest_count_trigger_stream_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_ALLOWED_LATENESS") {
        return allowed_lateness_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_LATE_COUNT") {
        return late_count_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_LATE_COUNT_ACCUMULATING") {
        return accumulating_late_count_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_COMBINED_COUNT") {
        return combined_count_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_SLIDING_LATENESS") {
        return sliding_allowed_lateness_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_COUNT_ACCUMULATING_BOUNDED") {
        return pane_metadata_fixture_with_accumulating_count(
            pipeline,
            path.into(),
            PaneFixtureWindow::Sliding,
        )
        .map(|_| ())
        .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_COUNT_BOUNDED") {
        let window = if std::env::var("IRONBEAM_V5_PRISM_COUNT_WINDOW").as_deref() == Ok("sliding") {
            PaneFixtureWindow::Sliding
        } else {
            PaneFixtureWindow::Fixed
        };
        return pane_metadata_fixture_with_trigger(
            pipeline,
            path.into(),
            window,
            true,
        )
        .map(|_| ())
        .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_MIXED_JOIN_COGROUP") {
        return mixed_boundedness_join_cogroup_fixture(pipeline, path.into()).map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_MIXED_HOMOGENEOUS") {
        return mixed_homogeneous_cogroup_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_MIXED_FLATTEN") {
        return mixed_flatten_fixture(pipeline, path.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_PRODUCTION_JOIN_COGROUP") {
        return production_join_cogroup_fixture(pipeline, path.into()).map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_GROUP_TIME") {
        return group_time_fixture(pipeline, path.into()).map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_GROUP_END_TIME") {
        return group_end_time_fixture(pipeline, path.into()).map(|_| ()).map_err(Into::into);
    }
    if let Ok(path) = std::env::var("IRONBEAM_V5_PRISM_GROUP_LATEST_TIME") {
        return group_latest_time_fixture(pipeline, path.into()).map(|_| ()).map_err(Into::into);
    }
    if let Ok(directory) = std::env::var("IRONBEAM_V5_PRISM_PRODUCTION_CAPTURE") {
        return production_stream_fixture(pipeline, directory).map_err(Into::into);
    }
    if let Ok(directory) = std::env::var("IRONBEAM_V5_PRISM_FAIL_AFTER_STAGE") {
        return finalization_failure_fixture(pipeline, directory).map_err(Into::into);
    }
    if let (Ok(nonempty), Ok(empty)) = (
        std::env::var("IRONBEAM_V5_PRISM_GLOBAL_FOLD_NONEMPTY"),
        std::env::var("IRONBEAM_V5_PRISM_GLOBAL_FOLD_EMPTY"),
    ) {
        return global_fold_fixture(pipeline, nonempty.into(), empty.into()).map_err(Into::into);
    }
    if let (Ok(nonempty), Ok(empty)) = (
        std::env::var("IRONBEAM_V5_PRISM_GLOBAL_VECTOR_NONEMPTY"),
        std::env::var("IRONBEAM_V5_PRISM_GLOBAL_VECTOR_EMPTY"),
    ) {
        return global_vector_fixture(pipeline, nonempty.into(), empty.into()).map_err(Into::into);
    }
    if let Ok(capture) = std::env::var("IRONBEAM_V5_PRISM_ORDERED_KEYED") {
        return ordered_keyed_fixture(pipeline, capture.into()).map(|_| ()).map_err(Into::into);
    }
    if let Ok(capture) = std::env::var("IRONBEAM_V5_PRISM_ORDERED_STREAM") {
        return streaming_ordered_keyed_fixture(pipeline, capture.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let (Ok(nonempty), Ok(empty)) = (
        std::env::var("IRONBEAM_V5_PRISM_ORDERED_GLOBAL_NONEMPTY"),
        std::env::var("IRONBEAM_V5_PRISM_ORDERED_GLOBAL_EMPTY"),
    ) {
        return ordered_global_fixture(pipeline, nonempty.into(), empty.into()).map(|_| ()).map_err(Into::into);
    }
    if let Ok(capture) = std::env::var("IRONBEAM_V5_PRISM_ORDERED_DUPLICATE") {
        return ordered_global_duplicate_fixture(pipeline, capture.into()).map(|_| ()).map_err(Into::into);
    }
    if let Ok(capture) = std::env::var("IRONBEAM_V5_PRISM_ORDERED_KEYED_DUPLICATE") {
        return ordered_keyed_duplicate_fixture(pipeline, capture.into()).map(|_| ()).map_err(Into::into);
    }
    if let Ok(capture) = std::env::var("IRONBEAM_V5_PRISM_ORDERED_BATCH_DUPLICATE") {
        return ordered_batch_duplicate_fixture(pipeline, capture.into()).map(|_| ()).map_err(Into::into);
    }
    if let (Ok(keyed), Ok(global)) = (
        std::env::var("IRONBEAM_V5_PRISM_LATEST_TIE_KEYED"),
        std::env::var("IRONBEAM_V5_PRISM_LATEST_TIE_GLOBAL"),
    ) {
        return latest_tie_fixture(pipeline, keyed.into(), global.into()).map(|_| ()).map_err(Into::into);
    }
    if let Ok(capture) = std::env::var("IRONBEAM_V5_PRISM_LATEST_TIE_DUPLICATE_KEYED") {
        return latest_tie_duplicate_fixture(pipeline, capture.into(), true).map(|_| ()).map_err(Into::into);
    }
    if let Ok(capture) = std::env::var("IRONBEAM_V5_PRISM_LATEST_TIE_DUPLICATE_GLOBAL") {
        return latest_tie_duplicate_fixture(pipeline, capture.into(), false).map(|_| ()).map_err(Into::into);
    }
    if let Ok(capture) = std::env::var("IRONBEAM_V5_PRISM_LATEST_TIE_EMPTY") {
        return latest_tie_empty_fixture(pipeline, capture.into()).map(|_| ()).map_err(Into::into);
    }
    if let Ok(directory) = std::env::var("IRONBEAM_V5_PRISM_STREAM_GROUP") {
        return streaming_group_fixture(pipeline, directory.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(directory) = std::env::var("IRONBEAM_V5_PRISM_STREAM_PARTITION") {
        return streaming_partition_fixture(pipeline, directory.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(directory) = std::env::var("IRONBEAM_V5_PRISM_STREAM_SLIDE") {
        return streaming_sliding_group_fixture(pipeline, directory.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(directory) = std::env::var("IRONBEAM_V5_PRISM_STREAM_SESSION") {
        return streaming_session_group_fixture(pipeline, directory.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(directory) = std::env::var("IRONBEAM_V5_PRISM_STREAM_ORDERED_BATCH") {
        return streaming_ordered_batch_fixture(pipeline, directory.into())
            .map(|_| ())
            .map_err(Into::into);
    }
    if let Ok(capture_path) = std::env::var("IRONBEAM_V5_PRISM_STREAM_CAPTURE") {
        let sink_path = capture_path.clone();
        pipeline.test_stream_source("stream-events", vec![
            TestStreamEvent::Elements(vec![(1_i32, 1_000_000)]),
            TestStreamEvent::AdvanceWatermark(2_000_000),
            TestStreamEvent::AdvanceProcessingTime(1_000_000),
            TestStreamEvent::Elements(vec![(2_i32, 3_000_000)]),
            TestStreamEvent::AdvanceWatermark(4_000_000),
        ])?
        .apply_transform_with_semantic_named(
            "stream-double",
            1,
            "prism-proof/stream-double",
            Version { major: 1, minor: 0 },
            |values| values.into_iter().map(|value| value * 2).collect(),
        )?
        .map_batches_named("stream-batch-adjust", 1, |values| vec![values[0] + 1])?
        .apply_dyn_transform_with_semantic_named::<i32>(
            "stream-expand",
            1,
            "prism-proof/stream-expand",
            Version { major: 1, minor: 0 },
            Arc::new(ExpandSingleton),
        )?
        .map_named("stream-rows", |id| UserRow { id: id + 10, label: "stream".into() })?
        .fixed_windows("stream-windows", 2_000_000)?
        .with_window_named("stream-window-context")?
        .map_named("stream-label", |(window, mut row)| {
            row.label = format!("{}..{}", window.start_ns, window.end_ns);
            row
        })?
        .sink_named_with_config(
            "stream-capture",
            SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
            format!("file:{capture_path}"),
            move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: sink_path.clone() })),
        )?;
        return Ok(());
    }
    let input_path = std::env::var("IRONBEAM_V5_PRISM_INPUT")?;
    let capture_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE")?;
    let right_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_RIGHT")?;
    let merged_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_MERGED")?;
    let timestamp_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_TIME")?;
    let window_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_WINDOW")?;
    let window_grouped_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_WINDOW_GROUPED")?;
    let jsonl_capture_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_JSONL")?;
    let object_capture_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_OBJECT")?;
    let avro_capture_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_AVRO")?;
    let csv_capture_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_CSV")?;
    let xml_capture_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_XML")?;
    let parquet_capture_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_PARQUET")?;
    let proto_capture_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_PROTO")?;
    let reshuffle_capture_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_RESHUFFLE")?;
    let grouped_capture_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_GROUPED")?;
    let latest_capture_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_LATEST")?;
    let large_group_capture_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_LARGE_GROUP")?;
    let wait_capture_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_WAIT")?;
    let wait_marker = std::path::PathBuf::from(std::env::var("IRONBEAM_V5_PRISM_WAIT_MARKER")?);
    let join_window_capture_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_JOIN_WINDOW")?;
    let side_capture_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_SIDE")?;
    let join_capture_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_JOIN")?;
    let cogrouped_capture_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_COGROUPED")?;
    let cogroup_three_capture_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_COGROUP_THREE")?;
    let cogroup_window_capture_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_COGROUP_WINDOW")?;
    let cogroup_ten_capture_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_COGROUP_TEN")?;
    let keyed_fanout_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_KEYED_FANOUT")?;
    let jsonl_input_path = std::env::var("IRONBEAM_V5_PRISM_JSONL_INPUT")?;
    let object_root = std::env::var("IRONBEAM_V5_PRISM_OBJECT_ROOT")?;
    let avro_input_path = std::env::var("IRONBEAM_V5_PRISM_AVRO_INPUT")?;
    let csv_input_path = std::env::var("IRONBEAM_V5_PRISM_CSV_INPUT")?;
    let xml_input_path = std::env::var("IRONBEAM_V5_PRISM_XML_INPUT")?;
    let parquet_input_path = std::env::var("IRONBEAM_V5_PRISM_PARQUET_INPUT")?;
    let proto_input_path = std::env::var("IRONBEAM_V5_PRISM_PROTO_INPUT")?;
    let proto_output_path = std::env::var("IRONBEAM_V5_PRISM_PROTO_OUTPUT")?;
    let tfrecord_input_path = std::env::var("IRONBEAM_V5_PRISM_TFRECORD_INPUT")?;
    let tfrecord_capture_path = std::env::var("IRONBEAM_V5_PRISM_CAPTURE_TFRECORD")?;
    let tfrecord_output_path = std::env::var("IRONBEAM_V5_PRISM_TFRECORD_OUTPUT")?;
    let open_count_path = std::env::var("IRONBEAM_V5_PRISM_SOURCE_OPENS")?;
    let finalization_dir = std::env::var("IRONBEAM_V5_PRISM_FINALIZATION_DIR")?;
    let tfrecord_finalization_dir = std::env::var("IRONBEAM_V5_PRISM_TFRECORD_FINALIZATION_DIR")?;
    let avro_finalization_dir = std::env::var("IRONBEAM_V5_PRISM_AVRO_FINALIZATION_DIR")?;
    let msgpack_finalization_dir = std::env::var("IRONBEAM_V5_PRISM_MSGPACK_FINALIZATION_DIR")?;
    let jsonl_finalization_dir = std::env::var("IRONBEAM_V5_PRISM_JSONL_FINALIZATION_DIR")?;
    let csv_finalization_dir = std::env::var("IRONBEAM_V5_PRISM_CSV_FINALIZATION_DIR")?;
    let xml_finalization_dir = std::env::var("IRONBEAM_V5_PRISM_XML_FINALIZATION_DIR")?;
    let parquet_finalization_dir = std::env::var("IRONBEAM_V5_PRISM_PARQUET_FINALIZATION_DIR")?;
    let cbor_finalization_dir = std::env::var("IRONBEAM_V5_PRISM_CBOR_FINALIZATION_DIR")?;
    let protobuf_finalization_dir = std::env::var("IRONBEAM_V5_PRISM_PROTOBUF_FINALIZATION_DIR")?;
    let digest = format!("sha256-v1:{}", hex::encode(Sha256::digest(std::fs::read(&input_path)?)));
    let source = pipeline
        .bounded_source_with_semantic(
            "source", "tests/prism-file-source",
            SourceProgress { stable_split_id: true, replay: Replay::AtLeastOnce, reports_event_time: false },
            digest, Some(3),
            move || {
                use std::io::Write;
                writeln!(std::fs::OpenOptions::new().create(true).append(true)
                    .open(&open_count_path).map_err(|error| Error::incompatible(error.to_string()))?, "open")
                    .map_err(|error| Error::incompatible(error.to_string()))?;
                let text = std::fs::read_to_string(&input_path)
                    .map_err(|error| Error::incompatible(error.to_string()))?;
                let rows = text.lines().map(|line| {
                    let (id, label) = line.split_once(',')
                        .ok_or_else(|| Error::incompatible("invalid input row"))?;
                    let id = id.parse().map_err(|_| Error::incompatible("invalid row id"))?;
                    Ok(UserRow { id, label: label.into() })
                }).collect::<Result<Vec<_>, Error>>()?;
                Ok(Box::new(rows.into_iter().map(Ok)))
            },
        )?;
    finalization_probe_sink(
        &source.reshuffle_named("finalization-boundary")?,
        "capture-finalization", &finalization_dir,
    )?;
    let tfrecord_final = source.map_named("tfrecord-final-encode", |row| {
        row.encode().expect("fixture row coder")
    })?.reshuffle_named("tfrecord-finalization-boundary")?;
    ironbeam_io_tfrecord::tfrecord_fragment_sink(
        &tfrecord_final, "capture-tfrecord-final", &tfrecord_finalization_dir,
        &tfrecord_finalization_dir, "prism-run-fixture",
    )?;
    let avro_final = source.reshuffle_named("avro-finalization-boundary")?;
    let avro_schema = r#"{"type":"record","name":"UserRow","fields":[{"name":"id","type":"int"},{"name":"label","type":"string"}]}"#;
    ironbeam_io_avro::avro_fragment_sink(
        &avro_final, "capture-avro-final", &avro_finalization_dir,
        &avro_finalization_dir, "prism-run-fixture", avro_schema,
    )?;
    ironbeam_io_msgpack::msgpack_fragment_sink(
        &source.reshuffle_named("msgpack-finalization-boundary")?,
        "capture-msgpack-final", &msgpack_finalization_dir,
        &msgpack_finalization_dir, "prism-run-fixture",
    )?;
    ironbeam_io_jsonl::jsonl_fragment_sink(
        &source.reshuffle_named("jsonl-finalization-boundary")?,
        "capture-jsonl-final", &jsonl_finalization_dir,
        &jsonl_finalization_dir, "prism-run-fixture",
    )?;
    ironbeam_io_csv::csv_fragment_sink(
        &source.reshuffle_named("csv-finalization-boundary")?,
        "capture-csv-final", &csv_finalization_dir,
        &csv_finalization_dir, "prism-run-fixture",
    )?;
    ironbeam_io_xml::xml_fragment_sink(
        &source.reshuffle_named("xml-finalization-boundary")?,
        "capture-xml-final", &xml_finalization_dir,
        &xml_finalization_dir, "prism-run-fixture",
    )?;
    ironbeam_io_parquet::parquet_fragment_sink(
        &source.reshuffle_named("parquet-finalization-boundary")?,
        "capture-parquet-final", &parquet_finalization_dir,
        &parquet_finalization_dir, "prism-run-fixture",
    )?;
    ironbeam_io_cbor::cbor_fragment_sink(
        &source.reshuffle_named("cbor-finalization-boundary")?,
        "capture-cbor-final", &cbor_finalization_dir,
        &cbor_finalization_dir, "prism-run-fixture",
    )?;
    let protobuf_final = source.map_named("protobuf-final-encode", |row| ProtoRow {
        id: row.id, label: row.label,
    })?.reshuffle_named("protobuf-finalization-boundary")?;
    ironbeam_io_protobuf::protobuf_fragment_sink(
        &protobuf_final, "capture-protobuf-final", &protobuf_finalization_dir,
        &protobuf_finalization_dir, "prism-run-fixture",
    )?;
    let signal = completion_signal(pipeline, wait_marker.clone())?
        .assign_timestamps_named("wait-signal-time", |_| 1_000_000)?
        .fixed_windows_with_offset("wait-signal-window", 2_000_000, 0)?;
    let wait_sink_path = wait_capture_path.clone();
    source.assign_timestamps_named("wait-main-time", |row| i64::from(row.id) * 1_000_000)?
        .fixed_windows_with_offset("wait-main-window", 2_000_000, 1_000_000)?
        .wait_on_named("wait-for-signal", &signal)?
        .with_window_named("wait-window-context")?
        .map_named("wait-row", |(window, mut row)| {
            row.label = format!("{}..{}:{}", window.start_ns, window.end_ns, row.label);
            row
        })?
        .sink_named_with_config(
            "capture-wait",
            SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
            format!("file:{wait_capture_path}"),
            move || Ok(Box::new(CompletionCheckedSink {
                inner: CaptureSink { rows: Vec::new(), path: wait_sink_path.clone() },
                marker: wait_marker.clone(),
            })),
        )?;
    let extra = pipeline.from_vec("source-extra", vec![UserRow { id: 4, label: "d".into() }])?;
    let jsonl = ironbeam_io_jsonl::jsonl_source::<UserRow>(pipeline, "jsonl-source", &jsonl_input_path)?;
    let jsonl_output = jsonl.map_named("jsonl-add-ten", |mut row| { row.id += 10; row })?;
    ironbeam_io_jsonl::jsonl_append_sink(&jsonl_output, "capture-jsonl", &jsonl_capture_path)?;
    let proto_source = ironbeam_io_protobuf::protobuf_source::<ProtoRow>(pipeline, "proto-source", &proto_input_path)?;
    ironbeam_io_protobuf::protobuf_append_sink(&proto_source, "write-proto-append", &proto_output_path)?;
    let proto_rows = proto_source.map_named("proto-to-user", |row| UserRow { id: row.id + 10, label: row.label })?;
    let proto_sink_path = proto_capture_path.clone();
    proto_rows.sink_named_with_config(
        "capture-proto",
        SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
        format!("file:{proto_capture_path}"),
        move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: proto_sink_path.clone() })),
    )?;
    let tfrecord_sink_path = tfrecord_capture_path.clone();
    let tfrecord_rows = ironbeam_io_tfrecord::tfrecord_source(pipeline, "tfrecord-source", &tfrecord_input_path)?;
    ironbeam_io_tfrecord::tfrecord_append_sink(&tfrecord_rows, "write-tfrecord-append", &tfrecord_output_path)?;
    tfrecord_rows
        .map_named("tfrecord-to-user", |bytes| {
            let row = <ProtoRow as prost_v5::Message>::decode(bytes.as_slice()).unwrap();
            UserRow { id: row.id + 40, label: row.label }
        })?
        .sink_named_with_config(
            "capture-tfrecord",
            SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
            format!("file:{tfrecord_capture_path}"),
            move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: tfrecord_sink_path.clone() })),
        )?;
    let object_sink_path = object_capture_path.clone();
    let mut objects = ObjectStores::new();
    objects.register("file", LocalObjectIO::new(object_root)?)?;
    ironbeam_io_jsonl::cloud_jsonl_uri_source::<UserRow>(
        pipeline,
        "object-source",
        &objects,
        "file://bucket/input/row.jsonl",
    )?
    .map_named("object-add-ten", |mut row| { row.id += 10; row })?
    .sink_named_with_config(
        "capture-object",
        SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
        format!("file:{object_capture_path}"),
        move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: object_sink_path.clone() })),
    )?;
    let avro_sink_path = avro_capture_path.clone();
    ironbeam_io_avro::avro_source::<UserRow>(pipeline, "avro-source", avro_input_path)?
        .map_named("avro-add-twenty", |mut row| { row.id += 20; row })?
        .sink_named_with_config(
            "capture-avro",
            SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
            format!("file:{avro_capture_path}"),
            move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: avro_sink_path.clone() })),
        )?;
    let csv_sink_path = csv_capture_path.clone();
    ironbeam_io_csv::csv_source::<UserRow>(pipeline, "csv-source", csv_input_path)?
        .map_named("csv-add-thirty", |mut row| { row.id += 30; row })?
        .sink_named_with_config(
            "capture-csv",
            SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
            format!("file:{csv_capture_path}"),
            move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: csv_sink_path.clone() })),
        )?;
    let xml_sink_path = xml_capture_path.clone();
    ironbeam_io_xml::xml_source::<UserRow>(pipeline, "xml-source", xml_input_path)?
        .map_named("xml-add-fifty", |mut row| { row.id += 50; row })?
        .sink_named_with_config(
            "capture-xml",
            SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
            format!("file:{xml_capture_path}"),
            move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: xml_sink_path.clone() })),
        )?;
    let parquet_sink_path = parquet_capture_path.clone();
    ironbeam_io_parquet::parquet_source::<UserRow>(pipeline, "parquet-source", parquet_input_path)?
        .map_named("parquet-add-sixty", |mut row| { row.id += 60; row })?
        .sink_named_with_config(
            "capture-parquet",
            SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
            format!("file:{parquet_capture_path}"),
            move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: parquet_sink_path.clone() })),
        )?;
    let pairs = pipeline.from_vec("group-pairs", vec![("a".to_owned(), 1_i32),
        ("b".to_owned(), 2), ("a".to_owned(), 3)])?;
    let grouped_sink_path = grouped_capture_path.clone();
    pairs.group_by_key_named("group-pairs-by-key")?
        .map_named("group-count", |(label, values)| UserRow { id: values.len() as i32, label })?
        .sink_named_with_config(
            "capture-grouped",
            SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
            format!("file:{grouped_capture_path}"),
            move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: grouped_sink_path.clone() })),
        )?;
    let latest_sink_path = latest_capture_path.clone();
    pairs.assign_timestamps_named("latest-time", |(_, value)| i64::from(*value) * 1_000_000)?
        .fixed_windows("latest-window", 4_000_000)?
        .latest_per_key_named("latest-per-key")?
        .reify_timestamps_named("latest-result-time")?
        .with_window_named("latest-result-window")?
        .map_named("latest-result-row", |(window, (timestamp, (key, value)))| UserRow {
            id: value,
            label: format!("{key}:{timestamp}:{}..{}", window.start_ns, window.end_ns),
        })?
        .sink_named_with_config(
            "capture-latest",
            SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
            format!("file:{latest_capture_path}"),
            move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: latest_sink_path.clone() })),
        )?;
    let large_group_sink_path = large_group_capture_path.clone();
    pipeline.from_vec("large-group-pairs", large_group_rows())?
        .group_by_key_named("large-group-by-key")?
        .map_named("large-group-summary", |(key, values)| {
            let (id, bytes, digest) = large_group_summary(values);
            UserRow { id, label: format!("{key}:{bytes}:{digest}") }
        })?
        .sink_named_with_config(
            "capture-large-group",
            SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
            format!("file:{large_group_capture_path}"),
            move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: large_group_sink_path.clone() })),
        )?;
    let keyed_fanout_sink_path = keyed_fanout_path.clone();
    pairs.map_named("keyed-fanout", |(label, id)| UserRow { id, label })?
        .sink_named_with_config(
            "capture-keyed-fanout",
            SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
            format!("file:{keyed_fanout_path}"),
            move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: keyed_fanout_sink_path.clone() })),
        )?;
    let join_right = pipeline.from_vec("join-right-source", vec![
        ("a".to_owned(), 10_i32), ("a".to_owned(), 20), ("c".to_owned(), 30),
    ])?;
    let join_inner = pairs.join_inner_named("join-inner", &join_right)?
        .map_named("join-inner-row", |(key, (left, right))| {
            let (id, label) = (left + right, format!("inner:{key}:{left}:{right}"));
            UserRow { id, label }
        })?;
    let join_left = pairs.join_left_named("join-left", &join_right)?
        .map_named("join-left-row", |(key, (left, right))| {
            let (id, label) = (left + right.unwrap_or_default(), format!("left:{key}:{left}:{right:?}"));
            UserRow { id, label }
        })?;
    let join_right_rows = pairs.join_right_named("join-right", &join_right)?
        .map_named("join-right-row", |(key, (left, right))| {
            let (id, label) = (left.unwrap_or_default() + right, format!("right:{key}:{left:?}:{right}"));
            UserRow { id, label }
        })?;
    let join_full = pairs.join_full_named("join-full", &join_right)?
        .map_named("join-full-row", |(key, (left, right))| {
            let (id, label) = (left.unwrap_or_default() + right.unwrap_or_default(), format!("full:{key}:{left:?}:{right:?}"));
            UserRow { id, label }
        })?;
    let join_sink_path = join_capture_path.clone();
    join_inner.flatten_named("join-all", &[join_left, join_right_rows, join_full])?
        .sink_named_with_config(
            "capture-join",
            SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
            format!("file:{join_capture_path}"),
            move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: join_sink_path.clone() })),
        )?;
    let join_window_left = pairs
        .assign_timestamps_named("join-window-left-time", |pair| i64::from(pair.1) * 1_000_000)?
        .fixed_windows_with_offset("join-window-left", 2_000_000, 1_000_000)?;
    let join_window_right = join_right
        .assign_timestamps_named("join-window-right-time", |pair| if pair.1 == 10 { 1_000_000 } else { 3_000_000 })?
        .fixed_windows_with_offset("join-window-right", 2_000_000, 1_000_000)?;
    let join_window_rows = join_window_left.join_full_named("join-window", &join_window_right)?
        .reify_timestamps_named("join-window-stamp")?
        .with_window_named("join-window-context")?
        .map_named("join-window-row", |(window, (timestamp, (key, (left, right))))| UserRow {
            id: left.unwrap_or_default() + right.unwrap_or_default(),
            label: format!("{key}:{left:?}:{right:?}:{timestamp}:{}..{}", window.start_ns, window.end_ns),
        })?;
    let join_window_sink_path = join_window_capture_path.clone();
    join_window_rows.sink_named_with_config(
        "capture-join-window",
        SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
        format!("file:{join_window_capture_path}"),
        move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: join_window_sink_path.clone() })),
    )?;
    let side_values = pipeline.from_vec("side-values", vec![10_i32, 20])?
        .assign_timestamps_named("side-time", |value| (i64::from(*value) / 10 * 2 - 1) * 1_000_000)?
        .fixed_windows_with_offset("side-window", 2_000_000, 1_000_000)?;
    let side_main = pairs.assign_timestamps_named("side-main-time", |pair| i64::from(pair.1) * 1_000_000)?
        .fixed_windows_with_offset("side-main-window", 2_000_000, 1_000_000)?;
    let side_rows = side_main.map_with_side_named("add-side", &side_values, |(label, id), values| UserRow {
        id: id + values.iter().sum::<i32>(), label,
    })?;
    let side_sink_path = side_capture_path.clone();
    side_rows.sink_named_with_config(
        "capture-side",
        SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
        format!("file:{side_capture_path}"),
        move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: side_sink_path.clone() })),
    )?;
    let cogroup_right = pipeline.from_vec("cogroup-right", vec![
        ("a".to_owned(), true), ("b".to_owned(), false),
    ])?;
    let cogrouped_sink_path = cogrouped_capture_path.clone();
    ironbeam_core::cogroup_by_key_2_named("cogroup-pairs", &pairs, &cogroup_right)?
        .map_named("cogroup-count", |(key, (left, right))| UserRow {
            id: left.iter().sum(),
            label: format!("{key}:{}:{}", left.len(), right.len()),
        })?
        .sink_named_with_config(
            "capture-cogrouped",
            SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
            format!("file:{cogrouped_capture_path}"),
            move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: cogrouped_sink_path.clone() })),
        )?;
    let cogroup_third = pipeline.from_vec("cogroup-third", vec![
        ("a".to_owned(), "x".to_owned()), ("a".to_owned(), "y".to_owned()),
        ("b".to_owned(), "z".to_owned()),
    ])?;
    let cogroup_three_sink_path = cogroup_three_capture_path.clone();
    ironbeam_core::cogroup_by_key_3_named("cogroup-three", &pairs, &cogroup_right, &cogroup_third)?
        .map_named("cogroup-three-count", |(key, (left, right, third))| UserRow {
            id: left.iter().sum::<i32>() + right.len() as i32 + third.len() as i32,
            label: format!("{key}:{}:{}:{}", left.len(), right.len(), third.len()),
        })?
        .sink_named_with_config(
            "capture-cogroup-three",
            SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
            format!("file:{cogroup_three_capture_path}"),
            move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: cogroup_three_sink_path.clone() })),
        )?;
    let cogroup_window_left = pairs
        .assign_timestamps_named("cogroup-window-left-time", |pair| i64::from(pair.1) * 1_000_000)?
        .fixed_windows_with_offset("cogroup-window-left", 2_000_000, 1_000_000)?;
    let cogroup_window_right = cogroup_right
        .assign_timestamps_named("cogroup-window-right-time", |pair| if pair.0 == "a" { 1_000_000 } else { 3_000_000 })?
        .fixed_windows_with_offset("cogroup-window-right", 2_000_000, 1_000_000)?;
    let cogroup_window_sink_path = cogroup_window_capture_path.clone();
    ironbeam_core::cogroup_by_key_2_named("cogroup-window", &cogroup_window_left, &cogroup_window_right)?
        .with_window_named("cogroup-window-metadata")?
        .map_named("cogroup-window-row", |(window, (key, (left, right)))| UserRow {
            id: left.iter().sum(),
            label: format!("{key}:{}:{}:{}..{}", left.len(), right.len(), window.start_ns, window.end_ns),
        })?
        .sink_named_with_config(
            "capture-cogroup-window",
            SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
            format!("file:{cogroup_window_capture_path}"),
            move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: cogroup_window_sink_path.clone() })),
        )?;
    let ten_inputs = (0..10).map(|index| pipeline.from_vec(
        &format!("cogroup-ten-source-{index}"), vec![("a".to_owned(), index)]))
        .collect::<Result<Vec<_>, _>>()?;
    let cogroup_ten_sink_path = cogroup_ten_capture_path.clone();
    ironbeam_core::cogroup_by_key_10_named("cogroup-ten",
        &ten_inputs[0], &ten_inputs[1], &ten_inputs[2], &ten_inputs[3], &ten_inputs[4],
        &ten_inputs[5], &ten_inputs[6], &ten_inputs[7], &ten_inputs[8], &ten_inputs[9])?
        .map_named("cogroup-ten-sum", |(key, (a, b, c, d, e, f, g, h, i, j))| UserRow {
            id: [a, b, c, d, e, f, g, h, i, j].into_iter().flatten().sum(),
            label: key,
        })?
        .sink_named_with_config(
            "capture-cogroup-ten",
            SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
            format!("file:{cogroup_ten_capture_path}"),
            move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: cogroup_ten_sink_path.clone() })),
        )?;
    let left_sink_path = capture_path.clone();
    let doubled = source.map_named("double", |mut row| { row.id *= 2; row })?;
    let reshuffle_sink_path = reshuffle_capture_path.clone();
    source.reshuffle_named("redistribute")?
        .map_named("reshuffle-mark", |mut row| { row.label.push_str("-shuffled"); row })?
        .sink_named_with_config(
            "capture-reshuffle",
            SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
            format!("file:{reshuffle_capture_path}"),
            move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: reshuffle_sink_path.clone() })),
        )?;
    doubled
        .sink_named_with_config(
            "capture",
            SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
            format!("file:{capture_path}"),
            move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: left_sink_path.clone() })),
        )?;
    let right_sink_path = right_path.clone();
    let added = source.map_named("add-hundred", |mut row| { row.id += 100; row })?;
    added
        .sink_named_with_config(
            "capture-right",
            SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
            format!("file:{right_path}"),
            move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: right_sink_path.clone() })),
        )?;
    let merged_sink_path = merged_path.clone();
    doubled.flatten_named("merged", &[added, extra])?
        .sink_named_with_config(
            "capture-merged",
            SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
            format!("file:{merged_path}"),
            move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: merged_sink_path.clone() })),
        )?;
    let time_sink_path = timestamp_path.clone();
    source.assign_timestamps_named("event-time", |row| i64::from(row.id) * 1_000_000)?
        .reify_timestamps_named("reify-time")?
        .sink_named_with_config(
            "capture-time",
            SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
            format!("file:{timestamp_path}"),
            move || Ok(Box::new(TimestampSink { rows: Vec::new(), path: time_sink_path.clone() })),
        )?;
    let window_sink_path = window_path.clone();
    source.assign_timestamps_named("window-time", |row| i64::from(row.id) * 1_000_000)?
        .fixed_windows_with_offset("fixed-window", 2_000_000, 1_000_000)?
        .reshuffle_named("window-redistribute")?
        .with_window_named("window-metadata")?
        .map_named("window-row", |(window, row)| UserRow {
            id: row.id,
            label: format!("{}..{}:{}", window.start_ns, window.end_ns, row.label),
        })?
        .sink_named_with_config(
            "capture-window",
            SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
            format!("file:{window_path}"),
            move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: window_sink_path.clone() })),
        )?;
    let window_grouped_sink_path = window_grouped_path.clone();
    source.map_named("group-window-pairs", |row| ("all".to_owned(), row.id))?
        .assign_timestamps_named("group-window-time", |pair| i64::from(pair.1) * 1_000_000)?
        .fixed_windows_with_offset("group-window-set", 2_000_000, 1_000_000)?
        .group_by_key_named("group-window-gbk")?
        .with_window_named("group-window-metadata")?
        .map_named("group-window-row", |(window, (key, values))| UserRow {
            id: values.len() as i32,
            label: format!("{}..{}:{key}", window.start_ns, window.end_ns),
        })?
        .sink_named_with_config(
            "capture-window-grouped",
            SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
            format!("file:{window_grouped_path}"),
            move || Ok(Box::new(CaptureSink { rows: Vec::new(), path: window_grouped_sink_path.clone() })),
        )?;
    Ok(())
    }, "prism-run-fixture").await?;
    Ok(())
}
