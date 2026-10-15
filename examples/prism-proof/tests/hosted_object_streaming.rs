//! Format readers through the hosted object adapter's sequential-read contract.

use ironbeam_cloud_object_store::RemoteObjectIO;
use ironbeam_cloud_objects::ObjectStores;
use ironbeam_coder_postcard::impl_postcard_element;
use ironbeam_core::{ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_io_avro::{avro_sink, cloud_avro_streaming_uri_source};
use ironbeam_io_parquet::{cloud_parquet_streaming_uri_source, parquet_sink};
use object_store::{chunked::ChunkedStore, memory::InMemory, ObjectStore};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct Record {
    id: i32,
    label: String,
}

impl_postcard_element!(Record, "tests/hosted-object-record/v1");

const AVRO_SCHEMA: &str = r#"{"type":"record","name":"Record","fields":[{"name":"id","type":"int"},{"name":"label","type":"string"}]}"#;

#[test]
fn hosted_adapter_streams_avro_and_parquet_into_direct() {
    let directory = tempfile::tempdir().unwrap();
    let rows = vec![
        Record {
            id: 7,
            label: "Grace".into(),
        },
        Record {
            id: 8,
            label: "Ada".into(),
        },
    ];
    let avro_path = directory.path().join("rows.avro");
    let parquet_path = directory.path().join("rows.parquet");

    let avro_seed = Pipeline::default();
    let input = avro_seed.from_vec("avro-seed", rows.clone()).unwrap();
    let sink = avro_sink(&input, "write-avro", &avro_path, AVRO_SCHEMA).unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();

    let parquet_seed = Pipeline::default();
    let input = parquet_seed.from_vec("parquet-seed", rows.clone()).unwrap();
    let sink = parquet_sink(&input, "write-parquet", &parquet_path).unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();

    let mut stores = ObjectStores::new();
    stores
        .register(
            "remote",
            RemoteObjectIO::new(|_| {
                let memory = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
                Ok(Arc::new(ChunkedStore::new(memory, 47)) as Arc<dyn ObjectStore>)
            })
            .unwrap(),
        )
        .unwrap();
    let avro_uri = "remote://bucket/rows.avro";
    let parquet_uri = "remote://bucket/rows.parquet";
    stores
        .write(avro_uri, &std::fs::read(avro_path).unwrap())
        .unwrap();
    stores
        .write(parquet_uri, &std::fs::read(parquet_path).unwrap())
        .unwrap();

    let avro = Pipeline::default();
    let avro_output =
        cloud_avro_streaming_uri_source::<Record>(&avro, "read-avro", &stores, avro_uri)
            .unwrap()
            .output("avro-output")
            .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&avro_output, &JobConfig::default())
            .unwrap()
            .0,
        rows
    );

    let parquet = Pipeline::default();
    let parquet_output = cloud_parquet_streaming_uri_source::<Record>(
        &parquet,
        "read-parquet",
        &stores,
        parquet_uri,
    )
    .unwrap()
    .output("parquet-output")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&parquet_output, &JobConfig::default())
            .unwrap()
            .0,
        rows
    );

    stores.write(avro_uri, b"changed").unwrap();
    stores.write(parquet_uri, b"changed").unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&avro_output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
    assert_eq!(
        DirectRunner
            .run_collect(&parquet_output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
}
