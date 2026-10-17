#![cfg(feature = "cloud")]

use ironbeam_cloud_objects::{
    CloudIOError, CloudResult, LocalObjectIO, ObjectIO, ObjectMetadata, ObjectStores,
};
use ironbeam_core::{Element, ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_io_csv::{cloud_csv_streaming_uri_prefix_source, cloud_csv_streaming_uri_source};
use serde::{Deserialize, Serialize};
use std::io::{self, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct Row {
    id: i32,
    name: String,
}

impl Element for Row {
    const TYPE_ID: &'static str = "tests/cloud-csv-row";
    const CODER_ID: &'static str = "tests/cloud-csv-row-json";
}

struct OpenOnly {
    inner: LocalObjectIO,
    opens: AtomicUsize,
    fail_reads: AtomicBool,
}

struct BrokenReader;

impl Read for BrokenReader {
    fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::other("provider stream failed"))
    }
}

impl ObjectIO for OpenOnly {
    fn put_object(&self, bucket: &str, key: &str, data: &[u8]) -> CloudResult<()> {
        self.inner.put_object(bucket, key, data)
    }

    fn open_object(&self, bucket: &str, key: &str) -> CloudResult<Box<dyn Read + Send>> {
        self.opens.fetch_add(1, Ordering::Relaxed);
        if self.fail_reads.load(Ordering::Relaxed) {
            return Ok(Box::new(BrokenReader));
        }
        self.inner.open_object(bucket, key)
    }

    fn get_object(&self, _bucket: &str, _key: &str) -> CloudResult<Vec<u8>> {
        Err(CloudIOError::new(
            ironbeam_cloud_objects::ErrorKind::InvalidInput,
            "whole-object reads are disabled",
        ))
    }

    fn delete_object(&self, bucket: &str, key: &str) -> CloudResult<()> {
        self.inner.delete_object(bucket, key)
    }

    fn list_objects(&self, bucket: &str, prefix: Option<&str>) -> CloudResult<Vec<ObjectMetadata>> {
        self.inner.list_objects(bucket, prefix)
    }

    fn object_exists(&self, bucket: &str, key: &str) -> CloudResult<bool> {
        self.inner.object_exists(bucket, key)
    }

    fn get_metadata(&self, bucket: &str, key: &str) -> CloudResult<ObjectMetadata> {
        self.inner.get_metadata(bucket, key)
    }

    fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
    ) -> CloudResult<()> {
        self.inner
            .copy_object(src_bucket, src_key, dst_bucket, dst_key)
    }
}

fn stores(root: &std::path::Path) -> (ObjectStores, Arc<OpenOnly>) {
    let adapter = Arc::new(OpenOnly {
        inner: LocalObjectIO::new(root).unwrap(),
        opens: AtomicUsize::new(0),
        fail_reads: AtomicBool::new(false),
    });
    let mut stores = ObjectStores::new();
    stores.register("file", Arc::clone(&adapter)).unwrap();
    (stores, adapter)
}

fn collect(output: &ironbeam_core::Output<Row>) -> Vec<Row> {
    DirectRunner
        .run_collect(output, &JobConfig::default())
        .unwrap()
        .0
}

#[test]
fn streaming_csv_reads_without_whole_download_and_rejects_changed_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let (stores, adapter) = stores(directory.path());
    let uri = "file://bucket/data/rows.csv";
    let mut bytes = b"id,name\n1,\"Ada\nLovelace\"\n".to_vec();
    for id in 2..30_002 {
        bytes.extend_from_slice(format!("{id},name-{id}\n").as_bytes());
    }
    stores.write(uri, &bytes).unwrap();
    let pipeline = Pipeline::default();
    let output = cloud_csv_streaming_uri_source::<Row>(&pipeline, "read", &stores, uri)
        .unwrap()
        .output("out")
        .unwrap();
    let rows = collect(&output);
    assert_eq!(rows.len(), 30_001);
    assert_eq!(rows[0].name, "Ada\nLovelace");
    assert_eq!(rows.last().unwrap().id, 30_001);
    assert_eq!(adapter.opens.load(Ordering::Relaxed), 2);

    stores.write(uri, b"id,name\n1,changed\n").unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
}

#[test]
fn streaming_csv_prefix_fixes_sorted_object_set() {
    let directory = tempfile::tempdir().unwrap();
    let (stores, _) = stores(directory.path());
    stores
        .write("file://bucket/data/b.csv", b"id,name\n2,Lin\n")
        .unwrap();
    stores
        .write("file://bucket/data/a.csv", b"id,name\n1,Ada\n")
        .unwrap();
    let pipeline = Pipeline::default();
    let output = cloud_csv_streaming_uri_prefix_source::<Row>(
        &pipeline,
        "read",
        &stores,
        "file://bucket/data/",
    )
    .unwrap()
    .output("out")
    .unwrap();
    assert_eq!(
        collect(&output)
            .into_iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    stores
        .write("file://bucket/data/c.csv", b"id,name\n3,New\n")
        .unwrap();
    assert_eq!(
        collect(&output)
            .into_iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert!(
        cloud_csv_streaming_uri_prefix_source::<Row>(
            &Pipeline::default(),
            "missing",
            &stores,
            "file://bucket/absent/",
        )
        .is_err()
    );
}

#[test]
fn streaming_csv_validates_binding_and_reads_a_single_prefix_object() {
    let directory = tempfile::tempdir().unwrap();
    let (stores, _) = stores(directory.path());
    for uri in [
        "not-a-uri",
        "missing://bucket/data/rows.csv",
        "file://bucket/data/absent.csv",
    ] {
        assert!(
            cloud_csv_streaming_uri_source::<Row>(&Pipeline::default(), "read", &stores, uri)
                .is_err(),
            "{uri} should fail during graph construction"
        );
    }
    assert!(
        cloud_csv_streaming_uri_prefix_source::<Row>(
            &Pipeline::default(),
            "read",
            &stores,
            "missing://bucket/data/",
        )
        .is_err()
    );

    stores
        .write("file://bucket/data/only.csv", b"id,name\n9,Only\n")
        .unwrap();
    let pipeline = Pipeline::default();
    let output = cloud_csv_streaming_uri_prefix_source::<Row>(
        &pipeline,
        "read",
        &stores,
        "file://bucket/data/",
    )
    .unwrap()
    .output("out")
    .unwrap();
    assert_eq!(
        collect(&output),
        [Row {
            id: 9,
            name: "Only".into()
        }]
    );

    stores
        .write("file://bucket/data/no-extension", b"id,name\n10,Plain\n")
        .unwrap();
    let plain = Pipeline::default();
    let output = cloud_csv_streaming_uri_source::<Row>(
        &plain,
        "plain",
        &stores,
        "file://bucket/data/no-extension",
    )
    .unwrap()
    .output("out")
    .unwrap();
    assert_eq!(collect(&output)[0].name, "Plain");
}

#[test]
fn streaming_csv_rejects_provider_read_failure_before_binding() {
    let directory = tempfile::tempdir().unwrap();
    let (stores, adapter) = stores(directory.path());
    let uri = "file://bucket/data/rows.csv";
    stores.write(uri, b"id,name\n1,Ada\n").unwrap();
    adapter.fail_reads.store(true, Ordering::Relaxed);
    let error = cloud_csv_streaming_uri_source::<Row>(&Pipeline::default(), "read", &stores, uri)
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Io);
    assert!(error.to_string().contains("provider stream failed"));
}

#[cfg(feature = "compression-gzip")]
#[test]
fn streaming_csv_decodes_gzip_after_verification() {
    use ironbeam_io_csv::csv_sink;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("rows.csv.gz");
    let seed = Pipeline::default();
    let input = seed
        .from_vec(
            "input",
            vec![Row {
                id: 7,
                name: "Zip".into(),
            }],
        )
        .unwrap();
    DirectRunner
        .run_sink(
            &csv_sink(&input, "write", &path).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let (stores, _) = stores(directory.path().join("objects").as_path());
    let uri = "file://bucket/data/rows.csv.gz";
    stores.write(uri, &std::fs::read(path).unwrap()).unwrap();
    let pipeline = Pipeline::default();
    let output = cloud_csv_streaming_uri_source::<Row>(&pipeline, "read", &stores, uri)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        collect(&output),
        [Row {
            id: 7,
            name: "Zip".into()
        }]
    );
}
