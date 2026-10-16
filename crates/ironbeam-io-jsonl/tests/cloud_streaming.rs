#![cfg(feature = "cloud")]

use ironbeam_cloud_objects::{
    CloudIOError, CloudResult, LocalObjectIO, ObjectIO, ObjectMetadata, ObjectStores,
};
use ironbeam_core::{ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_io_jsonl::{cloud_jsonl_streaming_uri_glob_source, cloud_jsonl_streaming_uri_source};
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct OpenOnly {
    inner: LocalObjectIO,
    opens: AtomicUsize,
}

impl ObjectIO for OpenOnly {
    fn put_object(&self, bucket: &str, key: &str, data: &[u8]) -> CloudResult<()> {
        self.inner.put_object(bucket, key, data)
    }

    fn open_object(&self, bucket: &str, key: &str) -> CloudResult<Box<dyn Read + Send>> {
        self.opens.fetch_add(1, Ordering::Relaxed);
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
    });
    let mut stores = ObjectStores::new();
    stores.register("file", Arc::clone(&adapter)).unwrap();
    (stores, adapter)
}

#[test]
fn streaming_jsonl_reads_without_whole_object_download_and_rejects_changed_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let (stores, adapter) = stores(directory.path());
    let uri = "file://bucket/data/large.jsonl";
    let mut bytes = Vec::new();
    for value in 0..100_000_u32 {
        bytes.extend_from_slice(format!("{value}\n").as_bytes());
    }
    stores.write(uri, &bytes).unwrap();
    let pipeline = Pipeline::default();
    let output = cloud_jsonl_streaming_uri_source::<u32>(&pipeline, "read", &stores, uri)
        .unwrap()
        .output("out")
        .unwrap();
    let rows = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap()
        .0;
    assert_eq!(rows.len(), 100_000);
    assert_eq!(rows.first(), Some(&0));
    assert_eq!(rows.last(), Some(&99_999));
    assert_eq!(adapter.opens.load(Ordering::Relaxed), 2);

    stores.write(uri, b"0\nchanged\n").unwrap();
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
fn streaming_glob_fixes_sorted_object_set_and_reports_line_errors() {
    let directory = tempfile::tempdir().unwrap();
    let (stores, _) = stores(directory.path());
    stores.write("file://bucket/data/b.jsonl", b"2\n").unwrap();
    stores
        .write("file://bucket/data/a.jsonl", b"\n1\n")
        .unwrap();
    let pipeline = Pipeline::default();
    let output = cloud_jsonl_streaming_uri_glob_source::<u32>(
        &pipeline,
        "read",
        &stores,
        "file://bucket/data/*.jsonl",
    )
    .unwrap()
    .output("out")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        [1, 2]
    );
    stores.write("file://bucket/data/c.jsonl", b"3\n").unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        [1, 2]
    );
    assert!(
        cloud_jsonl_streaming_uri_glob_source::<u32>(
            &Pipeline::default(),
            "missing",
            &stores,
            "file://bucket/absent/*.jsonl",
        )
        .is_err()
    );

    let bad = "file://bucket/data/bad.jsonl";
    stores.write(bad, b"1\nnot-json\n").unwrap();
    let pipeline = Pipeline::default();
    let output = cloud_jsonl_streaming_uri_source::<u32>(&pipeline, "bad", &stores, bad)
        .unwrap()
        .output("out")
        .unwrap();
    let error = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .err()
        .unwrap();
    assert!(error.to_string().contains("line 2"));
}

#[cfg(feature = "compression-gzip")]
#[test]
fn streaming_jsonl_decodes_gzip_after_content_verification() {
    let directory = tempfile::tempdir().unwrap();
    let (stores, _) = stores(directory.path());
    let uri = "file://bucket/data/rows.jsonl.gz";
    let compressed = ironbeam_compression::write_to_vec_for_path("rows.jsonl.gz", |writer| {
        writer.write_all(b"10\n20\n")?;
        Ok(())
    })
    .unwrap();
    stores.write(uri, &compressed).unwrap();
    let pipeline = Pipeline::default();
    let output = cloud_jsonl_streaming_uri_source::<u32>(&pipeline, "read", &stores, uri)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        [10, 20]
    );
}
