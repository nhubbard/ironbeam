#![cfg(feature = "cloud")]

use ironbeam_cloud_objects::{
    CloudIOError, CloudResult, LocalObjectIO, ObjectIO, ObjectMetadata, ObjectStores,
};
use ironbeam_core::{ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_io_tfrecord::{
    cloud_tfrecord_streaming_uri_prefix_source, cloud_tfrecord_streaming_uri_source, tfrecord_sink,
};
use std::io::{self, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct OpenOnly {
    inner: LocalObjectIO,
    opens: AtomicUsize,
    fail_reads: AtomicBool,
    interrupt_once: AtomicBool,
}

struct BrokenReader;

impl Read for BrokenReader {
    fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::other("provider stream failed"))
    }
}

struct InterruptOnce {
    inner: Box<dyn Read + Send>,
    interrupted: AtomicBool,
}

impl Read for InterruptOnce {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if !self.interrupted.swap(true, Ordering::Relaxed) {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "retry read"));
        }
        self.inner.read(buffer)
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
        let reader = self.inner.open_object(bucket, key)?;
        if self.interrupt_once.load(Ordering::Relaxed) {
            Ok(Box::new(InterruptOnce {
                inner: reader,
                interrupted: AtomicBool::new(false),
            }))
        } else {
            Ok(reader)
        }
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
        interrupt_once: AtomicBool::new(false),
    });
    let mut stores = ObjectStores::new();
    stores.register("file", Arc::clone(&adapter)).unwrap();
    (stores, adapter)
}

fn frames(directory: &std::path::Path, values: Vec<Vec<u8>>) -> Vec<u8> {
    let path = directory.join("frames.tfrecord");
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", values).unwrap();
    DirectRunner
        .run_sink(
            &tfrecord_sink(&input, "write", &path).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    std::fs::read(path).unwrap()
}

fn collect(output: &ironbeam_core::Output<Vec<u8>>) -> Vec<Vec<u8>> {
    DirectRunner
        .run_collect(output, &JobConfig::default())
        .unwrap()
        .0
}

#[test]
fn streaming_tfrecord_reads_without_whole_download_and_rejects_changes() {
    let directory = tempfile::tempdir().unwrap();
    let (stores, adapter) = stores(&directory.path().join("objects"));
    let values = (0..10_000_u32)
        .map(|value| value.to_le_bytes().to_vec())
        .collect::<Vec<_>>();
    let uri = "file://bucket/data/rows.tfrecord";
    stores
        .write(uri, &frames(directory.path(), values.clone()))
        .unwrap();
    let pipeline = Pipeline::default();
    let output = cloud_tfrecord_streaming_uri_source(&pipeline, "read", &stores, uri)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(collect(&output), values);
    assert_eq!(adapter.opens.load(Ordering::Relaxed), 2);

    stores
        .write(uri, &frames(directory.path(), vec![b"changed".to_vec()]))
        .unwrap();
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
fn streaming_tfrecord_prefix_fixes_sorted_objects_and_checks_frame_crc() {
    let directory = tempfile::tempdir().unwrap();
    let (stores, _) = stores(&directory.path().join("objects"));
    stores
        .write(
            "file://bucket/data/b.tfrecord",
            &frames(directory.path(), vec![b"B".to_vec()]),
        )
        .unwrap();
    stores
        .write(
            "file://bucket/data/a.tfrecord",
            &frames(directory.path(), vec![b"A".to_vec()]),
        )
        .unwrap();
    let pipeline = Pipeline::default();
    let output = cloud_tfrecord_streaming_uri_prefix_source(
        &pipeline,
        "read",
        &stores,
        "file://bucket/data/",
    )
    .unwrap()
    .output("out")
    .unwrap();
    assert_eq!(collect(&output), [b"A".to_vec(), b"B".to_vec()]);

    stores
        .write(
            "file://bucket/data/c.tfrecord",
            &frames(directory.path(), vec![b"C".to_vec()]),
        )
        .unwrap();
    assert_eq!(collect(&output), [b"A".to_vec(), b"B".to_vec()]);

    let mut broken = frames(directory.path(), vec![b"bad".to_vec()]);
    broken[12] ^= 1;
    stores
        .write("file://bucket/broken.tfrecord", &broken)
        .unwrap();
    let pipeline = Pipeline::default();
    let output = cloud_tfrecord_streaming_uri_source(
        &pipeline,
        "broken",
        &stores,
        "file://bucket/broken.tfrecord",
    )
    .unwrap()
    .output("out")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
}

#[test]
fn streaming_tfrecord_rejects_missing_bindings_and_empty_prefixes() {
    let directory = tempfile::tempdir().unwrap();
    let (stores, _) = stores(directory.path());
    for uri in [
        "not-a-uri",
        "missing://bucket/data/rows.tfrecord",
        "file://bucket/data/absent.tfrecord",
    ] {
        assert!(
            cloud_tfrecord_streaming_uri_source(&Pipeline::default(), "read", &stores, uri)
                .is_err()
        );
    }
    assert!(
        cloud_tfrecord_streaming_uri_prefix_source(
            &Pipeline::default(),
            "read",
            &stores,
            "file://bucket/absent/",
        )
        .is_err()
    );
}

#[test]
fn streaming_tfrecord_handles_single_prefix_object_and_interrupted_reads() {
    let directory = tempfile::tempdir().unwrap();
    let (stores, adapter) = stores(&directory.path().join("objects"));
    stores
        .write(
            "file://bucket/only/frames",
            &frames(directory.path(), vec![b"one".to_vec()]),
        )
        .unwrap();
    adapter.interrupt_once.store(true, Ordering::Relaxed);
    let pipeline = Pipeline::default();
    let output = cloud_tfrecord_streaming_uri_prefix_source(
        &pipeline,
        "read",
        &stores,
        "file://bucket/only/",
    )
    .unwrap()
    .output("out")
    .unwrap();
    assert_eq!(collect(&output), [b"one".to_vec()]);
    assert_eq!(adapter.opens.load(Ordering::Relaxed), 2);
}

#[test]
fn streaming_tfrecord_rejects_provider_read_failure_at_binding() {
    let directory = tempfile::tempdir().unwrap();
    let (stores, adapter) = stores(&directory.path().join("objects"));
    let uri = "file://bucket/rows.tfrecord";
    stores
        .write(uri, &frames(directory.path(), vec![b"one".to_vec()]))
        .unwrap();
    adapter.fail_reads.store(true, Ordering::Relaxed);
    let error = cloud_tfrecord_streaming_uri_source(&Pipeline::default(), "read", &stores, uri)
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Io);
    assert!(error.to_string().contains("provider stream failed"));
}

#[cfg(feature = "compression-gzip")]
#[test]
fn streaming_tfrecord_decodes_gzip_after_verification() {
    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("rows.tfrecord.gz");
    let pipeline = Pipeline::default();
    let values = pipeline
        .from_vec("seed", vec![b"compressed".to_vec()])
        .unwrap();
    DirectRunner
        .run_sink(
            &tfrecord_sink(&values, "write", &input).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let (stores, _) = stores(&directory.path().join("objects"));
    let uri = "file://bucket/rows.tfrecord.gz";
    stores.write(uri, &std::fs::read(input).unwrap()).unwrap();
    let pipeline = Pipeline::default();
    let output = cloud_tfrecord_streaming_uri_source(&pipeline, "read", &stores, uri)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(collect(&output), [b"compressed".to_vec()]);
}
