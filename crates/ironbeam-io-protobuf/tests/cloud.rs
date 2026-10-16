#![cfg(feature = "cloud")]

use ironbeam_cloud_objects::{
    CloudIOError, CloudResult, LocalObjectIO, ObjectIO, ObjectMetadata, ObjectStores,
};
use ironbeam_core::{Element, ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_io_protobuf::{
    cloud_protobuf_streaming_uri_prefix_source, cloud_protobuf_streaming_uri_source, protobuf_sink,
};
use std::io::{self, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Clone, PartialEq, prost::Message)]
struct Row {
    #[prost(uint32, tag = "1")]
    id: u32,
    #[prost(string, tag = "2")]
    label: String,
}

impl Element for Row {
    const TYPE_ID: &'static str = "tests/cloud-protobuf-row";
    const CODER_ID: &'static str = "tests/cloud-protobuf-row-protobuf";
}

struct ReadFault;

impl Read for ReadFault {
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

struct OpenOnly {
    inner: LocalObjectIO,
    opens: AtomicUsize,
    fail_reads: AtomicBool,
    interrupt_once: AtomicBool,
}

impl ObjectIO for OpenOnly {
    fn put_object(&self, bucket: &str, key: &str, data: &[u8]) -> CloudResult<()> {
        self.inner.put_object(bucket, key, data)
    }

    fn open_object(&self, bucket: &str, key: &str) -> CloudResult<Box<dyn Read + Send>> {
        self.opens.fetch_add(1, Ordering::Relaxed);
        if self.fail_reads.load(Ordering::Relaxed) {
            return Ok(Box::new(ReadFault));
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

fn encoded(directory: &std::path::Path, file: &str, rows: Vec<Row>) -> Vec<u8> {
    let path = directory.join(file);
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("seed", rows).unwrap();
    DirectRunner
        .run_sink(
            &protobuf_sink(&input, "write", &path).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    std::fs::read(path).unwrap()
}

fn collect(output: &ironbeam_core::Output<Row>) -> Vec<Row> {
    DirectRunner
        .run_collect(output, &JobConfig::default())
        .unwrap()
        .0
}

#[test]
fn streaming_protobuf_reads_without_whole_download_and_rejects_changes() {
    let directory = tempfile::tempdir().unwrap();
    let (stores, adapter) = stores(&directory.path().join("objects"));
    let rows = (0..10_000)
        .map(|id| Row {
            id,
            label: format!("row-{id}"),
        })
        .collect::<Vec<_>>();
    let uri = "file://bucket/data/rows.pb";
    stores
        .write(uri, &encoded(directory.path(), "rows.pb", rows.clone()))
        .unwrap();
    let pipeline = Pipeline::default();
    let output = cloud_protobuf_streaming_uri_source::<Row>(&pipeline, "read", &stores, uri)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(collect(&output), rows);
    assert_eq!(adapter.opens.load(Ordering::Relaxed), 2);
    stores
        .write(
            uri,
            &encoded(
                directory.path(),
                "changed.pb",
                vec![Row {
                    id: 1,
                    label: "changed".into(),
                }],
            ),
        )
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
fn streaming_protobuf_prefix_fixes_sorted_objects_and_checks_messages() {
    let directory = tempfile::tempdir().unwrap();
    let (stores, _) = stores(&directory.path().join("objects"));
    for (name, id) in [("b.pb", 2), ("a.pb", 1)] {
        stores
            .write(
                &format!("file://bucket/data/{name}"),
                &encoded(
                    directory.path(),
                    name,
                    vec![Row {
                        id,
                        label: name.into(),
                    }],
                ),
            )
            .unwrap();
    }
    let pipeline = Pipeline::default();
    let output = cloud_protobuf_streaming_uri_prefix_source::<Row>(
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
        .write(
            "file://bucket/data/c.pb",
            &encoded(
                directory.path(),
                "c.pb",
                vec![Row {
                    id: 3,
                    label: "c".into(),
                }],
            ),
        )
        .unwrap();
    assert_eq!(collect(&output).len(), 2);

    stores
        .write("file://bucket/broken.pb", b"\x04\x08")
        .unwrap();
    let pipeline = Pipeline::default();
    let output = cloud_protobuf_streaming_uri_source::<Row>(
        &pipeline,
        "broken",
        &stores,
        "file://bucket/broken.pb",
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
fn streaming_protobuf_rejects_bad_bindings_and_provider_reads() {
    let directory = tempfile::tempdir().unwrap();
    let (stores, adapter) = stores(&directory.path().join("objects"));
    for uri in [
        "bad-uri",
        "missing://bucket/rows.pb",
        "file://bucket/missing.pb",
    ] {
        assert!(
            cloud_protobuf_streaming_uri_source::<Row>(&Pipeline::default(), "read", &stores, uri)
                .is_err()
        );
    }
    assert!(
        cloud_protobuf_streaming_uri_prefix_source::<Row>(
            &Pipeline::default(),
            "read",
            &stores,
            "file://bucket/absent/",
        )
        .is_err()
    );
    let uri = "file://bucket/rows.pb";
    stores.write(uri, b"\x00").unwrap();
    adapter.fail_reads.store(true, Ordering::Relaxed);
    assert_eq!(
        cloud_protobuf_streaming_uri_source::<Row>(&Pipeline::default(), "read", &stores, uri)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
}

#[test]
fn streaming_protobuf_single_prefix_object_recovers_interrupted_reads() {
    let directory = tempfile::tempdir().unwrap();
    let (stores, adapter) = stores(&directory.path().join("objects"));
    let row = Row {
        id: 9,
        label: "only".into(),
    };
    stores
        .write(
            "file://bucket/only/message",
            &encoded(directory.path(), "only.pb", vec![row.clone()]),
        )
        .unwrap();
    adapter.interrupt_once.store(true, Ordering::Relaxed);
    let pipeline = Pipeline::default();
    let output = cloud_protobuf_streaming_uri_prefix_source::<Row>(
        &pipeline,
        "read",
        &stores,
        "file://bucket/only/",
    )
    .unwrap()
    .output("out")
    .unwrap();
    assert_eq!(collect(&output), [row]);
    assert_eq!(adapter.opens.load(Ordering::Relaxed), 2);
}

#[cfg(feature = "compression-gzip")]
#[test]
fn streaming_protobuf_decodes_gzip_after_verification() {
    let directory = tempfile::tempdir().unwrap();
    let (stores, _) = stores(&directory.path().join("objects"));
    let row = Row {
        id: 7,
        label: "zip".into(),
    };
    let uri = "file://bucket/rows.pb.gz";
    stores
        .write(
            uri,
            &encoded(directory.path(), "rows.pb.gz", vec![row.clone()]),
        )
        .unwrap();
    let pipeline = Pipeline::default();
    let output = cloud_protobuf_streaming_uri_source::<Row>(&pipeline, "read", &stores, uri)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(collect(&output), [row]);
}
