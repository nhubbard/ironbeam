#![cfg(feature = "cloud")]

use std::io::Read;
use std::sync::Arc;

use ironbeam_cloud_object_store::RemoteObjectIO;
use ironbeam_cloud_objects::ObjectIO;
use ironbeam_io::object_final::ObjectFinalResult;
use object_store::{ObjectStore, memory::InMemory};

fn count_lines(reader: &mut dyn Read) -> Result<u64, ironbeam_core::Error> {
    let mut count = 0_u64;
    let mut buffer = [0_u8; 4096];
    loop {
        let size = reader
            .read(&mut buffer)
            .map_err(|error| ironbeam_core::Error::io("object-final", "test", error.to_string()))?;
        if size == 0 {
            return Ok(count);
        }
        for byte in &buffer[..size] {
            if *byte == b'\n' {
                count += 1;
            }
        }
    }
}

#[test]
fn completed_fragments_publish_through_hosted_streaming_adapter() {
    let memory = Arc::new(InMemory::new());
    let backing = Arc::clone(&memory);
    let storage: Arc<dyn ObjectIO> = Arc::new(
        RemoteObjectIO::new(move |_| Ok(Arc::clone(&backing) as Arc<dyn ObjectStore>)).unwrap(),
    );
    let result = ObjectFinalResult::new(
        Arc::clone(&storage),
        ("stage", "job/result"),
        ("output", "results/complete.jsonl"),
        "job",
        "graph",
        "sink",
    )
    .unwrap();
    let first = b"first\n".repeat(450_000);
    let second = b"second\n".repeat(450_000);
    result.stage("bundle-a", 450_000, &first).unwrap();
    result.stage("bundle-b", 450_000, &second).unwrap();
    result.accept("bundle-a").unwrap();
    result.accept("bundle-b").unwrap();
    let bound = result
        .require_bundle_attempts(["bundle-a", "bundle-b"])
        .unwrap();
    let published = bound
        .publish_validated_streaming(900_000, count_lines)
        .unwrap();
    assert_eq!(published.rows, 900_000);
    assert_eq!(published.fragments, 2);
    assert_eq!(published.bytes, (first.len() + second.len()) as u64);
    assert_eq!(
        bound
            .publish_validated_streaming(900_000, count_lines)
            .unwrap(),
        published
    );
    let output = storage
        .get_object("output", "results/complete.jsonl")
        .unwrap();
    assert_eq!(output, [first, second].concat());
    assert!(
        storage
            .list_objects("output", Some("__ironbeam_uploads/"))
            .unwrap()
            .is_empty()
    );
}
