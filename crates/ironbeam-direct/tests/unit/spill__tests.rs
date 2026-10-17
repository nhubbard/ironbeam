// Unit tests for src/spill.rs.
use super::*;
use std::io;

#[test]
fn frame_reader_rejects_truncation_oversize_and_visitor_errors() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("frames");
    fs::write(&path, []).unwrap();
    assert_eq!(visit_frames(&path, "test", |_| Ok(())).unwrap(), 0);
    fs::write(&path, [1_u8]).unwrap();
    assert_eq!(
        visit_frames(&path, "test", |_| Ok(())).unwrap_err().kind,
        ironbeam_core::ErrorKind::Recovery
    );
    fs::write(&path, (MAX_FRAME_BYTES + 1).to_le_bytes()).unwrap();
    assert_eq!(
        visit_frames(&path, "test", |_| Ok(())).unwrap_err().kind,
        ironbeam_core::ErrorKind::Recovery
    );
    fs::write(&path, [2_u64.to_le_bytes().as_slice(), &[1_u8]].concat()).unwrap();
    assert_eq!(
        visit_frames(&path, "test", |_| Ok(())).unwrap_err().kind,
        ironbeam_core::ErrorKind::Recovery
    );
    let mut valid = Vec::new();
    assert_eq!(write_frame(&mut valid, &[4, 5], "test").unwrap(), 10);
    fs::write(&path, valid).unwrap();
    assert_eq!(
        visit_frames(&path, "test", |_| Err(Error::resource("test", "visitor")))
            .unwrap_err()
            .kind,
        ironbeam_core::ErrorKind::Resource
    );
    assert_eq!(
        visit_frames(&path, "test", |bytes| {
            assert_eq!(bytes, [4, 5]);
            Ok(())
        })
        .unwrap(),
        1
    );
    assert_eq!(
        visit_frames(&dir.path().join("missing"), "test", |_| Ok(()))
            .unwrap_err()
            .kind,
        ironbeam_core::ErrorKind::Io
    );
}

#[test]
fn frame_writer_propagates_io_failure() {
    struct Fail;
    impl Write for Fail {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("disk full"))
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    assert_eq!(
        write_frame(&mut Fail, &[1], "test").unwrap_err().kind,
        ironbeam_core::ErrorKind::Io
    );
}

#[test]
fn store_rejects_invalid_coders_and_corrupt_segments() {
    struct Opaque;
    impl Element for Opaque {
        const TYPE_ID: &'static str = "test/opaque";
        const CODER_ID: &'static str = "test/opaque";
    }
    let dir = tempfile::tempdir().unwrap();
    let metrics = Arc::new(MetricsState::default());
    let config = SpillConfig {
        directory: dir.path().to_path_buf(),
        threshold_bytes: 1,
    };
    assert!(matches!(
        OutputStore::<Opaque>::new(10, Some(&config), metrics.clone())
            .err()
            .unwrap()
            .kind,
        ironbeam_core::ErrorKind::UnsupportedCapability { .. }
    ));
    let invalid = SpillConfig {
        threshold_bytes: 0,
        ..config
    };
    assert_eq!(
        OutputStore::<bool>::new(10, Some(&invalid), metrics.clone())
            .err()
            .unwrap()
            .kind,
        ironbeam_core::ErrorKind::Graph
    );
    let mut oversize = OutputStore::<i32>::new(
        1,
        Some(&SpillConfig {
            directory: dir.path().to_path_buf(),
            threshold_bytes: 1,
        }),
        metrics.clone(),
    )
    .unwrap();
    oversize.push(7).unwrap();
    assert_eq!(metrics.spill_files.load(Ordering::Relaxed), 1);
    let mut plain = OutputStore::<Opaque>::new(10, None, metrics.clone()).unwrap();
    plain.flush().unwrap();
    plain.push(Opaque).unwrap();
    assert_eq!(
        plain.flush().unwrap_err().kind,
        ironbeam_core::ErrorKind::Resource
    );
    let mut broken = OutputStore::<bool>::new(10, None, metrics).unwrap();
    broken.paths.push(dir.path().join("missing"));
    assert_eq!(
        broken.for_each_encoded(|_| Ok(())).unwrap_err().kind,
        ironbeam_core::ErrorKind::Io
    );
    broken.paths.clear();
    let path = dir.path().join("invalid-bool");
    let mut encoded = Vec::new();
    write_frame(&mut encoded, &[2], "test").unwrap();
    fs::write(&path, encoded).unwrap();
    broken.paths.push(path);
    assert_eq!(
        broken.into_vec().unwrap_err().kind,
        ironbeam_core::ErrorKind::Incompatible
    );
}
