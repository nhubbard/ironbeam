
use super::*;
use std::io::Read;

#[test]
fn compressed_lazy_reader_decodes_on_read() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("rows.jsonl.gz");
    let expected = b"{\"value\":1}\n{\"value\":2}\n";
    fs::write(
        &path,
        ironbeam_compression::compress_bytes_for_path(expected, &path).unwrap(),
    )
    .unwrap();
    let mut reader = open_lazy_input(&path, "rows").unwrap();
    let mut actual = Vec::new();
    reader.read_to_end(&mut actual).unwrap();
    assert_eq!(actual, expected);

    fs::write(&path, b"invalid gzip bytes").unwrap();
    let mut reader = open_lazy_input(&path, "rows").unwrap();
    assert!(reader.read_to_end(&mut Vec::new()).is_err());
}
