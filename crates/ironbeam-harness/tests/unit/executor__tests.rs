// Unit tests for src/executor.rs.
use super::*;

#[test]
fn element_count_info_encodes_varint_payload_and_pcollection_label() {
    let info = element_count_info("pc_foo", 128).unwrap();
    assert_eq!(info.urn, "beam:metric:element_count:v1");
    assert_eq!(info.r#type, "beam:metrics:sum_int64:v1");
    assert_eq!(info.labels.get("PCOLLECTION"), Some(&"pc_foo".to_string()));
    let mut cursor = std::io::Cursor::new(&info.payload);
    assert_eq!(crate::coders::decode_varint(&mut cursor).unwrap(), 128);
}

#[test]
fn element_count_info_varint_boundary() {
    let a = element_count_info("p", 127).unwrap();
    assert_eq!(a.payload.len(), 1);
    let b = element_count_info("p", 128).unwrap();
    assert_eq!(b.payload.len(), 2);
    let mut cursor = std::io::Cursor::new(&b.payload);
    assert_eq!(crate::coders::decode_varint(&mut cursor).unwrap(), 128);
}
