use ironbeam_core::{Element, ErrorKind, Window};

struct Uncoded;

impl Element for Uncoded {
    const TYPE_ID: &'static str = "test/uncoded";
    const CODER_ID: &'static str = "test/uncoded";
}

#[test]
fn default_element_coder_reports_unsupported_for_all_encoding_entry_points() {
    assert!(!Uncoded::supports_coder());
    assert_eq!(Uncoded::type_identity(), "test/uncoded");
    assert_eq!(Uncoded::coder_identity(), "test/uncoded");
    assert_eq!(Uncoded::fixed_encoded_len(), None);
    let mut bytes = vec![1_u8];
    assert!(matches!(
        Uncoded.encode().unwrap_err().kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    assert!(matches!(
        Uncoded.encode_into(&mut bytes).unwrap_err().kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    assert_eq!(bytes, [1]);
    assert!(matches!(
        Uncoded.encode_reusing(&mut bytes).unwrap_err().kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    assert_eq!(bytes, [1]);
    assert!(matches!(
        Uncoded.encoded_len().unwrap_err().kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    assert!(matches!(
        Uncoded::decode(&[]).err().unwrap().kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
}

#[test]
fn fixed_window_rejects_invalid_size_and_timestamp_overflow() {
    assert_eq!(
        Window::Fixed { size_ns: 0 }.assign(0).unwrap_err().kind,
        ErrorKind::Graph
    );
    assert_eq!(
        Window::Fixed { size_ns: 10 }
            .assign(i64::MAX)
            .unwrap_err()
            .kind,
        ErrorKind::Graph
    );
}
