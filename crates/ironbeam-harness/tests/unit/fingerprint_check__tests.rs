// Unit tests for src/fingerprint_check.rs.
use super::*;
use beam_proto::pipeline_v1::DisplayData;

#[test]
fn extracts_fingerprint_from_display_data() {
    let dd = vec![
        DisplayData {
            urn: "other".into(),
            payload: vec![1, 2, 3],
        },
        DisplayData {
            urn: FINGERPRINT_URN.into(),
            payload: 0xcafe_babe_dead_beef_u64.to_le_bytes().to_vec(),
        },
    ];
    assert_eq!(
        fingerprint_from_pipeline_display_data(&dd),
        Some(0xcafe_babe_dead_beef)
    );
}

#[test]
fn returns_none_when_missing_or_malformed() {
    let dd = vec![DisplayData {
        urn: FINGERPRINT_URN.into(),
        payload: vec![1, 2, 3], // too short
    }];
    assert_eq!(fingerprint_from_pipeline_display_data(&dd), None);
    assert_eq!(fingerprint_from_pipeline_display_data(&[]), None);
}

#[test]
fn compare_classifies_correctly() {
    assert_eq!(compare(None, 42), FingerprintStatus::Missing);
    assert_eq!(compare(Some(42), 42), FingerprintStatus::Match);
    assert_eq!(
        compare(Some(7), 42),
        FingerprintStatus::Mismatch {
            driver: 7,
            local: 42
        }
    );
}
