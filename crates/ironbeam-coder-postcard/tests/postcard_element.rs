use ironbeam_coder_postcard::{impl_postcard_element, ironbeam_core::Element};
use ironbeam_direct::DirectCollectExt;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Row {
    id: i32,
    label: String,
}

impl_postcard_element!(Row, "tests/postcard-row/v1");

#[derive(Clone, serde::Deserialize)]
struct BadEncode;

impl serde::Serialize for BadEncode {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(<S::Error as serde::ser::Error>::custom(
            "deliberate encode failure",
        ))
    }
}

impl_postcard_element!(BadEncode, "tests/bad-encode/v1", "custom/postcard-bad/v2");

#[test]
fn user_record_round_trips_with_stable_identity() {
    let row = Row {
        id: 7,
        label: "portable".into(),
    };
    assert!(Row::supports_coder());
    assert_eq!(Row::CODER_ID, "ironbeam/postcard-v1/tests/postcard-row/v1");
    assert_eq!(Row::decode(&row.encode().unwrap()).unwrap(), row);
    assert!(Row::decode(&[0xff]).is_err());
}

#[test]
fn explicit_coder_identity_and_encode_error_are_preserved() {
    assert_eq!(BadEncode::CODER_ID, "custom/postcard-bad/v2");
    let error = BadEncode.encode().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Postcard encode tests/bad-encode/v1")
    );
}

#[test]
fn user_record_executes_through_direct_coded_graph() {
    let pipeline = ironbeam_coder_postcard::ironbeam_core::Pipeline::default();
    let rows = pipeline
        .from_vec(
            "rows",
            vec![
                Row {
                    id: 2,
                    label: "b".into(),
                },
                Row {
                    id: 1,
                    label: "a".into(),
                },
            ],
        )
        .unwrap()
        .filter_named("keep", |row| row.id > 1)
        .unwrap();
    let result = rows.collect_seq().unwrap();
    assert_eq!(
        result,
        vec![Row {
            id: 2,
            label: "b".into()
        }]
    );
}
