use ironbeam_core::{Element, Error, Pipeline};

#[derive(Clone, Debug, PartialEq, Eq)]
struct UserRow(u32);

impl Element for UserRow {
    const TYPE_ID: &'static str = "tests/user-row";
    const CODER_ID: &'static str = "tests/user-row-u32-le";

    fn supports_coder() -> bool {
        true
    }

    fn encode(&self) -> Result<Vec<u8>, Error> {
        Ok(self.0.to_le_bytes().to_vec())
    }

    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let raw: [u8; 4] = bytes
            .try_into()
            .map_err(|_| Error::incompatible("invalid user row length"))?;
        Ok(Self(u32::from_le_bytes(raw)))
    }
}

#[derive(Clone)]
struct LocalOnly;
impl Element for LocalOnly {
    const TYPE_ID: &'static str = "tests/local-only";
    const CODER_ID: &'static str = "tests/no-coder";
}

#[test]
fn graph_registry_encodes_custom_source_and_transform_outputs() {
    let pipeline = Pipeline::default();
    let rows = pipeline.from_vec("source", vec![UserRow(7)]).unwrap();
    rows.map_named("next", |row| UserRow(row.0 + 1))
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    for key in ["source", "next", "result"] {
        assert!(registry.has_coder(key));
        let encoded = registry.encode_element(key, &UserRow(42)).unwrap();
        let decoded = registry.decode_element(key, &encoded).unwrap();
        assert_eq!(*decoded.downcast::<UserRow>().unwrap(), UserRow(42));
        assert_eq!(graph.node(key).unwrap().coder_id, UserRow::CODER_ID);
    }
    assert!(registry.encode_element("source", &12_i32).is_err());
    assert!(registry.decode_element("source", &[1, 2]).is_err());
}

#[test]
fn uncoded_types_remain_local_only() {
    let pipeline = Pipeline::default();
    pipeline.from_vec("source", vec![LocalOnly]).unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(!registry.has_coder("source"));
    assert!(registry.encode_element("source", &LocalOnly).is_err());
}
