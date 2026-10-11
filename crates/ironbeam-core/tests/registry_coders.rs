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

#[test]
fn cogroup_registry_tags_heterogeneous_inputs_and_reassembles_typed_values() {
    let pipeline = Pipeline::default();
    let left = pipeline.from_vec("left", vec![("a".to_owned(), 1_i32)]).unwrap();
    let right = pipeline.from_vec("right", vec![("a".to_owned(), UserRow(7))]).unwrap();
    ironbeam_core::cogroup_by_key_2_named("cogroup", &left, &right).unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(registry.has_cogroup_coder("cogroup"));
    let left_bytes = ("a".to_owned(), 1_i32).encode().unwrap();
    let right_bytes = ("a".to_owned(), UserRow(7)).encode().unwrap();
    let (key, left_tagged) = registry.split_cogroup_input("cogroup", 0, &left_bytes).unwrap();
    let (right_key, right_tagged) = registry.split_cogroup_input("cogroup", 1, &right_bytes).unwrap();
    assert_eq!(key, right_key);
    assert_eq!(left_tagged[0], 0);
    assert_eq!(right_tagged[0], 1);
    let output = registry.assemble_cogroup_output("cogroup", &key,
        &[right_tagged.clone(), left_tagged.clone(), left_tagged]).unwrap();
    assert_eq!(<(String, (Vec<i32>, Vec<UserRow>))>::decode(&output).unwrap(),
        ("a".to_owned(), (vec![1, 1], vec![UserRow(7)])));
    assert!(registry.split_cogroup_input("cogroup", 2, &left_bytes).is_err());
    assert!(registry.split_cogroup_input("missing", 0, &left_bytes).is_err());
    assert!(registry.assemble_cogroup_output("cogroup", &key, &[Vec::new()]).is_err());
    assert!(registry.assemble_cogroup_output("cogroup", &key, &[vec![2]]).is_err());
    assert!(registry.assemble_cogroup_output("cogroup", &key, &[vec![1, 0]]).is_err());
}

#[test]
fn cogroup_registry_handles_three_inputs_and_empty_value_groups() {
    let pipeline = Pipeline::default();
    let first = pipeline.from_vec("first", vec![("a".to_owned(), 4_i32)]).unwrap();
    let second = pipeline.from_vec("second", vec![("a".to_owned(), UserRow(9))]).unwrap();
    let third = pipeline.from_vec("third", vec![("b".to_owned(), true)]).unwrap();
    ironbeam_core::cogroup_by_key_3_named("three", &first, &second, &third).unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let (key, first_value) = registry.split_cogroup_input("three", 0,
        &("a".to_owned(), 4_i32).encode().unwrap()).unwrap();
    let (_, second_value) = registry.split_cogroup_input("three", 1,
        &("a".to_owned(), UserRow(9)).encode().unwrap()).unwrap();
    let (_, third_value) = registry.split_cogroup_input("three", 2,
        &("b".to_owned(), true).encode().unwrap()).unwrap();
    assert_eq!(third_value[0], 2);
    let output = registry.assemble_cogroup_output("three", &key,
        &[first_value, second_value]).unwrap();
    assert_eq!(<(String, (Vec<i32>, Vec<UserRow>, Vec<bool>))>::decode(&output).unwrap(),
        ("a".to_owned(), (vec![4], vec![UserRow(9)], Vec::new())));
}
