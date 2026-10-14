use ironbeam_core::{Element, Envelope, Error, ErrorKind, Pipeline, Window};

fn scalar_event(value: i32) -> Envelope {
    Envelope {
        pane: ironbeam_core::PaneInfo::NO_FIRING,
        value: Box::new(value),
        timestamp_ns: 0,
        window: Window::Global,
        assigned_window: None,
    }
}

#[test]
fn registry_rejects_missing_portable_coder_contracts() {
    let pipeline = Pipeline::default();
    pipeline.from_vec("source", vec![1_i32]).unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    macro_rules! incompatible {
        ($result:expr) => {
            assert_eq!($result.err().unwrap().kind, ErrorKind::Incompatible)
        };
    }
    incompatible!(registry.encode_element("missing", &1_i32));
    incompatible!(registry.decode_element("missing", &[]));
    incompatible!(registry.materialize_side_input("missing", Vec::new()));
    incompatible!(registry.apply_side_input("missing", scalar_event(1), &()));
    incompatible!(registry.split_group_input("missing", &[]));
    incompatible!(registry.assemble_group_input("missing", &[], &[]));
    incompatible!(registry.assemble_group_output("missing", &[], &[]));
    incompatible!(registry.split_join_input("missing", 0, &[], 0));
    incompatible!(registry.execute_join_group("missing", &[], &[], &Window::Global, None));
    incompatible!(registry.split_cogroup_input("missing", 0, &[]));
    incompatible!(registry.assemble_cogroup_output("missing", &[], &[]));
    assert_eq!(
        registry.open_fold("missing").err().unwrap().kind,
        ErrorKind::Execution
    );
}

#[test]
fn global_combine_group_coder_accepts_only_unkeyed_valid_elements() {
    let pipeline = Pipeline::default();
    let values = pipeline.from_vec("source", vec![7_i32]).unwrap();
    values
        .combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(registry.has_group_coder("sum"));

    let encoded = 7_i32.encode().unwrap();
    let (key, value) = registry.split_group_input("sum", &encoded).unwrap();
    assert_eq!(key, Vec::<u8>::new());
    assert_eq!(value, encoded);
    assert_eq!(
        registry.assemble_group_input("sum", &key, &value).unwrap(),
        encoded
    );
    assert_eq!(
        registry
            .assemble_group_input("sum", &[1], &value)
            .unwrap_err()
            .kind,
        ErrorKind::Incompatible
    );
    assert!(registry.split_group_input("sum", &[1]).is_err());
    assert!(registry.assemble_group_input("sum", &[], &[1]).is_err());
    assert!(
        registry
            .assemble_group_output("sum", &[], &[value])
            .is_err()
    );
}

#[test]
fn registry_distinguishes_single_and_multi_result_dispatch() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    source.map_named("one", |value| value + 1).unwrap();
    source.filter_named("even", |value| value % 2 == 0).unwrap();
    source
        .flat_map_named("many", |value| (0..value).collect())
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(registry.has_single_transform("one"));
    assert!(registry.single_transform("one").is_some());
    assert!(!registry.has_single_transform("many"));
    assert!(registry.single_transform("many").is_none());
    assert_eq!(
        registry.apply_many("one", scalar_event(1)).unwrap().len(),
        1
    );
    assert!(
        registry
            .apply_many("even", scalar_event(1))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        registry.apply_many("even", scalar_event(2)).unwrap().len(),
        1
    );
    assert!(registry.apply("many", scalar_event(0)).unwrap().is_none());
    let one = registry.apply("many", scalar_event(1)).unwrap().unwrap();
    assert_eq!(*one.value.downcast::<i32>().unwrap(), 0);
    assert_eq!(
        registry.apply("many", scalar_event(2)).err().unwrap().kind,
        ErrorKind::Execution
    );
    assert_eq!(
        registry
            .apply_many("missing", scalar_event(1))
            .err()
            .unwrap()
            .kind,
        ErrorKind::Execution
    );
}

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
    let left = pipeline
        .from_vec("left", vec![("a".to_owned(), 1_i32)])
        .unwrap();
    let right = pipeline
        .from_vec("right", vec![("a".to_owned(), UserRow(7))])
        .unwrap();
    ironbeam_core::cogroup_by_key_2_named("cogroup", &left, &right).unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(registry.has_cogroup_coder("cogroup"));
    let left_bytes = ("a".to_owned(), 1_i32).encode().unwrap();
    let right_bytes = ("a".to_owned(), UserRow(7)).encode().unwrap();
    let (key, left_tagged) = registry
        .split_cogroup_input("cogroup", 0, &left_bytes)
        .unwrap();
    let (right_key, right_tagged) = registry
        .split_cogroup_input("cogroup", 1, &right_bytes)
        .unwrap();
    assert_eq!(key, right_key);
    assert_eq!(left_tagged[0], 0);
    assert_eq!(right_tagged[0], 1);
    let output = registry
        .assemble_cogroup_output(
            "cogroup",
            &key,
            &[right_tagged, left_tagged.clone(), left_tagged],
        )
        .unwrap();
    assert_eq!(
        <(String, (Vec<i32>, Vec<UserRow>))>::decode(&output).unwrap(),
        ("a".to_owned(), (vec![1, 1], vec![UserRow(7)]))
    );
    assert!(
        registry
            .split_cogroup_input("cogroup", 2, &left_bytes)
            .is_err()
    );
    assert!(
        registry
            .split_cogroup_input("missing", 0, &left_bytes)
            .is_err()
    );
    assert!(
        registry
            .assemble_cogroup_output("cogroup", &key, &[Vec::new()])
            .is_err()
    );
    assert!(
        registry
            .assemble_cogroup_output("cogroup", &key, &[vec![2]])
            .is_err()
    );
    assert!(
        registry
            .assemble_cogroup_output("cogroup", &key, &[vec![1, 0]])
            .is_err()
    );
}

#[test]
fn cogroup_registry_handles_three_inputs_and_empty_value_groups() {
    let pipeline = Pipeline::default();
    let first = pipeline
        .from_vec("first", vec![("a".to_owned(), 4_i32)])
        .unwrap();
    let second = pipeline
        .from_vec("second", vec![("a".to_owned(), UserRow(9))])
        .unwrap();
    let third = pipeline
        .from_vec("third", vec![("b".to_owned(), true)])
        .unwrap();
    ironbeam_core::cogroup_by_key_3_named("three", &first, &second, &third).unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let (key, first_value) = registry
        .split_cogroup_input("three", 0, &("a".to_owned(), 4_i32).encode().unwrap())
        .unwrap();
    let (_, second_value) = registry
        .split_cogroup_input("three", 1, &("a".to_owned(), UserRow(9)).encode().unwrap())
        .unwrap();
    let (_, third_value) = registry
        .split_cogroup_input("three", 2, &("b".to_owned(), true).encode().unwrap())
        .unwrap();
    assert_eq!(third_value[0], 2);
    let output = registry
        .assemble_cogroup_output("three", &key, &[first_value, second_value])
        .unwrap();
    assert_eq!(
        <(String, (Vec<i32>, Vec<UserRow>, Vec<bool>))>::decode(&output).unwrap(),
        ("a".to_owned(), (vec![4], vec![UserRow(9)], Vec::new()))
    );
}

#[test]
fn portable_join_reuses_direct_matching_and_input_timestamps() {
    let pipeline = Pipeline::default();
    let left = pipeline
        .from_vec("left", vec![("a".to_owned(), 1_i32)])
        .unwrap();
    let right = pipeline
        .from_vec("right", vec![("a".to_owned(), true)])
        .unwrap();
    left.join_full_named("join", &right).unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(registry.has_join_coder("join"));
    let (key, left_one) = registry
        .split_join_input("join", 0, &("a".to_owned(), 1_i32).encode().unwrap(), 11)
        .unwrap();
    let (_, left_two) = registry
        .split_join_input("join", 0, &("a".to_owned(), 2_i32).encode().unwrap(), 12)
        .unwrap();
    let (_, right_one) = registry
        .split_join_input("join", 1, &("a".to_owned(), true).encode().unwrap(), 21)
        .unwrap();
    let rows = registry
        .execute_join_group(
            "join",
            &key,
            &[left_one, left_two, right_one],
            &Window::Global,
            None,
        )
        .unwrap();
    let mut decoded = rows
        .into_iter()
        .map(|(bytes, timestamp)| {
            (
                <(String, (Option<i32>, Option<bool>))>::decode(&bytes).unwrap(),
                timestamp,
            )
        })
        .collect::<Vec<_>>();
    decoded.sort_by_key(|((_, (left, _)), _)| *left);
    assert_eq!(
        decoded,
        vec![
            (("a".to_owned(), (Some(1), Some(true))), 11),
            (("a".to_owned(), (Some(2), Some(true))), 12),
        ]
    );
    assert!(
        registry
            .execute_join_group("join", &key, &[vec![2]], &Window::Global, None)
            .is_err()
    );
    assert!(registry.split_join_input("join", 2, &[], 0).is_err());
}

#[test]
fn side_input_registration_reuses_direct_materialization_and_mapping() {
    let pipeline = Pipeline::default();
    let main = pipeline.from_vec("main", vec![1_i32]).unwrap();
    let side = pipeline.from_vec("side", vec![10_i32, 20]).unwrap();
    main.map_with_materialized_side_named(
        "with-side",
        &side,
        |values| Ok(values.iter().sum::<i32>()),
        |value, sum| Ok(value + sum),
    )
    .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(registry.has_side_input("with-side"));
    let values = [10_i32, 20]
        .into_iter()
        .map(|value| Box::new(value) as _)
        .collect();
    let view = registry
        .materialize_side_input("with-side", values)
        .unwrap();
    let output = registry
        .apply_side_input(
            "with-side",
            Envelope {
                pane: ironbeam_core::PaneInfo::NO_FIRING,
                value: Box::new(1_i32),
                timestamp_ns: 4,
                window: Window::Global,
                assigned_window: None,
            },
            view.as_ref(),
        )
        .unwrap();
    assert_eq!(*output.value.downcast::<i32>().unwrap(), 31);
    assert_eq!(output.timestamp_ns, 4);
    assert!(
        registry
            .apply_side_input(
                "with-side",
                Envelope {
                    pane: ironbeam_core::PaneInfo::NO_FIRING,
                    value: Box::new(1_i32),
                    timestamp_ns: 0,
                    window: Window::Global,
                    assigned_window: None,
                },
                &"wrong view"
            )
            .is_err()
    );
    assert!(
        registry
            .materialize_side_input("with-side", vec![Box::new(true)])
            .is_err()
    );
}
