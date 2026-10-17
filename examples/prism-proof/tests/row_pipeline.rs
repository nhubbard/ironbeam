use beam_proto::pipeline_v1::{
    atomic_type_value, field_type, field_value, AtomicType, AtomicTypeValue, Field, FieldType,
    FieldValue, Row, Schema,
};
use ironbeam_coder_row::{BeamRow, RowSchema};
use ironbeam_core::{Element, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_graphx::urns;
use ironbeam_graphx::v5::{translate_v5, validate_beam_contract};
use prost_beam::Message;

struct DemoSchema;

impl RowSchema for DemoSchema {
    const TYPE_ID: &'static str = "tests/beam-row-demo-v1";

    fn schema() -> Schema {
        Schema {
            id: "780b20a9-c0ba-438c-94e8-10c0a6253040".into(),
            fields: vec![Field {
                name: "id".into(),
                r#type: Some(FieldType {
                    nullable: false,
                    type_info: Some(field_type::TypeInfo::AtomicType(AtomicType::Int32 as i32)),
                }),
                ..Default::default()
            }],
            ..Default::default()
        }
    }
}

#[test]
fn beam_rows_keep_schema_payload_in_graph_and_direct_bytes() {
    let pipeline = Pipeline::default();
    let row = BeamRow::<DemoSchema>::new(Row {
        values: vec![FieldValue {
            field_value: Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                value: Some(atomic_type_value::Value::Int32(42)),
            })),
        }],
    })
    .unwrap();
    let output = pipeline
        .from_vec("rows", vec![row])
        .unwrap()
        .map_named("row-bytes", |row| row.encode().unwrap())
        .unwrap()
        .output("result")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        [vec![1, 0, 42]]
    );
    let (graph, _) = pipeline.snapshot().unwrap();
    graph.validate().unwrap();
    assert_eq!(
        graph.node("rows").unwrap().coder_payload,
        DemoSchema::schema().encode_to_vec()
    );
    let mut damaged = graph.clone();
    damaged.nodes[0].coder_payload.clear();
    assert!(damaged.validate().is_err());
    let mut changed = graph.clone();
    changed.nodes[0].coder_payload = {
        let mut schema = DemoSchema::schema();
        schema.id = "different-schema-id".into();
        schema.encode_to_vec()
    };
    assert_ne!(graph.fingerprint().unwrap(), changed.fingerprint().unwrap());

    let translated = translate_v5(&pipeline, "example.test/ironbeam:v5", "row-test-build").unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    let source = components
        .pcollections
        .values()
        .find(|pc| pc.unique_name == "rows")
        .unwrap();
    let outer = &components.coders[&source.coder_id];
    assert_eq!(outer.spec.as_ref().unwrap().urn, urns::CODER_LENGTH_PREFIX);
    let inner = &components.coders[&outer.component_coder_ids[0]];
    assert_eq!(inner.spec.as_ref().unwrap().urn, urns::CODER_ROW);
    assert_eq!(
        inner.spec.as_ref().unwrap().payload,
        DemoSchema::schema().encode_to_vec()
    );
}

#[test]
fn beam_row_rejects_values_outside_its_fixed_schema() {
    assert!(BeamRow::<DemoSchema>::new(Row { values: vec![] }).is_err());
}

#[test]
fn writer_and_reader_row_schemas_lower_to_distinct_beam_coders() {
    let directory = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    ironbeam_prism_proof::beam_row_projection_fixture(
        &pipeline,
        directory.path().join("projected-rows.txt"),
    )
    .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let translated =
        translate_v5(&pipeline, "example.test/ironbeam:v5", "projection-build").unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    let payloads = ["beam-projection-build", "beam-projection-project"].map(|name| {
        let collection = components
            .pcollections
            .values()
            .find(|collection| collection.unique_name == name)
            .unwrap();
        let outer = &components.coders[&collection.coder_id];
        assert_eq!(outer.spec.as_ref().unwrap().urn, urns::CODER_LENGTH_PREFIX);
        let inner = &components.coders[&outer.component_coder_ids[0]];
        assert_eq!(inner.spec.as_ref().unwrap().urn, urns::CODER_ROW);
        inner.spec.as_ref().unwrap().payload.clone()
    });
    assert_ne!(payloads[0], payloads[1]);
}

#[test]
fn unbounded_beam_row_source_lowers_with_row_coder() {
    let directory = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    ironbeam_prism_proof::beam_row_stream_fixture(&pipeline, directory.path().join("stream-rows"))
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let translated =
        translate_v5(&pipeline, "example.test/ironbeam:v5", "row-stream-build").unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    let source = components
        .pcollections
        .values()
        .find(|pc| pc.unique_name == "beam-row-stream")
        .unwrap();
    let outer = &components.coders[&source.coder_id];
    assert_eq!(outer.spec.as_ref().unwrap().urn, urns::CODER_LENGTH_PREFIX);
    let row = &components.coders[&outer.component_coder_ids[0]];
    assert_eq!(row.spec.as_ref().unwrap().urn, urns::CODER_ROW);
    assert!(!row.spec.as_ref().unwrap().payload.is_empty());
}

#[test]
fn unbounded_row_projection_keeps_distinct_writer_and_reader_coders() {
    let directory = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    ironbeam_prism_proof::beam_row_stream_projection_fixture(
        &pipeline,
        directory.path().join("stream-projection"),
    )
    .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let translated = translate_v5(
        &pipeline,
        "example.test/ironbeam:v5",
        "stream-projection-build",
    )
    .unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    let schemas = [
        "beam-stream-projection-source",
        "beam-stream-projection-project",
    ]
    .map(|name| {
        let collection = components
            .pcollections
            .values()
            .find(|collection| collection.unique_name == name)
            .unwrap();
        let outer = &components.coders[&collection.coder_id];
        assert_eq!(outer.spec.as_ref().unwrap().urn, urns::CODER_LENGTH_PREFIX);
        let row = &components.coders[&outer.component_coder_ids[0]];
        assert_eq!(row.spec.as_ref().unwrap().urn, urns::CODER_ROW);
        row.spec.as_ref().unwrap().payload.clone()
    });
    assert_ne!(schemas[0], schemas[1]);
}
