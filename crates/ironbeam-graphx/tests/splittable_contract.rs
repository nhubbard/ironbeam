use beam_proto::pipeline_v1::ParDoPayload;
use ironbeam_core::{OffsetRange, Pipeline, Version};
use ironbeam_graphx::{
    urns,
    v5::{translate_v5, validate_beam_contract},
};
use prost::Message;

#[test]
fn user_splittable_transform_lowers_with_sdf_requirement_and_restriction_coder() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("input", vec![2_i32])
        .unwrap()
        .splittable_flat_map_named(
            "offsets",
            "tests/portable-offsets",
            Version { major: 1, minor: 0 },
            |_| OffsetRange::new(0, 2),
            |_, context| {
                for offset in 0..2 {
                    assert!(context.try_claim(offset)?);
                    context.emit(offset, offset * 1_000_000)?;
                }
                Ok(())
            },
        )
        .unwrap()
        .output("out")
        .unwrap();
    let translated = translate_v5(&pipeline, "example.test/ironbeam:v5", "test-build").unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    assert!(translated
        .pipeline
        .requirements
        .contains(&urns::REQUIREMENT_SDF.to_owned()));
    let components = translated.pipeline.components.as_ref().unwrap();
    let transform = components
        .transforms
        .values()
        .find(|transform| transform.unique_name == "offsets")
        .unwrap();
    let pardo = ParDoPayload::decode(transform.spec.as_ref().unwrap().payload.as_slice()).unwrap();
    assert!(!pardo.restriction_coder_id.is_empty());
    assert_eq!(
        components.coders[&pardo.restriction_coder_id]
            .spec
            .as_ref()
            .unwrap()
            .urn,
        urns::CODER_BYTES
    );
    let mut altered = translated;
    altered.pipeline.requirements.clear();
    assert!(validate_beam_contract(&graph, &altered).is_err());
}
