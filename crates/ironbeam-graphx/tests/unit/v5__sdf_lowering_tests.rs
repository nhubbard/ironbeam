// Unit tests for src/v5.rs.
use super::*;
use ironbeam_core::{CombineFn, Error, Replay, SourceProgress};

struct Sum;

impl CombineFn<i32, i32, i32> for Sum {
    fn create(&self) -> i32 {
        0
    }
    fn add_input(&self, total: &mut i32, value: i32) {
        *total += value;
    }
    fn merge(&self, total: &mut i32, other: i32) {
        *total += other;
    }
    fn finish(&self, total: i32) -> i32 {
        total
    }
}

#[test]
fn keyed_group_propagates_kv_coder_through_nested_flatten_inputs() {
    let pipeline = Pipeline::default();
    let left = pipeline.from_vec("left", vec![(1_i32, 2_i32)]).unwrap();
    let right = pipeline.from_vec("right", vec![(1_i32, 3_i32)]).unwrap();
    let extra = pipeline.from_vec("extra", vec![(2_i32, 4_i32)]).unwrap();
    left.flatten_named("first-merge", &[right])
        .unwrap()
        .flatten_named("second-merge", &[extra])
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let translated = translate_v5(&pipeline, "image", "flatten-group-build").unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let collections = &translated
        .pipeline
        .components
        .as_ref()
        .unwrap()
        .pcollections;
    for key in ["left", "right", "extra", "first-merge", "second-merge"] {
        assert_eq!(
            collections[&collection_id(key)].coder_id,
            CODER_V5_KV,
            "{key}"
        );
    }
    assert_eq!(
        collections[&collection_id("group")].coder_id,
        CODER_V5_KV_ITER
    );
}

#[test]
fn keyed_fold_lowers_to_native_gbk_then_typed_pardo() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![(1_i32, 2_i32), (1, 3), (2, 4)])
        .unwrap()
        .combine_values_lifted_named("sum", Sum)
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    assert!(registry.has_group_coder("sum"));
    assert!(registry.has_fold("sum"));
    let translated = translate_v5(&pipeline, "image", "fold-build").unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    assert_ne!(
        components.pcollections[&collection_id("pairs")].coder_id,
        CODER_V5_KV
    );
    assert_eq!(
        components.pcollections[&fold_tag_collection_id("sum")].coder_id,
        CODER_V5_KV
    );
    assert_eq!(
        components.pcollections[&fold_group_collection_id("sum")].coder_id,
        CODER_V5_KV_ITER
    );
    assert_eq!(
        components.transforms[&fold_tag_transform_id("sum")]
            .spec
            .as_ref()
            .unwrap()
            .urn,
        urns::TRANSFORM_PARDO
    );
    assert_eq!(
        components.transforms[&fold_group_transform_id("sum")]
            .spec
            .as_ref()
            .unwrap()
            .urn,
        urns::TRANSFORM_GROUP_BY_KEY
    );
    assert_eq!(
        components.transforms[&id("v5_t", "sum")]
            .spec
            .as_ref()
            .unwrap()
            .urn,
        urns::TRANSFORM_PARDO
    );
    let mut altered = translated;
    altered
        .pipeline
        .components
        .as_mut()
        .unwrap()
        .transforms
        .get_mut(&fold_group_transform_id("sum"))
        .unwrap()
        .spec
        .as_mut()
        .unwrap()
        .urn = urns::TRANSFORM_FLATTEN.into();
    assert!(validate_beam_contract(&graph, &altered).is_err());
}

#[test]
fn bounded_global_fold_seeds_empty_input_before_gbk() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("numbers", Vec::<i32>::new())
        .unwrap()
        .combine_globally_lifted_named("sum", Sum)
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    assert!(registry.has_group_coder("sum"));
    assert!(registry.has_fold("sum"));
    let translated = translate_v5(&pipeline, "image", "global-fold-build").unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    assert_eq!(
        components.pcollections[&fold_seed_collection_id("sum")].coder_id,
        CODER_V5_KV
    );
    assert_eq!(
        components.pcollections[&fold_flatten_collection_id("sum")].coder_id,
        CODER_V5_KV
    );
    assert_eq!(
        components.transforms[&fold_group_transform_id("sum")].inputs["in"],
        fold_flatten_collection_id("sum")
    );
    let mut altered = translated;
    altered
        .pipeline
        .components
        .as_mut()
        .unwrap()
        .transforms
        .get_mut(&fold_seed_transform_id("sum"))
        .unwrap()
        .inputs
        .insert("in".into(), collection_id("numbers"));
    assert!(validate_beam_contract(&graph, &altered).is_err());
}

#[test]
fn vector_global_combine_uses_typed_gbk_and_empty_seed() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("numbers", Vec::<i32>::new())
        .unwrap()
        .combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    assert!(registry.has_group_coder("sum"));
    assert!(registry.has_aggregate("sum"));
    let translated = translate_v5(&pipeline, "image", "vector-global-build").unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    assert_eq!(
        components.transforms[&fold_group_transform_id("sum")]
            .spec
            .as_ref()
            .unwrap()
            .urn,
        urns::TRANSFORM_GROUP_BY_KEY
    );
    assert_eq!(
        components.transforms[&fold_group_transform_id("sum")].inputs["in"],
        fold_flatten_collection_id("sum")
    );
}

#[test]
fn production_source_declares_splittable_pardo_for_runner_expansion() {
    let pipeline = Pipeline::default();
    pipeline
        .unbounded_source_with_factory::<i32>(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "events-config-v1".into(),
            Time::default().initial_timestamp_ns,
            |_| Err(Error::incompatible("lowering must not open source")),
        )
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let identity = execution_identity(&graph, &registry, "sdf-build").unwrap();
    let (beam, payloads) = lower(
        &graph,
        &registry,
        &["events".into()],
        &identity,
        "image",
        &HashSet::new(),
        &HashMap::new(),
    )
    .unwrap();
    assert!(payloads.is_empty());
    assert_eq!(beam.requirements, vec![urns::REQUIREMENT_SDF]);
    let components = beam.components.unwrap();
    let output_pc = id("v5_pc", "events");
    assert_eq!(
        components.pcollections[&output_pc].is_bounded,
        is_bounded::Enum::Unbounded as i32
    );
    assert_eq!(components.transforms.len(), 2);
    let transform = &components.transforms[&id("v5_t", "events")];
    assert_eq!(transform.spec.as_ref().unwrap().urn, urns::TRANSFORM_PARDO);
    assert_eq!(transform.inputs["in"], "v5_impulse_pc");
    assert_eq!(transform.outputs["out"], output_pc);
    let payload =
        ParDoPayload::decode(transform.spec.as_ref().unwrap().payload.as_slice()).unwrap();
    assert_eq!(payload.restriction_coder_id, ids::CODER_BYTES);
    let do_fn = payload.do_fn.unwrap();
    assert_eq!(do_fn.urn, urns::DOFN_IRONBEAM_V5);
    assert_eq!(V5NodeRef::decode(&do_fn.payload).unwrap().key, "events");
    let translated = translate_v5(&pipeline, "image", "sdf-build").unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
}
