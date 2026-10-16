use super::*;
use ironbeam_core::{BeamShardedKey, TestStreamEvent, Version};

#[test]
fn sharded_key_lowers_as_standard_coder_with_one_key_component() {
    fn check<K: ironbeam_core::BeamPortableKey>(key: K, expected_urn: &str) {
        let pipeline = Pipeline::default();
        pipeline
            .from_vec("shards", vec![BeamShardedKey::new(key, vec![1])])
            .unwrap()
            .output("result")
            .unwrap();
        let (graph, _) = pipeline.snapshot().unwrap();
        let translated = translate_v5(&pipeline, "image", "sharded-key-build").unwrap();
        validate_beam_contract(&graph, &translated).unwrap();
        let components = translated.pipeline.components.as_ref().unwrap();
        let outer_id = &components.pcollections[&collection_id("shards")].coder_id;
        let outer = &components.coders[outer_id];
        assert_eq!(outer.spec.as_ref().unwrap().urn, urns::CODER_LENGTH_PREFIX);
        let sharded = &components.coders[&outer.component_coder_ids[0]];
        assert_eq!(sharded.spec.as_ref().unwrap().urn, urns::CODER_SHARDED_KEY);
        assert_eq!(sharded.spec.as_ref().unwrap().payload, Vec::<u8>::new());
        assert_eq!(sharded.component_coder_ids.len(), 1);
        let key_coder = &components.coders[&sharded.component_coder_ids[0]];
        assert_eq!(key_coder.spec.as_ref().unwrap().urn, expected_urn);

        let key_coder_id = sharded.component_coder_ids[0].clone();
        let mut changed = translated;
        let changed_components = changed.pipeline.components.as_mut().unwrap();
        changed_components
            .coders
            .get_mut(&key_coder_id)
            .unwrap()
            .spec
            .as_mut()
            .unwrap()
            .urn = urns::CODER_VARINT.into();
        assert!(validate_beam_contract(&graph, &changed).is_err());
    }
    check("key".to_owned(), urns::CODER_STRING_UTF8);
    check(vec![0, 255], urns::CODER_BYTES);
}

#[test]
fn stateful_sharded_batch_keeps_kv_coder_before_reshuffle() {
    let pipeline = Pipeline::default();
    pipeline
        .test_stream_source(
            "input",
            vec![
                TestStreamEvent::Elements(vec![
                    (("hot".to_owned(), (1_u8, "a".to_owned())), 1_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .group_into_batches_with_limits_and_sharded_key_named(
            "batches",
            2,
            100,
            "tests/shard-selection",
            Version { major: 1, minor: 0 },
            |_, (shard, _)| vec![*shard],
        )
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let translated = translate_v5(&pipeline, "image", "sharded-state-build").unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    for key in ["batches/assign-shard", "batches/shard-boundary"] {
        assert_eq!(
            components.pcollections[&collection_id(key)].coder_id,
            CODER_V5_KV,
            "{key} must retain key framing if Prism elides the reshuffle"
        );
    }
}

#[test]
fn keyed_test_stream_reaches_stateful_consumer_through_reshuffle() {
    let pipeline = Pipeline::default();
    pipeline
        .test_stream_source(
            "source",
            vec![TestStreamEvent::Elements(vec![((1_i32, 2_i32), 0)])],
        )
        .unwrap()
        .reshuffle_named("boundary")
        .unwrap()
        .stateful_value_map_named::<i32, i32>(
            "state",
            "tests/reshuffled-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let next = prior.unwrap_or(0) + value;
                Ok((Some(next), next))
            },
        )
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let translated = translate_v5(&pipeline, "image", "reshuffled-state-build").unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    assert_eq!(components.pcollections[&collection_id("source")].coder_id, CODER_V5_KV);
    assert_eq!(components.pcollections[&collection_id("boundary")].coder_id, CODER_V5_KV);
}
