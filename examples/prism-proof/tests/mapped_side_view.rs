use beam_proto::pipeline_v1::{FixedWindowsPayload, ParDoPayload, SlidingWindowsPayload};
use ironbeam_core::Pipeline;
use ironbeam_graphx::{
    urns,
    v5::{collection_id, translate_v5, validate_beam_contract},
};
use prost_beam::Message;

#[test]
fn cross_window_side_views_keep_native_side_windows_and_mapping_specs() {
    let directory = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    ironbeam_prism_proof::mapped_side_view_fixture(
        &pipeline,
        directory.path().join("mapped-side.txt"),
    )
    .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    graph.validate().unwrap();
    let translated =
        translate_v5(&pipeline, "example.test/ironbeam:v5", "mapped-side-build").unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    for (key, side, urn) in [
        (
            "mapped-side-fixed",
            "mapped-side-fixed-windows",
            urns::WINDOW_MAPPING_FIXED,
        ),
        (
            "mapped-side-sliding",
            "mapped-side-sliding-windows",
            urns::WINDOW_MAPPING_SLIDING,
        ),
        (
            "mapped-side-offset",
            "mapped-side-offset-windows",
            urns::WINDOW_MAPPING_FIXED,
        ),
    ] {
        let transform = components
            .transforms
            .values()
            .find(|transform| transform.unique_name == key)
            .unwrap();
        assert_eq!(transform.inputs["side"], collection_id(side));
        let payload =
            ParDoPayload::decode(transform.spec.as_ref().unwrap().payload.as_slice()).unwrap();
        let mapping = payload.side_inputs["side"]
            .window_mapping_fn
            .as_ref()
            .unwrap();
        assert_eq!(mapping.urn, urn);
        if urn == urns::WINDOW_MAPPING_FIXED {
            let fixed = FixedWindowsPayload::decode(mapping.payload.as_slice()).unwrap();
            assert_eq!(fixed.size.unwrap().nanos, 10_000_000);
            assert_eq!(
                fixed.offset.unwrap().nanos,
                if key == "mapped-side-offset" {
                    2_000_000
                } else {
                    0
                }
            );
        } else {
            let sliding = SlidingWindowsPayload::decode(mapping.payload.as_slice()).unwrap();
            assert_eq!(sliding.size.unwrap().nanos, 10_000_000);
            assert_eq!(sliding.period.unwrap().nanos, 5_000_000);
        }
    }
}
