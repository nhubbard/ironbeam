use ironbeam_core::Pipeline;
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_transforms::{KeyedSamplingExt, SamplingExt};

#[test]
fn seeded_samples_are_stable_and_have_v4_shape() {
    let p = Pipeline::default();
    let input = p.from_vec("input", (0_i32..20).collect()).unwrap();
    let sample = input
        .sample_reservoir_vec_named("seeded", 5, 42)
        .unwrap()
        .output("seeded-out")
        .unwrap();
    let flattened = input
        .sample_reservoir_named("flat-sample", "flat", 5, 42)
        .unwrap()
        .output("flat-out")
        .unwrap();
    let default_sample = input
        .sample_globally_named("default", 5)
        .unwrap()
        .output("default-out")
        .unwrap();
    let zero = input
        .sample_globally_with_seed_named("zero", 0, 42)
        .unwrap()
        .output("zero-out")
        .unwrap();
    for parallelism in [1, 3] {
        let cfg = JobConfig {
            parallelism,
            ..JobConfig::default()
        };
        let values = DirectRunner.run_collect(&sample, &cfg).unwrap().0;
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], [15, 11, 19, 9, 4]);
        assert_eq!(
            DirectRunner.run_collect(&flattened, &cfg).unwrap().0,
            values[0]
        );
        assert_eq!(
            DirectRunner.run_collect(&default_sample, &cfg).unwrap().0[0].len(),
            5
        );
        assert_eq!(
            DirectRunner.run_collect(&zero, &cfg).unwrap().0,
            [Vec::<i32>::new()]
        );
    }
}

#[test]
fn per_key_reservoir_keeps_each_group_independent() {
    let p = Pipeline::default();
    let input = p
        .from_vec(
            "pairs",
            vec![
                ("a".to_string(), 1_i32),
                ("a".into(), 2),
                ("a".into(), 3),
                ("b".into(), 9),
            ],
        )
        .unwrap();
    let sampled = input
        .sample_per_key_with_seed_named("group", "sample", 2, 17)
        .unwrap()
        .output("out")
        .unwrap();
    let values = DirectRunner
        .run_collect(&sampled, &JobConfig::default())
        .unwrap()
        .0;
    assert_eq!(values.len(), 2);
    assert_eq!(values[0].0, "a");
    assert_eq!(values[0].1.len(), 2);
    assert!(values[0].1.iter().all(|value| [1, 2, 3].contains(value)));
    assert_eq!(values[1], ("b".to_string(), vec![9]));
}
