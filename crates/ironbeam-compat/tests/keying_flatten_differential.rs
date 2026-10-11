//! Keying and flatten behavior across the published v4 API and v5 Direct.

use ironbeam_core::Pipeline as V5Pipeline;
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_transforms::ElementwiseExt;
use ironbeam_v4::{Pipeline as V4Pipeline, flatten, from_vec};

#[test]
fn keying_and_flatten_multisets_match_v4() {
    for count in 0_i32..12 {
        let left_rows = (0..count).map(|value| value % 5).collect::<Vec<_>>();
        let right_rows = (0..count / 2).map(|value| value % 3).collect::<Vec<_>>();

        let v4 = V4Pipeline::default();
        let left_v4 = from_vec(&v4, left_rows.clone());
        let right_v4 = from_vec(&v4, right_rows.clone());
        let mut expected_keyed = left_v4
            .clone()
            .with_keys(|value: &i32| value % 3)
            .collect_seq()
            .unwrap();
        let mut expected_constant = left_v4
            .clone()
            .with_constant_key(7_i32)
            .collect_seq()
            .unwrap();
        let mut expected_flatten = flatten(&[&left_v4, &right_v4]).collect_seq().unwrap();
        expected_keyed.sort_unstable();
        expected_constant.sort_unstable();
        expected_flatten.sort_unstable();

        let v5 = V5Pipeline::default();
        let left_v5 = v5.from_vec("left", left_rows).unwrap();
        let right_v5 = v5.from_vec("right", right_rows).unwrap();
        let keyed = left_v5
            .with_keys_named("keyed", |value| value % 3)
            .unwrap()
            .output("keyed-out")
            .unwrap();
        let constant = left_v5
            .with_constant_key_named("constant", 7_i32)
            .unwrap()
            .output("constant-out")
            .unwrap();
        let flattened = left_v5
            .flatten_named("flatten", &[right_v5])
            .unwrap()
            .output("flatten-out")
            .unwrap();
        for parallelism in [1, 3] {
            let config = JobConfig {
                parallelism,
                ..JobConfig::default()
            };
            let mut actual_keyed = DirectRunner.run_collect(&keyed, &config).unwrap().0;
            let mut actual_constant = DirectRunner.run_collect(&constant, &config).unwrap().0;
            let mut actual_flatten = DirectRunner.run_collect(&flattened, &config).unwrap().0;
            actual_keyed.sort_unstable();
            actual_constant.sort_unstable();
            actual_flatten.sort_unstable();
            assert_eq!(
                actual_keyed, expected_keyed,
                "keying count {count}, workers {parallelism}"
            );
            assert_eq!(
                actual_constant, expected_constant,
                "constant key count {count}, workers {parallelism}"
            );
            assert_eq!(
                actual_flatten, expected_flatten,
                "flatten count {count}, workers {parallelism}"
            );
        }
    }
}
