use ironbeam_core::Pipeline;
use ironbeam_direct::{DirectRunner, JobConfig};

#[test]
fn one_input_cogroup_keeps_one_bucket() {
    let pipeline = Pipeline::default();
    let result = pipeline
        .from_vec("single", vec![(1_i32, 2_i32), (1, 3)])
        .unwrap()
        .co_group_homogeneous_named("group", &[])
        .unwrap()
        .output("result")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&result, &JobConfig::default())
            .unwrap()
            .0,
        [(1, vec![vec![2, 3]])]
    );
}

#[test]
fn twelve_bounded_inputs_keep_each_bucket_and_group_timestamp() {
    let pipeline = Pipeline::default();
    let inputs = (0..12)
        .map(|index| {
            let mut rows = if index == 4 {
                Vec::new()
            } else {
                vec![(1_000_000_i64, ("a".to_owned(), index))]
            };
            if index == 0 {
                rows.push((2_000_000, ("a".to_owned(), 100)));
            }
            if index == 11 {
                rows.push((0, ("z".to_owned(), 99)));
            }
            pipeline
                .from_vec(&format!("input-{index}"), rows)
                .unwrap()
                .timestamps_from_pairs_named(&format!("time-{index}"))
                .unwrap()
                .fixed_windows(&format!("window-{index}"), 10_000_000)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let result = inputs[0]
        .co_group_homogeneous_named("group", &inputs[1..])
        .unwrap()
        .reify_timestamps_named("group-time")
        .unwrap()
        .output("result")
        .unwrap();
    let mut rows = DirectRunner
        .run_collect(&result, &JobConfig::default())
        .unwrap()
        .0;
    rows.sort_by(|left, right| left.1.0.cmp(&right.1.0));
    assert_eq!(rows.len(), 2);
    let (a_time, (a_key, mut a_buckets)) = rows.remove(0);
    assert_eq!((a_time, a_key.as_str()), (1_000_000, "a"));
    for bucket in &mut a_buckets {
        bucket.sort_unstable();
    }
    assert_eq!(a_buckets.len(), 12);
    for (index, bucket) in a_buckets.iter().enumerate() {
        assert_eq!(
            bucket,
            &if index == 4 {
                Vec::new()
            } else if index == 0 {
                vec![0, 100]
            } else {
                vec![i32::try_from(index).unwrap()]
            }
        );
    }
    let (z_time, (z_key, z_buckets)) = rows.remove(0);
    assert_eq!((z_time, z_key.as_str()), (0, "z"));
    assert_eq!(z_buckets.len(), 12);
    assert!(z_buckets.iter().take(11).all(Vec::is_empty));
    assert_eq!(z_buckets[11], [99]);
}
