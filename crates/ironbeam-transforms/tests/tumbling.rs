use ironbeam_core::{Pipeline, WindowInterval};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_transforms::{KeyedWindowedExt, WindowedExt};

#[test]
fn tumbling_helpers_keep_v4_group_shapes_with_signed_offsets() {
    let pipeline = Pipeline::default();
    let values = pipeline
        .from_vec(
            "values",
            vec![
                (1_i64, "a".to_owned()),
                (9, "b".to_owned()),
                (11, "c".to_owned()),
            ],
        )
        .unwrap()
        .timestamps_from_pairs_named("times")
        .unwrap();
    let keyed = values
        .key_by_window_named("window-key", 10, 0)
        .unwrap()
        .output("window-key-out")
        .unwrap();
    let grouped = values
        .group_by_window_named("window-group", 10, 0)
        .unwrap()
        .output("window-group-out")
        .unwrap();
    let kv = pipeline
        .from_vec(
            "kv",
            vec![
                (1_i64, ("k".to_owned(), 10)),
                (9, ("k".to_owned(), 20)),
                (11, ("k".to_owned(), 30)),
            ],
        )
        .unwrap()
        .timestamps_from_pairs_named("kv-times")
        .unwrap();
    let keyed_group = kv
        .group_by_key_and_window_named("key-window-group", 10, 0)
        .unwrap()
        .output("key-window-group-out")
        .unwrap();
    let keyed_items = kv
        .key_by_key_and_window_named("key-window", 10, 0)
        .unwrap()
        .output("key-window-out")
        .unwrap();
    let first = WindowInterval {
        start_ns: 0,
        end_ns: 10,
    };
    let second = WindowInterval {
        start_ns: 10,
        end_ns: 20,
    };
    for parallelism in [1, 3] {
        let config = JobConfig {
            parallelism,
            ..JobConfig::default()
        };
        assert_eq!(
            DirectRunner.run_collect(&keyed, &config).unwrap().0,
            vec![
                (first, "a".to_owned()),
                (first, "b".to_owned()),
                (second, "c".to_owned())
            ]
        );
        assert_eq!(
            DirectRunner.run_collect(&grouped, &config).unwrap().0,
            vec![
                (first, vec!["a".to_owned(), "b".to_owned()]),
                (second, vec!["c".to_owned()])
            ]
        );
        assert_eq!(
            DirectRunner.run_collect(&keyed_group, &config).unwrap().0,
            vec![
                (("k".to_owned(), first), vec![10, 20]),
                (("k".to_owned(), second), vec![30])
            ]
        );
        assert_eq!(
            DirectRunner.run_collect(&keyed_items, &config).unwrap().0,
            vec![
                (("k".to_owned(), first), 10),
                (("k".to_owned(), first), 20),
                (("k".to_owned(), second), 30),
            ]
        );
    }
    assert!(values.key_by_window_named("invalid", 0, 0).is_err());
}
