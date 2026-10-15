use super::*;

#[tokio::test]
async fn prism_reads_provider_bound_avro_with_a_defaulted_reader_field() {
    let directory = tempfile::tempdir().unwrap();
    let object_root = directory.path().join("objects");
    let seed_path = directory.path().join("seed.avro");
    let seed = Pipeline::default()
        .from_vec(
            "object-avro-seed",
            vec![
                ProductionAvroRow {
                    id: 6,
                    label: "first".into(),
                },
                ProductionAvroRow {
                    id: 7,
                    label: "second".into(),
                },
            ],
        )
        .unwrap();
    DirectRunner
        .run_sink(
            &ironbeam_io_avro::avro_sink(
                &seed,
                "object-avro-seed-sink",
                &seed_path,
                PRODUCTION_AVRO_SCHEMA,
            )
            .unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let mut stores = ObjectStores::new();
    stores
        .register("file", LocalObjectIO::new(&object_root).unwrap())
        .unwrap();
    stores
        .write(
            "file://bucket/input/rows.avro",
            &std::fs::read(seed_path).unwrap(),
        )
        .unwrap();
    let direct_pipeline = Pipeline::default();
    let direct = portable_object_avro_collection(&direct_pipeline, &object_root)
        .unwrap()
        .output("object-avro-direct-output")
        .unwrap()
        .collect_seq()
        .unwrap();
    assert_eq!(direct, ["6:first:5", "7:second:5"]);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let path = directory.path().join("object-avro-result.txt");
    let portable = Pipeline::default();
    portable_object_avro_fixture(&portable, &object_root, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "provider-bound-avro",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_OBJECT_AVRO_ROOT".into(),
                object_root.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_OBJECT_AVRO_RESULT".into(),
                path.to_string_lossy().into_owned(),
            ),
        ]),
    )
    .await;
    assert_eq!(terminal.0, 3, "portable object Avro job: {terminal:?}");
    assert_eq!(
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        ["6:first:5", "7:second:5"]
    );
}

#[tokio::test]
async fn prism_reopens_provider_bound_avro_after_execution_child_loss() {
    let directory = tempfile::tempdir().unwrap();
    let object_root = directory.path().join("objects");
    let seed_path = directory.path().join("seed.avro");
    let seed = Pipeline::default()
        .from_vec(
            "object-avro-replay-seed",
            vec![
                ProductionAvroRow {
                    id: 6,
                    label: "first".into(),
                },
                ProductionAvroRow {
                    id: 7,
                    label: "second".into(),
                },
            ],
        )
        .unwrap();
    DirectRunner
        .run_sink(
            &ironbeam_io_avro::avro_sink(
                &seed,
                "object-avro-replay-seed-sink",
                &seed_path,
                PRODUCTION_AVRO_SCHEMA,
            )
            .unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let mut stores = ObjectStores::new();
    stores
        .register("file", LocalObjectIO::new(&object_root).unwrap())
        .unwrap();
    stores
        .write(
            "file://bucket/input/rows.avro",
            &std::fs::read(seed_path).unwrap(),
        )
        .unwrap();
    let direct_path = directory.path().join("direct-avro-rows");
    let direct = Pipeline::default();
    let direct_sink =
        portable_object_avro_replay_fixture(&direct, &object_root, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    assert_eq!(read_rows(&direct_path), ["6:first:5", "7:second:5"]);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_path = directory.path().join("portable-avro-rows");
    let trigger = directory.path().join("avro-replay-trigger");
    let killed = directory.path().join("killed-avro-child");
    let portable = Pipeline::default();
    portable_object_avro_replay_fixture(&portable, &object_root, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "replayed-provider-bound-avro",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_OBJECT_AVRO_REPLAY_ROOT".into(),
                object_root.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_OBJECT_AVRO_REPLAY_RESULT".into(),
                portable_path.to_string_lossy().into_owned(),
            ),
            ("IRONBEAM_V5_PRISM_KILL_OBJECT_AVRO".into(), "1".into()),
            ("IRONBEAM_V5_SUPERVISE_EXECUTION".into(), "1".into()),
            (
                "IRONBEAM_V5_SUPERVISOR_TRIGGER".into(),
                trigger.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_SUPERVISOR_KILLED".into(),
                killed.to_string_lossy().into_owned(),
            ),
        ]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "Prism provider Avro replay failed: {terminal:?}"
    );
    assert_eq!(
        std::fs::read_to_string(&killed).unwrap(),
        std::fs::read_to_string(&trigger).unwrap()
    );
    assert_eq!(read_rows(&portable_path), ["6:first:5", "7:second:5"]);
}

#[tokio::test]
async fn prism_reads_provider_bound_parquet_rows() {
    let directory = tempfile::tempdir().unwrap();
    let object_root = directory.path().join("objects");
    let seed_path = directory.path().join("seed.parquet");
    let seed = Pipeline::default()
        .from_vec(
            "object-parquet-seed",
            vec![
                ProductionAvroRow {
                    id: 8,
                    label: "third".into(),
                },
                ProductionAvroRow {
                    id: 9,
                    label: "fourth".into(),
                },
            ],
        )
        .unwrap();
    DirectRunner
        .run_sink(
            &ironbeam_io_parquet::parquet_sink(&seed, "object-parquet-seed-sink", &seed_path)
                .unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let mut stores = ObjectStores::new();
    stores
        .register("file", LocalObjectIO::new(&object_root).unwrap())
        .unwrap();
    stores
        .write(
            "file://bucket/input/a.parquet",
            &std::fs::read(seed_path).unwrap(),
        )
        .unwrap();
    let second_path = directory.path().join("second.parquet");
    let second = Pipeline::default()
        .from_vec(
            "object-parquet-second-seed",
            vec![ProductionAvroRow {
                id: 10,
                label: "fifth".into(),
            }],
        )
        .unwrap();
    DirectRunner
        .run_sink(
            &ironbeam_io_parquet::parquet_sink(
                &second,
                "object-parquet-second-seed-sink",
                &second_path,
            )
            .unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    stores
        .write(
            "file://bucket/input/b.parquet",
            &std::fs::read(second_path).unwrap(),
        )
        .unwrap();
    let direct_pipeline = Pipeline::default();
    let mut direct = portable_object_parquet_collection(&direct_pipeline, &object_root)
        .unwrap()
        .output("object-parquet-direct-output")
        .unwrap()
        .collect_seq()
        .unwrap();
    direct.sort();
    assert_eq!(direct, ["10:fifth", "8:third", "9:fourth"]);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let path = directory.path().join("object-parquet-result.txt");
    let portable = Pipeline::default();
    portable_object_parquet_fixture(&portable, &object_root, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "provider-bound-parquet",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_OBJECT_PARQUET_ROOT".into(),
                object_root.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_OBJECT_PARQUET_RESULT".into(),
                path.to_string_lossy().into_owned(),
            ),
        ]),
    )
    .await;
    assert_eq!(terminal.0, 3, "portable object Parquet job: {terminal:?}");
    let result = std::fs::read_to_string(path).unwrap();
    let mut portable_rows = result.lines().collect::<Vec<_>>();
    portable_rows.sort_unstable();
    assert_eq!(portable_rows, ["10:fifth", "8:third", "9:fourth"]);
}

#[tokio::test]
async fn prism_reopens_provider_bound_parquet_prefix_after_execution_child_loss() {
    let directory = tempfile::tempdir().unwrap();
    let object_root = directory.path().join("objects");
    let mut stores = ObjectStores::new();
    stores
        .register("file", LocalObjectIO::new(&object_root).unwrap())
        .unwrap();
    for (name, rows) in [
        (
            "a.parquet",
            vec![
                ProductionAvroRow {
                    id: 8,
                    label: "third".into(),
                },
                ProductionAvroRow {
                    id: 9,
                    label: "fourth".into(),
                },
            ],
        ),
        (
            "b.parquet",
            vec![ProductionAvroRow {
                id: 10,
                label: "fifth".into(),
            }],
        ),
    ] {
        let seed_path = directory.path().join(name);
        let seed = Pipeline::default()
            .from_vec(&format!("seed-{name}"), rows)
            .unwrap();
        DirectRunner
            .run_sink(
                &ironbeam_io_parquet::parquet_sink(&seed, &format!("sink-{name}"), &seed_path)
                    .unwrap(),
                &JobConfig::default(),
            )
            .unwrap();
        stores
            .write(
                &format!("file://bucket/input/{name}"),
                &std::fs::read(seed_path).unwrap(),
            )
            .unwrap();
    }
    let direct_path = directory.path().join("direct-parquet-rows");
    let direct = Pipeline::default();
    let direct_sink =
        portable_object_parquet_replay_fixture(&direct, &object_root, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    assert_eq!(read_rows(&direct_path), ["10:fifth", "8:third", "9:fourth"]);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_path = directory.path().join("portable-parquet-rows");
    let trigger = directory.path().join("parquet-replay-trigger");
    let killed = directory.path().join("killed-parquet-child");
    let portable = Pipeline::default();
    portable_object_parquet_replay_fixture(&portable, &object_root, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "replayed-provider-bound-parquet-prefix",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_OBJECT_PARQUET_REPLAY_ROOT".into(),
                object_root.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_OBJECT_PARQUET_REPLAY_RESULT".into(),
                portable_path.to_string_lossy().into_owned(),
            ),
            ("IRONBEAM_V5_PRISM_KILL_OBJECT_PARQUET".into(), "1".into()),
            ("IRONBEAM_V5_SUPERVISE_EXECUTION".into(), "1".into()),
            (
                "IRONBEAM_V5_SUPERVISOR_TRIGGER".into(),
                trigger.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_SUPERVISOR_KILLED".into(),
                killed.to_string_lossy().into_owned(),
            ),
        ]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "Prism provider Parquet replay failed: {terminal:?}"
    );
    assert_eq!(
        std::fs::read_to_string(&killed).unwrap(),
        std::fs::read_to_string(&trigger).unwrap()
    );
    assert_eq!(
        read_rows(&portable_path),
        ["10:fifth", "8:third", "9:fourth"]
    );
}
