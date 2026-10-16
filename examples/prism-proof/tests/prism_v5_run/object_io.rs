use super::*;

#[tokio::test]
async fn prism_reopens_streaming_cbor_msgpack_and_arrow_prefixes_after_execution_child_loss() {
    let directory = tempfile::tempdir().unwrap();
    let object_root = directory.path().join("objects");
    for (format, name, row) in [
        (
            "cbor",
            "b.cbor",
            ProductionAvroRow {
                id: 2,
                label: "two".into(),
            },
        ),
        (
            "cbor",
            "a.cbor",
            ProductionAvroRow {
                id: 1,
                label: "one".into(),
            },
        ),
        (
            "msgpack",
            "b.msgpack",
            ProductionAvroRow {
                id: 4,
                label: "four".into(),
            },
        ),
        (
            "msgpack",
            "a.msgpack",
            ProductionAvroRow {
                id: 3,
                label: "three".into(),
            },
        ),
    ] {
        let path = object_root.join("bucket").join(format).join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let pipeline = Pipeline::default();
        let values = pipeline.from_vec("seed", vec![row]).unwrap();
        let sink = match format {
            "cbor" => ironbeam_io_cbor::cbor_sink(&values, "write", path),
            _ => ironbeam_io_msgpack::msgpack_sink(&values, "write", path),
        }
        .unwrap();
        DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    }

    for (name, row) in [
        (
            "b.arrow",
            ProductionAvroRow {
                id: 6,
                label: "six".into(),
            },
        ),
        (
            "a.arrow",
            ProductionAvroRow {
                id: 5,
                label: "five".into(),
            },
        ),
    ] {
        let path = object_root.join("bucket/arrow").join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let pipeline = Pipeline::default();
        let values = pipeline.from_vec("seed", vec![row]).unwrap();
        let sink = ironbeam_io_arrow::arrow_ipc_sink(&values, "write", path).unwrap();
        DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    }

    let expected = [
        "cbor-1:one",
        "cbor-2:two",
        "msgpack-3:three",
        "msgpack-4:four",
        "arrow-5:five",
        "arrow-6:six",
    ];
    let direct = Pipeline::default();
    assert_eq!(
        ironbeam_prism_proof::portable_object_binary_collection(&direct, &object_root)
            .unwrap()
            .collect_seq()
            .unwrap(),
        expected
    );
    let direct_path = directory.path().join("direct-binary-rows");
    let direct = Pipeline::default();
    let direct_sink = ironbeam_prism_proof::portable_object_binary_replay_fixture(
        &direct,
        &object_root,
        direct_path.clone(),
    )
    .unwrap();
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
    let mut sorted_expected = expected.map(str::to_owned);
    sorted_expected.sort();
    assert_eq!(read_rows(&direct_path), sorted_expected);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_path = directory.path().join("portable-binary-rows");
    let trigger = directory.path().join("binary-replay-trigger");
    let killed = directory.path().join("killed-binary-child");
    let portable = Pipeline::default();
    ironbeam_prism_proof::portable_object_binary_replay_fixture(
        &portable,
        &object_root,
        portable_path.clone(),
    )
    .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "replayed-streaming-provider-binary",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_OBJECT_BINARY_REPLAY_ROOT".into(),
                object_root.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_OBJECT_BINARY_REPLAY_RESULT".into(),
                portable_path.to_string_lossy().into_owned(),
            ),
            ("IRONBEAM_V5_PRISM_KILL_OBJECT_BINARY".into(), "1".into()),
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
    assert_eq!(terminal.0, 3, "Prism binary replay failed: {terminal:?}");
    assert_eq!(
        std::fs::read_to_string(&killed).unwrap(),
        std::fs::read_to_string(&trigger).unwrap()
    );
    assert_eq!(read_rows(&portable_path), sorted_expected);
}

#[tokio::test]
async fn prism_reopens_streaming_protobuf_prefix_after_execution_child_loss() {
    let directory = tempfile::tempdir().unwrap();
    let object_root = directory.path().join("objects");
    let input = object_root.join("bucket/input");
    std::fs::create_dir_all(&input).unwrap();
    for (name, value) in [("b.pb", 2), ("a.pb", 1)] {
        let pipeline = Pipeline::default();
        let values = pipeline
            .from_vec("seed", vec![ProductionProtoRow { value }])
            .unwrap();
        let sink = ironbeam_io_protobuf::protobuf_sink(&values, "write", input.join(name)).unwrap();
        DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    }

    let direct = Pipeline::default();
    assert_eq!(
        portable_object_protobuf_collection(&direct, &object_root)
            .unwrap()
            .collect_seq()
            .unwrap(),
        ["row-1", "row-2"]
    );
    let direct_path = directory.path().join("direct-protobuf-rows");
    let direct = Pipeline::default();
    let direct_sink =
        portable_object_protobuf_replay_fixture(&direct, &object_root, direct_path.clone())
            .unwrap();
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
    assert_eq!(read_rows(&direct_path), ["row-1", "row-2"]);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_path = directory.path().join("portable-protobuf-rows");
    let trigger = directory.path().join("protobuf-replay-trigger");
    let killed = directory.path().join("killed-protobuf-child");
    let portable = Pipeline::default();
    portable_object_protobuf_replay_fixture(&portable, &object_root, portable_path.clone())
        .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "replayed-streaming-provider-protobuf",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_OBJECT_PROTOBUF_REPLAY_ROOT".into(),
                object_root.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_OBJECT_PROTOBUF_REPLAY_RESULT".into(),
                portable_path.to_string_lossy().into_owned(),
            ),
            ("IRONBEAM_V5_PRISM_KILL_OBJECT_PROTOBUF".into(), "1".into()),
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
    assert_eq!(terminal.0, 3, "Prism Protobuf replay failed: {terminal:?}");
    assert_eq!(
        std::fs::read_to_string(&killed).unwrap(),
        std::fs::read_to_string(&trigger).unwrap()
    );
    assert_eq!(read_rows(&portable_path), ["row-1", "row-2"]);
}

#[tokio::test]
async fn prism_reopens_streaming_tfrecord_prefix_after_execution_child_loss() {
    let directory = tempfile::tempdir().unwrap();
    let object_root = directory.path().join("objects");
    let input = object_root.join("bucket/input");
    std::fs::create_dir_all(&input).unwrap();
    for (name, text) in [("b.tfrecord", "two"), ("a.tfrecord", "one")] {
        let pipeline = Pipeline::default();
        let values = pipeline
            .from_vec("seed", vec![text.as_bytes().to_vec()])
            .unwrap();
        let sink = ironbeam_io_tfrecord::tfrecord_sink(&values, "write", input.join(name)).unwrap();
        DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    }

    let direct = Pipeline::default();
    assert_eq!(
        portable_object_tfrecord_collection(&direct, &object_root)
            .unwrap()
            .collect_seq()
            .unwrap(),
        ["one", "two"]
    );
    let direct_path = directory.path().join("direct-tfrecord-rows");
    let direct = Pipeline::default();
    let direct_sink =
        portable_object_tfrecord_replay_fixture(&direct, &object_root, direct_path.clone())
            .unwrap();
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
    assert_eq!(read_rows(&direct_path), ["one", "two"]);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_path = directory.path().join("portable-tfrecord-rows");
    let trigger = directory.path().join("tfrecord-replay-trigger");
    let killed = directory.path().join("killed-tfrecord-child");
    let portable = Pipeline::default();
    portable_object_tfrecord_replay_fixture(&portable, &object_root, portable_path.clone())
        .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "replayed-streaming-provider-tfrecord",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_OBJECT_TFRECORD_REPLAY_ROOT".into(),
                object_root.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_OBJECT_TFRECORD_REPLAY_RESULT".into(),
                portable_path.to_string_lossy().into_owned(),
            ),
            ("IRONBEAM_V5_PRISM_KILL_OBJECT_TFRECORD".into(), "1".into()),
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
    assert_eq!(terminal.0, 3, "Prism TFRecord replay failed: {terminal:?}");
    assert_eq!(
        std::fs::read_to_string(&killed).unwrap(),
        std::fs::read_to_string(&trigger).unwrap()
    );
    assert_eq!(read_rows(&portable_path), ["one", "two"]);
}

#[tokio::test]
async fn prism_reopens_streaming_csv_prefix_after_execution_child_loss() {
    let directory = tempfile::tempdir().unwrap();
    let object_root = directory.path().join("objects");
    let mut stores = ObjectStores::new();
    stores
        .register("file", LocalObjectIO::new(&object_root).unwrap())
        .unwrap();
    stores
        .write("file://bucket/input/b.csv", b"id,label\n2,two\n")
        .unwrap();
    stores
        .write("file://bucket/input/a.csv", b"id,label\n1,one\n")
        .unwrap();

    let direct = Pipeline::default();
    assert_eq!(
        portable_object_csv_collection(&direct, &object_root)
            .unwrap()
            .collect_seq()
            .unwrap(),
        ["1:one", "2:two"]
    );
    let direct_path = directory.path().join("direct-csv-rows");
    let direct = Pipeline::default();
    let direct_sink =
        portable_object_csv_replay_fixture(&direct, &object_root, direct_path.clone()).unwrap();
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
    assert_eq!(read_rows(&direct_path), ["1:one", "2:two"]);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_path = directory.path().join("portable-csv-rows");
    let trigger = directory.path().join("csv-replay-trigger");
    let killed = directory.path().join("killed-csv-child");
    let portable = Pipeline::default();
    portable_object_csv_replay_fixture(&portable, &object_root, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "replayed-streaming-provider-csv",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_OBJECT_CSV_REPLAY_ROOT".into(),
                object_root.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_OBJECT_CSV_REPLAY_RESULT".into(),
                portable_path.to_string_lossy().into_owned(),
            ),
            ("IRONBEAM_V5_PRISM_KILL_OBJECT_CSV".into(), "1".into()),
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
        "Prism streaming CSV replay failed: {terminal:?}"
    );
    assert_eq!(
        std::fs::read_to_string(&killed).unwrap(),
        std::fs::read_to_string(&trigger).unwrap()
    );
    assert_eq!(read_rows(&portable_path), ["1:one", "2:two"]);
}

#[tokio::test]
async fn prism_reopens_xml_object_prefix_after_execution_child_loss() {
    let directory = tempfile::tempdir().unwrap();
    let object_root = directory.path().join("objects");
    let mut stores = ObjectStores::new();
    stores
        .register("file", LocalObjectIO::new(&object_root).unwrap())
        .unwrap();
    stores
        .write(
            "file://bucket/input/b.xml",
            b"<records><item><id>2</id><label>two</label></item></records>",
        )
        .unwrap();
    stores
        .write(
            "file://bucket/input/a.xml",
            b"<records><item><id>1</id><label>one</label></item></records>",
        )
        .unwrap();

    let direct = Pipeline::default();
    assert_eq!(
        ironbeam_prism_proof::portable_object_xml_collection(&direct, &object_root)
            .unwrap()
            .collect_seq()
            .unwrap(),
        ["1:one", "2:two"]
    );
    let direct_path = directory.path().join("direct-xml-rows");
    let direct = Pipeline::default();
    let direct_sink = ironbeam_prism_proof::portable_object_xml_replay_fixture(
        &direct,
        &object_root,
        direct_path.clone(),
    )
    .unwrap();
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
    assert_eq!(read_rows(&direct_path), ["1:one", "2:two"]);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_path = directory.path().join("portable-xml-rows");
    let trigger = directory.path().join("xml-replay-trigger");
    let killed = directory.path().join("killed-xml-child");
    let portable = Pipeline::default();
    ironbeam_prism_proof::portable_object_xml_replay_fixture(
        &portable,
        &object_root,
        portable_path.clone(),
    )
    .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "replayed-provider-xml",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_OBJECT_XML_REPLAY_ROOT".into(),
                object_root.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_OBJECT_XML_REPLAY_RESULT".into(),
                portable_path.to_string_lossy().into_owned(),
            ),
            ("IRONBEAM_V5_PRISM_KILL_OBJECT_XML".into(), "1".into()),
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
    assert_eq!(terminal.0, 3, "Prism XML replay failed: {terminal:?}");
    assert_eq!(
        std::fs::read_to_string(&killed).unwrap(),
        std::fs::read_to_string(&trigger).unwrap()
    );
    assert_eq!(read_rows(&portable_path), ["1:one", "2:two"]);
}

#[tokio::test]
async fn prism_reopens_streaming_jsonl_glob_after_execution_child_loss() {
    let directory = tempfile::tempdir().unwrap();
    let object_root = directory.path().join("objects");
    let mut stores = ObjectStores::new();
    stores
        .register("file", LocalObjectIO::new(&object_root).unwrap())
        .unwrap();
    stores.write("file://bucket/input/b.jsonl", b"2\n").unwrap();
    stores.write("file://bucket/input/a.jsonl", b"1\n").unwrap();

    let direct = Pipeline::default();
    assert_eq!(
        portable_object_jsonl_collection(&direct, &object_root)
            .unwrap()
            .collect_seq()
            .unwrap(),
        ["row-1", "row-2"]
    );
    let direct_path = directory.path().join("direct-jsonl-rows");
    let direct = Pipeline::default();
    let direct_sink =
        portable_object_jsonl_replay_fixture(&direct, &object_root, direct_path.clone()).unwrap();
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
    assert_eq!(read_rows(&direct_path), ["row-1", "row-2"]);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_path = directory.path().join("portable-jsonl-rows");
    let trigger = directory.path().join("jsonl-replay-trigger");
    let killed = directory.path().join("killed-jsonl-child");
    let portable = Pipeline::default();
    portable_object_jsonl_replay_fixture(&portable, &object_root, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "replayed-streaming-provider-jsonl",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_OBJECT_JSONL_REPLAY_ROOT".into(),
                object_root.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_OBJECT_JSONL_REPLAY_RESULT".into(),
                portable_path.to_string_lossy().into_owned(),
            ),
            ("IRONBEAM_V5_PRISM_KILL_OBJECT_JSONL".into(), "1".into()),
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
        "Prism streaming JSONL replay failed: {terminal:?}"
    );
    assert_eq!(
        std::fs::read_to_string(&killed).unwrap(),
        std::fs::read_to_string(&trigger).unwrap()
    );
    assert_eq!(read_rows(&portable_path), ["row-1", "row-2"]);
}

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
