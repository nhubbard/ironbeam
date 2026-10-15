// Unit tests for src/data.rs.
use super::*;
use ironbeam_core::{StatefulParDoContract, UserTimerDomain, UserTimerFamily, Version};
use std::collections::BTreeMap;
use std::sync::Barrier;

fn timer_contract() -> StatefulParDoContract {
    StatefulParDoContract {
        semantic_id: "tests/timer-callback".into(),
        version: Version { major: 1, minor: 0 },
        key_coder_id: "beam:coder:bytes:v1".into(),
        window_coder_id: "beam:coder:global_window:v1".into(),
        states: BTreeMap::new(),
        timers: BTreeMap::from([(
            "expiry".into(),
            UserTimerFamily {
                domain: UserTimerDomain::EventTime,
                coder_id: "timer-coder".into(),
            },
        )]),
        on_window_expiration_timer: None,
    }
}

#[test]
fn concurrent_reader_registration_keeps_the_complete_runner_chunk() {
    for iteration in 0..1_000_u32 {
        let (outbound_tx, _outbound_rx) = mpsc::unbounded_channel();
        let inbound = Arc::new(Mutex::new(InboundState::default()));
        let client = DataClient {
            outbound_tx,
            inbound: Arc::clone(&inbound),
            replay: false,
            replay_output_budget: None,
            outbound_progress: None,
        };
        let barrier = Arc::new(Barrier::new(2));
        let producer_barrier = Arc::clone(&barrier);
        let producer = std::thread::spawn(move || {
            producer_barrier.wait();
            let mut state = inbound.lock().unwrap();
            state.route(ElemData {
                instruction_id: "bundle".into(),
                transform_id: "source".into(),
                data: iteration.to_le_bytes().to_vec(),
                is_last: true,
            });
        });
        barrier.wait();
        let mut reader = client.open_read("bundle", "source");
        producer.join().unwrap();
        assert_eq!(reader.try_recv().unwrap(), iteration.to_le_bytes());
        assert!(matches!(
            reader.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }
}

#[test]
fn timer_registration_keeps_early_chunks_and_isolates_families() {
    for iteration in 0..1_000_u32 {
        let (outbound_tx, _outbound_rx) = mpsc::unbounded_channel();
        let inbound = Arc::new(Mutex::new(InboundState::default()));
        let client = DataClient {
            outbound_tx,
            inbound: Arc::clone(&inbound),
            replay: false,
            replay_output_budget: None,
            outbound_progress: None,
        };
        let barrier = Arc::new(Barrier::new(2));
        let producer_barrier = Arc::clone(&barrier);
        let producer = std::thread::spawn(move || {
            producer_barrier.wait();
            let mut state = inbound.lock().unwrap();
            state.route_timer(ElemTimers {
                instruction_id: "bundle".into(),
                transform_id: "pardo".into(),
                timer_family_id: "first".into(),
                timers: iteration.to_le_bytes().to_vec(),
                is_last: true,
            });
            state.route_timer(ElemTimers {
                instruction_id: "bundle".into(),
                transform_id: "pardo".into(),
                timer_family_id: "second".into(),
                timers: vec![9],
                is_last: true,
            });
        });
        barrier.wait();
        let mut first = client.open_timer_read("bundle", "pardo", "first");
        producer.join().unwrap();
        assert_eq!(first.try_recv().unwrap(), iteration.to_le_bytes());
        assert!(matches!(
            first.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
        let mut second = client.open_timer_read("bundle", "pardo", "second");
        assert_eq!(second.try_recv().unwrap(), vec![9]);
        assert!(matches!(
            second.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }
}

#[tokio::test]
async fn outbound_flush_waits_for_queued_data_and_terminal_marker() {
    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel();
    let progress = Arc::new(OutboundProgress::default());
    let client = DataClient {
        outbound_tx,
        inbound: Arc::new(Mutex::new(InboundState::default())),
        replay: false,
        replay_output_budget: None,
        outbound_progress: Some(Arc::clone(&progress)),
    };
    client.write_chunk("bundle", "sink", vec![7]).unwrap();
    client.close_write("bundle", "sink").unwrap();
    let flush = tokio::spawn(async move { client.flush_outbound().await });
    tokio::task::yield_now().await;
    assert!(!flush.is_finished());
    assert_eq!(outbound_rx.recv().await.unwrap().data[0].data, vec![7]);
    progress.consumed.fetch_add(1, Ordering::Release);
    progress.changed.notify_waiters();
    tokio::task::yield_now().await;
    assert!(!flush.is_finished());
    assert!(outbound_rx.recv().await.unwrap().data[0].is_last);
    progress.consumed.fetch_add(1, Ordering::Release);
    progress.changed.notify_waiters();
    flush.await.unwrap().unwrap();
}

#[test]
fn replay_client_delivers_input_once_and_keeps_output_local() {
    let (client, mut outbound) =
        DataClient::for_replay("bundle", "source", vec![vec![1, 2], vec![3]]);
    let mut reader = client.open_read("bundle", "source");
    assert_eq!(reader.try_recv().unwrap(), vec![1, 2]);
    assert_eq!(reader.try_recv().unwrap(), vec![3]);
    assert!(matches!(
        reader.try_recv(),
        Err(mpsc::error::TryRecvError::Disconnected)
    ));
    client.write_chunk("bundle", "sink", vec![4]).unwrap();
    client.close_write("bundle", "sink").unwrap();
    let first = outbound.try_recv().unwrap();
    assert_eq!(first.data[0].data, vec![4]);
    assert!(!first.data[0].is_last);
    let second = outbound.try_recv().unwrap();
    assert!(second.data[0].is_last);
}

#[test]
fn replay_output_budget_is_shared_by_cloned_clients() {
    let (client, mut outbound) = DataClient::for_replay_with_limit("bundle", "source", vec![], 3);
    client.write_chunk("bundle", "sink", vec![1, 2]).unwrap();
    let clone = client.clone();
    assert!(clone.write_chunk("bundle", "sink", vec![3, 4]).is_err());
    clone.write_chunk("bundle", "sink", vec![3]).unwrap();
    client.close_write("bundle", "sink").unwrap();
    assert_eq!(outbound.try_recv().unwrap().data[0].data, vec![1, 2]);
    assert_eq!(outbound.try_recv().unwrap().data[0].data, vec![3]);
    assert!(outbound.try_recv().unwrap().data[0].is_last);
}

#[test]
fn outbound_timer_stream_keeps_family_identity_and_shares_replay_budget() {
    let (client, mut outbound) = DataClient::for_replay_with_limit("bundle", "source", vec![], 3);
    client
        .write_timer_chunk("bundle", "pardo", "expiry", vec![1, 2])
        .unwrap();
    assert!(client
        .write_timer_chunk("bundle", "pardo", "expiry", vec![3, 4])
        .is_err());
    client.write_chunk("bundle", "sink", vec![3]).unwrap();
    client
        .close_timer_write("bundle", "pardo", "expiry")
        .unwrap();
    let first = outbound.try_recv().unwrap();
    assert!(first.data.is_empty());
    assert_eq!(first.timers[0].instruction_id, "bundle");
    assert_eq!(first.timers[0].transform_id, "pardo");
    assert_eq!(first.timers[0].timer_family_id, "expiry");
    assert_eq!(first.timers[0].timers, vec![1, 2]);
    assert!(!first.timers[0].is_last);
    assert_eq!(outbound.try_recv().unwrap().data[0].data, vec![3]);
    let last = outbound.try_recv().unwrap();
    assert!(last.timers[0].is_last);
    assert!(last.timers[0].timers.is_empty());
}

#[test]
fn outbound_timer_record_uses_beam_timer_codec_and_family_route() {
    let record = TimerRecord {
        encoded_key: vec![1, b'k'],
        dynamic_tag: "tag".into(),
        encoded_windows: vec![Vec::new()],
        clear: false,
        fire_timestamp_millis: Some(5),
        hold_timestamp_millis: Some(4),
        pane: Some(ironbeam_core::PaneInfo::NO_FIRING),
    };
    let (client, mut outbound) = DataClient::for_replay_with_limit("bundle", "source", vec![], 128);
    client
        .write_timer_record("bundle", "pardo", "expiry", &record)
        .unwrap();
    client
        .close_timer_write("bundle", "pardo", "expiry")
        .unwrap();
    let sent = outbound.try_recv().unwrap();
    assert_eq!(sent.timers[0].timer_family_id, "expiry");
    let (decoded, used) = crate::timer_coder::decode_timer_record(
        &sent.timers[0].timers,
        |input| Ok(crate::coders::decode_length_prefix(input)?.1),
        |_| Ok(0),
    )
    .unwrap();
    assert_eq!(used, sent.timers[0].timers.len());
    assert_eq!(decoded, record);
    assert!(outbound.try_recv().unwrap().timers[0].is_last);
}

#[tokio::test]
async fn declared_timer_family_dispatches_fragmented_records_to_callbacks() {
    let (outbound_tx, _outbound_rx) = mpsc::unbounded_channel();
    let inbound = Arc::new(Mutex::new(InboundState::default()));
    let client = DataClient {
        outbound_tx,
        inbound: Arc::clone(&inbound),
        replay: false,
        replay_output_budget: None,
        outbound_progress: None,
    };
    let records = [5_i64, 8].map(|fire| TimerRecord {
        encoded_key: vec![1, b'k'],
        dynamic_tag: "tag".into(),
        encoded_windows: vec![Vec::new()],
        clear: false,
        fire_timestamp_millis: Some(fire),
        hold_timestamp_millis: Some(fire),
        pane: Some(ironbeam_core::PaneInfo::NO_FIRING),
    });
    let mut encoded = Vec::new();
    for record in &records {
        record.encode(&mut encoded).unwrap();
    }
    for (index, part) in encoded.chunks(3).enumerate() {
        inbound.lock().unwrap().route_timer(ElemTimers {
            instruction_id: "bundle".into(),
            transform_id: "pardo".into(),
            timer_family_id: "expiry".into(),
            timers: part.to_vec(),
            is_last: index == (encoded.len() - 1) / 3,
        });
    }
    let received = Arc::new(Mutex::new(Vec::new()));
    let callback_received = Arc::clone(&received);
    let contract = timer_contract();
    let family = TimerFamilyScope::new(&client, &contract, "bundle", "pardo", "expiry").unwrap();
    let count = family
        .dispatch(
            crate::timer_coder::try_nested_bytes_len,
            |_| Ok(Some(0)),
            move |record| {
                let received = Arc::clone(&callback_received);
                async move {
                    received.lock().unwrap().push(record);
                    Ok(())
                }
            },
        )
        .await
        .unwrap();
    assert_eq!(count, 2);
    assert_eq!(*received.lock().unwrap(), records);
}

#[tokio::test]
async fn timer_callback_runs_before_runner_closes_the_family_stream() {
    let (outbound_tx, _outbound_rx) = mpsc::unbounded_channel();
    let inbound = Arc::new(Mutex::new(InboundState::default()));
    let client = DataClient {
        outbound_tx,
        inbound: Arc::clone(&inbound),
        replay: false,
        replay_output_budget: None,
        outbound_progress: None,
    };
    let (called_tx, mut called_rx) = mpsc::unbounded_channel();
    let dispatch = tokio::spawn(async move {
        let contract = timer_contract();
        let family =
            TimerFamilyScope::new(&client, &contract, "bundle", "pardo", "expiry").unwrap();
        family
            .dispatch(
                crate::timer_coder::try_nested_bytes_len,
                |_| Ok(Some(0)),
                move |record| {
                    let called_tx = called_tx.clone();
                    async move {
                        called_tx.send(record.fire_timestamp_millis).unwrap();
                        Ok(())
                    }
                },
            )
            .await
            .unwrap()
    });
    let mut encoded = Vec::new();
    TimerRecord {
        encoded_key: vec![1, b'k'],
        dynamic_tag: String::new(),
        encoded_windows: vec![Vec::new()],
        clear: false,
        fire_timestamp_millis: Some(5),
        hold_timestamp_millis: Some(5),
        pane: Some(ironbeam_core::PaneInfo::NO_FIRING),
    }
    .encode(&mut encoded)
    .unwrap();
    for part in encoded.chunks(2) {
        inbound.lock().unwrap().route_timer(ElemTimers {
            instruction_id: "bundle".into(),
            transform_id: "pardo".into(),
            timer_family_id: "expiry".into(),
            timers: part.to_vec(),
            is_last: false,
        });
    }
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), called_rx.recv())
            .await
            .unwrap(),
        Some(Some(5))
    );
    assert!(!dispatch.is_finished());
    inbound.lock().unwrap().route_timer(ElemTimers {
        instruction_id: "bundle".into(),
        transform_id: "pardo".into(),
        timer_family_id: "expiry".into(),
        timers: Vec::new(),
        is_last: true,
    });
    assert_eq!(dispatch.await.unwrap(), 1);
}

#[tokio::test]
async fn timer_dispatch_rejects_undeclared_family_and_callback_failure() {
    let (outbound_tx, _outbound_rx) = mpsc::unbounded_channel();
    let inbound = Arc::new(Mutex::new(InboundState::default()));
    let client = DataClient {
        outbound_tx,
        inbound: Arc::clone(&inbound),
        replay: false,
        replay_output_budget: None,
        outbound_progress: None,
    };
    let contract = timer_contract();
    assert!(TimerFamilyScope::new(&client, &contract, "bundle", "pardo", "missing").is_err());
    let mut encoded = Vec::new();
    TimerRecord {
        encoded_key: vec![1, b'k'],
        dynamic_tag: String::new(),
        encoded_windows: vec![Vec::new()],
        clear: false,
        fire_timestamp_millis: Some(5),
        hold_timestamp_millis: Some(5),
        pane: Some(ironbeam_core::PaneInfo::NO_FIRING),
    }
    .encode(&mut encoded)
    .unwrap();
    inbound.lock().unwrap().route_timer(ElemTimers {
        instruction_id: "bundle".into(),
        transform_id: "pardo".into(),
        timer_family_id: "expiry".into(),
        timers: encoded,
        is_last: true,
    });
    let family = TimerFamilyScope::new(&client, &contract, "bundle", "pardo", "expiry").unwrap();
    let error = family
        .dispatch(
            crate::timer_coder::try_nested_bytes_len,
            |_| Ok(Some(0)),
            |_| async { anyhow::bail!("callback failed") },
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("callback failed"));
}

#[test]
fn declared_timer_scope_emits_and_closes_its_family() {
    let (client, mut outbound) = DataClient::for_replay("bundle", "source", vec![]);
    let contract = timer_contract();
    let family = TimerFamilyScope::new(&client, &contract, "bundle", "pardo", "expiry").unwrap();
    let record = TimerRecord {
        encoded_key: vec![1, b'k'],
        dynamic_tag: String::new(),
        encoded_windows: vec![Vec::new()],
        clear: false,
        fire_timestamp_millis: Some(5),
        hold_timestamp_millis: Some(5),
        pane: Some(ironbeam_core::PaneInfo::NO_FIRING),
    };
    family.emit(&record).unwrap();
    family.finish_output().unwrap();
    let first = outbound.try_recv().unwrap();
    assert_eq!(first.timers[0].instruction_id, "bundle");
    assert_eq!(first.timers[0].transform_id, "pardo");
    assert_eq!(first.timers[0].timer_family_id, "expiry");
    let (decoded, used) = crate::timer_coder::decode_timer_record(
        &first.timers[0].timers,
        |bytes| Ok(crate::coders::decode_length_prefix(bytes)?.1),
        |_| Ok(0),
    )
    .unwrap();
    assert_eq!(used, first.timers[0].timers.len());
    assert_eq!(decoded, record);
    assert!(outbound.try_recv().unwrap().timers[0].is_last);
}
