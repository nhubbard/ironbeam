// Unit tests for src/data.rs.
use super::*;
use std::sync::Barrier;

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
