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
