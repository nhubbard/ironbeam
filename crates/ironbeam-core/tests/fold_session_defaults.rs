use std::any::TypeId;

use ironbeam_core::{Envelope, Error, ErrorKind, FoldSession, PaneInfo, Payload, Window};

struct MinimalFold;

impl FoldSession for MinimalFold {
    fn add(&mut self, _: Envelope) -> Result<(), Error> {
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<Vec<Envelope>, Error> {
        Ok(vec![Envelope {
            pane: PaneInfo::NO_FIRING,
            value: Box::new(7_i32),
            timestamp_ns: 0,
            window: Window::Global,
            assigned_window: None,
        }])
    }
}

#[test]
fn minimal_fold_advertises_only_its_implemented_capabilities() {
    let mut fold = MinimalFold;
    for error in [
        fold.drain_ready_count(1).err().unwrap(),
        fold.arm_processing_time(0, 1).err().unwrap(),
        fold.drain_ready_processing_time(1).err().unwrap(),
        fold.drain_closed(1).err().unwrap(),
        fold.drain_late(false).err().unwrap(),
        fold.restore_checkpoint_state(&[]).err().unwrap(),
        fold.add_vec_batch(Box::new(vec![1_i32]) as Payload)
            .err()
            .unwrap(),
        fold.add_timed_vec_batch(Box::new(vec![1_i32]) as Payload, &0_i32, "timestamp")
            .err()
            .unwrap(),
    ] {
        assert!(matches!(
            error.kind,
            ErrorKind::UnsupportedCapability { .. }
        ));
    }
    assert_eq!(fold.next_processing_time_deadline(), None);
    assert_eq!(fold.checkpoint_state().unwrap(), None);
    assert!(!fold.accepts_vec_batch(TypeId::of::<Vec<i32>>()));
    assert!(!fold.accepts_timed_vec_batch(TypeId::of::<Vec<i32>>()));

    let mut values = Vec::new();
    Box::new(fold)
        .finish_into(&mut |event| {
            values.push(*event.value.downcast::<i32>().unwrap());
            Ok(())
        })
        .unwrap();
    assert_eq!(values, [7]);
}
