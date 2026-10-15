use std::sync::mpsc;
use std::thread;

use ironbeam_core::{OffsetRange, OffsetRangeTracker, RestrictionTracker};

fn range(start: i64, stop: i64) -> OffsetRange {
    OffsetRange::new(start, stop).unwrap()
}

#[test]
fn initial_chunks_cover_the_range_without_gaps_or_overlap() {
    let mut chunks = range(-3, 5).chunks(3).unwrap();
    assert_eq!(chunks.size_hint(), (3, Some(3)));
    assert_eq!(chunks.next(), Some(range(-3, 0)));
    assert_eq!(chunks.size_hint(), (2, Some(2)));
    assert_eq!(chunks.collect::<Vec<_>>(), vec![range(0, 3), range(3, 5)]);
    assert_eq!(range(4, 4).chunks(1).unwrap().next(), None);
    assert!(range(0, 1).chunks(0).is_err());
    assert!(OffsetRange { start: 2, stop: 1 }.chunks(1).is_err());
}

#[test]
fn initial_chunks_handle_the_full_signed_span_lazily() {
    let full = range(i64::MIN, i64::MAX);
    let mut chunks = full.chunks(u64::MAX).unwrap();
    assert_eq!(chunks.size_hint(), (1, Some(1)));
    assert_eq!(chunks.next(), Some(full));
    assert_eq!(chunks.next(), None);

    let mut chunks = full.chunks(u64::MAX / 2).unwrap();
    let first = chunks.next().unwrap();
    let second = chunks.next().unwrap();
    let third = chunks.next().unwrap();
    assert_eq!(first.start, i64::MIN);
    assert_eq!(first.stop, second.start);
    assert_eq!(second.stop, third.start);
    assert_eq!(third.stop, i64::MAX);
    assert_eq!(chunks.next(), None);
}

#[test]
fn beam_offset_range_coder_matches_two_signed_varints() {
    assert_eq!(range(0, 10).encode_beam().unwrap(), [0, 10]);
    assert_eq!(OffsetRange::decode_beam(&[0, 10]).unwrap(), range(0, 10));
    let negative_start = [
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01, 0x01,
    ];
    assert_eq!(range(-1, 1).encode_beam().unwrap(), negative_start);
    assert_eq!(
        OffsetRange::decode_beam(&negative_start).unwrap(),
        range(-1, 1)
    );
    let extreme = range(i64::MIN, i64::MAX);
    assert_eq!(
        OffsetRange::decode_beam(&extreme.encode_beam().unwrap()).unwrap(),
        extreme
    );
}

#[test]
fn beam_offset_range_coder_rejects_invalid_bodies() {
    for bytes in [
        vec![],
        vec![0],
        vec![0x80],
        vec![0xff; 11],
        vec![
            0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02, 0x00,
        ],
        vec![2, 1],
        vec![0, 1, 2],
    ] {
        assert!(
            OffsetRange::decode_beam(&bytes).is_err(),
            "accepted {bytes:?}"
        );
    }
    assert!(OffsetRange { start: 2, stop: 1 }.encode_beam().is_err());
    // Beam's Java VarInt reader accepts an overlong zero representation.
    assert_eq!(
        OffsetRange::decode_beam(&[0x80, 0, 1]).unwrap(),
        range(0, 1)
    );
}

fn claim_to_end<T: RestrictionTracker<Position = i64, Restriction = OffsetRange>>(
    tracker: &T,
    start: i64,
    stop: i64,
) {
    for position in start..stop {
        assert!(tracker.try_claim(position).unwrap());
    }
    assert!(tracker.is_done().unwrap());
    tracker.check_done().unwrap();
}

fn generic_split<T: RestrictionTracker<Position = i64, Restriction = OffsetRange>>(
    tracker: &T,
) -> (OffsetRange, OffsetRange) {
    assert_eq!(tracker.restriction().unwrap(), range(0, 10));
    assert_eq!(tracker.progress().unwrap(), (0.0, 10.0));
    tracker.try_split(0.5).unwrap().unwrap()
}

#[test]
fn claims_increase_and_completion_requires_a_final_position() {
    let tracker = OffsetRangeTracker::new(range(3, 6)).unwrap();
    assert_eq!(tracker.progress().unwrap(), (0.0, 3.0));
    assert!(tracker.check_done().is_err());
    assert!(tracker.try_claim(3).unwrap());
    assert_eq!(tracker.progress().unwrap(), (1.0, 2.0));
    assert!(tracker.try_claim(3).is_err());
    assert!(tracker.try_claim(2).is_err());
    assert!(tracker.try_claim(5).unwrap());
    tracker.check_done().unwrap();
    assert!(!tracker.try_claim(6).unwrap());
    assert!(tracker.try_claim(7).is_err());
}

#[test]
fn a_zero_fraction_split_checkpoints_without_losing_unclaimed_work() {
    let tracker = OffsetRangeTracker::new(range(0, 10)).unwrap();
    for position in 0..3 {
        assert!(tracker.try_claim(position).unwrap());
    }
    let (primary, residual) = tracker.try_split(0.0).unwrap().unwrap();
    assert_eq!(primary, range(0, 3));
    assert_eq!(residual, range(3, 10));
    tracker.check_done().unwrap();
    assert!(!tracker.try_claim(3).unwrap());

    let resumed = OffsetRangeTracker::new(residual).unwrap();
    claim_to_end(&resumed, 3, 10);
    assert_eq!(resumed.progress().unwrap(), (7.0, 0.0));
}

#[test]
fn a_fractional_split_keeps_primary_and_residual_disjoint() {
    let tracker = OffsetRangeTracker::new(range(0, 10)).unwrap();
    for position in 0..4 {
        assert!(tracker.try_claim(position).unwrap());
    }
    let (primary, residual) = tracker.try_split(0.5).unwrap().unwrap();
    assert_eq!(primary, range(0, 7));
    assert_eq!(residual, range(7, 10));
    assert_eq!(tracker.progress().unwrap(), (4.0, 3.0));
    for position in 4..7 {
        assert!(tracker.try_claim(position).unwrap());
    }
    tracker.check_done().unwrap();
    assert!(!tracker.try_claim(7).unwrap());
    assert!(tracker.try_split(0.0).unwrap().is_none());
}

#[test]
fn empty_and_extreme_ranges_do_not_overflow() {
    assert!(OffsetRange::new(2, 1).is_err());
    assert!(OffsetRangeTracker::new(OffsetRange { start: 2, stop: 1 }).is_err());
    let empty = OffsetRangeTracker::new(range(i64::MAX, i64::MAX)).unwrap();
    empty.check_done().unwrap();
    assert!(empty.try_split(0.0).unwrap().is_none());

    let full = OffsetRangeTracker::new(range(i64::MIN, i64::MAX)).unwrap();
    let (primary, residual) = full.try_split(0.5).unwrap().unwrap();
    assert_eq!(primary.stop, residual.start);
    assert_eq!(primary.start, i64::MIN);
    assert_eq!(residual.stop, i64::MAX);
    assert!(primary.stop > primary.start);
    assert!(residual.stop > residual.start);
}

#[test]
fn invalid_split_fractions_leave_the_restriction_unchanged() {
    let tracker = OffsetRangeTracker::new(range(0, 10)).unwrap();
    for fraction in [f64::NAN, f64::INFINITY, -0.1, 1.1] {
        assert!(tracker.try_split(fraction).is_err());
    }
    assert_eq!(tracker.restriction().unwrap(), range(0, 10));
    assert!(tracker.try_split(1.0).unwrap().is_none());
}

#[test]
fn trait_accessors_and_fractional_split_use_the_same_tracker_state() {
    let tracker = OffsetRangeTracker::new(range(0, 10)).unwrap();
    let (primary, residual) = generic_split(&tracker);
    assert_eq!(primary, range(0, 5));
    assert_eq!(residual, range(5, 10));
    claim_to_end(&tracker, 0, 5);
}

#[test]
fn concurrent_claim_and_split_share_one_primary_boundary() {
    let tracker = OffsetRangeTracker::new(range(0, 100)).unwrap();
    let claimant = tracker.clone();
    let (claimed_tx, claimed_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        for position in 0..20 {
            assert!(claimant.try_claim(position).unwrap());
        }
        claimed_tx.send(()).unwrap();
        resume_rx.recv().unwrap();
        let mut claimed = Vec::new();
        for position in 20..100 {
            if !claimant.try_claim(position).unwrap() {
                break;
            }
            claimed.push(position);
        }
        claimant.check_done().unwrap();
        claimed
    });
    claimed_rx.recv().unwrap();
    let (primary, residual) = tracker.try_split(0.5).unwrap().unwrap();
    resume_tx.send(()).unwrap();
    let claimed = worker.join().unwrap();
    assert_eq!(primary, range(0, 60));
    assert_eq!(residual, range(60, 100));
    assert_eq!(claimed, (20..60).collect::<Vec<_>>());
}
