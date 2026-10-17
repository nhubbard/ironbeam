#![allow(dead_code)] // Different integration test binaries use different parts of these shared fixtures.

use ironbeam_core::{
    Error, ProgressSignal, Timestamped, TypedSink, UnboundedPoll, UnboundedSource,
};
use std::collections::{HashSet, VecDeque};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

pub type Value = (i32, i32);

pub struct TimedScript {
    pub polls: VecDeque<(UnboundedPoll<Timestamped<Value>>, i64)>,
    pub clock_ns: i64,
}

impl UnboundedSource<Timestamped<Value>> for TimedScript {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<Value>>, Error> {
        let Some((poll, clock_ns)) = self.polls.pop_front() else {
            return Ok(UnboundedPoll::Pending);
        };
        self.clock_ns = clock_ns;
        Ok(poll)
    }

    fn processing_time_ns(&self) -> Option<i64> {
        Some(self.clock_ns)
    }
}

pub struct WallPending {
    pub first: Option<UnboundedPoll<Timestamped<Value>>>,
}

impl UnboundedSource<Timestamped<Value>> for WallPending {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<Value>>, Error> {
        Ok(self.first.take().unwrap_or(UnboundedPoll::Pending))
    }
}

pub struct Capture {
    pub committed: Arc<Mutex<Vec<String>>>,
    pub accepted: Arc<Mutex<HashSet<String>>>,
    pub fail_once: Option<Arc<AtomicBool>>,
    pub panic_once: Option<Arc<AtomicBool>>,
    pub fail_write_once: Option<Arc<AtomicBool>>,
    pub bundle_ids: Option<Arc<Mutex<Vec<String>>>>,
    pub staged: Vec<String>,
    pub bundle: Option<String>,
}

impl TypedSink<String> for Capture {
    fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
        if let Some(bundle_ids) = &self.bundle_ids {
            bundle_ids.lock().unwrap().push(id.to_owned());
        }
        self.bundle = Some(id.to_owned());
        Ok(())
    }
    fn write(&mut self, value: String) -> Result<(), Error> {
        if self
            .fail_write_once
            .as_ref()
            .is_some_and(|flag| flag.swap(false, Ordering::SeqCst))
        {
            return Err(Error::execution(
                "sink",
                "test",
                "injected timer write failure",
            ));
        }
        self.staged.push(value);
        Ok(())
    }
    fn commit(&mut self) -> Result<(), Error> {
        if self
            .panic_once
            .as_ref()
            .is_some_and(|flag| flag.swap(false, Ordering::SeqCst))
        {
            panic!("injected timer commit panic");
        }
        if self
            .fail_once
            .as_ref()
            .is_some_and(|flag| flag.swap(false, Ordering::SeqCst))
        {
            return Err(Error::execution(
                "sink",
                "test",
                "injected timer commit failure",
            ));
        }
        if self
            .accepted
            .lock()
            .unwrap()
            .insert(self.bundle.take().unwrap())
        {
            self.committed.lock().unwrap().append(&mut self.staged);
        }
        Ok(())
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.staged.clear();
        Ok(())
    }
}

pub struct CancelCapture {
    pub cancellation: Arc<AtomicBool>,
}

impl TypedSink<String> for CancelCapture {
    fn write(&mut self, _: String) -> Result<(), Error> {
        self.cancellation.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        Err(Error::execution(
            "sink",
            "test",
            "cancelled timer was committed",
        ))
    }

    fn abort(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

pub fn progress(cursor: u8, watermark_ns: i64, completed: bool) -> ProgressSignal {
    ProgressSignal {
        source_key: "source".into(),
        split_id: "split-0".into(),
        checkpoint_position: Some(vec![cursor]),
        event_time_ns: Some(watermark_ns),
        completed,
    }
}
