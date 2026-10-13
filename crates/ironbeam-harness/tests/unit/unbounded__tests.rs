// Unit tests for src/unbounded.rs.
use super::*;
use ironbeam_core::{
    Pipeline, ProgressSignal, SourceProgress, Timestamped, TypedUnboundedSourceReader,
    UnboundedBatch, UnboundedSource,
};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

struct Script(VecDeque<UnboundedPoll<Timestamped<i32>>>);
impl UnboundedSource<Timestamped<i32>> for Script {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<i32>>, Error> {
        Ok(self.0.pop_front().unwrap_or(UnboundedPoll::Pending))
    }
}

fn signal(cursor: Option<u8>, completed: bool) -> ProgressSignal {
    ProgressSignal {
        source_key: "events".into(),
        split_id: "split-a".into(),
        checkpoint_position: cursor.map(|cursor| vec![cursor]),
        event_time_ns: Some(10),
        completed,
    }
}

fn source(opened: Arc<Mutex<Vec<Option<Vec<u8>>>>>) -> Pipeline {
    let pipeline = Pipeline::default();
    pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "source-v1".into(),
            0,
            move |position| {
                opened.lock().unwrap().push(position.clone());
                let script = match position.as_deref() {
                    None => vec![
                        UnboundedPoll::Pending,
                        UnboundedPoll::Ready(UnboundedBatch {
                            elements: Vec::new(),
                            progress: signal(None, false),
                        }),
                        UnboundedPoll::Ready(UnboundedBatch {
                            elements: vec![Timestamped {
                                value: 7_i32,
                                timestamp_ns: 42,
                            }],
                            progress: signal(Some(1), false),
                        }),
                    ],
                    Some([1]) => vec![UnboundedPoll::Ready(UnboundedBatch {
                        elements: vec![Timestamped {
                            value: 8_i32,
                            timestamp_ns: 43,
                        }],
                        progress: signal(Some(2), true),
                    })],
                    _ => panic!("unexpected position"),
                };
                Ok(Box::new(Script(script.into())) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap();
    pipeline
}

#[test]
fn polls_encoded_values_and_resumes_from_restriction() {
    let opened = Arc::new(Mutex::new(Vec::new()));
    let pipeline = source(Arc::clone(&opened));
    let (graph, registry) = pipeline.snapshot().unwrap();
    let mut poller = SourcePoller::open(
        &graph,
        &registry,
        "events",
        SourceRestriction::initial("events"),
    )
    .unwrap();
    assert_eq!(poller.poll(1).unwrap(), SourcePoll::Pending);
    assert_eq!(poller.poll(1).unwrap(), SourcePoll::Pending);
    let SourcePoll::Ready {
        elements,
        residual: Some(residual),
        watermark_ns,
    } = poller.poll(1).unwrap()
    else {
        panic!("expected a resumable data batch");
    };
    assert_eq!(watermark_ns, Some(10));
    assert_eq!(elements[0].timestamp_ns, 42);
    assert_eq!(
        *registry
            .decode_element("events", &elements[0].bytes)
            .unwrap()
            .downcast::<i32>()
            .unwrap(),
        7
    );
    let mut resumed = SourcePoller::open(&graph, &registry, "events", residual).unwrap();
    let SourcePoll::Ready {
        elements,
        residual: None,
        ..
    } = resumed.poll(1).unwrap()
    else {
        panic!("expected final batch");
    };
    assert_eq!(
        *registry
            .decode_element("events", &elements[0].bytes)
            .unwrap()
            .downcast::<i32>()
            .unwrap(),
        8
    );
    assert_eq!(opened.lock().unwrap().as_slice(), &[None, Some(vec![1])]);
}

#[test]
fn validates_restriction_before_source_open() {
    let opened = Arc::new(Mutex::new(Vec::new()));
    let pipeline = source(Arc::clone(&opened));
    let (graph, registry) = pipeline.snapshot().unwrap();
    assert!(SourcePoller::open(
        &graph,
        &registry,
        "events",
        SourceRestriction::initial("other")
    )
    .is_err());
    assert!(opened.lock().unwrap().is_empty());
}

#[test]
fn rejects_unregistered_reader_and_repeated_data_cursor() {
    let pipeline = Pipeline::default();
    pipeline
        .unbounded_source::<i32>(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            0,
        )
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    assert!(SourcePoller::open(
        &graph,
        &registry,
        "events",
        SourceRestriction::initial("events")
    )
    .is_err());

    let pipeline = Pipeline::default();
    pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "source-v1".into(),
            0,
            |_| {
                Ok(Box::new(Script(
                    vec![
                        UnboundedPoll::Ready(UnboundedBatch {
                            elements: vec![Timestamped {
                                value: 1_i32,
                                timestamp_ns: 1,
                            }],
                            progress: signal(Some(1), false),
                        }),
                        UnboundedPoll::Ready(UnboundedBatch {
                            elements: vec![Timestamped {
                                value: 2_i32,
                                timestamp_ns: 2,
                            }],
                            progress: signal(Some(1), true),
                        }),
                    ]
                    .into(),
                )) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let mut poller = SourcePoller::open(
        &graph,
        &registry,
        "events",
        SourceRestriction::initial("events"),
    )
    .unwrap();
    assert!(matches!(poller.poll(1).unwrap(), SourcePoll::Ready { .. }));
    assert!(matches!(poller.poll(1), Err(SourcePollError::Core(_))));
}
