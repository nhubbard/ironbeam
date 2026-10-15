use ironbeam_cloud_fakes::FakePubSubIO;
use ironbeam_cloud_service_api::{ErrorKind, PubSubIO};
use std::collections::HashMap;

#[test]
fn subscriptions_receive_independent_deliveries_and_acknowledge_by_attempt() {
    let service = FakePubSubIO::new();
    service.subscribe("events", "first").unwrap();
    service.subscribe("events", "second").unwrap();
    let id = service
        .publish(
            "events",
            b"row",
            HashMap::from([("kind".into(), "new".into())]),
        )
        .unwrap();
    let first = service.pull_with_ack("first", 1).unwrap().remove(0);
    let second = service.pull_with_ack("second", 1).unwrap().remove(0);
    assert_eq!(first.message.id, id);
    assert_eq!(second.message.id, id);
    assert_eq!(first.message.data, b"row");
    assert_eq!(first.message.attributes["kind"], "new");
    assert_ne!(first.ack_id, second.ack_id);
    assert!(service.pull_with_ack("first", 1).unwrap().is_empty());

    service.acknowledge("first", vec![first.ack_id]).unwrap();
    service.redeliver_unacked("first").unwrap();
    assert!(service.pull_with_ack("first", 1).unwrap().is_empty());

    service.redeliver_unacked("second").unwrap();
    let retried = service.pull_with_ack("second", 1).unwrap().remove(0);
    assert_eq!(retried.message.id, id);
    assert_ne!(retried.ack_id, second.ack_id);
    assert_eq!(
        service
            .acknowledge("second", vec![second.ack_id])
            .unwrap_err()
            .kind,
        ErrorKind::InvalidInput
    );
    service.acknowledge("second", vec![retried.ack_id]).unwrap();
    service.redeliver_unacked("second").unwrap();
    assert!(service.pull_with_ack("second", 1).unwrap().is_empty());
}

#[test]
fn invalid_acknowledgment_is_atomic_and_legacy_pull_consumes() {
    let service = FakePubSubIO::new();
    service.subscribe("events", "sub").unwrap();
    service.publish("events", b"one", HashMap::new()).unwrap();
    service.publish("events", b"two", HashMap::new()).unwrap();
    let first = service.pull_with_ack("sub", 1).unwrap().remove(0);
    assert_eq!(
        service
            .acknowledge("sub", vec![first.ack_id, "unknown".into()])
            .unwrap_err()
            .kind,
        ErrorKind::InvalidInput
    );
    service.redeliver_unacked("sub").unwrap();
    let rows = service.pull("sub", 3).unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.data.as_slice())
            .collect::<Vec<_>>(),
        [b"one", b"two"]
    );
    service.redeliver_unacked("sub").unwrap();
    assert!(service.pull_with_ack("sub", 1).unwrap().is_empty());
}

#[test]
fn subscription_scope_and_unknown_handles_fail_closed() {
    let service = FakePubSubIO::new();
    assert_eq!(
        service.pull_with_ack("missing", 1).unwrap_err().kind,
        ErrorKind::NotFound
    );
    assert_eq!(
        service.subscribe("", "sub").unwrap_err().kind,
        ErrorKind::InvalidInput
    );
    service.subscribe("events", "sub").unwrap();
    service.subscribe("events", "sub").unwrap();
    assert_eq!(
        service.subscribe("other", "sub").unwrap_err().kind,
        ErrorKind::AlreadyExists
    );
    assert!(service.topic_exists("events").unwrap());
    assert!(!service.topic_exists("other").unwrap());
    service.publish("events", b"one", HashMap::new()).unwrap();
    assert!(service.pull_with_ack("sub", 0).unwrap().is_empty());
    let delivery = service.pull_with_ack("sub", 1).unwrap().remove(0);
    assert_eq!(
        service
            .acknowledge("other", vec![delivery.ack_id.clone()])
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
    service.acknowledge("sub", vec![delivery.ack_id]).unwrap();
}
