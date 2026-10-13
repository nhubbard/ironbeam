// Unit tests for src/portable_metrics.rs.
use super::*;
use beam_proto::pipeline_v1::MonitoringInfo;

fn metric(pc: &str, payload: Vec<u8>) -> MonitoringInfo {
    MonitoringInfo {
        urn: "beam:metric:element_count:v1".into(),
        r#type: "beam:metrics:sum_int64:v1".into(),
        labels: [("PCOLLECTION".into(), pc.into())].into(),
        payload,
        ..Default::default()
    }
}

#[test]
fn requires_one_well_formed_committed_count() {
    let mut metrics = MetricResults {
        attempted: vec![metric("target", vec![99])],
        committed: vec![metric("target", vec![3])],
    };
    assert_eq!(committed_element_count(&metrics, "target").unwrap(), 3);
    assert!(committed_element_count(&metrics, "missing").is_err());
    metrics.committed.push(metric("target", vec![3]));
    assert!(committed_element_count(&metrics, "target").is_err());
    metrics.committed.pop();
    metrics.committed[0].payload = vec![0x80];
    assert!(committed_element_count(&metrics, "target").is_err());
    metrics.committed[0].payload = vec![3, 0];
    assert!(committed_element_count(&metrics, "target").is_err());
}

#[test]
fn sink_count_is_scoped_to_transform() {
    let mut sink = metric("unused", vec![3]);
    sink.urn = "beam:metric:user:sum_int64:v1".into();
    sink.labels = [
        ("PTRANSFORM".into(), "sink-one".into()),
        ("NAMESPACE".into(), "ironbeam.sink".into()),
        ("NAME".into(), "input_elements".into()),
    ]
    .into();
    let metrics = MetricResults {
        attempted: Vec::new(),
        committed: vec![sink],
    };
    assert_eq!(committed_sink_input_count(&metrics, "sink-one").unwrap(), 3);
    assert!(committed_sink_input_count(&metrics, "sink-two").is_err());
}
