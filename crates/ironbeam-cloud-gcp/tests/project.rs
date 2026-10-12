use ironbeam_cloud_gcp::{GcpError, GcpProject};
use ironbeam_cloud_service_api::CloudConfig;

#[test]
fn project_names_and_locations_are_checked() {
    assert!(GcpProject::new("").is_err());
    assert!(GcpProject::new("bad/project").is_err());
    let project = GcpProject::new("ironbeam-prod")
        .unwrap()
        .with_location("US")
        .unwrap();
    assert_eq!(project.id(), "ironbeam-prod");
    assert_eq!(project.region(), Some("US"));
    assert!(matches!(
        project.with_location("bad/location"),
        Err(GcpError::InvalidResource(_))
    ));
}

#[test]
fn names_accept_short_or_qualified_resources() {
    let project = GcpProject::new("ironbeam-prod").unwrap();
    assert_eq!(
        project.topic_name("events").unwrap(),
        "projects/ironbeam-prod/topics/events"
    );
    assert_eq!(
        project.topic_name("projects/other/topics/events").unwrap(),
        "projects/other/topics/events"
    );
    assert!(
        project
            .topic_name("projects/other/subscriptions/events")
            .is_err()
    );
    assert!(
        project
            .topic_name("projects/bad project/topics/events")
            .is_err()
    );
    assert_eq!(
        project.secret_name("projects/other/secrets/token").unwrap(),
        "projects/other/secrets/token"
    );
    assert_eq!(
        GcpProject::bucket("gs://batch-input").unwrap(),
        "projects/_/buckets/batch-input"
    );
    assert!(GcpProject::bucket("gs://bucket/path").is_err());
}
