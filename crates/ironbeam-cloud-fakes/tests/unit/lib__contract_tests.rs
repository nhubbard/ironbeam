// Unit tests for src/lib.rs.
use super::{FakeCredentials, FakeObjectIO, FakeWarehouseIO};
use ironbeam_cloud_service_api::{CloudCredentials, ErrorKind, ObjectIO, WarehouseIO};

#[test]
fn credentials_and_warehouse_report_expected_results() {
    let credentials = FakeCredentials {
        identifier: String::new(),
        credential_type: "test".into(),
    };
    assert_eq!(
        credentials.validate().unwrap_err().kind,
        ErrorKind::Authentication
    );

    let warehouse = FakeWarehouseIO::new();
    warehouse.add_table(
        "events",
        vec![("id".into(), "TEXT".into())],
        vec![vec!["a".into()]],
    );
    let result = warehouse.query("SELECT * FROM events").unwrap();
    assert_eq!(result.columns, ["id"]);
    assert_eq!(result.rows, [vec!["a"]]);
    assert_eq!(result.row_count, 1);
    assert_eq!(
        warehouse.query("SELECT * FROM missing").unwrap_err().kind,
        ErrorKind::NotFound
    );
}

#[test]
fn object_store_clones_share_data_and_preserve_bucket_isolation() {
    let first = FakeObjectIO::new();
    let second = first.clone();
    first.put_object("one", "key", b"value").unwrap();
    assert_eq!(second.get_object("one", "key").unwrap(), b"value");
    assert_eq!(
        second.get_object("two", "key").unwrap_err().kind,
        ErrorKind::NotFound
    );
}
