use ironbeam_cloud::{
    CloudConfig, CloudCredentials, CloudIOError, ErrorKind,
    utils::{block_on_async, test_config, test_credentials},
};

#[test]
fn blocking_helper_resolves_result_and_error() {
    assert_eq!(
        block_on_async(async { Ok::<_, CloudIOError>(42) }).unwrap(),
        42
    );
    let error =
        block_on_async(async { Err::<(), _>(CloudIOError::new(ErrorKind::NotFound, "missing")) })
            .unwrap_err();
    assert_eq!(error.kind, ErrorKind::NotFound);
}

#[test]
fn compatibility_test_credentials_and_config_stay_available() {
    let credentials = test_credentials();
    assert_eq!(credentials.identifier(), "test-user");
    assert!(credentials.validate().is_ok());
    let config = test_config();
    assert_eq!(config.timeout_secs(), 30);
    assert_eq!(config.max_retries(), 3);
}
