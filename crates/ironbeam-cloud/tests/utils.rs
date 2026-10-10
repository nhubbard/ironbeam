use ironbeam_cloud::{CloudIOError, ErrorKind, utils::block_on_async};

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
