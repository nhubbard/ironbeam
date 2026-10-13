// Unit tests for src/stage.rs.
use super::*;

#[test]
fn parse_gs_url_bucket_only() {
    let (b, p) = parse_gs_url("gs://my-bucket").unwrap();
    assert_eq!(b, "my-bucket");
    assert_eq!(p, "");
}

#[test]
fn parse_gs_url_with_prefix() {
    let (b, p) = parse_gs_url("gs://my-bucket/a/b/").unwrap();
    assert_eq!(b, "my-bucket");
    assert_eq!(p, "a/b");
}

#[test]
fn parse_gs_url_rejects_non_gs() {
    assert!(parse_gs_url("s3://x").is_err());
    assert!(parse_gs_url("gs://").is_err());
}

#[tokio::test]
async fn memory_stager_records_uploads() {
    let m = MemoryStager::new("test-bkt");
    let out = m.put("job/pipeline.pb", b"abc".to_vec()).await.unwrap();
    assert_eq!(out.url, "gs://test-bkt/job/pipeline.pb");
    // sha256("abc") = ba7816bf...
    assert_eq!(
        out.sha256,
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert_eq!(m.stored().len(), 1);
}
