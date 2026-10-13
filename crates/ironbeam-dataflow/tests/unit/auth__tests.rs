// Unit tests for src/auth.rs.
use super::*;

#[tokio::test]
async fn static_token_source_returns_configured_token() {
    let s = StaticTokenSource::new("ya29.fake");
    assert_eq!(s.token().await.unwrap(), "ya29.fake");
}

#[tokio::test]
async fn arc_token_source_delegates() {
    let s: Arc<dyn TokenSource> = Arc::new(StaticTokenSource::new("ya29.arc"));
    assert_eq!(s.token().await.unwrap(), "ya29.arc");
}
