// Unit tests for src/poll.rs.
use super::*;

#[test]
fn terminal_state_classification() {
    assert!(is_terminal("JOB_STATE_DONE"));
    assert!(is_terminal("JOB_STATE_FAILED"));
    assert!(is_terminal("JOB_STATE_CANCELLED"));
    assert!(is_terminal("JOB_STATE_UPDATED"));
    assert!(is_terminal("JOB_STATE_DRAINED"));

    assert!(!is_terminal("JOB_STATE_RUNNING"));
    assert!(!is_terminal("JOB_STATE_PENDING"));
    assert!(!is_terminal("JOB_STATE_QUEUED"));
    assert!(!is_terminal("JOB_STATE_UNKNOWN"));
}

#[test]
fn log_message_handles_unknown_importance() {
    // Should not panic.
    log_message(&serde_json::json!({
        "time": "2026-04-21T00:00:00Z",
        "messageImportance": "JOB_MESSAGE_WHATEVER",
        "messageText": "hello"
    }));
    log_message(&serde_json::json!({}));
}
