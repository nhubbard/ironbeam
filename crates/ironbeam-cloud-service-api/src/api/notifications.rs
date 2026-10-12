use super::{CloudResult, HashMap};

// ============================================================================
// NotificationIO - Push Notifications
// ============================================================================

/// A notification to be sent
#[derive(Debug, Clone)]
pub struct Notification {
    pub target: String, // Topic ARN, device token, email, etc.
    pub subject: Option<String>,
    pub message: String,
    pub attributes: HashMap<String, String>,
}

/// Result of sending a notification
#[derive(Debug, Clone)]
pub struct NotificationResult {
    pub message_id: String,
    pub status: NotificationStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotificationStatus {
    Sent,
    Failed,
    Pending,
}

/// Trait for notification operations
pub trait NotificationIO: Send + Sync {
    /// Send a notification
    ///
    /// # Errors
    ///
    /// Returns an error if the target is invalid, permissions are not enough, or sending fails
    fn send(&self, notification: Notification) -> CloudResult<NotificationResult>;

    /// Send multiple notifications
    ///
    /// # Errors
    ///
    /// Returns an error if any target is invalid, permissions are not enough, or sending fails
    fn send_batch(&self, notifications: Vec<Notification>) -> CloudResult<Vec<NotificationResult>>;

    /// Subscribe an endpoint to a topic
    ///
    /// # Errors
    ///
    /// Returns an error if the topic doesn't exist, the endpoint is invalid, or subscription fails
    fn subscribe(&self, topic: &str, endpoint: &str, protocol: &str) -> CloudResult<String>;

    /// Unsubscribe from a topic
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription doesn't exist, permissions are not enough, or unsubscription fails
    fn unsubscribe(&self, subscription_id: &str) -> CloudResult<()>;

    /// Create a topic
    ///
    /// # Errors
    ///
    /// Returns an error if the topic already exists, permissions are not enough, or creation fails
    fn create_topic(&self, name: &str) -> CloudResult<String>;

    /// Delete a topic
    ///
    /// # Errors
    ///
    /// Returns an error if the topic doesn't exist, permissions are not enough, or deletion fails
    fn delete_topic(&self, topic: &str) -> CloudResult<()>;
}
