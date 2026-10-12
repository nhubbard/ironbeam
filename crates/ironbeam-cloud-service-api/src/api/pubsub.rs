use super::*;

// ============================================================================
// PubSubIO - Message Streaming
// ============================================================================

/// A message in a pub/sub system
#[derive(Debug, Clone)]
pub struct Message {
    pub id: String,
    pub data: Vec<u8>,
    pub attributes: HashMap<String, String>,
    pub publish_time: Option<i64>, // Unix timestamp
}

/// Trait for pub/sub streaming operations
pub trait PubSubIO: Send + Sync {
    /// Publish a message to a topic
    ///
    /// # Errors
    ///
    /// Returns an error if the topic doesn't exist, permissions are not enough, or publishing fails
    fn publish(
        &self,
        topic: &str,
        data: &[u8],
        attributes: HashMap<String, String>,
    ) -> CloudResult<String>;

    /// Publish multiple messages to a topic
    ///
    /// # Errors
    ///
    /// Returns an error if the topic doesn't exist, permissions are not enough, or publishing fails
    fn publish_batch(
        &self,
        topic: &str,
        messages: Vec<(Vec<u8>, HashMap<String, String>)>,
    ) -> CloudResult<Vec<String>>;

    /// Subscribe to a topic (creates subscription if it doesn't exist)
    ///
    /// # Errors
    ///
    /// Returns an error if the topic doesn't exist, permissions are not enough, or subscription creation fails
    fn subscribe(&self, topic: &str, subscription_name: &str) -> CloudResult<()>;

    /// Pull messages from a subscription
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription doesn't exist, permissions are not enough, or pulling fails
    fn pull(&self, subscription: &str, max_messages: u32) -> CloudResult<Vec<Message>>;

    /// Acknowledge messages (mark as processed)
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription doesn't exist, acknowledgment IDs are invalid, or the operation fails
    fn acknowledge(&self, subscription: &str, ack_ids: Vec<String>) -> CloudResult<()>;

    /// Check if a topic exists
    ///
    /// # Errors
    ///
    /// Returns an error if there's a connection issue or permissions are not enough
    fn topic_exists(&self, topic: &str) -> CloudResult<bool>;
}
