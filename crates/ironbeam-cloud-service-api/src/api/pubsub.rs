use super::{CloudResult, HashMap};

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

/// A delivery whose acknowledgment handle is valid for this attempt only.
/// Redelivery may keep the message ID while assigning a new acknowledgment ID.
#[derive(Debug, Clone)]
pub struct ReceivedMessage {
    pub message: Message,
    pub ack_id: String,
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

    /// Pull messages without acknowledgment handles. This compatibility
    /// method cannot support a replay-safe consumer. Use `pull_with_ack` for
    /// processing that must survive a failed worker.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription doesn't exist, permissions are not enough, or pulling fails
    fn pull(&self, subscription: &str, max_messages: u32) -> CloudResult<Vec<Message>>;

    /// Pull deliveries with the handles needed to acknowledge them after
    /// processing. Providers that only implement the legacy `pull` method
    /// fail closed instead of inventing acknowledgment IDs.
    ///
    /// # Errors
    ///
    /// Returns an error if the provider lacks acknowledgment handles or the
    /// subscription cannot be read.
    fn pull_with_ack(
        &self,
        _subscription: &str,
        _max_messages: u32,
    ) -> CloudResult<Vec<ReceivedMessage>> {
        Err(super::CloudIOError::new(
            super::ErrorKind::InvalidInput,
            "provider does not expose Pub/Sub acknowledgment handles",
        ))
    }

    /// Acknowledge deliveries using handles returned by `pull_with_ack`.
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
