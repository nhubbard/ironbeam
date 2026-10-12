use super::*;

// ============================================================================
// QueueIO - Message Queues
// ============================================================================

/// A queue message
#[derive(Debug, Clone)]
pub struct QueueMessage {
    pub id: String,
    pub receipt_handle: String, // For acknowledgment
    pub body: String,
    pub attributes: HashMap<String, String>,
    pub receive_count: u32,
}

/// Trait for message queue operations
pub trait QueueIO: Send + Sync {
    /// Send a message to a queue
    ///
    /// # Errors
    ///
    /// Returns an error if the queue doesn't exist, permissions are not enough, or sending fails
    fn send(
        &self,
        queue: &str,
        body: &str,
        attributes: HashMap<String, String>,
    ) -> CloudResult<String>;

    /// Send multiple messages to a queue
    ///
    /// # Errors
    ///
    /// Returns an error if the queue doesn't exist, permissions are not enough, or sending fails
    fn send_batch(
        &self,
        queue: &str,
        messages: Vec<(String, HashMap<String, String>)>,
    ) -> CloudResult<Vec<String>>;

    /// Receive messages from a queue
    ///
    /// # Errors
    ///
    /// Returns an error if the queue doesn't exist, permissions don't grant access, or receiving fails
    fn receive(
        &self,
        queue: &str,
        max_messages: u32,
        visibility_timeout_secs: u32,
    ) -> CloudResult<Vec<QueueMessage>>;

    /// Delete a message from the queue
    ///
    /// # Errors
    ///
    /// Returns an error if the queue doesn't exist, the receipt handle is invalid, or deletion fails
    fn delete(&self, queue: &str, receipt_handle: &str) -> CloudResult<()>;

    /// Delete multiple messages from the queue
    ///
    /// # Errors
    ///
    /// Returns an error if the queue doesn't exist, receipt handles are invalid, or deletion fails
    fn delete_batch(&self, queue: &str, receipt_handles: Vec<String>) -> CloudResult<()>;

    /// Get approximate queue size
    ///
    /// # Errors
    ///
    /// Returns an error if the queue doesn't exist, permissions are not enough, or the operation fails
    fn queue_size(&self, queue: &str) -> CloudResult<u64>;

    /// Purge all messages from a queue
    ///
    /// # Errors
    ///
    /// Returns an error if the queue doesn't exist, permissions are not enough, or purging fails
    fn purge(&self, queue: &str) -> CloudResult<()>;
}
