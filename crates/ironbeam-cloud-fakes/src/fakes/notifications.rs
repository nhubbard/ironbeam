use super::*;

// ============================================================================
// FakeNotificationIO
// ============================================================================

#[derive(Clone)]
pub struct FakeNotificationIO {
    topics: Arc<Mutex<HashMap<String, Vec<Notification>>>>,
    subscriptions: Arc<Mutex<HashMap<String, String>>>,
    message_counter: Arc<Mutex<u64>>,
}

impl FakeNotificationIO {
    #[must_use]
    /// Create an empty in-memory notifications service for tests.
    pub fn new() -> Self {
        Self {
            topics: Arc::new(Mutex::new(HashMap::new())),
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            message_counter: Arc::new(Mutex::new(0)),
        }
    }

    fn next_id(&self) -> String {
        let mut counter = self
            .message_counter
            .lock()
            .expect("message_counter mutex poisoned");
        *counter += 1;
        let id = *counter;
        drop(counter);
        format!("msg-{id}")
    }
}

impl Default for FakeNotificationIO {
    fn default() -> Self {
        Self::new()
    }
}

impl NotificationIO for FakeNotificationIO {
    fn send(&self, notification: Notification) -> CloudResult<NotificationResult> {
        let msg_id = self.next_id();
        let target = notification.target.clone();
        self.topics
            .lock()
            .expect("topics mutex poisoned")
            .entry(target)
            .or_default()
            .push(notification);

        Ok(NotificationResult {
            message_id: msg_id,
            status: NotificationStatus::Sent,
        })
    }

    fn send_batch(&self, notifications: Vec<Notification>) -> CloudResult<Vec<NotificationResult>> {
        notifications.into_iter().map(|n| self.send(n)).collect()
    }

    fn subscribe(&self, topic: &str, endpoint: &str, _protocol: &str) -> CloudResult<String> {
        let sub_id = format!("sub-{topic}-{endpoint}");
        self.subscriptions
            .lock()
            .expect("subscriptions mutex poisoned")
            .insert(sub_id.clone(), topic.to_string());
        Ok(sub_id)
    }

    fn unsubscribe(&self, subscription_id: &str) -> CloudResult<()> {
        self.subscriptions
            .lock()
            .expect("subscriptions mutex poisoned")
            .remove(subscription_id);
        Ok(())
    }

    fn create_topic(&self, name: &str) -> CloudResult<String> {
        self.topics
            .lock()
            .expect("topics mutex poisoned")
            .insert(name.to_string(), Vec::new());
        Ok(name.to_string())
    }

    fn delete_topic(&self, topic: &str) -> CloudResult<()> {
        self.topics
            .lock()
            .expect("topics mutex poisoned")
            .remove(topic);
        Ok(())
    }
}
