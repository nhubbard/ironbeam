use super::*;

// ============================================================================
// FakePubSubIO
// ============================================================================

#[derive(Clone)]
pub struct FakePubSubIO {
    topics: Arc<Mutex<HashMap<String, Vec<Message>>>>,
    subscriptions: Arc<Mutex<HashMap<String, Vec<Message>>>>,
    message_counter: Arc<Mutex<u64>>,
}

impl FakePubSubIO {
    #[must_use]
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

impl Default for FakePubSubIO {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSubIO for FakePubSubIO {
    fn publish(
        &self,
        topic: &str,
        data: &[u8],
        attributes: HashMap<String, String>,
    ) -> CloudResult<String> {
        let msg_id = self.next_id();
        let message = Message {
            id: msg_id.clone(),
            data: data.to_vec(),
            attributes,
            publish_time: Some(0),
        };

        self.topics
            .lock()
            .expect("topics mutex poisoned")
            .entry(topic.to_string())
            .or_default()
            .push(message);

        Ok(msg_id)
    }

    fn publish_batch(
        &self,
        topic: &str,
        messages: Vec<(Vec<u8>, HashMap<String, String>)>,
    ) -> CloudResult<Vec<String>> {
        messages
            .into_iter()
            .map(|(data, attrs)| self.publish(topic, &data, attrs))
            .collect()
    }

    fn subscribe(&self, topic: &str, subscription_name: &str) -> CloudResult<()> {
        self.subscriptions
            .lock()
            .expect("subscriptions mutex poisoned")
            .insert(format!("{topic}/{subscription_name}"), Vec::new());
        Ok(())
    }

    fn pull(&self, subscription: &str, max_messages: u32) -> CloudResult<Vec<Message>> {
        let mut subscriptions = self
            .subscriptions
            .lock()
            .expect("subscriptions mutex poisoned");
        let messages = subscriptions.entry(subscription.to_string()).or_default();

        let count = std::cmp::min(max_messages as usize, messages.len());
        let pulled = messages.drain(0..count).collect();
        drop(subscriptions);
        Ok(pulled)
    }

    fn acknowledge(&self, _subscription: &str, _ack_ids: Vec<String>) -> CloudResult<()> {
        Ok(())
    }

    fn topic_exists(&self, topic: &str) -> CloudResult<bool> {
        Ok(self
            .topics
            .lock()
            .expect("topics mutex poisoned")
            .contains_key(topic))
    }
}
