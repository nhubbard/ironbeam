use super::*;
use ironbeam_cloud_service_api::ReceivedMessage;
use std::collections::{HashSet, VecDeque};

/// In-memory subscriptions with explicit lease expiry for retry tests.
/// The compatibility `pull` method acknowledges deliveries immediately.
#[derive(Clone, Default)]
pub struct FakePubSubIO {
    state: Arc<Mutex<PubSubState>>,
}

#[derive(Default)]
struct PubSubState {
    topics: HashMap<String, Vec<Message>>,
    subscriptions: HashMap<String, Subscription>,
    next_message_id: u64,
    next_ack_id: u64,
}

struct Subscription {
    topic: String,
    ready: VecDeque<Message>,
    leased: VecDeque<(String, Message)>,
}

impl FakePubSubIO {
    #[must_use]
    /// Create an empty in-memory pubsub service for tests.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a topic before publishing or attaching subscriptions.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` for an empty name or `AlreadyExists` for an
    /// existing topic.
    pub fn create_topic(&self, topic: &str) -> CloudResult<()> {
        if topic.is_empty() {
            return Err(CloudIOError::new(
                ErrorKind::InvalidInput,
                "topic name must not be empty",
            ));
        }
        let mut state = self.lock_state()?;
        if state.topics.contains_key(topic) {
            return Err(CloudIOError::new(
                ErrorKind::AlreadyExists,
                "topic already exists",
            ));
        }
        state.topics.insert(topic.to_owned(), Vec::new());
        Ok(())
    }

    fn lock_state(&self) -> CloudResult<std::sync::MutexGuard<'_, PubSubState>> {
        self.state.lock().map_err(|_| {
            CloudIOError::new(ErrorKind::InternalError, "Pub/Sub state mutex poisoned")
        })
    }

    /// Return outstanding deliveries to the subscription with fresh handles
    /// on the next pull. This simulates an expired lease or worker loss.
    ///
    /// # Errors
    ///
    /// Returns `NotFound` for an unknown subscription.
    pub fn redeliver_unacked(&self, subscription: &str) -> CloudResult<()> {
        let mut state = self.lock_state()?;
        let subscription = state
            .subscriptions
            .get_mut(subscription)
            .ok_or_else(|| CloudIOError::new(ErrorKind::NotFound, "subscription does not exist"))?;
        let mut returned = subscription
            .leased
            .drain(..)
            .map(|(_, message)| message)
            .collect::<VecDeque<_>>();
        returned.append(&mut subscription.ready);
        subscription.ready = returned;
        drop(state);
        Ok(())
    }
}

impl PubSubIO for FakePubSubIO {
    fn publish(
        &self,
        topic: &str,
        data: &[u8],
        attributes: HashMap<String, String>,
    ) -> CloudResult<String> {
        let mut state = self.lock_state()?;
        if !state.topics.contains_key(topic) {
            return Err(CloudIOError::new(
                ErrorKind::NotFound,
                "topic does not exist",
            ));
        }
        state.next_message_id = state.next_message_id.checked_add(1).ok_or_else(|| {
            CloudIOError::new(ErrorKind::InternalError, "message ID space exhausted")
        })?;
        let id = format!("msg-{}", state.next_message_id);
        let message = Message {
            id: id.clone(),
            data: data.to_vec(),
            attributes,
            publish_time: Some(0),
        };
        state
            .topics
            .get_mut(topic)
            .expect("topic checked above")
            .push(message.clone());
        for subscription in state.subscriptions.values_mut() {
            if subscription.topic == topic {
                subscription.ready.push_back(message.clone());
            }
        }
        drop(state);
        Ok(id)
    }

    fn publish_batch(
        &self,
        topic: &str,
        messages: Vec<(Vec<u8>, HashMap<String, String>)>,
    ) -> CloudResult<Vec<String>> {
        if !self.topic_exists(topic)? {
            return Err(CloudIOError::new(
                ErrorKind::NotFound,
                "topic does not exist",
            ));
        }
        messages
            .into_iter()
            .map(|(data, attributes)| self.publish(topic, &data, attributes))
            .collect()
    }

    fn subscribe(&self, topic: &str, subscription_name: &str) -> CloudResult<()> {
        if topic.is_empty() || subscription_name.is_empty() {
            return Err(CloudIOError::new(
                ErrorKind::InvalidInput,
                "topic and subscription names must not be empty",
            ));
        }
        let mut state = self.lock_state()?;
        if let Some(existing) = state.subscriptions.get(subscription_name) {
            return if existing.topic == topic {
                Ok(())
            } else {
                Err(CloudIOError::new(
                    ErrorKind::AlreadyExists,
                    "subscription already belongs to another topic",
                ))
            };
        }
        if !state.topics.contains_key(topic) {
            return Err(CloudIOError::new(
                ErrorKind::NotFound,
                "topic does not exist",
            ));
        }
        state.subscriptions.insert(
            subscription_name.to_owned(),
            Subscription {
                topic: topic.to_owned(),
                ready: VecDeque::new(),
                leased: VecDeque::new(),
            },
        );
        drop(state);
        Ok(())
    }

    fn pull(&self, subscription: &str, max_messages: u32) -> CloudResult<Vec<Message>> {
        let received = self.pull_with_ack(subscription, max_messages)?;
        self.acknowledge(
            subscription,
            received
                .iter()
                .map(|delivery| delivery.ack_id.clone())
                .collect(),
        )?;
        Ok(received
            .into_iter()
            .map(|delivery| delivery.message)
            .collect())
    }

    fn pull_with_ack(
        &self,
        subscription: &str,
        max_messages: u32,
    ) -> CloudResult<Vec<ReceivedMessage>> {
        let mut state = self.lock_state()?;
        if !state.subscriptions.contains_key(subscription) {
            return Err(CloudIOError::new(
                ErrorKind::NotFound,
                "subscription does not exist",
            ));
        }
        let mut received = Vec::new();
        for _ in 0..max_messages {
            if state
                .subscriptions
                .get(subscription)
                .is_none_or(|subscription| subscription.ready.is_empty())
            {
                break;
            }
            let next_ack_id = state.next_ack_id.checked_add(1).ok_or_else(|| {
                CloudIOError::new(
                    ErrorKind::InternalError,
                    "acknowledgment ID space exhausted",
                )
            })?;
            let Some(message) = state
                .subscriptions
                .get_mut(subscription)
                .expect("subscription checked above")
                .ready
                .pop_front()
            else {
                break;
            };
            state.next_ack_id = next_ack_id;
            let ack_id = format!("ack-{}", state.next_ack_id);
            state
                .subscriptions
                .get_mut(subscription)
                .expect("subscription checked above")
                .leased
                .push_back((ack_id.clone(), message.clone()));
            received.push(ReceivedMessage { message, ack_id });
        }
        drop(state);
        Ok(received)
    }

    fn acknowledge(&self, subscription: &str, ack_ids: Vec<String>) -> CloudResult<()> {
        let mut state = self.lock_state()?;
        let subscription = state
            .subscriptions
            .get_mut(subscription)
            .ok_or_else(|| CloudIOError::new(ErrorKind::NotFound, "subscription does not exist"))?;
        let requested = ack_ids.into_iter().collect::<HashSet<_>>();
        if requested
            .iter()
            .any(|id| !subscription.leased.iter().any(|(leased, _)| leased == id))
        {
            return Err(CloudIOError::new(
                ErrorKind::InvalidInput,
                "acknowledgment ID is not leased by this subscription",
            ));
        }
        subscription
            .leased
            .retain(|(id, _)| !requested.contains(id));
        drop(state);
        Ok(())
    }

    fn topic_exists(&self, topic: &str) -> CloudResult<bool> {
        Ok(self.lock_state()?.topics.contains_key(topic))
    }
}
