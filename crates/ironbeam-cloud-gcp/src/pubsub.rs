use crate::{GcpError, GcpProject};
use google_cloud_pubsub::{
    client::{Publisher, Subscriber, SubscriptionAdmin, TopicAdmin},
    model::Message,
    subscriber::MessageStream,
};
use std::collections::HashMap;
use tokio::sync::Mutex;

/// Google Cloud Pub/Sub publisher and subscriber clients for one project.
pub struct GcpPubSub {
    project: GcpProject,
    subscriber: Subscriber,
    subscriptions: SubscriptionAdmin,
    topics: TopicAdmin,
    publishers: Mutex<HashMap<String, Publisher>>,
}

impl GcpProject {
    /// Connect to Pub/Sub with Application Default Credentials.
    ///
    /// # Errors
    ///
    /// Returns an error if credentials or a client transport cannot be initialized.
    pub async fn pubsub(&self) -> Result<GcpPubSub, GcpError> {
        GcpPubSub::connect(self.clone()).await
    }
}

impl GcpPubSub {
    /// Build clients for publishing and subscription management.
    ///
    /// # Errors
    ///
    /// Returns an error if credentials or a client transport cannot be initialized.
    pub async fn connect(project: GcpProject) -> Result<Self, GcpError> {
        let subscriber = Subscriber::builder()
            .build()
            .await
            .map_err(GcpError::client)?;
        let subscriptions = SubscriptionAdmin::builder()
            .build()
            .await
            .map_err(GcpError::client)?;
        let topics = TopicAdmin::builder()
            .build()
            .await
            .map_err(GcpError::client)?;
        Ok(Self {
            project,
            subscriber,
            subscriptions,
            topics,
            publishers: Mutex::new(HashMap::new()),
        })
    }

    /// Get a reusable publisher for a topic. Short and qualified names work.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid topic or a publisher initialization failure.
    pub async fn publisher(&self, topic: &str) -> Result<Publisher, GcpError> {
        let topic = self.project.topic_name(topic)?;
        if let Some(client) = self.publishers.lock().await.get(&topic) {
            return Ok(client.clone());
        }
        let client = Publisher::builder(topic.clone())
            .build()
            .await
            .map_err(GcpError::client)?;
        let mut publishers = self.publishers.lock().await;
        Ok(publishers.entry(topic).or_insert(client).clone())
    }

    /// Publish one message and return its server-assigned ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the topic is invalid or publishing fails.
    pub async fn publish(
        &self,
        topic: &str,
        data: Vec<u8>,
        attributes: HashMap<String, String>,
    ) -> Result<String, GcpError> {
        let publisher = self.publisher(topic).await?;
        publisher
            .publish(Message::new().set_data(data).set_attributes(attributes))
            .await
            .map_err(GcpError::client)
    }

    /// Start the official streaming subscriber for a subscription.
    /// Received messages include process-local acknowledgment handlers. They
    /// cannot be persisted as Ironbeam source checkpoint positions.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription name is invalid.
    pub fn stream(&self, subscription: &str) -> Result<MessageStream, GcpError> {
        let name = self.project.subscription_name(subscription)?;
        Ok(self.subscriber.subscribe(name).build())
    }

    /// Create a pull subscription attached to a topic.
    ///
    /// # Errors
    ///
    /// Returns an error if a resource name is invalid or the service rejects creation.
    pub async fn create_subscription(
        &self,
        topic: &str,
        subscription: &str,
    ) -> Result<(), GcpError> {
        let name = self.project.subscription_name(subscription)?;
        let topic = self.project.topic_name(topic)?;
        self.subscriptions
            .create_subscription()
            .set_name(name)
            .set_topic(topic)
            .send()
            .await
            .map_err(GcpError::client)?;
        Ok(())
    }

    /// Create a topic before publishing to it.
    ///
    /// # Errors
    ///
    /// Returns an error if the topic name is invalid or the service rejects creation.
    pub async fn create_topic(&self, topic: &str) -> Result<(), GcpError> {
        self.topics
            .create_topic()
            .set_name(self.project.topic_name(topic)?)
            .send()
            .await
            .map_err(GcpError::client)?;
        Ok(())
    }

    /// Access the official topic administration client.
    #[must_use]
    pub const fn topic_admin(&self) -> &TopicAdmin {
        &self.topics
    }

    /// Access the official subscription administration client.
    #[must_use]
    pub const fn subscription_admin(&self) -> &SubscriptionAdmin {
        &self.subscriptions
    }

    /// Access the official subscriber for flow control and stream options.
    #[must_use]
    pub const fn subscriber(&self) -> &Subscriber {
        &self.subscriber
    }
}
