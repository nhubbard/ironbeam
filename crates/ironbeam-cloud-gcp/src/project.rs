use ironbeam_cloud_service_api::CloudConfig;
use std::error::Error as StdError;

/// Errors from resource validation or an official Google Cloud client.
#[derive(Debug, thiserror::Error)]
pub enum GcpError {
    #[error("invalid GCP project ID: {0}")]
    InvalidProject(String),
    #[error("invalid GCP resource name: {0}")]
    InvalidResource(String),
    #[error("Google Cloud client error: {0}")]
    Client(#[source] Box<dyn StdError + Send + Sync>),
    #[error("secret payload is missing from the response")]
    MissingPayload,
    #[error("secret payload is not valid UTF-8: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}

#[cfg(any(
    feature = "storage",
    feature = "pubsub",
    feature = "bigquery",
    feature = "secrets"
))]
impl GcpError {
    pub(crate) fn client(error: impl StdError + Send + Sync + 'static) -> Self {
        Self::Client(Box::new(error))
    }
}

/// Project scope shared by the service clients.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcpProject {
    id: String,
    location: Option<String>,
}

impl GcpProject {
    /// Create a project scope. A project ID or numeric project number is accepted.
    ///
    /// # Errors
    ///
    /// Returns an error if the project identifier is empty or malformed.
    pub fn new(id: impl Into<String>) -> Result<Self, GcpError> {
        let id = id.into();
        if !valid_project_name(&id) {
            return Err(GcpError::InvalidProject(id));
        }
        Ok(Self { id, location: None })
    }

    /// Return the configured GCP project ID.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Set the `BigQuery` job location, such as `US` or `us-central1`.
    ///
    /// # Errors
    ///
    /// Returns an error if the location is empty or malformed.
    pub fn with_location(mut self, location: impl Into<String>) -> Result<Self, GcpError> {
        let location = location.into();
        if location.is_empty()
            || !location
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(GcpError::InvalidResource(location));
        }
        self.location = Some(location);
        Ok(self)
    }

    /// Return the configured BigQuery job location, if any.
    #[must_use]
    pub fn location(&self) -> Option<&str> {
        self.location.as_deref()
    }

    /// Expand a topic ID or accept a fully qualified topic name.
    ///
    /// # Errors
    ///
    /// Returns an error if the topic name is malformed.
    pub fn topic_name(&self, name: &str) -> Result<String, GcpError> {
        self.resource("topics", name)
    }

    /// Expand a subscription ID or accept a fully qualified subscription name.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription name is malformed.
    pub fn subscription_name(&self, name: &str) -> Result<String, GcpError> {
        self.resource("subscriptions", name)
    }

    /// Expand a secret ID or accept a fully qualified secret name.
    ///
    /// # Errors
    ///
    /// Returns an error if the secret name is malformed.
    pub fn secret_name(&self, name: &str) -> Result<String, GcpError> {
        self.resource("secrets", name)
    }

    fn resource(&self, kind: &str, name: &str) -> Result<String, GcpError> {
        let segments: Vec<_> = name.split('/').collect();
        if segments.len() == 4
            && segments[0] == "projects"
            && valid_project_name(segments[1])
            && segments[2] == kind
            && valid_short_name(segments[3])
        {
            return Ok(name.to_string());
        }
        if valid_short_name(name) {
            return Ok(format!("projects/{}/{kind}/{name}", self.id));
        }
        Err(GcpError::InvalidResource(name.to_string()))
    }

    /// Expand a bucket ID or a `gs://bucket` URI for the Storage client.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket name or URI is malformed.
    pub fn bucket(bucket: &str) -> Result<String, GcpError> {
        let bucket = bucket.strip_prefix("gs://").unwrap_or(bucket);
        let segments: Vec<_> = bucket.split('/').collect();
        if segments.len() == 4
            && segments[0] == "projects"
            && segments[1] == "_"
            && segments[2] == "buckets"
            && valid_short_name(segments[3])
        {
            return Ok(bucket.to_string());
        }
        if valid_short_name(bucket) {
            return Ok(format!("projects/_/buckets/{bucket}"));
        }
        Err(GcpError::InvalidResource(bucket.to_string()))
    }
}

fn valid_short_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.~+%".contains(&byte))
}

fn valid_project_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

impl CloudConfig for GcpProject {
    fn region(&self) -> Option<&str> {
        self.location()
    }
}
