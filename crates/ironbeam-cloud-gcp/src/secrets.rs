use crate::{GcpError, GcpProject};
use google_cloud_gax::paginator::ItemPaginator as _;
use google_cloud_secretmanager_v1::{
    client::SecretManagerService,
    model::{Replication, Secret, SecretPayload, replication},
};

/// Secret Manager client scoped to a Google Cloud project.
#[derive(Clone)]
pub struct GcpSecrets {
    project: GcpProject,
    client: SecretManagerService,
}

impl GcpProject {
    /// Connect to Secret Manager with Application Default Credentials.
    pub async fn secrets(&self) -> Result<GcpSecrets, GcpError> {
        GcpSecrets::connect(self.clone()).await
    }
}

impl GcpSecrets {
    /// Create a Secret Manager client for a project.
    pub async fn connect(project: GcpProject) -> Result<Self, GcpError> {
        let client = SecretManagerService::builder()
            .build()
            .await
            .map_err(GcpError::client)?;
        Ok(Self { project, client })
    }

    /// Access the official client for version and replication controls.
    #[must_use]
    pub fn client(&self) -> &SecretManagerService {
        &self.client
    }

    /// Read the latest version of a secret as bytes.
    pub async fn access_latest(&self, secret: &str) -> Result<Vec<u8>, GcpError> {
        let name = format!("{}/versions/latest", self.project.secret_name(secret)?);
        let response = self
            .client
            .access_secret_version()
            .set_name(name)
            .send()
            .await
            .map_err(GcpError::client)?;
        let payload = response.payload.ok_or(GcpError::MissingPayload)?;
        Ok(payload.data.to_vec())
    }

    /// Read a UTF-8 secret value from its latest version.
    pub async fn access_latest_utf8(&self, secret: &str) -> Result<String, GcpError> {
        Ok(String::from_utf8(self.access_latest(secret).await?)?)
    }

    /// Create a secret with automatic replication. The first version is added separately.
    pub async fn create_secret(&self, secret: &str) -> Result<(), GcpError> {
        let name = self.project.secret_name(secret)?;
        let (parent, id) = name
            .rsplit_once("/secrets/")
            .ok_or_else(|| GcpError::InvalidResource(name.clone()))?;
        self.client
            .create_secret()
            .set_parent(parent)
            .set_secret_id(id)
            .set_secret(
                Secret::new().set_replication(
                    Replication::new().set_automatic(replication::Automatic::new()),
                ),
            )
            .send()
            .await
            .map_err(GcpError::client)?;
        Ok(())
    }

    /// Add a version to an existing secret and return the new version name.
    pub async fn add_version(&self, secret: &str, data: Vec<u8>) -> Result<String, GcpError> {
        let version = self
            .client
            .add_secret_version()
            .set_parent(self.project.secret_name(secret)?)
            .set_payload(SecretPayload::new().set_data(data))
            .send()
            .await
            .map_err(GcpError::client)?;
        Ok(version.name)
    }

    /// List fully qualified secret names in the project.
    pub async fn list(&self) -> Result<Vec<String>, GcpError> {
        let mut items = self
            .client
            .list_secrets()
            .set_parent(format!("projects/{}", self.project.id()))
            .by_item();
        let mut names = Vec::new();
        while let Some(item) = items.next().await.transpose().map_err(GcpError::client)? {
            names.push(item.name);
        }
        Ok(names)
    }
}
