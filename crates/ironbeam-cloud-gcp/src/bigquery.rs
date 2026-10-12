use crate::{GcpError, GcpProject};
use google_cloud_bigquery::{client::BigQuery, query::Row};

/// `BigQuery` SQL queries in a configured project and location.
#[derive(Clone)]
pub struct GcpBigQuery {
    project: GcpProject,
    client: BigQuery,
}

impl GcpProject {
    /// Connect to `BigQuery` with Application Default Credentials.
    ///
    /// # Errors
    ///
    /// Returns an error if credentials or the client transport cannot be initialized.
    pub async fn bigquery(&self) -> Result<GcpBigQuery, GcpError> {
        GcpBigQuery::connect(self.clone()).await
    }
}

impl GcpBigQuery {
    /// Create a `BigQuery` client for a project.
    ///
    /// # Errors
    ///
    /// Returns an error if credentials or the client transport cannot be initialized.
    pub async fn connect(project: GcpProject) -> Result<Self, GcpError> {
        let client = BigQuery::builder()
            .with_project_id(project.id().to_string())
            .build()
            .await
            .map_err(GcpError::client)?;
        Ok(Self { project, client })
    }

    /// Access the official client for query parameters and advanced jobs.
    #[must_use]
    pub const fn client(&self) -> &BigQuery {
        &self.client
    }

    /// Run SQL and collect its result rows. Each `Row` supports typed `get`.
    /// For large results, use [`client`](Self::client) to stream rows.
    ///
    /// # Errors
    ///
    /// Returns an error for empty SQL, a failed query, or a failed row read.
    pub async fn query_rows(&self, sql: &str) -> Result<Vec<Row>, GcpError> {
        if sql.trim().is_empty() {
            return Err(GcpError::InvalidResource("empty BigQuery SQL".into()));
        }
        let mut query = self.client.query(sql);
        if let Some(location) = self.project.location() {
            query = query.set_location(location);
        }
        let mut rows = query.until_done().await.map_err(GcpError::client)?.read();
        let mut result = Vec::new();
        while let Some(row) = rows.next().await.transpose().map_err(GcpError::client)? {
            result.push(row);
        }
        Ok(result)
    }

    /// Execute SQL and wait for its job to complete.
    ///
    /// # Errors
    ///
    /// Returns an error for empty SQL or a failed query job.
    pub async fn execute(&self, sql: &str) -> Result<(), GcpError> {
        if sql.trim().is_empty() {
            return Err(GcpError::InvalidResource("empty BigQuery SQL".into()));
        }
        let mut query = self.client.query(sql);
        if let Some(location) = self.project.location() {
            query = query.set_location(location);
        }
        query.until_done().await.map_err(GcpError::client)?;
        Ok(())
    }
}
