use super::fmt;

// ============================================================================
// Resource Identifier Types
// ============================================================================

/// Represents a resource in the cloud (bucket, table, queue, etc.)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResourceId {
    pub provider: String,
    pub resource_type: String,
    pub name: String,
    pub namespace: Option<String>, // project, account, etc.
}

impl ResourceId {
    pub fn new(
        provider: impl Into<String>,
        resource_type: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            provider: provider.into(),
            resource_type: resource_type.into(),
            name: name.into(),
            namespace: None,
        }
    }

    #[must_use]
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }
}

impl fmt::Display for ResourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ns) = &self.namespace {
            write!(
                f,
                "{}:{}:{}/{}",
                self.provider, self.resource_type, ns, self.name
            )
        } else {
            write!(f, "{}:{}:{}", self.provider, self.resource_type, self.name)
        }
    }
}
