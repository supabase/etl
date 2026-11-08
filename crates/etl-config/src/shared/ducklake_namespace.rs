//! Stable DuckLake namespace paths shared by replication and maintenance.

use serde::{Deserialize, Serialize};

use crate::shared::{Validate, ValidationError};

/// A physical DuckLake table, with an optional source namespace above its
/// schema.
///
/// Missing namespaces retain the original metadata representation and replay
/// key.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct DuckLakeTableName {
    /// Source namespace; absent for existing flat destinations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    namespace: Option<String>,
    /// Source schema, kept as one identifier even when it contains dots.
    schema: String,
    /// Source table name.
    table: String,
}

impl DuckLakeTableName {
    /// Creates a legacy, unnamespaced table reference.
    pub fn new(schema: impl Into<String>, table: impl Into<String>) -> Self {
        Self { namespace: None, schema: schema.into(), table: table.into() }
    }

    /// Sets the destination's persisted source namespace.
    pub fn with_namespace(mut self, namespace: Option<String>) -> Self {
        self.namespace = namespace;
        self
    }

    /// Returns the source namespace.
    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    /// Returns the leaf source schema.
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Returns the source table.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Returns a stable replay key, preserving all pre-namespace keys.
    ///
    /// Length-prefixed components prevent collisions between names containing
    /// dots.
    pub fn id(&self) -> String {
        match &self.namespace {
            None => format!("{}.{}", self.schema, self.table),
            Some(namespace) => format!(
                "v2:{}:{}{}:{}{}:{}",
                namespace.len(),
                namespace,
                self.schema.len(),
                self.schema,
                self.table.len(),
                self.table,
            ),
        }
    }

    /// Renders a schema path for DuckLake functions accepting a schema string.
    pub fn schema_path(&self) -> String {
        match &self.namespace {
            None => self.schema.clone(),
            Some(namespace) => format!(
                "\"{}\".\"{}\"",
                namespace.replace('"', "\"\""),
                self.schema.replace('"', "\"\"")
            ),
        }
    }

    /// Returns whether this is ETL bookkeeping rather than a source table.
    pub fn is_internal_helper(&self) -> bool {
        self.table.starts_with("__etl_")
    }
}

impl std::fmt::Display for DuckLakeTableName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}.{}", self.schema_path(), self.table)
    }
}

/// Validates a namespace without treating its contents as a SQL path.
pub fn validate_ducklake_table_namespace(namespace: Option<&str>) -> Result<(), ValidationError> {
    if namespace.is_some_and(|namespace| namespace.is_empty() || namespace.contains('\0')) {
        return Err(ValidationError::InvalidFieldValue {
            field: "table_namespace".to_owned(),
            constraint: "must be nonempty and contain no NUL characters".to_owned(),
        });
    }
    Ok(())
}

impl Validate for DuckLakeTableName {
    fn validate(&self) -> Result<(), ValidationError> {
        validate_ducklake_table_namespace(self.namespace())
    }
}

#[cfg(test)]
mod tests {
    use crate::shared::DuckLakeTableName;

    #[test]
    fn legacy_metadata_and_replay_keys_are_unchanged() {
        let table: DuckLakeTableName =
            serde_json::from_str(r#"{"schema":"public","table":"orders"}"#).unwrap();
        assert_eq!(table.namespace(), None);
        assert_eq!(table.id(), "public.orders");
        assert_eq!(
            serde_json::to_string(&table).unwrap(),
            r#"{"schema":"public","table":"orders"}"#
        );
    }

    #[test]
    fn namespaced_keys_distinguish_projects_and_identifier_boundaries() {
        let a = DuckLakeTableName::new("public", "orders").with_namespace(Some("project_a".into()));
        let b = DuckLakeTableName::new("public", "orders").with_namespace(Some("project_b".into()));
        assert_ne!(a.id(), b.id());
        let dotted = DuckLakeTableName::new("a.b", "c").with_namespace(Some("project_a".into()));
        let other = DuckLakeTableName::new("a", "b.c").with_namespace(Some("project_a".into()));
        assert_ne!(dotted.id(), other.id());
        assert_eq!(
            serde_json::from_str::<DuckLakeTableName>(&serde_json::to_string(&a).unwrap()).unwrap(),
            a
        );
        assert_eq!(dotted.schema_path(), "\"project_a\".\"a.b\"");
    }
}
