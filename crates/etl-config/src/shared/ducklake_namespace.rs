//! Stable DuckLake namespace paths shared by replication and maintenance.

use serde::{Deserialize, Serialize};

use crate::shared::{Validate, ValidationError};

/// Durable table identities with a rollback-incompatible namespaced shape.
#[derive(Deserialize, Serialize)]
#[serde(untagged, deny_unknown_fields)]
enum DuckLakeTableMetadata {
    /// Ordered namespace, source schema, and table components.
    Namespaced { namespaced: [String; 3] },
    /// Original representation, including its serialized field order.
    Legacy { schema: String, table: String },
}

/// A physical DuckLake table, with an optional source namespace above its
/// schema.
///
/// Missing namespaces retain the original metadata representation and replay
/// key. Namespaced metadata omits the top-level schema and table fields so
/// pre-namespace binaries reject it instead of writing into the flat layout.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
#[serde(from = "DuckLakeTableMetadata", into = "DuckLakeTableMetadata")]
pub struct DuckLakeTableName {
    /// Source namespace; absent for existing flat destinations.
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
    /// Hex-encoded components contain no dots, so new keys cannot collide with
    /// any legacy `schema.table` key, even when identifiers contain
    /// dots/colons.
    pub fn id(&self) -> String {
        match &self.namespace {
            None => format!("{}.{}", self.schema, self.table),
            Some(namespace) => format!(
                "{}{}:{}",
                Self::namespace_id_prefix(namespace),
                encode_replay_component(&self.schema),
                encode_replay_component(&self.table),
            ),
        }
    }

    /// Prefix used to select replay state owned by one source namespace.
    pub fn namespace_id_prefix(namespace: &str) -> String {
        format!("v2:{}:", encode_replay_component(namespace))
    }

    /// Whether a replay key belongs to the selected layout and namespace.
    pub fn replay_key_matches_namespace(key: &str, namespace: Option<&str>) -> bool {
        match namespace {
            Some(namespace) => {
                !key.contains('.') && key.starts_with(&Self::namespace_id_prefix(namespace))
            }
            None => key.contains('.'),
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

impl From<DuckLakeTableMetadata> for DuckLakeTableName {
    fn from(metadata: DuckLakeTableMetadata) -> Self {
        match metadata {
            DuckLakeTableMetadata::Namespaced { namespaced: [namespace, schema, table] } => {
                Self { namespace: Some(namespace), schema, table }
            }
            DuckLakeTableMetadata::Legacy { schema, table } => {
                Self { namespace: None, schema, table }
            }
        }
    }
}

impl From<DuckLakeTableName> for DuckLakeTableMetadata {
    fn from(table_name: DuckLakeTableName) -> Self {
        match table_name.namespace {
            Some(namespace) => {
                Self::Namespaced { namespaced: [namespace, table_name.schema, table_name.table] }
            }
            None => Self::Legacy { schema: table_name.schema, table: table_name.table },
        }
    }
}

impl std::fmt::Display for DuckLakeTableName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}.{}", self.schema_path(), self.table)
    }
}

/// Encodes literal identifier bytes without the legacy replay-key separator.
fn encode_replay_component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value.bytes() {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 15)]));
    }
    encoded
}

/// Validates a namespace without treating its contents as a SQL path.
///
/// ASCII letters must be lowercase because DuckDB resolves even quoted
/// identifiers case-insensitively, while metadata and replay keys preserve
/// case. Reject aliases instead of normalizing a durable destination identity.
pub fn validate_ducklake_table_namespace(namespace: Option<&str>) -> Result<(), ValidationError> {
    if namespace.is_some_and(|namespace| namespace.is_empty() || namespace.contains('\0')) {
        return Err(ValidationError::InvalidFieldValue {
            field: "table_namespace".to_owned(),
            constraint: "must be nonempty and contain no NUL characters".to_owned(),
        });
    }
    if namespace.is_some_and(|namespace| namespace.bytes().any(|byte| byte.is_ascii_uppercase())) {
        return Err(ValidationError::InvalidFieldValue {
            field: "table_namespace".to_owned(),
            constraint: "must use lowercase ASCII letters to avoid case-insensitive schema aliases"
                .to_owned(),
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
    use serde::Deserialize;

    use crate::shared::{DuckLakeTableName, Validate, validate_ducklake_table_namespace};

    /// Namespace validation preserves flat layouts and literal identifier
    /// contents.
    #[test]
    fn namespace_validation_preserves_flat_and_canonical_names() {
        for namespace in [None, Some("projecta"), Some("project_123"), Some("quoted.\"名")] {
            validate_ducklake_table_namespace(namespace).unwrap();
            let table = DuckLakeTableName::new("Public", "Orders")
                .with_namespace(namespace.map(str::to_owned));
            table.validate().unwrap();
            assert_eq!(table.namespace(), namespace);
        }
    }

    /// Case variants cannot select the same DuckLake schema with distinct
    /// replay keys.
    #[test]
    fn namespace_validation_rejects_noncanonical_case() {
        for namespace in ["ProjectA", "projectA", "PROJECTA", "quoted.\"A名"] {
            let error = validate_ducklake_table_namespace(Some(namespace)).unwrap_err();
            assert!(error.to_string().contains("lowercase ASCII"));
            let table = DuckLakeTableName::new("public", "orders")
                .with_namespace(Some(namespace.to_owned()));
            assert!(table.validate().is_err());
            assert_eq!(table.namespace(), Some(namespace));
        }
    }

    /// Empty and NUL-containing namespaces remain invalid.
    #[test]
    fn namespace_validation_rejects_empty_and_nul() {
        for namespace in ["", "project\0a"] {
            assert!(validate_ducklake_table_namespace(Some(namespace)).is_err());
        }
    }

    /// Matches the permissive metadata parser in pre-namespace binaries.
    #[derive(Debug, Deserialize, PartialEq)]
    struct LegacyDuckLakeTableName {
        schema: String,
        table: String,
    }

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
        assert_eq!(
            serde_json::from_str::<LegacyDuckLakeTableName>(
                &serde_json::to_string(&table).unwrap()
            )
            .unwrap(),
            LegacyDuckLakeTableName { schema: "public".into(), table: "orders".into() }
        );
    }

    #[test]
    fn namespaced_metadata_is_rejected_by_pre_namespace_binaries() {
        for namespace in ["project_a", "project_b", "quoted.\"名"] {
            let table =
                DuckLakeTableName::new("public", "orders").with_namespace(Some(namespace.into()));
            let metadata = serde_json::to_string(&table).unwrap();
            assert!(serde_json::from_str::<LegacyDuckLakeTableName>(&metadata).is_err());
            assert_eq!(serde_json::from_str::<DuckLakeTableName>(&metadata).unwrap(), table);
        }
    }

    #[test]
    fn malformed_namespaced_metadata_cannot_fall_back_to_flat_layout() {
        for metadata in [
            r#"{"namespaced":["project_a","public"]}"#,
            r#"{"namespaced":["project_a","public","orders","extra"]}"#,
            r#"{"namespaced":null,"schema":"public","table":"orders"}"#,
            r#"{"namespace":"project_a","schema":"public","table":"orders"}"#,
        ] {
            assert!(serde_json::from_str::<DuckLakeTableName>(metadata).is_err());
        }
    }

    #[test]
    fn namespaced_keys_distinguish_projects_and_identifier_boundaries() {
        let a = DuckLakeTableName::new("public", "orders").with_namespace(Some("project_a".into()));
        let b = DuckLakeTableName::new("public", "orders").with_namespace(Some("project_b".into()));
        assert_ne!(a.id(), b.id());
        let dotted = DuckLakeTableName::new("a.b", "c").with_namespace(Some("project_a".into()));
        let other = DuckLakeTableName::new("a", "b.c").with_namespace(Some("project_a".into()));
        assert_ne!(dotted.id(), other.id());
        // This legacy name would collide with an unescaped length-prefixed key.
        let legacy = DuckLakeTableName::new("v2:9:project_a3:a", "b1:c");
        assert_ne!(dotted.id(), legacy.id());
        for table in [&a, &b, &dotted, &other] {
            assert!(!table.id().contains('.'));
            assert!(!DuckLakeTableName::replay_key_matches_namespace(&table.id(), None));
        }
        assert!(DuckLakeTableName::replay_key_matches_namespace(&a.id(), Some("project_a")));
        assert!(!DuckLakeTableName::replay_key_matches_namespace(&b.id(), Some("project_a")));
        assert!(DuckLakeTableName::replay_key_matches_namespace(&legacy.id(), None));
        assert!(!DuckLakeTableName::replay_key_matches_namespace(&legacy.id(), Some("project_a")));
        assert_eq!(
            serde_json::from_str::<DuckLakeTableName>(&serde_json::to_string(&a).unwrap()).unwrap(),
            a
        );
        assert_eq!(dotted.schema_path(), "\"project_a\".\"a.b\"");
    }
}
