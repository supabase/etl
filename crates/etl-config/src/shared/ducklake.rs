//! Typed DuckLake partition specifications shared by the API and replicator.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use tokio_postgres::types::Type;
#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

use crate::shared::{Validate, ValidationError};

/// An allowlisted DuckLake partition transform, never a SQL expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DuckLakePartitionTransform {
    /// Partition by the unmodified column value.
    Identity {},
    /// Partition by calendar year.
    Year {},
    /// Partition by month of year, from 1 through 12.
    Month {},
    /// Partition by day of month, from 1 through 31.
    Day {},
    /// Partition by hour of day, from 0 through 23.
    Hour {},
    /// Partition by a hash bucket.
    Bucket {
        /// Positive number of buckets, bounded by DuckLake's signed integer
        /// argument.
        count: u32,
    },
}

impl DuckLakePartitionTransform {
    /// Returns whether a source type supports this transform.
    pub fn supports_postgres_type(self, typ: &Type) -> bool {
        match self {
            Self::Identity {} | Self::Bucket { .. } => true,
            Self::Year {} | Self::Month {} | Self::Day {} => {
                matches!(*typ, Type::DATE | Type::TIMESTAMP | Type::TIMESTAMPTZ)
            }
            Self::Hour {} => matches!(*typ, Type::TIMESTAMP | Type::TIMESTAMPTZ),
        }
    }

    /// Returns the transform representation stored in DuckLake catalog
    /// metadata.
    pub fn catalog_name(self) -> String {
        match self {
            Self::Identity {} => "identity".to_owned(),
            Self::Year {} => "year".to_owned(),
            Self::Month {} => "month".to_owned(),
            Self::Day {} => "day".to_owned(),
            Self::Hour {} => "hour".to_owned(),
            Self::Bucket { count } => format!("bucket({count})"),
        }
    }
}

/// One ordered partition key referencing a source column by name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(deny_unknown_fields)]
pub struct DuckLakePartitionColumn {
    /// Source column name.
    column: String,
    /// Allowlisted transform applied to the column.
    transform: DuckLakePartitionTransform,
}

impl DuckLakePartitionColumn {
    /// Creates a partition key; configuration validation checks its arguments.
    pub fn new(column: String, transform: DuckLakePartitionTransform) -> Self {
        Self { column, transform }
    }

    /// Returns the source column name.
    pub fn column(&self) -> &str {
        &self.column
    }

    /// Returns the partition transform.
    pub fn transform(&self) -> DuckLakePartitionTransform {
        self.transform
    }
}

/// Ordered partition keys for one source table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(deny_unknown_fields)]
pub struct DuckLakeTablePartitionConfig {
    /// Source schema name.
    schema: String,
    /// Source table name.
    table: String,
    /// Ordered partition keys; an empty list explicitly clears partitioning.
    partition_by: Vec<DuckLakePartitionColumn>,
}

impl DuckLakeTablePartitionConfig {
    /// Creates a table partition specification.
    pub fn new(schema: String, table: String, partition_by: Vec<DuckLakePartitionColumn>) -> Self {
        Self { schema, table, partition_by }
    }

    /// Returns the source schema name.
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Returns the source table name.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Returns the ordered partition keys.
    pub fn partition_by(&self) -> &[DuckLakePartitionColumn] {
        &self.partition_by
    }
}

/// Stored partition declarations, boxed to keep containing destination config
/// enums small without changing their wire representation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(deny_unknown_fields)]
struct DuckLakePartitionDeclarations {
    /// Explicit specifications, including empty specifications to clear keys.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    tables: Vec<DuckLakeTablePartitionConfig>,
}

/// Per-table DuckLake partitioning; omitted tables are left unmanaged.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(transparent)]
pub struct DuckLakeTablePartitioningConfig {
    /// Immutable declarations allocated separately from destination options.
    declarations: Box<DuckLakePartitionDeclarations>,
}

impl DuckLakeTablePartitioningConfig {
    /// Creates per-table partitioning configuration.
    pub fn new(tables: Vec<DuckLakeTablePartitionConfig>) -> Self {
        Self { declarations: Box::new(DuckLakePartitionDeclarations { tables }) }
    }

    /// Returns the configured table specifications.
    pub fn tables(&self) -> &[DuckLakeTablePartitionConfig] {
        &self.declarations.tables
    }

    /// Returns whether no tables have managed partition specifications.
    pub fn is_empty(&self) -> bool {
        self.declarations.tables.is_empty()
    }
}

impl Validate for DuckLakeTablePartitioningConfig {
    fn validate(&self) -> Result<(), ValidationError> {
        let mut tables = HashSet::with_capacity(self.declarations.tables.len());
        for (index, table) in self.declarations.tables.iter().enumerate() {
            let field = format!("table_partitioning.tables[{index}]");
            if table.schema.is_empty() || table.table.is_empty() {
                return Err(ValidationError::InvalidFieldValue {
                    field,
                    constraint: "Schema and table names must not be empty".to_owned(),
                });
            }
            if !tables.insert((&table.schema, &table.table)) {
                return Err(ValidationError::InvalidFieldValue {
                    field,
                    constraint: "Table partition specifications must be unique".to_owned(),
                });
            }
            let mut keys = HashSet::with_capacity(table.partition_by.len());
            for key in &table.partition_by {
                if key.column.is_empty() || !keys.insert(key) {
                    return Err(ValidationError::InvalidFieldValue {
                        field,
                        constraint: "Partition keys must have nonempty columns and be unique"
                            .to_owned(),
                    });
                }
                if let DuckLakePartitionTransform::Bucket { count } = key.transform
                    && (count == 0 || i32::try_from(count).is_err())
                {
                    return Err(ValidationError::InvalidFieldValue {
                        field,
                        constraint: "Bucket count must be between 1 and 2147483647".to_owned(),
                    });
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tokio_postgres::types::Type;

    use crate::shared::{DuckLakePartitionTransform, DuckLakeTablePartitioningConfig, Validate};

    /// Rejects expressions, misspelled arguments, duplicate keys and invalid
    /// buckets.
    #[test]
    fn partition_configuration_rejects_invalid_keys() {
        for transform in [
            json!({"kind": "sql", "expression": "year(created_at)"}),
            json!({"kind": "year", "expression": "ignored"}),
        ] {
            assert!(serde_json::from_value::<DuckLakePartitionTransform>(transform).is_err());
        }
        for count in [0, u32::MAX] {
            let config: DuckLakeTablePartitioningConfig = serde_json::from_value(json!({
                "tables": [{"schema": "public", "table": "events", "partition_by": [
                    {"column": "id", "transform": {"kind": "bucket", "count": count}}
                ]}]
            }))
            .unwrap();
            assert!(config.validate().is_err());
        }
        let config: DuckLakeTablePartitioningConfig = serde_json::from_value(json!({
            "tables": [{"schema": "public", "table": "events", "partition_by": [
                {"column": "id", "transform": {"kind": "identity"}},
                {"column": "id", "transform": {"kind": "identity"}}
            ]}]
        }))
        .unwrap();
        assert!(config.validate().is_err());
    }

    /// Calendar transforms use date components, with hours restricted to
    /// timestamps.
    #[test]
    fn partition_transforms_check_temporal_types() {
        for transform in [
            DuckLakePartitionTransform::Year {},
            DuckLakePartitionTransform::Month {},
            DuckLakePartitionTransform::Day {},
            DuckLakePartitionTransform::Hour {},
        ] {
            assert!(transform.supports_postgres_type(&Type::TIMESTAMP));
            assert!(transform.supports_postgres_type(&Type::TIMESTAMPTZ));
            assert!(!transform.supports_postgres_type(&Type::TEXT));
            assert!(!transform.supports_postgres_type(&Type::INT8));
        }
        assert!(DuckLakePartitionTransform::Day {}.supports_postgres_type(&Type::DATE));
        assert!(!DuckLakePartitionTransform::Hour {}.supports_postgres_type(&Type::DATE));
    }
}
