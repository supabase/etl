//! Catalog definitions for ETL-owned source objects.

/// The schema reserved for ETL source helpers and state tables.
pub const ETL_SCHEMA_NAME: &str = "etl";

/// A table owned and managed by ETL in the source database.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct EtlTable {
    schema: &'static str,
    name: &'static str,
}

impl EtlTable {
    /// Creates a new ETL-owned table definition.
    pub const fn new(schema: &'static str, name: &'static str) -> Self {
        Self { schema, name }
    }

    /// Returns the table schema name.
    pub const fn schema(&self) -> &'static str {
        self.schema
    }

    /// Returns the unqualified table name.
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Returns the fully-qualified table name.
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }
}

/// The table storing per-table replication state history.
pub const REPLICATION_STATE_TABLE: EtlTable = EtlTable::new(ETL_SCHEMA_NAME, "replication_state");

/// The table storing destination table metadata.
pub const DESTINATION_TABLES_METADATA_TABLE: EtlTable =
    EtlTable::new(ETL_SCHEMA_NAME, "destination_tables_metadata");

/// The table storing versioned source table schemas.
pub const TABLE_SCHEMAS_TABLE: EtlTable = EtlTable::new(ETL_SCHEMA_NAME, "table_schemas");

/// The table storing columns for versioned source table schemas.
pub const TABLE_COLUMNS_TABLE: EtlTable = EtlTable::new(ETL_SCHEMA_NAME, "table_columns");

/// The table storing durable per-worker replication progress.
pub const REPLICATION_PROGRESS_TABLE: EtlTable =
    EtlTable::new(ETL_SCHEMA_NAME, "replication_progress");

/// Current source tables owned and managed by ETL.
pub const ETL_TABLES: [EtlTable; 5] = [
    REPLICATION_STATE_TABLE,
    DESTINATION_TABLES_METADATA_TABLE,
    TABLE_SCHEMAS_TABLE,
    TABLE_COLUMNS_TABLE,
    REPLICATION_PROGRESS_TABLE,
];

/// Core state tables needed for API state inspection and cleanup.
pub const ETL_CORE_STATE_TABLES: [EtlTable; 4] = [
    REPLICATION_STATE_TABLE,
    DESTINATION_TABLES_METADATA_TABLE,
    TABLE_SCHEMAS_TABLE,
    TABLE_COLUMNS_TABLE,
];
