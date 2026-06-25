use std::{
    fmt,
    hash::{Hash, Hasher},
    str::FromStr,
};

use pg_escape::quote_identifier;
use thiserror::Error;
use tokio_postgres::types::{FromSql, PgLsn, ToSql, Type};

/// Errors that can occur during schema operations.
#[derive(Debug, Error)]
pub enum SchemaError {
    /// Columns were received during replication that do not exist in the stored
    /// table schema.
    #[error("Received columns during replication that are not in the stored table schema: {0:?}")]
    UnknownReplicatedColumns(Vec<String>),

    /// A snapshot ID string could not be converted to the [`SnapshotId`] type.
    #[error("Invalid snapshot id '{0}'")]
    InvalidSnapshotId(String),
}

/// An object identifier in Postgres.
type Oid = u32;

/// Snapshot identifier for schema versioning.
///
/// Wraps a [`PgLsn`] to represent the start_lsn of the DDL message that created
/// a schema version. A value of 0/0 indicates the initial schema before any DDL
/// changes. Stored as `pg_lsn` in the database.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct SnapshotId(PgLsn);

impl SnapshotId {
    /// Returns the initial snapshot ID (0/0) for the first schema version.
    pub fn initial() -> Self {
        Self(PgLsn::from(0))
    }

    /// Returns the maximum possible snapshot ID.
    pub fn max() -> Self {
        Self(PgLsn::from(u64::MAX))
    }

    /// Creates a new [`SnapshotId`] from a [`PgLsn`].
    pub fn new(lsn: PgLsn) -> Self {
        Self(lsn)
    }

    /// Returns the inner [`PgLsn`] value.
    pub fn into_inner(self) -> PgLsn {
        self.0
    }

    /// Returns the underlying `u64` representation.
    pub fn as_u64(self) -> u64 {
        self.0.into()
    }

    /// Converts to a `pg_lsn` string.
    pub fn to_pg_lsn_string(self) -> String {
        self.0.to_string()
    }

    /// Parses a `pg_lsn` string.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::InvalidSnapshotId`] if the string is not a valid
    /// `pg_lsn` format.
    pub fn from_pg_lsn_string(s: &str) -> Result<Self, SchemaError> {
        s.parse::<PgLsn>().map(Self).map_err(|_| SchemaError::InvalidSnapshotId(s.to_owned()))
    }
}

impl Hash for SnapshotId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let value: u64 = self.0.into();
        value.hash(state);
    }
}

impl From<PgLsn> for SnapshotId {
    fn from(lsn: PgLsn) -> Self {
        Self(lsn)
    }
}

impl From<SnapshotId> for PgLsn {
    fn from(snapshot_id: SnapshotId) -> Self {
        snapshot_id.0
    }
}

impl From<u64> for SnapshotId {
    fn from(value: u64) -> Self {
        Self(PgLsn::from(value))
    }
}

impl From<SnapshotId> for u64 {
    fn from(snapshot_id: SnapshotId) -> Self {
        snapshot_id.0.into()
    }
}

impl fmt::Display for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A fully qualified Postgres table name consisting of a schema and table name.
///
/// This type represents a table identifier in Postgres, which requires both a
/// schema name and a table name. It provides methods for formatting the name in
/// different contexts.
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct TableName {
    /// The schema name containing the table
    pub schema: String,
    /// The name of the table within the schema
    pub name: String,
}

impl TableName {
    pub fn new(schema: String, name: String) -> TableName {
        Self { schema, name }
    }

    /// Returns the table name as a properly quoted Postgres identifier.
    ///
    /// This method ensures the schema and table names are properly escaped
    /// according to Postgres identifier quoting rules.
    pub fn as_quoted_identifier(&self) -> String {
        let quoted_schema = quote_identifier(&self.schema);
        let quoted_name = quote_identifier(&self.name);

        format!("{quoted_schema}.{quoted_name}")
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{0}.{1}", self.schema, self.name))
    }
}

/// A type alias for Postgres type modifiers.
///
/// Type modifiers in Postgres are used to specify additional type-specific
/// attributes, such as length for varchar or precision for numeric types.
type TypeModifier = i32;

/// Represents the schema of a single column in a Postgres table.
///
/// This type contains all metadata about a column including its name, data
/// type, type modifier, ordinal position, primary key information, nullability,
/// and default expression.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ColumnSchema {
    /// The name of the column.
    pub name: String,
    /// The Postgres data type of the column.
    pub typ: Type,
    /// Type-specific modifier value (e.g., length for varchar).
    pub modifier: TypeModifier,
    /// The 1-based ordinal position of the column in the table.
    pub ordinal_position: i32,
    /// The 1-based ordinal position of this column in the primary key, or None
    /// if not a primary key.
    pub primary_key_ordinal_position: Option<i32>,
    /// Whether the column can contain NULL values.
    pub nullable: bool,
    /// The source default expression for this column, if one is defined.
    pub default_expression: Option<String>,
}

impl ColumnSchema {
    /// Creates a new [`ColumnSchema`] without optional metadata.
    pub fn new(
        name: String,
        typ: Type,
        modifier: TypeModifier,
        ordinal_position: i32,
        nullable: bool,
    ) -> ColumnSchema {
        Self {
            name,
            typ,
            modifier,
            ordinal_position,
            primary_key_ordinal_position: None,
            nullable,
            default_expression: None,
        }
    }

    /// Creates a new [`ColumnSchemaBuilder`].
    pub fn builder(name: String, typ: Type, ordinal_position: i32) -> ColumnSchemaBuilder {
        ColumnSchemaBuilder::new(name, typ, ordinal_position)
    }

    /// Sets the primary key ordinal position for this column.
    pub fn with_primary_key(mut self, ordinal_position: i32) -> Self {
        self.primary_key_ordinal_position = Some(ordinal_position);
        self
    }

    /// Sets the optional primary key ordinal position for this column.
    pub fn with_primary_key_ordinal_position(mut self, ordinal_position: Option<i32>) -> Self {
        self.primary_key_ordinal_position = ordinal_position;
        self
    }

    /// Sets the source default expression for this column.
    pub fn with_default_expression(mut self, default_expression: String) -> Self {
        self.default_expression = Some(default_expression);
        self
    }

    /// Sets the optional source default expression for this column.
    pub fn with_default_expression_option(mut self, default_expression: Option<String>) -> Self {
        self.default_expression = default_expression;
        self
    }

    /// Returns whether this column is part of the table's primary key.
    pub fn primary_key(&self) -> bool {
        self.primary_key_ordinal_position.is_some()
    }
}

/// Builds a [`ColumnSchema`] with named optional settings.
#[derive(Debug, Clone)]
pub struct ColumnSchemaBuilder {
    name: String,
    typ: Type,
    modifier: TypeModifier,
    ordinal_position: i32,
    primary_key_ordinal_position: Option<i32>,
    nullable: bool,
    default_expression: Option<String>,
}

impl ColumnSchemaBuilder {
    /// Creates a new [`ColumnSchemaBuilder`] for a column.
    fn new(name: String, typ: Type, ordinal_position: i32) -> Self {
        Self {
            name,
            typ,
            modifier: -1,
            ordinal_position,
            primary_key_ordinal_position: None,
            nullable: true,
            default_expression: None,
        }
    }

    /// Sets the column type modifier.
    pub fn type_modifier(mut self, modifier: TypeModifier) -> Self {
        self.modifier = modifier;
        self
    }

    /// Sets the primary key ordinal position for this column.
    pub fn primary_key(mut self, ordinal_position: i32) -> Self {
        self.primary_key_ordinal_position = Some(ordinal_position);
        self
    }

    /// Sets the optional primary key ordinal position for this column.
    pub fn primary_key_ordinal_position(mut self, ordinal_position: Option<i32>) -> Self {
        self.primary_key_ordinal_position = ordinal_position;
        self
    }

    /// Marks the column as nullable or not nullable.
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Marks the column as not nullable.
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    /// Sets the source default expression for this column.
    pub fn default_expression(mut self, default_expression: String) -> Self {
        self.default_expression = Some(default_expression);
        self
    }

    /// Sets the optional source default expression for this column.
    pub fn default_expression_option(mut self, default_expression: Option<String>) -> Self {
        self.default_expression = default_expression;
        self
    }

    /// Builds the [`ColumnSchema`].
    pub fn build(self) -> ColumnSchema {
        ColumnSchema {
            name: self.name,
            typ: self.typ,
            modifier: self.modifier,
            ordinal_position: self.ordinal_position,
            primary_key_ordinal_position: self.primary_key_ordinal_position,
            nullable: self.nullable,
            default_expression: self.default_expression,
        }
    }
}

/// A type-safe wrapper for Postgres table OIDs.
///
/// Table OIDs are unique identifiers assigned to tables in Postgres.
///
/// This newtype provides type safety by preventing accidental use of raw
/// [`Oid`] values where a table identifier is expected.
#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct TableId(pub Oid);

impl TableId {
    /// Creates a new [`TableId`] from an [`Oid`].
    pub fn new(oid: Oid) -> Self {
        Self(oid)
    }

    /// Returns the underlying [`Oid`] value.
    pub fn into_inner(self) -> Oid {
        self.0
    }
}

impl From<Oid> for TableId {
    fn from(oid: Oid) -> Self {
        Self(oid)
    }
}

impl From<TableId> for Oid {
    fn from(table_id: TableId) -> Self {
        table_id.0
    }
}

impl fmt::Display for TableId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for TableId {
    type Err = <Oid as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<Oid>().map(TableId::new)
    }
}

impl<'a> FromSql<'a> for TableId {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(TableId::new(Oid::from_sql(ty, raw)?))
    }

    fn accepts(ty: &Type) -> bool {
        <Oid as FromSql>::accepts(ty)
    }
}

impl ToSql for TableId {
    fn to_sql(
        &self,
        ty: &Type,
        w: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        self.0.to_sql(ty, w)
    }

    fn accepts(ty: &Type) -> bool
    where
        Self: Sized,
    {
        <Oid as ToSql>::accepts(ty)
    }

    tokio_postgres::types::to_sql_checked!();
}

/// Represents the complete schema of a Postgres table.
///
/// This type contains all metadata about a table including its name, OID,
/// the schemas of all its columns, and a snapshot identifier for versioning.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TableSchema {
    /// The Postgres OID of the table.
    pub id: TableId,
    /// The fully qualified name of the table.
    pub name: TableName,
    /// The schemas of all columns in the table.
    pub column_schemas: Vec<ColumnSchema>,
    /// The snapshot identifier for this schema version.
    ///
    /// Value 0 indicates the initial schema, other values are start_lsn
    /// positions of DDL changes.
    pub snapshot_id: SnapshotId,
}

impl TableSchema {
    /// Creates a new [`TableSchema`] with the initial snapshot ID (0/0).
    pub fn new(id: TableId, name: TableName, column_schemas: Vec<ColumnSchema>) -> Self {
        Self::with_snapshot_id(id, name, column_schemas, SnapshotId::initial())
    }

    /// Creates a new [`TableSchema`] with a specific snapshot ID.
    pub fn with_snapshot_id(
        id: TableId,
        name: TableName,
        column_schemas: Vec<ColumnSchema>,
        snapshot_id: SnapshotId,
    ) -> Self {
        Self { id, name, column_schemas, snapshot_id }
    }

    /// Adds a new column schema to this [`TableSchema`].
    pub fn add_column_schema(&mut self, column_schema: ColumnSchema) {
        self.column_schemas.push(column_schema);
    }

    /// Returns whether the table has any primary key columns.
    ///
    /// This method checks if any column in the table is marked as part of the
    /// primary key.
    pub fn has_primary_keys(&self) -> bool {
        self.column_schemas.iter().any(ColumnSchema::primary_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_schema_builder_sets_optional_fields() {
        let schema = ColumnSchema::builder("status".to_owned(), Type::TEXT, 3)
            .primary_key(1)
            .not_null()
            .default_expression("'new'::text".to_owned())
            .build();

        assert_eq!(schema.name, "status");
        assert_eq!(schema.typ, Type::TEXT);
        assert_eq!(schema.modifier, -1);
        assert_eq!(schema.ordinal_position, 3);
        assert_eq!(schema.primary_key_ordinal_position, Some(1));
        assert!(!schema.nullable);
        assert_eq!(schema.default_expression.as_deref(), Some("'new'::text"));
    }
}
