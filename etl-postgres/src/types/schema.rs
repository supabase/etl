use pg_escape::quote_identifier;
use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use tokio_postgres::types::{FromSql, ToSql, Type};

/// Errors that can occur during schema operations.
#[derive(Debug, Error)]
pub enum SchemaError {
    /// Columns were received during replication that do not exist in the stored table schema.
    #[error("received columns during replication that are not in the stored table schema: {0:?}")]
    UnknownReplicatedColumns(Vec<String>),
}

/// An object identifier in Postgres.
type Oid = u32;

/// Snapshot identifier for schema versioning.
///
/// The value represents the start_lsn of the DDL message that created this schema version.
/// A value of 0 indicates the initial schema before any DDL changes.
pub type SnapshotId = i64;

/// The initial snapshot ID used for the first schema version.
pub const INITIAL_SNAPSHOT_ID: SnapshotId = 0;

/// A fully qualified Postgres table name consisting of a schema and table name.
///
/// This type represents a table identifier in Postgres, which requires both a schema name
/// and a table name. It provides methods for formatting the name in different contexts.
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
    /// This method ensures the schema and table names are properly escaped according to
    /// Postgres identifier quoting rules.
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
/// Type modifiers in Postgres are used to specify additional type-specific attributes,
/// such as length for varchar or precision for numeric types.
type TypeModifier = i32;

/// Represents the schema of a single column in a Postgres table.
///
/// This type contains all metadata about a column including its name, data type,
/// type modifier, ordinal position, primary key information, and nullability.
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
    /// The 1-based ordinal position of this column in the primary key, or None if not a primary key.
    pub primary_key_ordinal_position: Option<i32>,
    /// Whether the column can contain NULL values.
    pub nullable: bool,
}

impl ColumnSchema {
    /// Creates a new [`ColumnSchema`] with all fields specified.
    pub fn new(
        name: String,
        typ: Type,
        modifier: TypeModifier,
        ordinal_position: i32,
        primary_key_ordinal_position: Option<i32>,
        nullable: bool,
    ) -> ColumnSchema {
        Self {
            name,
            typ,
            modifier,
            ordinal_position,
            primary_key_ordinal_position,
            nullable,
        }
    }

    /// Returns whether this column is part of the table's primary key.
    pub fn primary_key(&self) -> bool {
        self.primary_key_ordinal_position.is_some()
    }
}

/// A type-safe wrapper for Postgres table OIDs.
///
/// Table OIDs are unique identifiers assigned to tables in Postgres.
///
/// This newtype provides type safety by preventing accidental use of raw [`Oid`] values
/// where a table identifier is expected.
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
    /// Value 0 indicates the initial schema, other values are start_lsn positions of DDL changes.
    pub snapshot_id: SnapshotId,
}

impl TableSchema {
    /// Creates a new [`TableSchema`] with the initial snapshot ID (0).
    pub fn new(id: TableId, name: TableName, column_schemas: Vec<ColumnSchema>) -> Self {
        Self::with_snapshot_id(id, name, column_schemas, INITIAL_SNAPSHOT_ID)
    }

    /// Creates a new [`TableSchema`] with a specific snapshot ID.
    pub fn with_snapshot_id(
        id: TableId,
        name: TableName,
        column_schemas: Vec<ColumnSchema>,
        snapshot_id: SnapshotId,
    ) -> Self {
        Self {
            id,
            name,
            column_schemas,
            snapshot_id,
        }
    }

    /// Adds a new column schema to this [`TableSchema`].
    pub fn add_column_schema(&mut self, column_schema: ColumnSchema) {
        self.column_schemas.push(column_schema);
    }

    /// Returns whether the table has any primary key columns.
    ///
    /// This method checks if any column in the table is marked as part of the primary key.
    pub fn has_primary_keys(&self) -> bool {
        self.column_schemas.iter().any(|cs| cs.primary_key())
    }
}

/// A bitmask indicating which columns are being replicated.
///
/// Each element is either 0 (not replicated) or 1 (replicated), with indices
/// corresponding to the columns in the table schema. Wrapped in [`Arc`] for
/// efficient sharing across multiple events.
#[derive(Debug, Clone)]
pub struct ReplicationMask(Arc<Vec<u8>>);

impl ReplicationMask {
    /// Tries to create a new [`ReplicationMask`] from a table schema and column names.
    ///
    /// The mask is constructed by checking which column names from the schema are present
    /// in the provided set of replicated column names.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::UnknownReplicatedColumns`] if any column in
    /// `replicated_column_names` does not exist in the table schema.
    ///
    /// The column validation occurs because we have to make sure that the stored table schema is always
    /// up to date, if not, it's a critical problem.
    pub fn try_build(
        table_schema: &TableSchema,
        replicated_column_names: &HashSet<String>,
    ) -> Result<Self, SchemaError> {
        let schema_column_names: HashSet<&str> = table_schema
            .column_schemas
            .iter()
            .map(|column_schema| column_schema.name.as_str())
            .collect();

        let unknown_columns: Vec<String> = replicated_column_names
            .iter()
            .filter(|name| !schema_column_names.contains(name.as_str()))
            .cloned()
            .collect();

        if !unknown_columns.is_empty() {
            return Err(SchemaError::UnknownReplicatedColumns(unknown_columns));
        }

        Ok(Self::build(table_schema, replicated_column_names))
    }

    /// Creates a new [`ReplicationMask`] from a table schema and column names, falling back
    /// to an all-replicated mask if validation fails.
    ///
    /// This method attempts to validate that all replicated column names exist in the schema.
    /// If validation succeeds, it builds a mask based on matching columns. If validation fails
    /// (unknown columns are present), it returns a mask with all columns marked as replicated.
    ///
    /// This fallback behavior handles the case where Postgres sends a `Relation` message on
    /// reconnection with the current schema, but the stored schema is from an earlier point
    /// before DDL changes. Rather than failing, we enable all columns and let the system
    /// converge when the actual DDL message is replayed.
    pub fn build_or_all(
        table_schema: &TableSchema,
        replicated_column_names: &HashSet<String>,
    ) -> Self {
        match Self::try_build(table_schema, replicated_column_names) {
            Ok(mask) => mask,
            Err(_) => Self::all(table_schema),
        }
    }

    /// Creates a new [`ReplicationMask`] from a table schema and column names.
    pub fn build(table_schema: &TableSchema, replicated_column_names: &HashSet<String>) -> Self {
        let mask = table_schema
            .column_schemas
            .iter()
            .map(|cs| {
                if replicated_column_names.contains(&cs.name) {
                    1
                } else {
                    0
                }
            })
            .collect();

        Self(Arc::new(mask))
    }

    /// Creates a [`ReplicationMask`] with all columns marked as replicated.
    pub fn all(table_schema: &TableSchema) -> Self {
        let mask = vec![1; table_schema.column_schemas.len()];
        Self(Arc::new(mask))
    }

    /// Returns the underlying mask as a slice.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Returns the number of columns in the mask.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the mask is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A wrapper around [`TableSchema`] that tracks which columns are being replicated.
///
/// This struct holds a reference to the underlying table schema and a [`ReplicationMask`]
/// indicating which columns are included in the replication.
#[derive(Debug, Clone)]
pub struct ReplicatedTableSchema {
    /// The underlying table schema.
    table_schema: Arc<TableSchema>,
    /// A bitmask where 1 indicates the column at that index is replicated.
    replication_mask: ReplicationMask,
}

impl ReplicatedTableSchema {
    /// Creates a [`ReplicatedTableSchema`] from a schema and a pre-computed mask.
    pub fn from_mask(table_schema: Arc<TableSchema>, replication_mask: ReplicationMask) -> Self {
        debug_assert_eq!(
            table_schema.column_schemas.len(),
            replication_mask.len(),
            "mask length must match column count"
        );

        Self {
            table_schema,
            replication_mask,
        }
    }

    /// Returns the table ID.
    pub fn id(&self) -> TableId {
        self.table_schema.id
    }

    /// Returns the table name.
    pub fn name(&self) -> &TableName {
        &self.table_schema.name
    }

    /// Returns the underlying table schema.
    pub fn get_inner(&self) -> &TableSchema {
        &self.table_schema
    }

    /// Returns the replication mask.
    pub fn replication_mask(&self) -> &ReplicationMask {
        &self.replication_mask
    }

    /// Returns an iterator over only the column schemas that are being replicated.
    ///
    /// This filters the columns based on the mask, returning only those where the
    /// corresponding mask value is 1.
    pub fn column_schemas(&self) -> impl Iterator<Item = &ColumnSchema> + Clone + '_ {
        // Assuming that the schema is created via the constructor, we can safely assume that the
        // column schemas and replication mask are of the same length.
        debug_assert!(
            self.replication_mask.len() == self.table_schema.column_schemas.len(),
            "the replication mask columns have a different len from the table schema columns, they should be the same"
        );

        self.table_schema
            .column_schemas
            .iter()
            .zip(self.replication_mask.as_slice().iter())
            .filter_map(|(cs, &m)| if m == 1 { Some(cs) } else { None })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_table_schema() -> TableSchema {
        TableSchema::new(
            TableId::new(123),
            TableName::new("public".to_string(), "test_table".to_string()),
            vec![
                ColumnSchema::new("id".to_string(), Type::INT4, -1, 1, Some(1), false),
                ColumnSchema::new("name".to_string(), Type::TEXT, -1, 2, None, true),
                ColumnSchema::new("age".to_string(), Type::INT4, -1, 3, None, true),
            ],
        )
    }

    #[test]
    fn test_replication_mask_try_build_all_columns_replicated() {
        let schema = create_test_table_schema();
        let replicated_columns: HashSet<String> = ["id", "name", "age"]
            .into_iter()
            .map(String::from)
            .collect();

        let mask = ReplicationMask::try_build(&schema, &replicated_columns).unwrap();

        assert_eq!(mask.as_slice(), &[1, 1, 1]);
    }

    #[test]
    fn test_replication_mask_try_build_partial_columns_replicated() {
        let schema = create_test_table_schema();
        let replicated_columns: HashSet<String> =
            ["id", "age"].into_iter().map(String::from).collect();

        let mask = ReplicationMask::try_build(&schema, &replicated_columns).unwrap();

        assert_eq!(mask.as_slice(), &[1, 0, 1]);
    }

    #[test]
    fn test_replication_mask_try_build_no_columns_replicated() {
        let schema = create_test_table_schema();
        let replicated_columns: HashSet<String> = HashSet::new();

        let mask = ReplicationMask::try_build(&schema, &replicated_columns).unwrap();

        assert_eq!(mask.as_slice(), &[0, 0, 0]);
    }

    #[test]
    fn test_replication_mask_try_build_unknown_column_error() {
        let schema = create_test_table_schema();
        let replicated_columns: HashSet<String> = ["id", "unknown_column"]
            .into_iter()
            .map(String::from)
            .collect();

        let result = ReplicationMask::try_build(&schema, &replicated_columns);

        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            SchemaError::UnknownReplicatedColumns(columns) => {
                assert_eq!(columns, vec!["unknown_column".to_string()]);
            }
        }
    }

    #[test]
    fn test_replication_mask_try_build_multiple_unknown_columns_error() {
        let schema = create_test_table_schema();
        let replicated_columns: HashSet<String> =
            ["id", "foo", "bar"].into_iter().map(String::from).collect();

        let result = ReplicationMask::try_build(&schema, &replicated_columns);

        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            SchemaError::UnknownReplicatedColumns(mut columns) => {
                columns.sort();
                assert_eq!(columns, vec!["bar".to_string(), "foo".to_string()]);
            }
        }
    }

    #[test]
    fn test_replication_mask_build_or_all_success() {
        let schema = create_test_table_schema();
        let replicated_columns: HashSet<String> =
            ["id", "age"].into_iter().map(String::from).collect();

        let mask = ReplicationMask::build_or_all(&schema, &replicated_columns);

        assert_eq!(mask.as_slice(), &[1, 0, 1]);
    }

    #[test]
    fn test_replication_mask_build_or_all_falls_back_to_all() {
        let schema = create_test_table_schema();
        let replicated_columns: HashSet<String> = ["id", "unknown_column"]
            .into_iter()
            .map(String::from)
            .collect();

        let mask = ReplicationMask::build_or_all(&schema, &replicated_columns);

        // Falls back to all columns being replicated.
        assert_eq!(mask.as_slice(), &[1, 1, 1]);
    }

    #[test]
    fn test_replication_mask_all() {
        let schema = create_test_table_schema();
        let mask = ReplicationMask::all(&schema);

        assert_eq!(mask.as_slice(), &[1, 1, 1]);
    }
}
