use pg_escape::quote_identifier;
use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use tokio_postgres::types::{FromSql, ToSql, Type};

/// An object identifier in Postgres.
type Oid = u32;

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
/// and the schemas of all its columns.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TableSchema {
    /// The Postgres OID of the table
    pub id: TableId,
    /// The fully qualified name of the table
    pub name: TableName,
    /// The schemas of all columns in the table
    pub column_schemas: Vec<ColumnSchema>,
}

impl TableSchema {
    pub fn new(id: TableId, name: TableName, column_schemas: Vec<ColumnSchema>) -> Self {
        Self {
            id,
            name,
            column_schemas,
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
    /// Creates a new [`ReplicationMask`] from a table schema and column names.
    ///
    /// The mask is constructed by checking which column names from the schema are present
    /// in the provided set of replicated column names.
    pub fn build(schema: &TableSchema, replicated_column_names: &HashSet<String>) -> Self {
        let mask = schema
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
    pub fn from_mask(schema: Arc<TableSchema>, mask: ReplicationMask) -> Self {
        debug_assert_eq!(
            schema.column_schemas.len(),
            mask.len(),
            "mask length must match column count"
        );
        Self {
            table_schema: schema,
            replication_mask: mask,
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
        debug_assert!(self.replication_mask.len() == self.table_schema.column_schemas.len());

        self.table_schema
            .column_schemas
            .iter()
            .zip(self.replication_mask.as_slice().iter())
            .filter_map(|(cs, &m)| if m == 1 { Some(cs) } else { None })
    }
}
