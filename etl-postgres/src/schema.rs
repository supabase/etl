use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;

use pg_escape::quote_identifier;
use tokio_postgres::types::{FromSql, ToSql, Type};

/// An object identifier in PostgreSQL.
pub type Oid = u32;

/// A fully qualified PostgreSQL table name consisting of a schema and table name.
///
/// This type represents a table identifier in PostgreSQL, which requires both a schema name
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

    /// Returns the table name as a properly quoted PostgreSQL identifier.
    ///
    /// This method ensures the schema and table names are properly escaped according to
    /// PostgreSQL identifier quoting rules.
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

/// A type alias for PostgreSQL type modifiers.
///
/// Type modifiers in PostgreSQL are used to specify additional type-specific attributes,
/// such as length for varchar or precision for numeric types.
type TypeModifier = i32;

/// Represents the schema of a single column in a PostgreSQL table.
///
/// This type contains all metadata about a column including its name, data type,
/// type modifier, nullability, and whether it's part of the primary key.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ColumnSchema {
    /// The name of the column
    pub name: String,
    /// The PostgreSQL data type of the column
    pub typ: Type,
    /// Type-specific modifier value (e.g., length for varchar)
    pub modifier: TypeModifier,
    /// Whether the column can contain NULL values
    pub nullable: bool,
    /// Whether the column is part of the table's primary key
    pub primary: bool,
}

impl ColumnSchema {
    pub fn new(
        name: String,
        typ: Type,
        modifier: TypeModifier,
        nullable: bool,
        primary: bool,
    ) -> ColumnSchema {
        Self {
            name,
            typ,
            modifier,
            nullable,
            primary,
        }
    }

    /// Compares two [`ColumnSchema`] instances, excluding the `nullable` field.
    ///
    /// Return `true` if all fields except `nullable` are equal, `false` otherwise.
    ///
    /// This method is used for comparing table schemas loaded via the initial table sync and the
    /// relation messages received via CDC. The reason for skipping the `nullable` field is that
    /// unfortunately Postgres doesn't seem to propagate nullable information of a column via
    /// relation messages. The reason for skipping the `primary` field is that if the replica
    /// identity of a table is set to full, the relation message sets all columns as primary
    /// key, irrespective of what the actual primary key in the table is.
    fn partial_eq(&self, other: &ColumnSchema) -> bool {
        self.name == other.name && self.typ == other.typ && self.modifier == other.modifier
    }
}

/// A type-safe wrapper for PostgreSQL table OIDs.
///
/// Table OIDs are unique identifiers assigned to tables in PostgreSQL.
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

/// Represents the complete schema of a PostgreSQL table.
///
/// This type contains all metadata about a table including its name, OID,
/// and the schemas of all its columns.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TableSchema {
    /// The PostgreSQL OID of the table
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
        self.column_schemas.iter().any(|cs| cs.primary)
    }

    /// Compares two [`TableSchema`] instances, excluding the [`ColumnSchema`]'s `nullable` field.
    ///
    /// Return `true` if all fields except `nullable` are equal, `false` otherwise.
    pub fn partial_eq(&self, other: &TableSchema) -> bool {
        self.id == other.id
            && self.name == other.name
            && self.column_schemas.len() == other.column_schemas.len()
            && self
                .column_schemas
                .iter()
                .zip(other.column_schemas.iter())
                .all(|(c1, c2)| c1.partial_eq(c2))
    }
}

impl PartialOrd for TableSchema {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TableSchema {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}
