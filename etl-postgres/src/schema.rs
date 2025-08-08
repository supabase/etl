use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;

use pg_escape::quote_identifier;
use tokio_postgres::types::{FromSql, ToSql, Type};

/// PostgreSQL object identifier.
pub type Oid = u32;

/// Fully qualified PostgreSQL table name with schema and table components.
///
/// Represents a complete table identifier that includes both schema and table name,
/// providing methods for proper SQL identifier quoting and formatting.
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct TableName {
    /// The schema name containing the table
    pub schema: String,
    /// The name of the table within the schema
    pub name: String,
}

impl TableName {
    /// Creates a new [`TableName`] with the given schema and table name.
    pub fn new(schema: String, name: String) -> TableName {
        Self { schema, name }
    }

    /// Returns the table name as a properly quoted PostgreSQL identifier.
    ///
    /// Escapes both schema and table names according to PostgreSQL identifier
    /// quoting rules to handle special characters and reserved keywords safely.
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

/// PostgreSQL type modifier for specifying type-specific attributes.
///
/// Used to store additional type information such as varchar length or numeric precision.
type TypeModifier = i32;

/// Schema metadata for a single PostgreSQL table column.
///
/// Contains complete column information including name, data type, type modifier,
/// nullability constraint, and primary key membership.
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
    /// Creates a new [`ColumnSchema`] with the specified metadata.
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

    /// Compares two [`ColumnSchema`] instances excluding the nullable field.
    ///
    /// Returns `true` if all fields except `nullable` are equal. Used for comparing
    /// schemas from initial table sync against CDC relation messages, which don't
    /// include nullability information.
    fn partial_eq(&self, other: &ColumnSchema) -> bool {
        self.name == other.name
            && self.typ == other.typ
            && self.modifier == other.modifier
            && self.primary == other.primary
    }
}

/// Type-safe wrapper for PostgreSQL table OIDs.
///
/// Provides type safety for table identifiers by wrapping raw [`Oid`] values
/// and preventing accidental misuse in function parameters.
#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct TableId(pub Oid);

impl TableId {
    /// Creates a new [`TableId`] from the given [`Oid`].
    pub fn new(oid: Oid) -> Self {
        Self(oid)
    }

    /// Returns the wrapped [`Oid`] value.
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

/// Complete schema metadata for a PostgreSQL table.
///
/// Contains table identification (OID and name) along with the schemas
/// of all columns in the table.
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
    /// Creates a new [`TableSchema`] with the given components.
    pub fn new(id: TableId, name: TableName, column_schemas: Vec<ColumnSchema>) -> Self {
        Self {
            id,
            name,
            column_schemas,
        }
    }

    /// Adds a column schema to this table schema.
    pub fn add_column_schema(&mut self, column_schema: ColumnSchema) {
        self.column_schemas.push(column_schema);
    }

    /// Returns whether the table has primary key columns.
    ///
    /// Checks if any column in the table is marked as part of the primary key.
    pub fn has_primary_keys(&self) -> bool {
        self.column_schemas.iter().any(|cs| cs.primary)
    }

    /// Compares two [`TableSchema`] instances excluding column nullable fields.
    ///
    /// Returns `true` if all fields match except for column nullability information.
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
