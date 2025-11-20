use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;

/// An object identifier in MySQL.
type Oid = u32;

/// A fully qualified MySQL table name consisting of a schema (database) and table name.
///
/// This type represents a table identifier in MySQL, which requires both a schema name
/// (database name) and a table name. It provides methods for formatting the name in different contexts.
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct TableName {
    /// The schema (database) name containing the table.
    pub schema: String,
    /// The name of the table within the schema.
    pub name: String,
}

impl TableName {
    pub fn new(schema: String, name: String) -> TableName {
        Self { schema, name }
    }

    /// Returns the table name as a properly quoted MySQL identifier.
    ///
    /// This method ensures the schema and table names are properly escaped according to
    /// MySQL identifier quoting rules using backticks.
    pub fn as_quoted_identifier(&self) -> String {
        format!("`{}`.`{}`", self.schema, self.name)
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{0}.{1}", self.schema, self.name))
    }
}

/// A type alias for MySQL type modifiers.
///
/// Type modifiers in MySQL are used to specify additional type-specific attributes,
/// such as length for varchar or precision for numeric types.
type TypeModifier = i32;

/// Represents the schema of a single column in a MySQL table.
///
/// This type contains all metadata about a column including its name, data type,
/// type modifier, nullability, and whether it's part of the primary key.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ColumnSchema {
    /// The name of the column.
    pub name: String,
    /// The MySQL data type of the column as a string.
    pub typ: String,
    /// Type-specific modifier value (e.g., length for varchar).
    pub modifier: TypeModifier,
    /// Whether the column can contain NULL values.
    pub nullable: bool,
    /// Whether the column is part of the table's primary key.
    pub primary: bool,
}

impl ColumnSchema {
    pub fn new(
        name: String,
        typ: String,
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
    /// unfortunately MySQL binlog doesn't always propagate nullable information of a column.
    pub fn partial_eq(&self, other: &ColumnSchema) -> bool {
        self.name == other.name && self.typ == other.typ && self.modifier == other.modifier
    }
}

/// A type-safe wrapper for MySQL table identifiers.
///
/// MySQL tables are uniquely identified by their schema (database) and table name combination.
/// This newtype provides type safety by preventing accidental use of raw values.
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

/// Represents the complete schema of a MySQL table.
///
/// This type contains all metadata about a table including its name, ID,
/// and the schemas of all its columns.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TableSchema {
    /// The identifier of the table.
    pub id: TableId,
    /// The fully qualified name of the table.
    pub name: TableName,
    /// The schemas of all columns in the table.
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

    /// Returns the number of columns in the table.
    pub fn num_columns(&self) -> usize {
        self.column_schemas.len()
    }

    /// Compares two [`TableSchema`] instances excluding nullable fields.
    ///
    /// This method checks if the table IDs match and all column schemas match
    /// using partial equality (excluding nullable fields).
    pub fn partial_eq(&self, other: &TableSchema) -> bool {
        if self.id != other.id {
            return false;
        }

        if self.column_schemas.len() != other.column_schemas.len() {
            return false;
        }

        self.column_schemas
            .iter()
            .zip(other.column_schemas.iter())
            .all(|(a, b)| a.partial_eq(b))
    }
}

impl PartialOrd for TableSchema {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.id.cmp(&other.id))
    }
}

impl Ord for TableSchema {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}
