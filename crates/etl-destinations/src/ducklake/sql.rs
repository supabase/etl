use crate::{
    ducklake::{DuckLakeTableName, LAKE_CATALOG},
    sql::quote_double_identifier,
};

/// Quotes a DuckDB SQL identifier for DuckLake SQL.
///
/// DuckDB quoted identifiers are enclosed in double quotes, and embedded double
/// quotes are escaped by repeating the quote character.
pub(super) fn quote_identifier(identifier: &str) -> String {
    quote_double_identifier(identifier)
}

/// Quotes a DuckLake schema reference and qualifies it with the DuckLake catalog.
///
/// Each identifier part is quoted separately so names containing dots remain
/// one identifier, not extra path components.
pub(super) fn qualified_lake_schema_name(schema_name: &str) -> String {
    format!("{}.{}", quote_identifier(LAKE_CATALOG), quote_identifier(schema_name))
}

/// Quotes a DuckLake table reference and qualifies it with the DuckLake catalog.
///
/// Each identifier part is quoted separately so names containing dots are
/// treated as one identifier, not as extra path components.
pub(super) fn qualified_lake_table_name(table_name: &DuckLakeTableName) -> String {
    format!(
        "{}.{}",
        qualified_lake_schema_name(table_name.schema()),
        quote_identifier(table_name.table())
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_identifier_escapes_duckdb_identifiers() {
        assert_eq!(quote_identifier("plain"), r#""plain""#);
        assert_eq!(
            quote_identifier(r#"users"; DROP TABLE other; --"#),
            r#""users""; DROP TABLE other; --""#
        );
    }

    #[test]
    fn qualified_lake_table_name_quotes_each_identifier_part() {
        let table_name = DuckLakeTableName::new("schema.name", r#"users"; DROP TABLE other; --"#);
        assert_eq!(
            qualified_lake_table_name(&table_name),
            r#""lake"."schema.name"."users""; DROP TABLE other; --""#
        );
    }
}
