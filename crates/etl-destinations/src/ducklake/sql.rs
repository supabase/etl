use crate::ducklake::LAKE_CATALOG;

/// Quotes a DuckDB SQL identifier for DuckLake SQL.
///
/// DuckDB quoted identifiers are enclosed in double quotes, and embedded double
/// quotes are escaped by repeating the quote character.
pub(super) fn quote_identifier(identifier: &str) -> String {
    crate::sql::quote_double_identifier(identifier)
}

/// Quotes a DuckLake table name and qualifies it with the DuckLake catalog.
///
/// Each identifier part is quoted separately so table names containing dots are
/// treated as a single table identifier, not as extra path components.
pub(super) fn qualified_lake_table_name(table_name: &str) -> String {
    format!("{}.{}", quote_identifier(LAKE_CATALOG), quote_identifier(table_name))
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
        assert_eq!(
            qualified_lake_table_name(r#"users"; DROP TABLE other; --"#),
            r#""lake"."users""; DROP TABLE other; --""#
        );
    }
}
