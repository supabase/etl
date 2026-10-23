//! Compatibility checks shared by DuckLake writers and maintenance.

use pg_escape::quote_literal;

/// Builds a setup check that rejects an engine incompatible with its signed
/// extensions before any extension is installed or loaded.
pub fn duckdb_version_check_sql(extension_version: &str) -> String {
    let expected = format!("v{extension_version}");
    let message = format!("DuckDB version mismatch: expected {expected}, got ");
    format!(
        "select case when library_version = {} then true else error({} || library_version || '. \
         Rebuild with the DuckDB version definitions in .cargo/config.toml; check CXXFLAGS and \
         its target-specific overrides. See DEVELOPMENT.md.') end from pragma_version();",
        quote_literal(&expected),
        quote_literal(&message),
    )
}

#[cfg(test)]
mod tests {
    use crate::ducklake::compatibility::duckdb_version_check_sql;

    /// The release build passes without loading any extensions.
    #[test]
    fn version_check_accepts_linked_release() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(&duckdb_version_check_sql("2.0.0")).unwrap();
    }

    /// Signed extension compatibility requires the exact release label.
    #[test]
    fn version_check_rejects_other_releases_and_development_labels() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        for version in ["1.5.6", "2.0.1", "2.1.0", "2.0.0-dev"] {
            let error = conn.execute_batch(&duckdb_version_check_sql(version)).unwrap_err();
            let message = error.to_string();
            assert!(message.contains(&format!("expected v{version}, got v2.0.0")));
            assert!(message.contains("Rebuild with the DuckDB version definitions"));
            assert!(message.contains("CXXFLAGS"));
        }
    }
}
