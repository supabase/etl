//! Source database connection and catalog lookup helpers.

use std::{collections::HashMap, num::NonZeroI32};

use etl_config::shared::{IntoConnectOptions, PgConnectionConfig, PgConnectionOptions};
use sqlx::{PgExecutor, PgPool, Row, postgres::PgPoolOptions};
use thiserror::Error;

use crate::schema::{TableId, TableName};

/// Errors that can occur during table lookups.
#[derive(Debug, Error)]
pub enum TableLookupError {
    /// Querying the source database failed.
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    /// No table exists for the requested source table OID.
    #[error("Table with ID {0} not found")]
    TableNotFound(TableId),
}

/// Connects to the source database with a connection pool.
///
/// Creates a Postgres connection pool with the specified minimum and maximum
/// connection counts for accessing the source database.
///
/// The `options` parameter allows specifying connection-specific Postgres
/// settings. Pass `None` to use server defaults, or `Some(&options)` to apply
/// custom settings.
pub async fn connect_to_source_database(
    config: &PgConnectionConfig,
    min_connections: u32,
    max_connections: u32,
    options: Option<&PgConnectionOptions>,
) -> Result<PgPool, sqlx::Error> {
    let connect_options = config.with_db(options);

    let pool = PgPoolOptions::new()
        .min_connections(min_connections)
        .max_connections(max_connections)
        .connect_with(connect_options)
        .await?;

    Ok(pool)
}

/// Retrieves table names for multiple table ids in a single query.
///
/// Looks up the schema and table names for all given table OIDs using
/// Postgres's pg_class and pg_namespace system tables. Returns a HashMap
/// mapping each TableId to its corresponding TableName.
pub async fn get_table_names_from_table_ids<'c, E>(
    executor: E,
    table_ids: &[TableId],
) -> Result<HashMap<TableId, TableName>, TableLookupError>
where
    E: PgExecutor<'c>,
{
    if table_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let query = "select c.oid::int as oid, n.nspname as schema_name, c.relname as table_name
    from pg_class c
    join pg_namespace n on c.relnamespace = n.oid
    where c.oid = any($1::oid[])
    ";

    let oids: Vec<i32> = table_ids.iter().map(|id| id.into_inner() as i32).collect();
    let rows = sqlx::query(query).bind(&oids).fetch_all(executor).await?;

    let mut result = HashMap::with_capacity(rows.len());

    for row in rows {
        let oid: i32 = row.try_get("oid")?;
        let schema_name: String = row.try_get("schema_name")?;
        let table_name: String = row.try_get("table_name")?;

        result
            .insert(TableId::new(oid as u32), TableName { schema: schema_name, name: table_name });
    }

    Ok(result)
}

/// Extracts the PostgreSQL server version from a version string.
///
/// Two-component versions from PostgreSQL 10 onward use `major * 10000 +
/// minor`, so "15.5 (Homebrew)" becomes `150005`. Earlier versions use
/// `major * 10000 + minor * 100 + patch`, so "9.6.3" becomes `90603`.
///
/// For compatibility, inputs with a third component retain the earlier
/// encoding, even for modern major versions: "15.5.1" remains `150501`, as in
/// libpq. This is not an official modern PostgreSQL release format.
///
/// Only the first whitespace-delimited token and its first three dot-separated
/// components are considered. Missing or unparseable components default to
/// zero. Returns `None` for negative components, arithmetic overflow, or a zero
/// result.
pub fn extract_server_version(server_version_str: impl AsRef<str>) -> Option<NonZeroI32> {
    let version_part = server_version_str.as_ref().split_whitespace().next().unwrap_or("0.0");

    let mut version_components = version_part.split('.');

    let major = version_components.next().and_then(|v| v.parse::<i32>().ok()).unwrap_or(0);
    let minor = version_components.next().and_then(|v| v.parse::<i32>().ok()).unwrap_or(0);
    let patch_component = version_components.next();
    let patch = patch_component.and_then(|v| v.parse::<i32>().ok()).unwrap_or(0);
    if major < 0 || minor < 0 || patch < 0 {
        return None;
    }

    let minor =
        if major >= 10 && patch_component.is_none() { minor } else { minor.checked_mul(100)? };

    let version = major.checked_mul(10_000)?.checked_add(minor)?.checked_add(patch)?;

    NonZeroI32::new(version)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroI32;

    use crate::source::extract_server_version;

    /// Modern release numbers use the minor component without scaling it.
    #[test]
    fn extract_server_version_basic_versions() {
        assert_eq!(extract_server_version("10.1"), NonZeroI32::new(100001));
        assert_eq!(extract_server_version("15.5"), NonZeroI32::new(150005));
        assert_eq!(extract_server_version("14.2"), NonZeroI32::new(140002));
        assert_eq!(extract_server_version("13.0"), NonZeroI32::new(130000));
        assert_eq!(extract_server_version("16.1"), NonZeroI32::new(160001));
        assert_eq!(extract_server_version("15"), NonZeroI32::new(150000));
    }

    /// Legacy releases retain the encoding of all three version components.
    #[test]
    fn extract_server_version_legacy_versions() {
        assert_eq!(extract_server_version("9.6.3"), NonZeroI32::new(90603));
        assert_eq!(extract_server_version("9.6.0"), NonZeroI32::new(90600));
        assert_eq!(extract_server_version("9.6"), NonZeroI32::new(90600));
        assert_eq!(extract_server_version("8.4.22"), NonZeroI32::new(80422));
    }

    /// Whitespace separates vendor and build suffixes from the numeric version.
    #[test]
    fn extract_server_version_with_suffixes() {
        assert_eq!(extract_server_version("15.5 (Homebrew)"), NonZeroI32::new(150005));
        assert_eq!(extract_server_version("14.2 on x86_64-pc-linux-gnu"), NonZeroI32::new(140002));
        assert_eq!(
            extract_server_version("13.7 (Ubuntu 13.7-1.pgdg20.04+1)"),
            NonZeroI32::new(130007)
        );
        assert_eq!(extract_server_version("16.0 devel"), NonZeroI32::new(160000));
        assert_eq!(extract_server_version("9.6.3 (Homebrew)"), NonZeroI32::new(90603));
    }

    /// Nonstandard modern three-component inputs preserve their existing
    /// values.
    #[test]
    fn extract_server_version_three_component_compatibility() {
        assert_eq!(extract_server_version("15.5.1"), NonZeroI32::new(150501));
        assert_eq!(extract_server_version("15.5.0"), NonZeroI32::new(150500));
        assert_eq!(extract_server_version("14.10.3"), NonZeroI32::new(141003));
        assert_eq!(extract_server_version("13.12.25"), NonZeroI32::new(131225));
        assert_eq!(extract_server_version("15.5.1 (Homebrew)"), NonZeroI32::new(150501));
    }

    /// Encoding changes do not tighten or expand the existing parsing rules.
    #[test]
    fn extract_server_version_permissive_parsing_compatibility() {
        assert_eq!(extract_server_version("15beta1"), None);
        assert_eq!(extract_server_version("15devel"), None);
        assert_eq!(extract_server_version("9.6devel"), NonZeroI32::new(90000));
        assert_eq!(extract_server_version("15.5-custom"), NonZeroI32::new(150000));
        assert_eq!(extract_server_version("15.invalid"), NonZeroI32::new(150000));
        assert_eq!(extract_server_version("15.5.invalid"), NonZeroI32::new(150500));
        assert_eq!(extract_server_version("15.5."), NonZeroI32::new(150500));
        assert_eq!(extract_server_version("15..1"), NonZeroI32::new(150001));
        assert_eq!(extract_server_version("15.5.1.2"), NonZeroI32::new(150501));
    }

    /// Inputs without any parseable nonzero components remain unknown.
    #[test]
    fn extract_server_version_invalid_inputs() {
        assert_eq!(extract_server_version(""), None);
        assert_eq!(extract_server_version("invalid"), None);
        assert_eq!(extract_server_version("not.a.version"), None);
        assert_eq!(extract_server_version("PostgreSQL"), None);
        assert_eq!(extract_server_version("   "), None);
        assert_eq!(extract_server_version("１５.５"), None);
        assert_eq!(extract_server_version("é"), None);
    }

    /// Zero results and negative components are rejected without panicking.
    #[test]
    fn extract_server_version_zero_versions() {
        assert_eq!(extract_server_version("0.0.0"), None);
        assert_eq!(extract_server_version("0.0"), None);
        assert_eq!(extract_server_version("-1.0"), None);
        assert_eq!(extract_server_version("15.-1"), None);
        assert_eq!(extract_server_version("9.6.-1"), None);
        assert_eq!(extract_server_version("15.5.-1"), None);
    }

    /// Leading and trailing whitespace do not change the extracted version.
    #[test]
    fn extract_server_version_whitespace_handling() {
        assert_eq!(extract_server_version("  15.5  "), NonZeroI32::new(150005));
        assert_eq!(extract_server_version("15.5\t(Homebrew)"), NonZeroI32::new(150005));
        assert_eq!(extract_server_version("15.5\n"), NonZeroI32::new(150005));
    }

    /// Both encodings allow the largest result and reject arithmetic overflow.
    #[test]
    fn extract_server_version_overflow_returns_none() {
        assert_eq!(extract_server_version("999999999.999999999.999999999"), None);
        assert_eq!(extract_server_version("214748.3647"), NonZeroI32::new(i32::MAX));
        assert_eq!(extract_server_version("214748.3648"), None);
        assert_eq!(extract_server_version("214749.0"), None);
        assert_eq!(extract_server_version("214748.36.47"), NonZeroI32::new(i32::MAX));
        assert_eq!(extract_server_version("214748.36.48"), None);
        assert_eq!(extract_server_version("9.21474837"), None);
    }
}
