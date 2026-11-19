//! Parquet writer options shared by DuckLake writers and maintenance.

use etl_config::shared::DuckLakeWriterConfig;
use pg_escape::{quote_identifier, quote_literal};

const PARQUET_COMPRESSION_OPTION_NAME: &str = "parquet_compression";
const PARQUET_COMPRESSION_OPTION_VALUE: &str = "zstd";
const PARQUET_ROW_GROUP_SIZE_BYTES_OPTION_NAME: &str = "parquet_row_group_size_bytes";
const PARQUET_ROW_GROUP_SIZE_OPTION_NAME: &str = "parquet_row_group_size";
const PARQUET_VERSION_OPTION_NAME: &str = "parquet_version";
const PARQUET_VERSION_OPTION_VALUE: u8 = 2;

/// Builds the SQL that sets DuckLake's global Parquet writer options on the
/// catalog attached as `catalog`.
///
/// Each `set_option` call outside a transaction commits to the catalog on its
/// own, and every connection setup runs these calls. One transaction writes
/// all four options in a single catalog commit.
pub fn configure_parquet_settings_sql(
    catalog: &str,
    writer_config: &DuckLakeWriterConfig,
) -> String {
    let catalog = quote_identifier(catalog);

    format!(
        "BEGIN; CALL {catalog}.set_option({}, {}); CALL {catalog}.set_option({}, {}); CALL \
         {catalog}.set_option({}, {}); CALL {catalog}.set_option({}, {}); COMMIT;",
        quote_literal(PARQUET_COMPRESSION_OPTION_NAME),
        quote_literal(PARQUET_COMPRESSION_OPTION_VALUE),
        quote_literal(PARQUET_ROW_GROUP_SIZE_BYTES_OPTION_NAME),
        quote_literal(writer_config.parquet_row_group_size_bytes()),
        quote_literal(PARQUET_ROW_GROUP_SIZE_OPTION_NAME),
        quote_literal(writer_config.parquet_row_group_size()),
        quote_literal(PARQUET_VERSION_OPTION_NAME),
        PARQUET_VERSION_OPTION_VALUE,
    )
}

#[cfg(test)]
mod tests {
    use etl_config::shared::DuckLakeWriterConfig;

    use crate::ducklake::writer_options::configure_parquet_settings_sql;

    #[test]
    fn writer_options_use_recommended_defaults() {
        let sql = configure_parquet_settings_sql("lake", &DuckLakeWriterConfig::default());

        assert!(sql.contains("lake.set_option('parquet_compression', 'zstd')"));
        assert!(sql.contains("'parquet_row_group_size_bytes', '128MiB'"));
        assert!(sql.contains("'parquet_row_group_size', '2500000'"));
        assert!(sql.contains("'parquet_version', 2"));
    }

    #[test]
    fn writer_options_use_configured_overrides() {
        let writer_config = DuckLakeWriterConfig::new(
            Some("64MB".to_owned()),
            Some("32MB".to_owned()),
            Some("500000".to_owned()),
        );

        let sql = configure_parquet_settings_sql("lake", &writer_config);

        assert!(sql.contains("'parquet_row_group_size_bytes', '32MB'"));
        assert!(sql.contains("'parquet_row_group_size', '500000'"));
    }
}
