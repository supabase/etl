use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
    types::{ColumnSchema, Type, is_array_type},
};
use etl_config::shared::ClickHouseEngine;

/// (For MergeTree engine) CDC operation column.
pub(crate) const CDC_OPERATION_COLUMN_NAME: &str = "cdc_operation";
/// (For MergeTree engine) CDC LSN column (commit_lsn).
pub(crate) const CDC_LSN_COLUMN_NAME: &str = "cdc_lsn";
/// (For ReplacingMergeTree engine) version column (start_lsn).
pub(crate) const ETL_LSN_COLUMN_NAME: &str = "_etl_lsn";
/// (For ReplacingMergeTree engine) tombstone column.
pub(crate) const ETL_DELETED_COLUMN_NAME: &str = "_etl_deleted";
/// Suffix for the auto-generated current-state view over RMT tables.
pub(crate) const CURRENT_VIEW_SUFFIX: &str = "__current";

/// Returns the base ClickHouse type string for a Postgres scalar type.
///
/// The returned string does not include `Nullable(...)` wrapping — callers are
/// responsible for applying that when the column is nullable. Arrays always use
/// `Array(Nullable(T))` since Postgres array elements are nullable.
fn postgres_column_type_to_clickhouse_sql(typ: &Type) -> &'static str {
    match typ {
        &Type::BOOL => "Boolean",
        &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT => "String",
        &Type::INT2 => "Int16",
        &Type::INT4 => "Int32",
        &Type::INT8 => "Int64",
        &Type::FLOAT4 => "Float32",
        &Type::FLOAT8 => "Float64",
        &Type::NUMERIC | &Type::MONEY => "String",
        &Type::DATE => "Date32",
        &Type::TIME => "String",
        &Type::TIMESTAMP => "DateTime64(6)",
        &Type::TIMESTAMPTZ => "DateTime64(6, 'UTC')",
        &Type::UUID => "UUID",
        &Type::JSON | &Type::JSONB => "String",
        &Type::BYTEA => "String",
        &Type::OID => "UInt32",
        _ => "String",
    }
}

/// Returns the ClickHouse array element type for a Postgres array type.
fn postgres_array_element_clickhouse_sql(typ: &Type) -> &'static str {
    match typ {
        &Type::BOOL_ARRAY => "Boolean",
        &Type::CHAR_ARRAY
        | &Type::BPCHAR_ARRAY
        | &Type::VARCHAR_ARRAY
        | &Type::NAME_ARRAY
        | &Type::TEXT_ARRAY
        | &Type::MONEY_ARRAY => "String",
        &Type::INT2_ARRAY => "Int16",
        &Type::INT4_ARRAY => "Int32",
        &Type::INT8_ARRAY => "Int64",
        &Type::FLOAT4_ARRAY => "Float32",
        &Type::FLOAT8_ARRAY => "Float64",
        &Type::NUMERIC_ARRAY => "String",
        &Type::DATE_ARRAY => "Date32",
        &Type::TIME_ARRAY => "String",
        &Type::TIMESTAMP_ARRAY => "DateTime64(6)",
        &Type::TIMESTAMPTZ_ARRAY => "DateTime64(6, 'UTC')",
        &Type::UUID_ARRAY => "UUID",
        &Type::JSON_ARRAY | &Type::JSONB_ARRAY => "String",
        &Type::BYTEA_ARRAY => "String",
        &Type::OID_ARRAY => "UInt32",
        _ => "String",
    }
}

/// Quotes a ClickHouse identifier, escaping embedded double quotes.
pub(crate) fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// Returns the full ClickHouse type string for a column, with Nullable
/// wrapping.
///
/// When `force_nullable` is true (ALTER TABLE ADD), all scalar columns become
/// Nullable since ClickHouse cannot backfill existing rows.
pub(super) fn clickhouse_column_type(col: &ColumnSchema, force_nullable: bool) -> String {
    if is_array_type(&col.typ) {
        let elem = postgres_array_element_clickhouse_sql(&col.typ);
        format!("Array(Nullable({elem}))")
    } else {
        let base = postgres_column_type_to_clickhouse_sql(&col.typ);
        if col.nullable || force_nullable { format!("Nullable({base})") } else { base.to_owned() }
    }
}

/// Trailing CDC column names appended to each replicated row, by engine.
pub(super) fn trailing_cdc_column_names(engine: ClickHouseEngine) -> &'static [&'static str] {
    match engine {
        ClickHouseEngine::MergeTree => &[CDC_OPERATION_COLUMN_NAME, CDC_LSN_COLUMN_NAME],
        ClickHouseEngine::ReplacingMergeTree => &[ETL_LSN_COLUMN_NAME, ETL_DELETED_COLUMN_NAME],
    }
}

/// Dispatches `CREATE TABLE IF NOT EXISTS` DDL by engine.
pub(super) fn create_table_sql<'a, I>(
    engine: ClickHouseEngine,
    table_name: &str,
    column_schemas: I,
) -> EtlResult<String>
where
    I: IntoIterator<Item = &'a ColumnSchema>,
    I::IntoIter: ExactSizeIterator,
{
    match engine {
        ClickHouseEngine::MergeTree => Ok(create_merge_tree_sql(table_name, column_schemas)),
        ClickHouseEngine::ReplacingMergeTree => {
            create_replacing_merge_tree_sql(table_name, column_schemas)
        }
    }
}

/// `MergeTree` DDL: appends `cdc_operation String` and `cdc_lsn UInt64`,
/// `ORDER BY tuple()`.
pub(super) fn create_merge_tree_sql<'a, I>(table_name: &str, column_schemas: I) -> String
where
    I: IntoIterator<Item = &'a ColumnSchema>,
    I::IntoIter: ExactSizeIterator,
{
    let iter = column_schemas.into_iter();
    let mut cols = Vec::with_capacity(iter.len() + 2);

    for col in iter {
        let col_type = clickhouse_column_type(col, false);
        cols.push(format!("  {} {}", quote_identifier(&col.name), col_type));
    }

    cols.push(format!("  {} String", quote_identifier(CDC_OPERATION_COLUMN_NAME)));
    cols.push(format!("  {} UInt64", quote_identifier(CDC_LSN_COLUMN_NAME)));

    let col_defs = cols.join(",\n");
    let quoted_table_name = quote_identifier(table_name);
    format!(
        "CREATE TABLE IF NOT EXISTS {quoted_table_name} (\n{col_defs}\n) ENGINE = \
         MergeTree()\nORDER BY tuple()"
    )
}

/// `ReplacingMergeTree` DDL: appends `_etl_lsn UInt64` (version) and
/// `_etl_deleted UInt8` (tombstone), `ORDER BY` the source PK columns in
/// `primary_key_ordinal_position` order.
///
/// Errors when the source schema has no PK columns.
pub(super) fn create_replacing_merge_tree_sql<'a, I>(
    table_name: &str,
    column_schemas: I,
) -> EtlResult<String>
where
    I: IntoIterator<Item = &'a ColumnSchema>,
    I::IntoIter: ExactSizeIterator,
{
    let columns: Vec<&ColumnSchema> = column_schemas.into_iter().collect();

    let mut pk_columns: Vec<&ColumnSchema> =
        columns.iter().copied().filter(|c| c.primary_key_ordinal_position.is_some()).collect();

    if pk_columns.is_empty() {
        return Err(etl_error!(
            ErrorKind::SourceSchemaError,
            "ClickHouse ReplacingMergeTree requires a primary key",
            format!(
                "Table '{}' has no primary-key columns; set `engine: merge_tree` or define a PK \
                 on the source table.",
                table_name
            )
        ));
    }

    pk_columns.sort_by_key(|c| c.primary_key_ordinal_position);

    let mut cols = Vec::with_capacity(columns.len() + 2);
    for col in &columns {
        let col_type = clickhouse_column_type(col, false);
        cols.push(format!("  {} {}", quote_identifier(&col.name), col_type));
    }
    cols.push(format!("  {} UInt64", quote_identifier(ETL_LSN_COLUMN_NAME)));
    cols.push(format!("  {} UInt8", quote_identifier(ETL_DELETED_COLUMN_NAME)));

    let col_defs = cols.join(",\n");
    let quoted_table_name = quote_identifier(table_name);
    let order_by =
        pk_columns.iter().map(|c| quote_identifier(&c.name)).collect::<Vec<_>>().join(", ");

    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {quoted_table_name} (\n{col_defs}\n) ENGINE = \
         ReplacingMergeTree({lsn}, {del})\nORDER BY ({order_by})",
        lsn = quote_identifier(ETL_LSN_COLUMN_NAME),
        del = quote_identifier(ETL_DELETED_COLUMN_NAME),
    ))
}

/// `CREATE VIEW IF NOT EXISTS "<table>__current"` for an RMT table.
///
/// Selects only user columns (drops `_etl_lsn` and `_etl_deleted`), reads via
/// `FINAL`, and filters tombstones.
pub(super) fn create_current_view_sql<'a, I>(table_name: &str, column_schemas: I) -> String
where
    I: IntoIterator<Item = &'a ColumnSchema>,
{
    let select_cols = column_schemas
        .into_iter()
        .map(|c| quote_identifier(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let view_name = format!("{table_name}{CURRENT_VIEW_SUFFIX}");
    format!(
        "CREATE VIEW IF NOT EXISTS {view} AS\nSELECT {select_cols}\nFROM {table} FINAL\nWHERE \
         {deleted} = 0",
        view = quote_identifier(&view_name),
        table = quote_identifier(table_name),
        deleted = quote_identifier(ETL_DELETED_COLUMN_NAME),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_identifier_escapes_embedded_quotes() {
        assert_eq!(quote_identifier("plain"), "\"plain\"");
        assert_eq!(quote_identifier("has\"quote"), "\"has\"\"quote\"");
    }

    #[test]
    fn create_merge_tree_sql_quotes_identifiers() {
        let schemas = vec![ColumnSchema {
            name: "id\"value".to_owned(),
            typ: Type::INT4,
            modifier: -1,
            ordinal_position: 1,
            primary_key_ordinal_position: Some(1),
            nullable: false,
        }];
        // Pre-encoded table name with embedded quotes to verify the SQL
        // builder quotes/escapes the identifier itself.
        let sql = create_merge_tree_sql("sche\"ma_ta\"ble", &schemas);

        assert!(
            sql.contains("CREATE TABLE IF NOT EXISTS \"sche\"\"ma_ta\"\"ble\""),
            "schema-derived table name should be quoted and escaped: {sql}"
        );
        assert!(
            sql.contains("\"id\"\"value\" Int32"),
            "column name should be quoted and escaped: {sql}"
        );
    }

    #[test]
    fn scalar_type_mapping() {
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::BOOL), "Boolean");
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::CHAR), "String");
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::BPCHAR), "String");
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::VARCHAR), "String");
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::NAME), "String");
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::TEXT), "String");
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::INT2), "Int16");
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::INT4), "Int32");
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::INT8), "Int64");
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::FLOAT4), "Float32");
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::FLOAT8), "Float64");
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::NUMERIC), "String");
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::MONEY), "String");
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::DATE), "Date32");
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::TIME), "String");
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::TIMESTAMP), "DateTime64(6)");
        assert_eq!(
            postgres_column_type_to_clickhouse_sql(&Type::TIMESTAMPTZ),
            "DateTime64(6, 'UTC')"
        );
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::UUID), "UUID");
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::JSON), "String");
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::JSONB), "String");
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::BYTEA), "String");
        assert_eq!(postgres_column_type_to_clickhouse_sql(&Type::OID), "UInt32");
    }

    #[test]
    fn array_type_mapping() {
        assert_eq!(postgres_array_element_clickhouse_sql(&Type::BOOL_ARRAY), "Boolean");
        assert_eq!(postgres_array_element_clickhouse_sql(&Type::TEXT_ARRAY), "String");
        assert_eq!(postgres_array_element_clickhouse_sql(&Type::MONEY_ARRAY), "String");
        assert_eq!(postgres_array_element_clickhouse_sql(&Type::INT4_ARRAY), "Int32");
        assert_eq!(postgres_array_element_clickhouse_sql(&Type::INT8_ARRAY), "Int64");
        assert_eq!(postgres_array_element_clickhouse_sql(&Type::FLOAT8_ARRAY), "Float64");
        assert_eq!(postgres_array_element_clickhouse_sql(&Type::UUID_ARRAY), "UUID");
        assert_eq!(postgres_array_element_clickhouse_sql(&Type::JSONB_ARRAY), "String");
    }

    #[test]
    fn create_merge_tree_sql_nullable() {
        let schemas = vec![
            ColumnSchema {
                name: "id".to_owned(),
                typ: Type::INT4,
                modifier: -1,
                ordinal_position: 1,
                primary_key_ordinal_position: Some(1),
                nullable: false,
            },
            ColumnSchema {
                name: "name".to_owned(),
                typ: Type::TEXT,
                modifier: -1,
                ordinal_position: 2,
                primary_key_ordinal_position: None,
                nullable: true,
            },
        ];
        let sql = create_merge_tree_sql("public_users", &schemas);
        assert!(sql.contains("\"id\" Int32"), "id should be non-nullable Int32");
        assert!(sql.contains("\"name\" Nullable(String)"), "name should be Nullable(String)");
    }

    #[test]
    fn create_merge_tree_sql_cdc_columns() {
        let schemas = vec![ColumnSchema {
            name: "id".to_owned(),
            typ: Type::INT4,
            modifier: -1,
            ordinal_position: 1,
            primary_key_ordinal_position: Some(1),
            nullable: false,
        }];
        let sql = create_merge_tree_sql("public_t", &schemas);
        assert!(sql.contains("\"cdc_operation\" String"), "cdc_operation should be non-nullable");
        assert!(sql.contains("\"cdc_lsn\" UInt64"), "cdc_lsn should be non-nullable UInt64");
        assert!(sql.contains("ENGINE = MergeTree()"));
        assert!(sql.contains("ORDER BY tuple()"));
    }

    #[test]
    fn create_merge_tree_sql_array_columns() {
        let schemas = vec![ColumnSchema {
            name: "tags".to_owned(),
            typ: Type::TEXT_ARRAY,
            modifier: -1,
            ordinal_position: 1,
            primary_key_ordinal_position: None,
            nullable: false,
        }];
        let sql = create_merge_tree_sql("public_t", &schemas);
        assert!(
            sql.contains("\"tags\" Array(Nullable(String))"),
            "array columns should always be Array(Nullable(T))"
        );
    }

    #[test]
    fn create_replacing_merge_tree_sql_single_pk() {
        // --- GIVEN: single-column PK with a nullable non-PK column ---
        let schemas = vec![
            ColumnSchema {
                name: "id".to_owned(),
                typ: Type::INT4,
                modifier: -1,
                ordinal_position: 1,
                primary_key_ordinal_position: Some(1),
                nullable: false,
            },
            ColumnSchema {
                name: "name".to_owned(),
                typ: Type::TEXT,
                modifier: -1,
                ordinal_position: 2,
                primary_key_ordinal_position: None,
                nullable: true,
            },
        ];
        // --- WHEN: build the RMT DDL ---
        let sql = create_replacing_merge_tree_sql("public_users", &schemas).unwrap();
        // --- THEN: trailing etl columns, engine, and ORDER BY are correct ---
        assert!(sql.contains("\"id\" Int32"));
        assert!(sql.contains("\"name\" Nullable(String)"));
        assert!(sql.contains("\"_etl_lsn\" UInt64"));
        assert!(sql.contains("\"_etl_deleted\" UInt8"));
        assert!(sql.contains("ENGINE = ReplacingMergeTree(\"_etl_lsn\", \"_etl_deleted\")"));
        assert!(sql.contains("ORDER BY (\"id\")"));
    }

    #[test]
    fn create_replacing_merge_tree_sql_composite_pk_orders_by_ordinal() {
        // --- GIVEN: composite PK whose ordinal order differs from table order ---
        let schemas = vec![
            ColumnSchema {
                name: "id".to_owned(),
                typ: Type::INT4,
                modifier: -1,
                ordinal_position: 1,
                primary_key_ordinal_position: Some(2),
                nullable: false,
            },
            ColumnSchema {
                name: "name".to_owned(),
                typ: Type::TEXT,
                modifier: -1,
                ordinal_position: 2,
                primary_key_ordinal_position: None,
                nullable: true,
            },
            ColumnSchema {
                name: "tenant_id".to_owned(),
                typ: Type::INT4,
                modifier: -1,
                ordinal_position: 3,
                primary_key_ordinal_position: Some(1),
                nullable: false,
            },
        ];
        // --- WHEN: build the RMT DDL ---
        let sql = create_replacing_merge_tree_sql("public_users", &schemas).unwrap();
        // --- THEN: ORDER BY follows PK ordinal, not table ordinal ---
        assert!(
            sql.contains("ORDER BY (\"tenant_id\", \"id\")"),
            "ORDER BY must follow PK ordinal: {sql}"
        );
    }

    #[test]
    fn create_replacing_merge_tree_sql_rejects_pkless_schema() {
        // --- GIVEN: schema with no PK columns ---
        let schemas = vec![ColumnSchema {
            name: "value".to_owned(),
            typ: Type::TEXT,
            modifier: -1,
            ordinal_position: 1,
            primary_key_ordinal_position: None,
            nullable: true,
        }];
        // --- WHEN: build the RMT DDL ---
        let err = create_replacing_merge_tree_sql("public_events", &schemas).unwrap_err();
        // --- THEN: builder rejects with SourceSchemaError ---
        assert_eq!(err.kind(), ErrorKind::SourceSchemaError);
    }

    #[test]
    fn create_table_sql_dispatches_on_engine() {
        // --- GIVEN: a schema with a single PK column ---
        let schemas = vec![ColumnSchema {
            name: "id".to_owned(),
            typ: Type::INT4,
            modifier: -1,
            ordinal_position: 1,
            primary_key_ordinal_position: Some(1),
            nullable: false,
        }];
        // --- WHEN/THEN: dispatcher selects the matching engine branch ---
        let mt = create_table_sql(ClickHouseEngine::MergeTree, "public_t", &schemas).unwrap();
        assert!(mt.contains("ENGINE = MergeTree()"));
        let rmt =
            create_table_sql(ClickHouseEngine::ReplacingMergeTree, "public_t", &schemas).unwrap();
        assert!(rmt.contains("ENGINE = ReplacingMergeTree"));
    }

    #[test]
    fn create_current_view_sql_selects_user_columns_only() {
        // --- GIVEN: a two-column schema ---
        let schemas = vec![
            ColumnSchema {
                name: "id".to_owned(),
                typ: Type::INT4,
                modifier: -1,
                ordinal_position: 1,
                primary_key_ordinal_position: Some(1),
                nullable: false,
            },
            ColumnSchema {
                name: "name".to_owned(),
                typ: Type::TEXT,
                modifier: -1,
                ordinal_position: 2,
                primary_key_ordinal_position: None,
                nullable: true,
            },
        ];
        // --- WHEN: build the current-state view DDL ---
        let sql = create_current_view_sql("public_users", &schemas);
        // --- THEN: __current suffix, FINAL read, tombstone filter, no etl cols ---
        assert!(sql.contains("CREATE VIEW IF NOT EXISTS \"public_users__current\""));
        assert!(sql.contains("SELECT \"id\", \"name\""));
        assert!(sql.contains("FROM \"public_users\" FINAL"));
        assert!(sql.contains("WHERE \"_etl_deleted\" = 0"));
        assert!(!sql.contains("_etl_lsn"), "view must not expose _etl_lsn: {sql}");
    }

    #[test]
    fn trailing_cdc_column_names_by_engine() {
        assert_eq!(
            trailing_cdc_column_names(ClickHouseEngine::MergeTree),
            &[CDC_OPERATION_COLUMN_NAME, CDC_LSN_COLUMN_NAME]
        );
        assert_eq!(
            trailing_cdc_column_names(ClickHouseEngine::ReplacingMergeTree),
            &[ETL_LSN_COLUMN_NAME, ETL_DELETED_COLUMN_NAME]
        );
    }
}
