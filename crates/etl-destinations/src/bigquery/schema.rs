use etl::{
    error::EtlResult,
    schema::{
        ColumnSchema, DefaultExpression, ReplicatedTableSchema, Type, is_array_type,
        parse_default_expression,
    },
};
use gcp_bigquery_client::storage::{ColumnMode, ColumnType, FieldDescriptor, TableDescriptor};
use tracing::warn;

use crate::bigquery::sql::quote_identifier;

/// Special column name for Change Data Capture operations in BigQuery.
const BIGQUERY_CDC_SPECIAL_COLUMN: &str = "_CHANGE_TYPE";

/// Special column name for Change Data Capture sequence ordering in BigQuery.
const BIGQUERY_CDC_SEQUENCE_COLUMN: &str = "_CHANGE_SEQUENCE_NUMBER";

/// Append-only metadata column names.
pub(super) const APPEND_ONLY_UUID_COLUMN: &str = "_etl_uuid";
pub(super) const APPEND_ONLY_SOURCE_TIMESTAMP_COLUMN: &str = "_etl_source_timestamp";
pub(super) const APPEND_ONLY_CHANGE_SEQUENCE_NUMBER_COLUMN: &str = "_etl_change_sequence_number";
pub(super) const APPEND_ONLY_CHANGE_TYPE_COLUMN: &str = "_etl_change_type";
pub(super) const APPEND_ONLY_SORT_KEYS_COLUMN: &str = "_etl_sort_keys";

/// Generates SQL column specification for CREATE TABLE statements.
pub(super) fn column_spec(column_schema: &ColumnSchema) -> EtlResult<String> {
    let column_name = quote_identifier(&column_schema.name, "BigQuery column name")?;

    let mut column_spec =
        format!("{} {}", column_name, postgres_to_bigquery_type(&column_schema.typ));

    if let Some(rendered_default_expression) =
        column_schema.default_expression.as_deref().and_then(|default_expression| {
            default_expression_sql(default_expression, &column_schema.typ)
        })
    {
        column_spec.push_str(&format!(" default {rendered_default_expression}"));
    } else if column_schema.default_expression.is_some() {
        warn!(
            column_name = %column_schema.name,
            "skipping unsupported source column default for BigQuery table creation"
        );
    }

    if !column_schema.nullable && !is_array_type(&column_schema.typ) {
        column_spec.push_str(" not null");
    };

    Ok(column_spec)
}

/// Returns whether a column default can be represented in BigQuery SQL.
pub(crate) fn supports_column_default(default_expression: &str, typ: &Type) -> bool {
    default_expression_sql(default_expression, typ).is_some()
}

/// Returns a rendered default expression for BigQuery, if supported.
pub(super) fn default_expression_sql(default_expression: &str, typ: &Type) -> Option<String> {
    parse_default_expression(default_expression, typ)
        .and_then(|expression| render_default_expression(&expression, typ))
}

/// Renders a parsed default expression as BigQuery SQL.
fn render_default_expression(expression: &DefaultExpression, typ: &Type) -> Option<String> {
    match expression {
        DefaultExpression::StringLiteral(expression) => {
            is_bigquery_string_default_type(typ).then(|| expression.clone())
        }
        DefaultExpression::NumericLiteral(expression) => {
            if is_bigquery_numeric_default_type(typ) {
                Some(expression.clone())
            } else if is_bigquery_numeric_string_default_type(typ) {
                Some(quote_numeric_literal_as_string(expression))
            } else {
                None
            }
        }
        DefaultExpression::BooleanLiteral(expression) => {
            matches!(typ, &Type::BOOL).then(|| expression.clone())
        }
        DefaultExpression::DateLiteral(expression) => {
            matches!(typ, &Type::DATE).then(|| format!("DATE {expression}"))
        }
        DefaultExpression::TimeLiteral(expression) => {
            matches!(typ, &Type::TIME).then(|| format!("TIME {expression}"))
        }
        DefaultExpression::TimeTzLiteral(expression) => {
            matches!(typ, &Type::TIMETZ).then(|| expression.clone())
        }
        DefaultExpression::TimestampLiteral(expression) => {
            matches!(typ, &Type::TIMESTAMP).then(|| format!("DATETIME {expression}"))
        }
        DefaultExpression::TimestampTzLiteral(expression) => {
            matches!(typ, &Type::TIMESTAMPTZ).then(|| format!("TIMESTAMP {expression}"))
        }
        DefaultExpression::IntervalLiteral(expression) => {
            matches!(typ, &Type::INTERVAL).then(|| expression.clone())
        }
        DefaultExpression::JsonLiteral(expression) => {
            is_json_type(typ).then(|| format!("JSON {expression}"))
        }
    }
}

/// Returns whether this Postgres type is created as a BigQuery string column
/// and can safely receive string-producing defaults.
fn is_bigquery_string_default_type(typ: &Type) -> bool {
    matches!(
        typ,
        &Type::CHAR
            | &Type::BPCHAR
            | &Type::VARCHAR
            | &Type::NAME
            | &Type::TEXT
            | &Type::MONEY
            | &Type::TIMETZ
            | &Type::INTERVAL
            | &Type::UUID
    )
}

/// Returns whether this Postgres type is created as a BigQuery numeric column
/// and can safely receive numeric defaults.
fn is_bigquery_numeric_default_type(typ: &Type) -> bool {
    matches!(
        typ,
        &Type::INT2
            | &Type::INT4
            | &Type::INT8
            | &Type::FLOAT4
            | &Type::FLOAT8
            | &Type::NUMERIC
            | &Type::OID
    )
}

/// Returns whether this Postgres numeric-like type is created as a BigQuery
/// string column.
fn is_bigquery_numeric_string_default_type(typ: &Type) -> bool {
    matches!(typ, &Type::MONEY)
}

/// Returns whether this Postgres type is created as a BigQuery JSON column and
/// can safely receive JSON defaults.
fn is_json_type(typ: &Type) -> bool {
    matches!(typ, &Type::JSON | &Type::JSONB)
}

/// Quotes a parser-validated numeric literal as a SQL string literal.
fn quote_numeric_literal_as_string(expression: &str) -> String {
    format!("'{expression}'")
}

/// Creates a primary key clause for table creation.
///
/// BigQuery tables are keyed by the source primary key, which is distinct from
/// PostgreSQL replica-identity metadata when `REPLICA IDENTITY FULL` is in use.
pub(super) fn add_primary_key_clause(
    replicated_table_schema: &ReplicatedTableSchema,
) -> EtlResult<Option<String>> {
    let mut primary_key_columns: Vec<_> =
        replicated_table_schema.primary_key_column_schemas().collect();

    if primary_key_columns.is_empty() {
        return Ok(None);
    }

    // Sort by primary_key_ordinal_position to ensure correct composite key
    // ordering. This is needed since the order of column schema returned above
    // is ordered by columns ordering, not primary key ordering.
    primary_key_columns.sort_by_key(|c| c.primary_key_ordinal_position);

    let primary_key_columns: Vec<String> = primary_key_columns
        .into_iter()
        .map(|c| quote_identifier(&c.name, "BigQuery primary key column"))
        .collect::<EtlResult<Vec<_>>>()?;

    let primary_key_clause =
        format!(", primary key ({}) not enforced", primary_key_columns.join(","));

    Ok(Some(primary_key_clause))
}

/// Builds complete column specifications for CREATE TABLE statements.
pub(super) fn create_columns_spec(
    replicated_table_schema: &ReplicatedTableSchema,
) -> EtlResult<String> {
    let mut column_spec = replicated_table_schema
        .column_schemas()
        .map(column_spec)
        .collect::<EtlResult<Vec<_>>>()?
        .join(",");

    if let Some(primary_key_clause) = add_primary_key_clause(replicated_table_schema)? {
        column_spec.push_str(&primary_key_clause);
    }

    Ok(format!("({column_spec})"))
}

/// Builds column specifications for append-only history tables.
pub(super) fn create_append_only_columns_spec(
    replicated_table_schema: &ReplicatedTableSchema,
) -> EtlResult<String> {
    let mut column_specs = replicated_table_schema
        .column_schemas()
        .map(append_only_column_spec)
        .collect::<EtlResult<Vec<_>>>()?;
    column_specs.extend(append_only_metadata_column_specs());

    Ok(format!("({})", column_specs.join(",")))
}

fn append_only_metadata_column_specs() -> Vec<String> {
    vec![
        format!("`{APPEND_ONLY_UUID_COLUMN}` string not null"),
        format!("`{APPEND_ONLY_SOURCE_TIMESTAMP_COLUMN}` int64 not null"),
        format!("`{APPEND_ONLY_CHANGE_SEQUENCE_NUMBER_COLUMN}` string not null"),
        format!("`{APPEND_ONLY_CHANGE_TYPE_COLUMN}` string not null"),
        format!("`{APPEND_ONLY_SORT_KEYS_COLUMN}` array<string>"),
    ]
}

/// Generates nullable SQL column specification for append-only history tables.
fn append_only_column_spec(column_schema: &ColumnSchema) -> EtlResult<String> {
    let column_name = quote_identifier(&column_schema.name, "BigQuery column name")?;
    Ok(format!("{} {}", column_name, postgres_to_bigquery_type(&column_schema.typ)))
}

/// Converts Postgres data types to BigQuery equivalent types.
pub(super) fn postgres_to_bigquery_type(typ: &Type) -> String {
    if is_array_type(typ) {
        let element_type = match typ {
            &Type::BOOL_ARRAY => "bool",
            &Type::INT2_ARRAY | &Type::INT4_ARRAY | &Type::INT8_ARRAY => "int64",
            &Type::FLOAT4_ARRAY | &Type::FLOAT8_ARRAY => "float64",
            &Type::NUMERIC_ARRAY => "bignumeric",
            &Type::DATE_ARRAY => "date",
            &Type::TIME_ARRAY => "time",
            &Type::TIMESTAMP_ARRAY => "datetime",
            &Type::TIMESTAMPTZ_ARRAY => "timestamp",
            &Type::JSON_ARRAY | &Type::JSONB_ARRAY => "json",
            &Type::OID_ARRAY => "int64",
            &Type::BYTEA_ARRAY => "bytes",
            _ => "string",
        };

        return format!("array<{element_type}>");
    }

    match typ {
        &Type::BOOL => "bool",
        &Type::INT2 | &Type::INT4 | &Type::INT8 => "int64",
        &Type::FLOAT4 | &Type::FLOAT8 => "float64",
        &Type::NUMERIC => "bignumeric",
        &Type::DATE => "date",
        &Type::TIME => "time",
        &Type::TIMESTAMP => "datetime",
        &Type::TIMESTAMPTZ => "timestamp",
        &Type::JSON | &Type::JSONB => "json",
        &Type::OID => "int64",
        &Type::BYTEA => "bytes",
        _ => "string",
    }
    .to_owned()
}

/// Converts Postgres column schemas to a BigQuery [`TableDescriptor`].
///
/// Maps data types and nullability to BigQuery column specifications, setting
/// appropriate column modes and automatically adding CDC special columns.
pub(crate) fn column_schemas_to_table_descriptor(
    replicated_table_schema: &ReplicatedTableSchema,
    use_cdc_sequence_column: bool,
) -> TableDescriptor {
    let mut field_descriptors = vec![];
    let mut number = 1;

    for column_schema in replicated_table_schema.column_schemas() {
        let typ = match column_schema.typ {
            Type::BOOL => ColumnType::Bool,
            Type::INT2 => ColumnType::Int32,
            Type::INT4 => ColumnType::Int32,
            Type::INT8 => ColumnType::Int64,
            Type::FLOAT4 => ColumnType::Float,
            Type::FLOAT8 => ColumnType::Double,
            Type::TIMESTAMPTZ => ColumnType::Int64,
            Type::OID => ColumnType::Int32,
            Type::BYTEA => ColumnType::Bytes,
            Type::BOOL_ARRAY => ColumnType::Bool,
            Type::INT2_ARRAY => ColumnType::Int32,
            Type::INT4_ARRAY => ColumnType::Int32,
            Type::INT8_ARRAY => ColumnType::Int64,
            Type::FLOAT4_ARRAY => ColumnType::Float,
            Type::FLOAT8_ARRAY => ColumnType::Double,
            Type::TIMESTAMPTZ_ARRAY => ColumnType::Int64,
            Type::OID_ARRAY => ColumnType::Int32,
            Type::BYTEA_ARRAY => ColumnType::Bytes,
            _ => ColumnType::String,
        };

        let mode = if is_array_type(&column_schema.typ) {
            ColumnMode::Repeated
        } else if use_cdc_sequence_column {
            // CDC delete rows can omit non-key columns, so the writer schema
            // must accept missing scalar fields even when the destination
            // table column is defined as NOT NULL.
            ColumnMode::Nullable
        } else if column_schema.nullable {
            ColumnMode::Nullable
        } else {
            ColumnMode::Required
        };

        field_descriptors.push(FieldDescriptor {
            number,
            name: column_schema.name.clone(),
            typ,
            mode,
        });
        number += 1;
    }

    field_descriptors.push(FieldDescriptor {
        number,
        name: BIGQUERY_CDC_SPECIAL_COLUMN.to_owned(),
        typ: ColumnType::String,
        mode: ColumnMode::Required,
    });
    number += 1;

    if use_cdc_sequence_column {
        field_descriptors.push(FieldDescriptor {
            number,
            name: BIGQUERY_CDC_SEQUENCE_COLUMN.to_owned(),
            typ: ColumnType::String,
            mode: ColumnMode::Required,
        });
    }

    TableDescriptor { field_descriptors }
}

/// Converts append-only table schemas to a BigQuery [`TableDescriptor`].
///
/// Source columns are nullable because delete rows contain sparse source
/// images. Metadata columns are appended as flat fields so the
/// existing Storage Write protobuf encoder can write append-only rows without
/// nested message support.
pub(crate) fn append_only_table_descriptor(
    replicated_table_schema: &ReplicatedTableSchema,
) -> TableDescriptor {
    let mut field_descriptors = vec![];
    let mut number = 1;

    for column_schema in replicated_table_schema.column_schemas() {
        field_descriptors.push(FieldDescriptor {
            number,
            name: column_schema.name.clone(),
            typ: column_schema_to_storage_type(&column_schema.typ),
            mode: if is_array_type(&column_schema.typ) {
                ColumnMode::Repeated
            } else {
                ColumnMode::Nullable
            },
        });
        number += 1;
    }

    field_descriptors.extend([
        FieldDescriptor {
            number,
            name: APPEND_ONLY_UUID_COLUMN.to_owned(),
            typ: ColumnType::String,
            mode: ColumnMode::Required,
        },
        FieldDescriptor {
            number: number + 1,
            name: APPEND_ONLY_SOURCE_TIMESTAMP_COLUMN.to_owned(),
            typ: ColumnType::Int64,
            mode: ColumnMode::Required,
        },
        FieldDescriptor {
            number: number + 2,
            name: APPEND_ONLY_CHANGE_SEQUENCE_NUMBER_COLUMN.to_owned(),
            typ: ColumnType::String,
            mode: ColumnMode::Required,
        },
        FieldDescriptor {
            number: number + 3,
            name: APPEND_ONLY_CHANGE_TYPE_COLUMN.to_owned(),
            typ: ColumnType::String,
            mode: ColumnMode::Required,
        },
        FieldDescriptor {
            number: number + 4,
            name: APPEND_ONLY_SORT_KEYS_COLUMN.to_owned(),
            typ: ColumnType::String,
            mode: ColumnMode::Repeated,
        },
    ]);

    TableDescriptor { field_descriptors }
}

/// Maps a Postgres source type to the scalar protobuf type used by the Storage
/// Write row descriptor.
fn column_schema_to_storage_type(typ: &Type) -> ColumnType {
    match typ {
        &Type::BOOL | &Type::BOOL_ARRAY => ColumnType::Bool,
        &Type::INT2 | &Type::INT4 | &Type::INT2_ARRAY | &Type::INT4_ARRAY => ColumnType::Int32,
        &Type::INT8 | &Type::TIMESTAMPTZ | &Type::INT8_ARRAY | &Type::TIMESTAMPTZ_ARRAY => {
            ColumnType::Int64
        }
        &Type::FLOAT4 | &Type::FLOAT4_ARRAY => ColumnType::Float,
        &Type::FLOAT8 | &Type::FLOAT8_ARRAY => ColumnType::Double,
        &Type::OID | &Type::OID_ARRAY => ColumnType::Int32,
        &Type::BYTEA | &Type::BYTEA_ARRAY => ColumnType::Bytes,
        _ => ColumnType::String,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use etl::schema::{
        ColumnSchema, IdentityMask, ReplicatedTableSchema, ReplicationMask, TableId, TableName,
        TableSchema, Type,
    };
    use gcp_bigquery_client::storage::{ColumnMode, ColumnType};

    use super::*;

    /// Creates a test column schema with common defaults.
    fn test_column(
        name: &str,
        typ: Type,
        ordinal_position: i32,
        nullable: bool,
        primary_key_ordinal: Option<i32>,
    ) -> ColumnSchema {
        ColumnSchema::new(name.to_owned(), typ, -1, ordinal_position, nullable)
            .with_primary_key_ordinal_position(primary_key_ordinal)
    }

    /// Creates a [`ReplicatedTableSchema`] from test columns with all columns
    /// replicated.
    fn test_replicated_schema(columns: Vec<ColumnSchema>) -> ReplicatedTableSchema {
        let column_names: HashSet<String> = columns.iter().map(|c| c.name.clone()).collect();
        let table_schema = Arc::new(TableSchema::new(
            TableId(1),
            TableName::new("public".to_owned(), "test_table".to_owned()),
            columns,
        ));
        let replication_mask = ReplicationMask::build_or_all(&table_schema, &column_names);

        ReplicatedTableSchema::from_mask(table_schema, replication_mask)
    }

    /// Creates a [`ReplicatedTableSchema`] from test columns with all columns
    /// replicated and an explicit runtime identity.
    fn test_replicated_schema_with_identity(
        columns: Vec<ColumnSchema>,
        identity_mask: IdentityMask,
    ) -> ReplicatedTableSchema {
        let column_names: HashSet<String> = columns.iter().map(|c| c.name.clone()).collect();
        let table_schema = Arc::new(TableSchema::new(
            TableId(1),
            TableName::new("public".to_owned(), "test_table".to_owned()),
            columns,
        ));
        let replication_mask = ReplicationMask::build_or_all(&table_schema, &column_names);

        ReplicatedTableSchema::from_masks(table_schema, replication_mask, identity_mask)
    }

    #[test]
    fn postgres_to_bigquery_type_basic_types() {
        assert_eq!(postgres_to_bigquery_type(&Type::BOOL), "bool");
        assert_eq!(postgres_to_bigquery_type(&Type::TEXT), "string");
        assert_eq!(postgres_to_bigquery_type(&Type::INT2), "int64");
        assert_eq!(postgres_to_bigquery_type(&Type::INT4), "int64");
        assert_eq!(postgres_to_bigquery_type(&Type::INT8), "int64");
        assert_eq!(postgres_to_bigquery_type(&Type::FLOAT4), "float64");
        assert_eq!(postgres_to_bigquery_type(&Type::FLOAT8), "float64");
        assert_eq!(postgres_to_bigquery_type(&Type::NUMERIC), "bignumeric");
        assert_eq!(postgres_to_bigquery_type(&Type::MONEY), "string");
        assert_eq!(postgres_to_bigquery_type(&Type::OID), "int64");
        assert_eq!(postgres_to_bigquery_type(&Type::DATE), "date");
        assert_eq!(postgres_to_bigquery_type(&Type::TIME), "time");
        assert_eq!(postgres_to_bigquery_type(&Type::TIMETZ), "string");
        assert_eq!(postgres_to_bigquery_type(&Type::TIMESTAMP), "datetime");
        assert_eq!(postgres_to_bigquery_type(&Type::TIMESTAMPTZ), "timestamp");
        assert_eq!(postgres_to_bigquery_type(&Type::INTERVAL), "string");
        assert_eq!(postgres_to_bigquery_type(&Type::JSON), "json");
        assert_eq!(postgres_to_bigquery_type(&Type::BYTEA), "bytes");
    }

    #[test]
    fn postgres_to_bigquery_type_array_types() {
        assert_eq!(postgres_to_bigquery_type(&Type::BOOL_ARRAY), "array<bool>");
        assert_eq!(postgres_to_bigquery_type(&Type::TEXT_ARRAY), "array<string>");
        assert_eq!(postgres_to_bigquery_type(&Type::INT2_ARRAY), "array<int64>");
        assert_eq!(postgres_to_bigquery_type(&Type::INT4_ARRAY), "array<int64>");
        assert_eq!(postgres_to_bigquery_type(&Type::INT8_ARRAY), "array<int64>");
        assert_eq!(postgres_to_bigquery_type(&Type::FLOAT4_ARRAY), "array<float64>");
        assert_eq!(postgres_to_bigquery_type(&Type::FLOAT8_ARRAY), "array<float64>");
        assert_eq!(postgres_to_bigquery_type(&Type::NUMERIC_ARRAY), "array<bignumeric>");
        assert_eq!(postgres_to_bigquery_type(&Type::MONEY_ARRAY), "array<string>");
        assert_eq!(postgres_to_bigquery_type(&Type::OID_ARRAY), "array<int64>");
        assert_eq!(postgres_to_bigquery_type(&Type::DATE_ARRAY), "array<date>");
        assert_eq!(postgres_to_bigquery_type(&Type::TIME_ARRAY), "array<time>");
        assert_eq!(postgres_to_bigquery_type(&Type::TIMETZ_ARRAY), "array<string>");
        assert_eq!(postgres_to_bigquery_type(&Type::TIMESTAMP_ARRAY), "array<datetime>");
        assert_eq!(postgres_to_bigquery_type(&Type::TIMESTAMPTZ_ARRAY), "array<timestamp>");
        assert_eq!(postgres_to_bigquery_type(&Type::INTERVAL_ARRAY), "array<string>");
        assert_eq!(postgres_to_bigquery_type(&Type::INET_ARRAY), "array<string>");
        assert_eq!(postgres_to_bigquery_type(&Type::INT4_RANGE_ARRAY), "array<string>");
    }

    #[test]
    fn column_spec_generates_sql_column_definition() {
        let column_schema = test_column("test_col", Type::TEXT, 1, true, None);
        let spec = column_spec(&column_schema).expect("column spec generation");
        assert_eq!(spec, "`test_col` string");

        let not_null_column = test_column("id", Type::INT4, 1, false, Some(1));
        let not_null_spec = column_spec(&not_null_column).expect("not null column spec");
        assert_eq!(not_null_spec, "`id` int64 not null");

        let array_column = test_column("tags", Type::TEXT_ARRAY, 1, false, None);
        let array_spec = column_spec(&array_column).expect("array column spec");
        assert_eq!(array_spec, "`tags` array<string>");
    }

    #[test]
    fn column_spec_includes_supported_default() {
        let column_schema = ColumnSchema::new("status".to_owned(), Type::TEXT, -1, 1, true)
            .with_default_expression("'pending'::text".to_owned());

        let spec = column_spec(&column_schema).expect("column spec generation");

        assert_eq!(spec, "`status` string default 'pending'");
    }

    #[test]
    fn default_expression_renders_portable_expressions() {
        let cases = [
            (Type::TEXT, "true", "'true'"),
            (Type::BOOL, "'true'::text", "true"),
            (Type::MONEY, "42", "'42'"),
            (Type::DATE, "'2026-01-01'::date", "DATE '2026-01-01'"),
            (Type::TIME, "'12:30:00'::time", "TIME '12:30:00'"),
            (Type::TIMETZ, "'12:30:00+02'::timetz", "'12:30:00+02'"),
            (Type::INTERVAL, "'30 days'::interval", "'30 days'"),
            (Type::TIMESTAMP, "'2026-01-01 12:30:00'::timestamp", "DATETIME '2026-01-01 12:30:00'"),
            (Type::JSONB, "'{}'::jsonb", "JSON '{}'"),
            (
                Type::UUID,
                "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid",
                "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'",
            ),
        ];

        for (typ, expression, expected) in cases {
            assert_eq!(default_expression_sql(expression, &typ).as_deref(), Some(expected));
        }
    }

    #[test]
    fn default_expression_rejects_bigquery_unsupported_expressions() {
        let cases = [
            (Type::INT4, "10 + 5"),
            (Type::INT4, "'abc'::text"),
            (Type::TIMESTAMPTZ, "now() + interval '30 days'"),
            (Type::UUID, "gen_random_uuid()"),
            (Type::TIMESTAMPTZ, "now()"),
            (Type::TIMESTAMP, "localtimestamp"),
            (Type::TEXT, "current_date"),
            (Type::DATE, "current_time"),
            (Type::TEXT, "lower('USER'::text)"),
        ];

        for (typ, expression) in cases {
            assert_eq!(default_expression_sql(expression, &typ), None);
            assert!(!supports_column_default(expression, &typ));
        }
    }

    #[test]
    fn column_spec_escapes_backticks() {
        let column_schema = test_column("pwn`name", Type::TEXT, 1, true, None);

        let spec = column_spec(&column_schema).expect("escaped column spec");

        assert_eq!(spec, "`pwn\\`name` string");
    }

    #[test]
    fn add_primary_key_clause_generates_bigquery_constraint() {
        let columns_with_pk = vec![
            test_column("id", Type::INT4, 1, false, Some(1)),
            test_column("name", Type::TEXT, 2, true, None),
        ];
        let schema_with_pk = test_replicated_schema(columns_with_pk);
        let pk_clause = add_primary_key_clause(&schema_with_pk).expect("pk clause").unwrap();
        assert_eq!(pk_clause, ", primary key (`id`) not enforced");

        // Composite primary key with correct ordinal positions.
        let columns_with_composite_pk = vec![
            test_column("tenant_id", Type::INT4, 1, false, Some(1)),
            test_column("id", Type::INT4, 2, false, Some(2)),
            test_column("name", Type::TEXT, 3, true, None),
        ];
        let schema_with_composite_pk = test_replicated_schema(columns_with_composite_pk);
        let composite_pk_clause = add_primary_key_clause(&schema_with_composite_pk)
            .unwrap()
            .expect("composite pk clause");
        assert_eq!(composite_pk_clause, ", primary key (`tenant_id`,`id`) not enforced");

        // BigQuery declares composite primary keys in primary-key ordinal
        // order, even when the table's physical column order differs.
        let columns_with_reversed_pk = vec![
            test_column("id", Type::INT4, 1, false, Some(2)),
            test_column("tenant_id", Type::INT4, 2, false, Some(1)),
            test_column("name", Type::TEXT, 3, true, None),
        ];
        let schema_with_reversed_pk = test_replicated_schema(columns_with_reversed_pk);
        let reversed_pk_clause =
            add_primary_key_clause(&schema_with_reversed_pk).unwrap().expect("reversed pk clause");
        assert_eq!(reversed_pk_clause, ", primary key (`tenant_id`,`id`) not enforced");

        let columns_with_full_identity = vec![
            test_column("id", Type::INT4, 1, false, Some(1)),
            test_column("name", Type::TEXT, 2, true, None),
        ];
        let schema_with_full_identity = test_replicated_schema_with_identity(
            columns_with_full_identity,
            IdentityMask::from_bytes(vec![1, 1]),
        );
        let full_identity_pk_clause = add_primary_key_clause(&schema_with_full_identity)
            .unwrap()
            .expect("full identity pk clause");
        assert_eq!(full_identity_pk_clause, ", primary key (`id`) not enforced");

        let columns_no_pk = vec![
            test_column("name", Type::TEXT, 1, true, None),
            test_column("age", Type::INT4, 2, true, None),
        ];
        let schema_no_pk = test_replicated_schema(columns_no_pk);
        let no_pk_clause = add_primary_key_clause(&schema_no_pk).expect("no pk clause");
        assert!(no_pk_clause.is_none());
    }

    #[test]
    fn column_schemas_to_table_descriptor_generates_storage_schema() {
        let columns = vec![
            test_column("id", Type::INT4, 1, false, Some(1)),
            test_column("name", Type::TEXT, 2, true, None),
            test_column("active", Type::BOOL, 3, false, None),
            test_column("tags", Type::TEXT_ARRAY, 4, false, None),
        ];
        let schema = test_replicated_schema(columns);

        let descriptor = column_schemas_to_table_descriptor(&schema, true);

        assert_eq!(descriptor.field_descriptors.len(), 6);

        assert_eq!(descriptor.field_descriptors[0].name, "id");
        assert!(matches!(descriptor.field_descriptors[0].typ, ColumnType::Int32));
        assert!(matches!(descriptor.field_descriptors[0].mode, ColumnMode::Nullable));

        assert_eq!(descriptor.field_descriptors[1].name, "name");
        assert!(matches!(descriptor.field_descriptors[1].typ, ColumnType::String));
        assert!(matches!(descriptor.field_descriptors[1].mode, ColumnMode::Nullable));

        assert_eq!(descriptor.field_descriptors[2].name, "active");
        assert!(matches!(descriptor.field_descriptors[2].typ, ColumnType::Bool));
        assert!(matches!(descriptor.field_descriptors[2].mode, ColumnMode::Nullable));

        assert_eq!(descriptor.field_descriptors[3].name, "tags");
        assert!(matches!(descriptor.field_descriptors[3].typ, ColumnType::String));
        assert!(matches!(descriptor.field_descriptors[3].mode, ColumnMode::Repeated));

        assert_eq!(descriptor.field_descriptors[4].name, BIGQUERY_CDC_SPECIAL_COLUMN);
        assert!(matches!(descriptor.field_descriptors[4].typ, ColumnType::String));
        assert!(matches!(descriptor.field_descriptors[4].mode, ColumnMode::Required));

        assert_eq!(descriptor.field_descriptors[5].name, BIGQUERY_CDC_SEQUENCE_COLUMN);
        assert!(matches!(descriptor.field_descriptors[5].typ, ColumnType::String));
        assert!(matches!(descriptor.field_descriptors[5].mode, ColumnMode::Required));

        let field_numbers: Vec<u32> =
            descriptor.field_descriptors.iter().map(|field| field.number).collect();
        assert_eq!(field_numbers, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn column_schemas_to_table_descriptor_preserves_required_mode_for_table_copy() {
        let columns = vec![
            test_column("id", Type::INT4, 1, false, Some(1)),
            test_column("name", Type::TEXT, 2, true, None),
        ];
        let schema = test_replicated_schema(columns);

        let descriptor = column_schemas_to_table_descriptor(&schema, false);

        assert!(matches!(descriptor.field_descriptors[0].mode, ColumnMode::Required));
        assert!(matches!(descriptor.field_descriptors[1].mode, ColumnMode::Nullable));
        assert_eq!(descriptor.field_descriptors.len(), 3);
    }

    #[test]
    fn column_schemas_to_table_descriptor_maps_complex_types() {
        let columns = vec![
            test_column("uuid_col", Type::UUID, 1, true, None),
            test_column("json_col", Type::JSON, 2, true, None),
            test_column("bytea_col", Type::BYTEA, 3, true, None),
            test_column("numeric_col", Type::NUMERIC, 4, true, None),
            test_column("date_col", Type::DATE, 5, true, None),
            test_column("time_col", Type::TIME, 6, true, None),
            test_column("timestamp_col", Type::TIMESTAMP, 7, true, None),
            test_column("timestamptz_col", Type::TIMESTAMPTZ, 8, true, None),
            test_column("timestamp_array_col", Type::TIMESTAMP_ARRAY, 9, true, None),
            test_column("timestamptz_array_col", Type::TIMESTAMPTZ_ARRAY, 10, true, None),
        ];
        let schema = test_replicated_schema(columns);

        let descriptor = column_schemas_to_table_descriptor(&schema, true);

        assert_eq!(descriptor.field_descriptors.len(), 12);

        assert!(matches!(descriptor.field_descriptors[0].typ, ColumnType::String));
        assert!(matches!(descriptor.field_descriptors[1].typ, ColumnType::String));
        assert!(matches!(descriptor.field_descriptors[2].typ, ColumnType::Bytes));
        assert!(matches!(descriptor.field_descriptors[3].typ, ColumnType::String));
        assert!(matches!(descriptor.field_descriptors[4].typ, ColumnType::String));
        assert!(matches!(descriptor.field_descriptors[5].typ, ColumnType::String));
        assert!(matches!(descriptor.field_descriptors[6].typ, ColumnType::String));
        assert!(matches!(descriptor.field_descriptors[7].typ, ColumnType::Int64));
        assert!(matches!(descriptor.field_descriptors[8].typ, ColumnType::String));
        assert!(matches!(descriptor.field_descriptors[8].mode, ColumnMode::Repeated));
        assert!(matches!(descriptor.field_descriptors[9].typ, ColumnType::Int64));
        assert!(matches!(descriptor.field_descriptors[9].mode, ColumnMode::Repeated));
    }

    #[test]
    fn create_append_only_columns_spec_appends_flat_metadata_without_pk() {
        let columns = vec![
            test_column("id", Type::INT4, 1, false, Some(1)),
            test_column("name", Type::TEXT, 2, true, None),
        ];
        let schema = test_replicated_schema(columns);

        let spec = create_append_only_columns_spec(&schema).unwrap();

        assert_eq!(
            spec,
            "(`id` int64,`name` string,`_etl_uuid` string not null,`_etl_source_timestamp` int64 \
             not null,`_etl_change_sequence_number` string not null,`_etl_change_type` string not \
             null,`_etl_sort_keys` array<string>)"
        );
        assert!(!spec.contains("primary key"));
    }

    #[test]
    fn append_only_table_descriptor_appends_flat_metadata_fields() {
        let columns = vec![
            test_column("id", Type::INT4, 1, false, Some(1)),
            test_column("payload", Type::JSONB, 2, true, None),
        ];
        let schema = test_replicated_schema(columns);

        let descriptor = append_only_table_descriptor(&schema);

        assert_eq!(descriptor.field_descriptors.len(), 7);
        assert_eq!(descriptor.field_descriptors[0].name, "id");
        assert!(matches!(descriptor.field_descriptors[0].mode, ColumnMode::Nullable));
        assert_eq!(descriptor.field_descriptors[1].name, "payload");
        assert!(matches!(descriptor.field_descriptors[1].typ, ColumnType::String));
        assert!(matches!(descriptor.field_descriptors[1].mode, ColumnMode::Nullable));

        let metadata = &descriptor.field_descriptors[2..];
        assert_eq!(metadata[0].name, APPEND_ONLY_UUID_COLUMN);
        assert!(matches!(metadata[0].typ, ColumnType::String));
        assert!(matches!(metadata[0].mode, ColumnMode::Required));
        assert_eq!(metadata[1].name, APPEND_ONLY_SOURCE_TIMESTAMP_COLUMN);
        assert!(matches!(metadata[1].typ, ColumnType::Int64));
        assert!(matches!(metadata[1].mode, ColumnMode::Required));
        assert_eq!(metadata[2].name, APPEND_ONLY_CHANGE_SEQUENCE_NUMBER_COLUMN);
        assert!(matches!(metadata[2].typ, ColumnType::String));
        assert!(matches!(metadata[2].mode, ColumnMode::Required));
        assert_eq!(metadata[3].name, APPEND_ONLY_CHANGE_TYPE_COLUMN);
        assert!(matches!(metadata[3].typ, ColumnType::String));
        assert!(matches!(metadata[3].mode, ColumnMode::Required));
        assert_eq!(metadata[4].name, APPEND_ONLY_SORT_KEYS_COLUMN);
        assert!(matches!(metadata[4].typ, ColumnType::String));
        assert!(matches!(metadata[4].mode, ColumnMode::Repeated));

        let field_numbers: Vec<u32> =
            descriptor.field_descriptors.iter().map(|field| field.number).collect();
        assert_eq!(field_numbers, vec![1, 2, 3, 4, 5, 6, 7]);
    }
}
