use std::sync::Arc;

use arrow::record_batch::RecordBatch as EtlRecordBatch;
use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use etl::types::TableSchema;

use crate::ducklake::encoding::PreparedRows;

type DuckDbRecordBatch = duckdb::arrow::record_batch::RecordBatch;

/// Converts one ETL Arrow record batch into DuckDB's Arrow type.
pub(super) fn prepare_arrow_rows(batch: &EtlRecordBatch) -> EtlResult<PreparedRows> {
    prepare_arrow_row_batches([batch])
}

/// Converts multiple ETL Arrow record batches into DuckDB's Arrow type.
pub(super) fn prepare_arrow_row_batches<'a, I>(batches: I) -> EtlResult<PreparedRows>
where
    I: IntoIterator<Item = &'a EtlRecordBatch>,
{
    let duckdb_batches: Vec<DuckDbRecordBatch> = batches
        .into_iter()
        .filter(|batch| batch.num_rows() > 0)
        .cloned()
        .collect();

    Ok(PreparedRows::Arrow(duckdb_batches))
}

/// Projects a record batch down to primary-key columns only.
pub(super) fn project_primary_key_batch(
    table_schema: &TableSchema,
    batch: &EtlRecordBatch,
) -> EtlResult<EtlRecordBatch> {
    if !table_schema.has_primary_keys() {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "DuckLake delete requires a primary key",
            format!("Table '{}' has no primary key columns", table_schema.name)
        ));
    }

    let projected_fields = table_schema
        .column_schemas
        .iter()
        .enumerate()
        .filter(|(_, column_schema)| column_schema.primary)
        .map(|(index, _)| batch.schema().field(index).clone())
        .collect::<Vec<_>>();
    let projected_columns = table_schema
        .column_schemas
        .iter()
        .enumerate()
        .filter(|(_, column_schema)| column_schema.primary)
        .map(|(index, _)| batch.column(index).clone())
        .collect::<Vec<_>>();

    arrow::record_batch::RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(projected_fields)),
        projected_columns,
    )
    .map_err(|error| {
        etl_error!(
            ErrorKind::ConversionError,
            "DuckLake primary-key projection failed",
            error.to_string(),
            source: error
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{FixedSizeBinaryArray, Int32Array, ListArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    #[test]
    fn test_prepare_arrow_rows_round_trips_nested_and_binary_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("uuid", DataType::FixedSizeBinary(16), true),
            Field::new(
                "scores",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
            Field::new("payload", DataType::LargeBinary, true),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]));
        let uuid_values = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![Some(*b"1234567890abcdef"), None].into_iter(),
            16,
        )
        .unwrap();
        let scores = ListArray::from_iter_primitive::<arrow::datatypes::Int32Type, _, _>([
            Some(vec![Some(1), None, Some(3)]),
            None,
        ]);
        let payload = arrow::array::LargeBinaryArray::from(vec![Some(b"abc".as_slice()), None]);
        let created_at = arrow::array::TimestampMicrosecondArray::from(vec![Some(42_i64), None])
            .with_timezone("UTC");
        let batch = EtlRecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as _,
                Arc::new(StringArray::from(vec![Some("alice"), None])) as _,
                Arc::new(uuid_values) as _,
                Arc::new(scores) as _,
                Arc::new(payload) as _,
                Arc::new(created_at) as _,
            ],
        )
        .unwrap();

        let PreparedRows::Arrow(batches) = prepare_arrow_rows(&batch).unwrap() else {
            unreachable!();
        };

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].schema().fields().len(), 6);
    }

    #[test]
    fn test_project_primary_key_batch_keeps_only_primary_columns() {
        let table_schema = TableSchema::new(
            etl::types::TableId::new(1),
            etl::types::TableName::new("public".to_string(), "users".to_string()),
            vec![
                etl::types::ColumnSchema::new(
                    "tenant_id".to_string(),
                    etl::types::Type::INT4,
                    -1,
                    false,
                    true,
                ),
                etl::types::ColumnSchema::new(
                    "id".to_string(),
                    etl::types::Type::INT4,
                    -1,
                    false,
                    true,
                ),
                etl::types::ColumnSchema::new(
                    "name".to_string(),
                    etl::types::Type::TEXT,
                    -1,
                    true,
                    false,
                ),
            ],
        );
        let batch = EtlRecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("tenant_id", DataType::Int32, false),
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![7])) as _,
                Arc::new(Int32Array::from(vec![42])) as _,
                Arc::new(StringArray::from(vec![Some("alice")])) as _,
            ],
        )
        .unwrap();

        let projected = project_primary_key_batch(&table_schema, &batch).unwrap();

        assert_eq!(projected.num_columns(), 2);
        assert_eq!(projected.schema().field(0).name(), "tenant_id");
        assert_eq!(projected.schema().field(1).name(), "id");
    }
}
