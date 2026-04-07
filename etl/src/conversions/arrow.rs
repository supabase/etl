use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, BooleanArray, BooleanBuilder, Date32Array,
    FixedSizeBinaryArray, FixedSizeBinaryBuilder, Float32Array, Float64Array, Int32Array,
    Int64Array, LargeBinaryArray, LargeBinaryBuilder, ListArray, ListBuilder, PrimitiveBuilder,
    RecordBatch, StringArray, StringBuilder, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{
    DataType, Date32Type, Field, FieldRef, Float32Type, Float64Type, Int32Type, Int64Type, Schema,
    Time64MicrosecondType, TimeUnit, TimestampMicrosecondType,
};
use arrow::error::ArrowError;
use chrono::{NaiveDate, NaiveTime};

use crate::error::{ErrorKind, EtlResult};
use crate::etl_error;
use crate::types::{ArrayCell, Cell, TableArrowBatch, TableRow, TableSchema, Type, is_array_type};

pub const UNIX_EPOCH: NaiveDate =
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("unix epoch is a valid date");

const MIDNIGHT: NaiveTime = NaiveTime::from_hms_opt(0, 0, 0).expect("midnight is a valid time");
const UUID_BYTE_WIDTH: i32 = 16;

/// Converts a source table schema to the canonical Arrow schema used across destinations.
pub fn table_schema_to_arrow_schema(table_schema: &TableSchema) -> Schema {
    Schema::new(
        table_schema
            .column_schemas
            .iter()
            .map(|column_schema| {
                Field::new(
                    &column_schema.name,
                    postgres_type_to_arrow_type(&column_schema.typ),
                    column_schema.nullable,
                )
            })
            .collect::<Vec<_>>(),
    )
}

/// Converts table rows to a destination-facing Arrow batch.
pub fn table_rows_to_arrow_batch(
    table_schema: Arc<TableSchema>,
    table_rows: &[TableRow],
) -> EtlResult<TableArrowBatch> {
    let schema = table_schema_to_arrow_schema(&table_schema);
    let batch = rows_to_record_batch(table_rows, schema).map_err(arrow_error_to_etl_error)?;

    Ok(TableArrowBatch::new(table_schema.id, table_schema, batch))
}

/// Converts rows to an Arrow record batch.
pub fn rows_to_record_batch(rows: &[TableRow], schema: Schema) -> Result<RecordBatch, ArrowError> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for (field_idx, field) in schema.fields().iter().enumerate() {
        arrays.push(build_array_for_field(rows, field_idx, field.data_type()));
    }

    RecordBatch::try_new(Arc::new(schema), arrays)
}

/// Converts an Arrow record batch back to ETL table rows.
pub fn record_batch_to_table_rows(batch: &RecordBatch) -> Vec<TableRow> {
    let mut rows = Vec::with_capacity(batch.num_rows());

    for row_idx in 0..batch.num_rows() {
        let mut cells = Vec::with_capacity(batch.num_columns());

        for column in batch.columns() {
            cells.push(arrow_value_to_cell(column, row_idx));
        }

        rows.push(TableRow::new(cells));
    }

    rows
}

fn arrow_error_to_etl_error(error: ArrowError) -> crate::error::EtlError {
    etl_error!(
        ErrorKind::ConversionError,
        "Arrow conversion failed",
        error.to_string(),
        source: error
    )
}

fn postgres_type_to_arrow_type(typ: &Type) -> DataType {
    if is_array_type(typ) {
        return DataType::List(Arc::new(Field::new(
            "item",
            postgres_array_element_type_to_arrow_type(typ),
            true,
        )));
    }

    postgres_scalar_type_to_arrow_type(typ)
}

fn postgres_scalar_type_to_arrow_type(typ: &Type) -> DataType {
    match typ {
        &Type::BOOL => DataType::Boolean,
        &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT => DataType::Utf8,
        &Type::INT2 | &Type::INT4 => DataType::Int32,
        &Type::INT8 | &Type::OID => DataType::Int64,
        &Type::FLOAT4 => DataType::Float32,
        &Type::FLOAT8 => DataType::Float64,
        &Type::NUMERIC | &Type::JSON | &Type::JSONB => DataType::Utf8,
        &Type::DATE => DataType::Date32,
        &Type::TIME => DataType::Time64(TimeUnit::Microsecond),
        &Type::TIMESTAMP => DataType::Timestamp(TimeUnit::Microsecond, None),
        &Type::TIMESTAMPTZ => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        &Type::UUID => DataType::FixedSizeBinary(UUID_BYTE_WIDTH),
        &Type::BYTEA => DataType::LargeBinary,
        _ => DataType::Utf8,
    }
}

fn postgres_array_element_type_to_arrow_type(typ: &Type) -> DataType {
    match typ {
        &Type::BOOL_ARRAY => DataType::Boolean,
        &Type::CHAR_ARRAY
        | &Type::BPCHAR_ARRAY
        | &Type::VARCHAR_ARRAY
        | &Type::NAME_ARRAY
        | &Type::TEXT_ARRAY
        | &Type::NUMERIC_ARRAY
        | &Type::JSON_ARRAY
        | &Type::JSONB_ARRAY => DataType::Utf8,
        &Type::INT2_ARRAY | &Type::INT4_ARRAY => DataType::Int32,
        &Type::INT8_ARRAY | &Type::OID_ARRAY => DataType::Int64,
        &Type::FLOAT4_ARRAY => DataType::Float32,
        &Type::FLOAT8_ARRAY => DataType::Float64,
        &Type::DATE_ARRAY => DataType::Date32,
        &Type::TIME_ARRAY => DataType::Time64(TimeUnit::Microsecond),
        &Type::TIMESTAMP_ARRAY => DataType::Timestamp(TimeUnit::Microsecond, None),
        &Type::TIMESTAMPTZ_ARRAY => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        &Type::UUID_ARRAY => DataType::FixedSizeBinary(UUID_BYTE_WIDTH),
        &Type::BYTEA_ARRAY => DataType::LargeBinary,
        _ => DataType::Utf8,
    }
}

fn build_array_for_field(rows: &[TableRow], field_idx: usize, data_type: &DataType) -> ArrayRef {
    match data_type {
        DataType::Boolean => build_boolean_array(rows, field_idx),
        DataType::Int32 => build_primitive_array::<Int32Type, _>(rows, field_idx, cell_to_i32),
        DataType::Int64 => build_primitive_array::<Int64Type, _>(rows, field_idx, cell_to_i64),
        DataType::Float32 => build_primitive_array::<Float32Type, _>(rows, field_idx, cell_to_f32),
        DataType::Float64 => build_primitive_array::<Float64Type, _>(rows, field_idx, cell_to_f64),
        DataType::Utf8 => build_string_array(rows, field_idx),
        DataType::LargeBinary => build_binary_array(rows, field_idx),
        DataType::Date32 => build_primitive_array::<Date32Type, _>(rows, field_idx, cell_to_date32),
        DataType::Time64(TimeUnit::Microsecond) => {
            build_primitive_array::<Time64MicrosecondType, _>(rows, field_idx, cell_to_time64)
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
            build_timestamptz_array(rows, field_idx, tz)
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            build_primitive_array::<TimestampMicrosecondType, _>(rows, field_idx, cell_to_timestamp)
        }
        DataType::FixedSizeBinary(UUID_BYTE_WIDTH) => build_uuid_array(rows, field_idx),
        DataType::List(field) => build_list_array(rows, field_idx, field.clone()),
        _ => build_string_array(rows, field_idx),
    }
}

fn build_primitive_array<T, F>(rows: &[TableRow], field_idx: usize, converter: F) -> ArrayRef
where
    T: ArrowPrimitiveType,
    F: Fn(&Cell) -> Option<T::Native>,
{
    let mut builder = PrimitiveBuilder::<T>::with_capacity(rows.len());

    for row in rows {
        builder.append_option(converter(&row.values()[field_idx]));
    }

    Arc::new(builder.finish())
}

fn build_boolean_array(rows: &[TableRow], field_idx: usize) -> ArrayRef {
    let mut builder = BooleanBuilder::new();

    for row in rows {
        builder.append_option(cell_to_bool(&row.values()[field_idx]));
    }

    Arc::new(builder.finish())
}

fn build_string_array(rows: &[TableRow], field_idx: usize) -> ArrayRef {
    let mut builder = StringBuilder::new();

    for row in rows {
        builder.append_option(cell_to_string(&row.values()[field_idx]));
    }

    Arc::new(builder.finish())
}

fn build_binary_array(rows: &[TableRow], field_idx: usize) -> ArrayRef {
    let mut builder = LargeBinaryBuilder::new();

    for row in rows {
        match cell_to_bytes(&row.values()[field_idx]) {
            Some(bytes) => builder.append_value(bytes),
            None => builder.append_null(),
        }
    }

    Arc::new(builder.finish())
}

fn build_timestamptz_array(rows: &[TableRow], field_idx: usize, tz: &str) -> ArrayRef {
    let mut builder = arrow::array::TimestampMicrosecondBuilder::new().with_timezone(tz);

    for row in rows {
        builder.append_option(cell_to_timestamptz(&row.values()[field_idx]));
    }

    Arc::new(builder.finish())
}

fn build_uuid_array(rows: &[TableRow], field_idx: usize) -> ArrayRef {
    let mut builder = FixedSizeBinaryBuilder::new(UUID_BYTE_WIDTH);

    for row in rows {
        match cell_to_uuid(&row.values()[field_idx]) {
            Some(value) => builder
                .append_value(value)
                .expect("uuid byte width must be 16"),
            None => builder.append_null(),
        }
    }

    Arc::new(builder.finish())
}

fn build_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    match field.data_type() {
        DataType::Boolean => build_boolean_list_array(rows, field_idx, field),
        DataType::Int32 => build_int32_list_array(rows, field_idx, field),
        DataType::Int64 => build_int64_list_array(rows, field_idx, field),
        DataType::Float32 => build_float32_list_array(rows, field_idx, field),
        DataType::Float64 => build_float64_list_array(rows, field_idx, field),
        DataType::Utf8 => build_string_list_array(rows, field_idx, field),
        DataType::LargeBinary => build_binary_list_array(rows, field_idx, field),
        DataType::Date32 => build_date32_list_array(rows, field_idx, field),
        DataType::Time64(TimeUnit::Microsecond) => build_time64_list_array(rows, field_idx, field),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            build_timestamp_list_array(rows, field_idx, field)
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            build_timestamptz_list_array(rows, field_idx, field)
        }
        DataType::FixedSizeBinary(UUID_BYTE_WIDTH) => build_uuid_list_array(rows, field_idx, field),
        _ => build_list_array_for_strings(rows, field_idx, field),
    }
}

fn build_boolean_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let mut builder = ListBuilder::new(BooleanBuilder::new()).with_field(field.clone());

    for row in rows {
        match cell_to_array_cell(&row.values()[field_idx]) {
            Some(ArrayCell::Bool(values)) => {
                for value in values {
                    builder.values().append_option(*value);
                }
                builder.append(true);
            }
            Some(_) => return build_list_array_for_strings(rows, field_idx, field),
            None => builder.append_null(),
        }
    }

    Arc::new(builder.finish())
}

fn build_int32_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let mut builder =
        ListBuilder::new(PrimitiveBuilder::<Int32Type>::new()).with_field(field.clone());

    for row in rows {
        match cell_to_array_cell(&row.values()[field_idx]) {
            Some(ArrayCell::I16(values)) => {
                for value in values {
                    builder.values().append_option(value.map(i32::from));
                }
                builder.append(true);
            }
            Some(ArrayCell::I32(values)) => {
                for value in values {
                    builder.values().append_option(*value);
                }
                builder.append(true);
            }
            Some(_) => return build_list_array_for_strings(rows, field_idx, field),
            None => builder.append_null(),
        }
    }

    Arc::new(builder.finish())
}

fn build_int64_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let mut builder =
        ListBuilder::new(PrimitiveBuilder::<Int64Type>::new()).with_field(field.clone());

    for row in rows {
        match cell_to_array_cell(&row.values()[field_idx]) {
            Some(ArrayCell::I64(values)) => {
                for value in values {
                    builder.values().append_option(*value);
                }
                builder.append(true);
            }
            Some(ArrayCell::U32(values)) => {
                for value in values {
                    builder.values().append_option(value.map(i64::from));
                }
                builder.append(true);
            }
            Some(_) => return build_list_array_for_strings(rows, field_idx, field),
            None => builder.append_null(),
        }
    }

    Arc::new(builder.finish())
}

fn build_float32_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let mut builder =
        ListBuilder::new(PrimitiveBuilder::<Float32Type>::new()).with_field(field.clone());

    for row in rows {
        match cell_to_array_cell(&row.values()[field_idx]) {
            Some(ArrayCell::F32(values)) => {
                for value in values {
                    builder.values().append_option(*value);
                }
                builder.append(true);
            }
            Some(_) => return build_list_array_for_strings(rows, field_idx, field),
            None => builder.append_null(),
        }
    }

    Arc::new(builder.finish())
}

fn build_float64_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let mut builder =
        ListBuilder::new(PrimitiveBuilder::<Float64Type>::new()).with_field(field.clone());

    for row in rows {
        match cell_to_array_cell(&row.values()[field_idx]) {
            Some(ArrayCell::F64(values)) => {
                for value in values {
                    builder.values().append_option(*value);
                }
                builder.append(true);
            }
            Some(_) => return build_list_array_for_strings(rows, field_idx, field),
            None => builder.append_null(),
        }
    }

    Arc::new(builder.finish())
}

fn build_string_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let mut builder = ListBuilder::new(StringBuilder::new()).with_field(field.clone());

    for row in rows {
        match cell_to_array_cell(&row.values()[field_idx]) {
            Some(ArrayCell::String(values)) => {
                for value in values {
                    match value {
                        Some(value) => builder.values().append_value(value),
                        None => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Some(ArrayCell::Numeric(values)) => {
                for value in values {
                    match value {
                        Some(value) => builder.values().append_value(value.to_string()),
                        None => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Some(ArrayCell::Json(values)) => {
                for value in values {
                    match value {
                        Some(value) => builder.values().append_value(value.to_string()),
                        None => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Some(_) => return build_list_array_for_strings(rows, field_idx, field),
            None => builder.append_null(),
        }
    }

    Arc::new(builder.finish())
}

fn build_binary_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let mut builder = ListBuilder::new(LargeBinaryBuilder::new()).with_field(field.clone());

    for row in rows {
        match cell_to_array_cell(&row.values()[field_idx]) {
            Some(ArrayCell::Bytes(values)) => {
                for value in values {
                    match value {
                        Some(value) => builder.values().append_value(value),
                        None => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Some(_) => return build_list_array_for_strings(rows, field_idx, field),
            None => builder.append_null(),
        }
    }

    Arc::new(builder.finish())
}

fn build_date32_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let mut builder =
        ListBuilder::new(PrimitiveBuilder::<Date32Type>::new()).with_field(field.clone());

    for row in rows {
        match cell_to_array_cell(&row.values()[field_idx]) {
            Some(ArrayCell::Date(values)) => {
                for value in values {
                    builder.values().append_option(
                        value.map(|date| date.signed_duration_since(UNIX_EPOCH).num_days() as i32),
                    );
                }
                builder.append(true);
            }
            Some(_) => return build_list_array_for_strings(rows, field_idx, field),
            None => builder.append_null(),
        }
    }

    Arc::new(builder.finish())
}

fn build_time64_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let mut builder = ListBuilder::new(PrimitiveBuilder::<Time64MicrosecondType>::new())
        .with_field(field.clone());

    for row in rows {
        match cell_to_array_cell(&row.values()[field_idx]) {
            Some(ArrayCell::Time(values)) => {
                for value in values {
                    builder.values().append_option(
                        value.and_then(|time| {
                            time.signed_duration_since(MIDNIGHT).num_microseconds()
                        }),
                    );
                }
                builder.append(true);
            }
            Some(_) => return build_list_array_for_strings(rows, field_idx, field),
            None => builder.append_null(),
        }
    }

    Arc::new(builder.finish())
}

fn build_timestamp_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let mut builder = ListBuilder::new(PrimitiveBuilder::<TimestampMicrosecondType>::new())
        .with_field(field.clone());

    for row in rows {
        match cell_to_array_cell(&row.values()[field_idx]) {
            Some(ArrayCell::Timestamp(values)) => {
                for value in values {
                    builder.values().append_option(
                        value.map(|timestamp| timestamp.and_utc().timestamp_micros()),
                    );
                }
                builder.append(true);
            }
            Some(_) => return build_list_array_for_strings(rows, field_idx, field),
            None => builder.append_null(),
        }
    }

    Arc::new(builder.finish())
}

fn build_timestamptz_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let tz = if let DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) = field.data_type() {
        tz.clone()
    } else {
        Arc::from("UTC".to_string())
    };
    let mut builder =
        ListBuilder::new(arrow::array::TimestampMicrosecondBuilder::new().with_timezone(tz))
            .with_field(field.clone());

    for row in rows {
        match cell_to_array_cell(&row.values()[field_idx]) {
            Some(ArrayCell::TimestampTz(values)) => {
                for value in values {
                    builder
                        .values()
                        .append_option(value.map(|timestamp| timestamp.timestamp_micros()));
                }
                builder.append(true);
            }
            Some(_) => return build_list_array_for_strings(rows, field_idx, field),
            None => builder.append_null(),
        }
    }

    Arc::new(builder.finish())
}

fn build_uuid_list_array(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let mut builder =
        ListBuilder::new(FixedSizeBinaryBuilder::new(UUID_BYTE_WIDTH)).with_field(field.clone());

    for row in rows {
        match cell_to_array_cell(&row.values()[field_idx]) {
            Some(ArrayCell::Uuid(values)) => {
                for value in values {
                    match value {
                        Some(value) => builder
                            .values()
                            .append_value(value.as_bytes())
                            .expect("uuid byte width must be 16"),
                        None => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Some(_) => return build_list_array_for_strings(rows, field_idx, field),
            None => builder.append_null(),
        }
    }

    Arc::new(builder.finish())
}

fn build_list_array_for_strings(rows: &[TableRow], field_idx: usize, field: FieldRef) -> ArrayRef {
    let field = Arc::new(
        Field::new(field.name(), DataType::Utf8, field.is_nullable())
            .with_metadata(field.metadata().clone()),
    );
    let mut builder = ListBuilder::new(StringBuilder::new()).with_field(field);

    for row in rows {
        match cell_to_array_cell(&row.values()[field_idx]) {
            Some(array_cell) => {
                append_array_cell_as_strings(&mut builder, array_cell);
                builder.append(true);
            }
            None => builder.append_null(),
        }
    }

    Arc::new(builder.finish())
}

fn append_array_cell_as_strings(builder: &mut ListBuilder<StringBuilder>, array_cell: &ArrayCell) {
    match array_cell {
        ArrayCell::Bool(values) => append_option_strings(
            builder,
            values
                .iter()
                .map(|value| value.map(|value| value.to_string())),
        ),
        ArrayCell::String(values) => append_option_strings(builder, values.iter().cloned()),
        ArrayCell::I16(values) => append_option_strings(
            builder,
            values
                .iter()
                .map(|value| value.map(|value| value.to_string())),
        ),
        ArrayCell::I32(values) => append_option_strings(
            builder,
            values
                .iter()
                .map(|value| value.map(|value| value.to_string())),
        ),
        ArrayCell::U32(values) => append_option_strings(
            builder,
            values
                .iter()
                .map(|value| value.map(|value| value.to_string())),
        ),
        ArrayCell::I64(values) => append_option_strings(
            builder,
            values
                .iter()
                .map(|value| value.map(|value| value.to_string())),
        ),
        ArrayCell::F32(values) => append_option_strings(
            builder,
            values
                .iter()
                .map(|value| value.map(|value| value.to_string())),
        ),
        ArrayCell::F64(values) => append_option_strings(
            builder,
            values
                .iter()
                .map(|value| value.map(|value| value.to_string())),
        ),
        ArrayCell::Numeric(values) => append_option_strings(
            builder,
            values
                .iter()
                .map(|value| value.as_ref().map(ToString::to_string)),
        ),
        ArrayCell::Date(values) => append_option_strings(
            builder,
            values
                .iter()
                .map(|value| value.map(|value| value.to_string())),
        ),
        ArrayCell::Time(values) => append_option_strings(
            builder,
            values
                .iter()
                .map(|value| value.map(|value| value.to_string())),
        ),
        ArrayCell::Timestamp(values) => append_option_strings(
            builder,
            values
                .iter()
                .map(|value| value.map(|value| value.to_string())),
        ),
        ArrayCell::TimestampTz(values) => append_option_strings(
            builder,
            values
                .iter()
                .map(|value| value.map(|value| value.to_rfc3339())),
        ),
        ArrayCell::Uuid(values) => append_option_strings(
            builder,
            values
                .iter()
                .map(|value| value.map(|value| value.to_string())),
        ),
        ArrayCell::Json(values) => append_option_strings(
            builder,
            values
                .iter()
                .map(|value| value.as_ref().map(ToString::to_string)),
        ),
        ArrayCell::Bytes(values) => append_option_strings(
            builder,
            values.iter().map(|value| {
                value
                    .as_ref()
                    .map(|value| String::from_utf8_lossy(value).to_string())
            }),
        ),
    }
}

fn append_option_strings<I>(builder: &mut ListBuilder<StringBuilder>, values: I)
where
    I: IntoIterator<Item = Option<String>>,
{
    for value in values {
        match value {
            Some(value) => builder.values().append_value(value),
            None => builder.values().append_null(),
        }
    }
}

fn cell_to_bool(cell: &Cell) -> Option<bool> {
    match cell {
        Cell::Bool(value) => Some(*value),
        _ => None,
    }
}

fn cell_to_i32(cell: &Cell) -> Option<i32> {
    match cell {
        Cell::I16(value) => Some(i32::from(*value)),
        Cell::I32(value) => Some(*value),
        _ => None,
    }
}

fn cell_to_i64(cell: &Cell) -> Option<i64> {
    match cell {
        Cell::I64(value) => Some(*value),
        Cell::U32(value) => Some(i64::from(*value)),
        _ => None,
    }
}

fn cell_to_f32(cell: &Cell) -> Option<f32> {
    match cell {
        Cell::F32(value) => Some(*value),
        _ => None,
    }
}

fn cell_to_f64(cell: &Cell) -> Option<f64> {
    match cell {
        Cell::F64(value) => Some(*value),
        _ => None,
    }
}

fn cell_to_bytes(cell: &Cell) -> Option<Vec<u8>> {
    match cell {
        Cell::Bytes(value) => Some(value.clone()),
        _ => None,
    }
}

fn cell_to_date32(cell: &Cell) -> Option<i32> {
    match cell {
        Cell::Date(value) => Some(value.signed_duration_since(UNIX_EPOCH).num_days() as i32),
        _ => None,
    }
}

fn cell_to_time64(cell: &Cell) -> Option<i64> {
    match cell {
        Cell::Time(value) => value.signed_duration_since(MIDNIGHT).num_microseconds(),
        _ => None,
    }
}

fn cell_to_timestamp(cell: &Cell) -> Option<i64> {
    match cell {
        Cell::Timestamp(value) => Some(value.and_utc().timestamp_micros()),
        _ => None,
    }
}

fn cell_to_timestamptz(cell: &Cell) -> Option<i64> {
    match cell {
        Cell::TimestampTz(value) => Some(value.timestamp_micros()),
        _ => None,
    }
}

fn cell_to_uuid(cell: &Cell) -> Option<&[u8; UUID_BYTE_WIDTH as usize]> {
    match cell {
        Cell::Uuid(value) => Some(value.as_bytes()),
        _ => None,
    }
}

fn cell_to_string(cell: &Cell) -> Option<String> {
    match cell {
        Cell::Null => None,
        Cell::String(value) => Some(value.clone()),
        Cell::Numeric(value) => Some(value.to_string()),
        Cell::Json(value) => Some(value.to_string()),
        Cell::Bytes(value) => Some(String::from_utf8_lossy(value).to_string()),
        Cell::Array(_) => None,
        _ => None,
    }
}

fn cell_to_array_cell(cell: &Cell) -> Option<&ArrayCell> {
    match cell {
        Cell::Array(array_cell) => Some(array_cell),
        _ => None,
    }
}

fn arrow_value_to_cell(array: &ArrayRef, row_idx: usize) -> Cell {
    if array.is_null(row_idx) {
        return Cell::Null;
    }

    match array.data_type() {
        DataType::Boolean => {
            let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Cell::Bool(array.value(row_idx))
        }
        DataType::Int32 => {
            let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Cell::I32(array.value(row_idx))
        }
        DataType::Int64 => {
            let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Cell::I64(array.value(row_idx))
        }
        DataType::Float32 => {
            let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Cell::F32(array.value(row_idx))
        }
        DataType::Float64 => {
            let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Cell::F64(array.value(row_idx))
        }
        DataType::Utf8 => {
            let array = array.as_any().downcast_ref::<StringArray>().unwrap();
            Cell::String(array.value(row_idx).to_string())
        }
        DataType::LargeBinary => {
            let array = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            Cell::Bytes(array.value(row_idx).to_vec())
        }
        DataType::Date32 => {
            let array = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = array.value(row_idx);
            Cell::Date(days_from_epoch_to_date(days))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let array = array
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .unwrap();
            Cell::Time(micros_to_time(array.value(row_idx)))
        }
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let micros = array.value(row_idx);

            if timezone.is_some() {
                Cell::TimestampTz(
                    chrono::DateTime::from_timestamp_micros(micros).expect("valid timestamp"),
                )
            } else {
                Cell::Timestamp(
                    chrono::DateTime::from_timestamp_micros(micros)
                        .expect("valid timestamp")
                        .naive_utc(),
                )
            }
        }
        DataType::FixedSizeBinary(UUID_BYTE_WIDTH) => {
            let array = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            let bytes: [u8; 16] = array.value(row_idx).try_into().expect("uuid bytes");
            Cell::Uuid(uuid::Uuid::from_bytes(bytes))
        }
        DataType::List(field) => arrow_list_value_to_cell(array, row_idx, field),
        _ => {
            let array = array.as_any().downcast_ref::<StringArray>().unwrap();
            Cell::String(array.value(row_idx).to_string())
        }
    }
}

fn arrow_list_value_to_cell(array: &ArrayRef, row_idx: usize, field: &FieldRef) -> Cell {
    let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
    let list_value = list_array.value(row_idx);

    match field.data_type() {
        DataType::Boolean => Cell::Array(ArrayCell::Bool(option_values::<BooleanArray, _, _>(
            &list_value,
            |array, index| array.value(index),
        ))),
        DataType::Int32 => Cell::Array(ArrayCell::I32(option_values::<Int32Array, _, _>(
            &list_value,
            |array, index| array.value(index),
        ))),
        DataType::Int64 => Cell::Array(ArrayCell::I64(option_values::<Int64Array, _, _>(
            &list_value,
            |array, index| array.value(index),
        ))),
        DataType::Float32 => Cell::Array(ArrayCell::F32(option_values::<Float32Array, _, _>(
            &list_value,
            |array, index| array.value(index),
        ))),
        DataType::Float64 => Cell::Array(ArrayCell::F64(option_values::<Float64Array, _, _>(
            &list_value,
            |array, index| array.value(index),
        ))),
        DataType::Utf8 => Cell::Array(ArrayCell::String(option_values::<StringArray, _, _>(
            &list_value,
            |array, index| array.value(index).to_string(),
        ))),
        DataType::LargeBinary => {
            Cell::Array(ArrayCell::Bytes(option_values::<LargeBinaryArray, _, _>(
                &list_value,
                |array, index| array.value(index).to_vec(),
            )))
        }
        DataType::Date32 => Cell::Array(ArrayCell::Date(option_values::<Date32Array, _, _>(
            &list_value,
            |array, index| days_from_epoch_to_date(array.value(index)),
        ))),
        DataType::Time64(TimeUnit::Microsecond) => {
            Cell::Array(ArrayCell::Time(
                option_values::<Time64MicrosecondArray, _, _>(&list_value, |array, index| {
                    micros_to_time(array.value(index))
                }),
            ))
        }
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            let array = list_value
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            if timezone.is_some() {
                let mut values = Vec::with_capacity(array.len());
                for index in 0..array.len() {
                    if array.is_null(index) {
                        values.push(None);
                    } else {
                        values.push(Some(
                            chrono::DateTime::from_timestamp_micros(array.value(index))
                                .expect("valid timestamp"),
                        ));
                    }
                }
                Cell::Array(ArrayCell::TimestampTz(values))
            } else {
                let mut values = Vec::with_capacity(array.len());
                for index in 0..array.len() {
                    if array.is_null(index) {
                        values.push(None);
                    } else {
                        values.push(Some(
                            chrono::DateTime::from_timestamp_micros(array.value(index))
                                .expect("valid timestamp")
                                .naive_utc(),
                        ));
                    }
                }
                Cell::Array(ArrayCell::Timestamp(values))
            }
        }
        DataType::FixedSizeBinary(UUID_BYTE_WIDTH) => {
            let array = list_value
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            let mut values = Vec::with_capacity(array.len());
            for index in 0..array.len() {
                if array.is_null(index) {
                    values.push(None);
                } else {
                    let bytes: [u8; 16] = array.value(index).try_into().expect("uuid bytes");
                    values.push(Some(uuid::Uuid::from_bytes(bytes)));
                }
            }
            Cell::Array(ArrayCell::Uuid(values))
        }
        _ => Cell::Array(ArrayCell::String(option_values::<StringArray, _, _>(
            &list_value,
            |array, index| array.value(index).to_string(),
        ))),
    }
}

fn option_values<A, T, F>(array: &ArrayRef, get: F) -> Vec<Option<T>>
where
    A: 'static + arrow::array::Array,
    F: Fn(&A, usize) -> T,
{
    let array = array.as_any().downcast_ref::<A>().unwrap();
    let mut values = Vec::with_capacity(array.len());

    for index in 0..array.len() {
        if array.is_null(index) {
            values.push(None);
        } else {
            values.push(Some(get(array, index)));
        }
    }

    values
}

fn days_from_epoch_to_date(days: i32) -> NaiveDate {
    if days >= 0 {
        UNIX_EPOCH
            .checked_add_days(chrono::Days::new(days as u64))
            .expect("valid positive date")
    } else {
        UNIX_EPOCH
            .checked_sub_days(chrono::Days::new((-days) as u64))
            .expect("valid negative date")
    }
}

fn micros_to_time(micros: i64) -> NaiveTime {
    NaiveTime::from_num_seconds_from_midnight_opt(
        (micros / 1_000_000) as u32,
        ((micros % 1_000_000) * 1_000) as u32,
    )
    .expect("valid microsecond time")
}
