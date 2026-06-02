use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, ArrowPrimitiveType, BooleanArray, BooleanBuilder, Date32Array,
        FixedSizeBinaryArray, FixedSizeBinaryBuilder, Float32Array, Float64Array, Int32Array,
        Int64Array, LargeBinaryArray, LargeBinaryBuilder, ListArray, ListBuilder, PrimitiveBuilder,
        RecordBatch, StringArray, StringBuilder, Time64MicrosecondArray, TimestampMicrosecondArray,
        TimestampMicrosecondBuilder,
    },
    datatypes::{
        DataType, Date32Type, Field, FieldRef, Float32Type, Float64Type, Int32Type, Int64Type,
        Schema, Time64MicrosecondType, TimeUnit, TimestampMicrosecondType,
    },
    error::ArrowError,
};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

use crate::{
    bail,
    conversions::{
        bool::parse_bool,
        hex,
        table_row::parse_postgres_copy_row_fields,
        text::{parse_cell_from_postgres_text, parse_postgres_timestamptz_micros},
    },
    error::{ErrorKind, EtlResult},
    etl_error,
    types::{
        ArrayCell, Cell, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, TableArrowBatch, TableRow,
        TableSchema, Type, is_array_type,
    },
};

pub const UNIX_EPOCH: NaiveDate =
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("unix epoch is a valid date");

const MIDNIGHT: NaiveTime = NaiveTime::from_hms_opt(0, 0, 0).expect("midnight is a valid time");
const UUID_BYTE_WIDTH: i32 = 16;

/// Converts a source table schema to the canonical Arrow schema used across
/// destinations.
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
    let batch = if table_schema.column_schemas.iter().any(|column| is_array_type(&column.typ)) {
        rows_to_record_batch(table_rows, schema).map_err(arrow_error_to_etl_error)?
    } else {
        scalar_rows_to_record_batch(&table_schema, table_rows, schema)?
    };

    Ok(TableArrowBatch::new(table_schema.id, table_schema, batch))
}

fn scalar_rows_to_record_batch(
    table_schema: &TableSchema,
    table_rows: &[TableRow],
    schema: Schema,
) -> EtlResult<RecordBatch> {
    let mut builders = table_schema
        .column_schemas
        .iter()
        .map(|column_schema| ScalarColumnBuilder::new(&column_schema.typ, table_rows.len()))
        .collect::<Vec<_>>();

    for table_row in table_rows {
        append_scalar_row(&mut builders, table_schema, table_row)?;
    }

    let arrays =
        builders.into_iter().map(ScalarColumnBuilder::finish).collect::<EtlResult<Vec<_>>>()?;
    RecordBatch::try_new(Arc::new(schema), arrays).map_err(arrow_error_to_etl_error)
}

/// Incrementally builds a scalar-only [`TableArrowBatch`].
pub(crate) struct ScalarTableArrowBatchBuilder {
    table_schema: Arc<TableSchema>,
    schema: Schema,
    builders: Vec<ScalarColumnBuilder>,
}

impl ScalarTableArrowBatchBuilder {
    /// Creates a builder when every table column has a scalar Arrow type.
    pub(crate) fn try_new(table_schema: Arc<TableSchema>, row_capacity: usize) -> Option<Self> {
        if table_schema.column_schemas.iter().any(|column| is_array_type(&column.typ)) {
            return None;
        }

        let schema = table_schema_to_arrow_schema(&table_schema);
        let builders = table_schema
            .column_schemas
            .iter()
            .map(|column_schema| ScalarColumnBuilder::new(&column_schema.typ, row_capacity))
            .collect();

        Some(Self { table_schema, schema, builders })
    }

    /// Appends one row after the caller has validated the row shape.
    pub(crate) fn append_trusted_row(&mut self, table_row: &TableRow) -> EtlResult<()> {
        debug_assert_eq!(table_row.values().len(), self.builders.len());
        for (builder, cell) in self.builders.iter_mut().zip(table_row.values()) {
            builder.append_trusted(cell)?;
        }

        Ok(())
    }

    /// Finishes the builder into a [`TableArrowBatch`].
    pub(crate) fn finish(self) -> EtlResult<TableArrowBatch> {
        let arrays = self
            .builders
            .into_iter()
            .map(ScalarColumnBuilder::finish)
            .collect::<EtlResult<Vec<_>>>()?;
        let batch = RecordBatch::try_new(Arc::new(self.schema), arrays)
            .map_err(arrow_error_to_etl_error)?;

        Ok(TableArrowBatch::new(self.table_schema.id, self.table_schema, batch))
    }
}

/// Appends a scalar table row to column builders.
fn append_scalar_row(
    builders: &mut [ScalarColumnBuilder],
    table_schema: &TableSchema,
    table_row: &TableRow,
) -> EtlResult<()> {
    if table_row.values().len() != builders.len() {
        bail!(
            ErrorKind::ConversionError,
            "Table row width does not match schema",
            format!("Expected {} row values, got {}", builders.len(), table_row.values().len())
        );
    }

    for ((builder, column_schema), cell) in
        builders.iter_mut().zip(table_schema.column_schemas.iter()).zip(table_row.values())
    {
        builder.append(&column_schema.typ, cell)?;
    }

    Ok(())
}

/// Converts Postgres COPY text rows directly to a destination-facing Arrow
/// batch.
pub(crate) fn postgres_copy_rows_to_arrow_batch<R>(
    table_schema: Arc<TableSchema>,
    copy_rows: &[R],
) -> EtlResult<TableArrowBatch>
where
    R: AsRef<[u8]>,
{
    let schema = table_schema_to_arrow_schema(&table_schema);
    let mut builders = table_schema
        .column_schemas
        .iter()
        .map(|column_schema| CopyColumnBuilder::new(&column_schema.typ, copy_rows.len()))
        .collect::<Vec<_>>();

    for copy_row in copy_rows {
        parse_postgres_copy_row_fields(
            copy_row.as_ref(),
            table_schema.column_schemas.iter(),
            |field_idx, column_schema, value| {
                builders[field_idx].append(&column_schema.typ, value).map_err(|error| {
                    etl_error!(
                        ErrorKind::ConversionError,
                        "Arrow COPY conversion failed",
                        format!(
                            "Column '{}' of type '{}' failed to convert",
                            column_schema.name, column_schema.typ
                        ),
                        source: error
                    )
                })
            },
        )?;
    }

    let arrays =
        builders.into_iter().map(CopyColumnBuilder::finish).collect::<EtlResult<Vec<_>>>()?;
    let batch = RecordBatch::try_new(Arc::new(schema), arrays).map_err(arrow_error_to_etl_error)?;

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

enum ScalarColumnBuilder {
    Boolean(BooleanBuilder),
    Int32(PrimitiveBuilder<Int32Type>),
    Int64(PrimitiveBuilder<Int64Type>),
    Float32(PrimitiveBuilder<Float32Type>),
    Float64(PrimitiveBuilder<Float64Type>),
    Utf8(StringBuilder),
    LargeBinary(LargeBinaryBuilder),
    Date32(PrimitiveBuilder<Date32Type>),
    Time64(PrimitiveBuilder<Time64MicrosecondType>),
    Timestamp(PrimitiveBuilder<TimestampMicrosecondType>),
    TimestampTz(TimestampMicrosecondBuilder),
    Uuid(FixedSizeBinaryBuilder),
}

impl ScalarColumnBuilder {
    fn new(typ: &Type, _row_capacity: usize) -> Self {
        match *typ {
            Type::BOOL => Self::Boolean(BooleanBuilder::new()),
            Type::INT2 | Type::INT4 => Self::Int32(PrimitiveBuilder::<Int32Type>::new()),
            Type::INT8 | Type::OID => Self::Int64(PrimitiveBuilder::<Int64Type>::new()),
            Type::FLOAT4 => Self::Float32(PrimitiveBuilder::<Float32Type>::new()),
            Type::FLOAT8 => Self::Float64(PrimitiveBuilder::<Float64Type>::new()),
            Type::BYTEA => Self::LargeBinary(LargeBinaryBuilder::new()),
            Type::DATE => Self::Date32(PrimitiveBuilder::<Date32Type>::new()),
            Type::TIME => Self::Time64(PrimitiveBuilder::<Time64MicrosecondType>::new()),
            Type::TIMESTAMP => Self::Timestamp(PrimitiveBuilder::<TimestampMicrosecondType>::new()),
            Type::TIMESTAMPTZ => {
                Self::TimestampTz(TimestampMicrosecondBuilder::new().with_timezone("UTC"))
            }
            Type::UUID => Self::Uuid(FixedSizeBinaryBuilder::new(UUID_BYTE_WIDTH)),
            _ => Self::Utf8(StringBuilder::new()),
        }
    }

    fn append(&mut self, typ: &Type, cell: &Cell) -> EtlResult<()> {
        if matches!(cell, Cell::Null) {
            self.append_null();
            return Ok(());
        }

        match self {
            Self::Boolean(builder) => builder.append_option(cell_to_bool(cell)),
            Self::Int32(builder) => builder.append_option(cell_to_i32(cell)),
            Self::Int64(builder) => builder.append_option(cell_to_i64(cell)),
            Self::Float32(builder) => builder.append_option(cell_to_f32(cell)),
            Self::Float64(builder) => builder.append_option(cell_to_f64(cell)),
            Self::Utf8(builder) => append_cell_as_utf8(builder, typ, cell)?,
            Self::LargeBinary(builder) => match cell {
                Cell::Bytes(value) => builder.append_value(value),
                _ => builder.append_null(),
            },
            Self::Date32(builder) => builder.append_option(cell_to_date32(cell)),
            Self::Time64(builder) => builder.append_option(cell_to_time64(cell)),
            Self::Timestamp(builder) => builder.append_option(cell_to_timestamp(cell)),
            Self::TimestampTz(builder) => builder.append_option(cell_to_timestamptz(cell)),
            Self::Uuid(builder) => match cell {
                Cell::Uuid(value) => {
                    builder.append_value(value.as_bytes()).map_err(arrow_error_to_etl_error)?;
                }
                _ => builder.append_null(),
            },
        }

        Ok(())
    }

    fn append_trusted(&mut self, cell: &Cell) -> EtlResult<()> {
        if matches!(cell, Cell::Null) {
            self.append_null();
            return Ok(());
        }

        match self {
            Self::Boolean(builder) => builder.append_option(cell_to_bool(cell)),
            Self::Int32(builder) => builder.append_option(cell_to_i32(cell)),
            Self::Int64(builder) => builder.append_option(cell_to_i64(cell)),
            Self::Float32(builder) => builder.append_option(cell_to_f32(cell)),
            Self::Float64(builder) => builder.append_option(cell_to_f64(cell)),
            Self::Utf8(builder) => append_trusted_cell_as_utf8(builder, cell)?,
            Self::LargeBinary(builder) => match cell {
                Cell::Bytes(value) => builder.append_value(value),
                _ => builder.append_null(),
            },
            Self::Date32(builder) => builder.append_option(cell_to_date32(cell)),
            Self::Time64(builder) => builder.append_option(cell_to_time64(cell)),
            Self::Timestamp(builder) => builder.append_option(cell_to_timestamp(cell)),
            Self::TimestampTz(builder) => builder.append_option(cell_to_timestamptz(cell)),
            Self::Uuid(builder) => match cell {
                Cell::Uuid(value) => {
                    builder.append_value(value.as_bytes()).map_err(arrow_error_to_etl_error)?;
                }
                _ => builder.append_null(),
            },
        }

        Ok(())
    }

    fn append_null(&mut self) {
        match self {
            Self::Boolean(builder) => builder.append_null(),
            Self::Int32(builder) => builder.append_null(),
            Self::Int64(builder) => builder.append_null(),
            Self::Float32(builder) => builder.append_null(),
            Self::Float64(builder) => builder.append_null(),
            Self::Utf8(builder) => builder.append_null(),
            Self::LargeBinary(builder) => builder.append_null(),
            Self::Date32(builder) => builder.append_null(),
            Self::Time64(builder) => builder.append_null(),
            Self::Timestamp(builder) => builder.append_null(),
            Self::TimestampTz(builder) => builder.append_null(),
            Self::Uuid(builder) => builder.append_null(),
        }
    }

    fn finish(self) -> EtlResult<ArrayRef> {
        Ok(match self {
            Self::Boolean(mut builder) => Arc::new(builder.finish()),
            Self::Int32(mut builder) => Arc::new(builder.finish()),
            Self::Int64(mut builder) => Arc::new(builder.finish()),
            Self::Float32(mut builder) => Arc::new(builder.finish()),
            Self::Float64(mut builder) => Arc::new(builder.finish()),
            Self::Utf8(mut builder) => Arc::new(builder.finish()),
            Self::LargeBinary(mut builder) => Arc::new(builder.finish()),
            Self::Date32(mut builder) => Arc::new(builder.finish()),
            Self::Time64(mut builder) => Arc::new(builder.finish()),
            Self::Timestamp(mut builder) => Arc::new(builder.finish()),
            Self::TimestampTz(mut builder) => Arc::new(builder.finish()),
            Self::Uuid(mut builder) => Arc::new(builder.finish()),
        })
    }
}

fn append_cell_as_utf8(builder: &mut StringBuilder, typ: &Type, cell: &Cell) -> EtlResult<()> {
    match cell {
        Cell::String(value) => builder.append_value(value),
        Cell::Numeric(value) => builder.append_value(value.to_string()),
        Cell::Json(value) => builder.append_value(value.to_string()),
        Cell::Bytes(value) => builder.append_value(String::from_utf8_lossy(value).as_ref()),
        _ if matches!(*typ, Type::NUMERIC | Type::JSON | Type::JSONB) => {
            builder.append_option(cell_to_string(cell));
        }
        _ => builder.append_null(),
    }

    Ok(())
}

fn append_trusted_cell_as_utf8(builder: &mut StringBuilder, cell: &Cell) -> EtlResult<()> {
    match cell {
        Cell::String(value) => builder.append_value(value),
        Cell::Numeric(value) => builder.append_value(value.to_string()),
        Cell::Json(value) => builder.append_value(value.to_string()),
        Cell::Bytes(value) => builder.append_value(String::from_utf8_lossy(value).as_ref()),
        _ => builder.append_null(),
    }

    Ok(())
}

enum CopyColumnBuilder {
    Boolean(BooleanBuilder),
    Int32(PrimitiveBuilder<Int32Type>),
    Int64(PrimitiveBuilder<Int64Type>),
    Float32(PrimitiveBuilder<Float32Type>),
    Float64(PrimitiveBuilder<Float64Type>),
    Utf8(StringBuilder),
    LargeBinary(LargeBinaryBuilder),
    Date32(PrimitiveBuilder<Date32Type>),
    Time64(PrimitiveBuilder<Time64MicrosecondType>),
    Timestamp(PrimitiveBuilder<TimestampMicrosecondType>),
    TimestampTz(TimestampMicrosecondBuilder),
    Uuid(FixedSizeBinaryBuilder),
    BooleanList(ListBuilder<BooleanBuilder>),
    Int32List(ListBuilder<PrimitiveBuilder<Int32Type>>),
    Int64List(ListBuilder<PrimitiveBuilder<Int64Type>>),
    Float32List(ListBuilder<PrimitiveBuilder<Float32Type>>),
    Float64List(ListBuilder<PrimitiveBuilder<Float64Type>>),
    Utf8List(ListBuilder<StringBuilder>),
    LargeBinaryList(ListBuilder<LargeBinaryBuilder>),
    Date32List(ListBuilder<PrimitiveBuilder<Date32Type>>),
    Time64List(ListBuilder<PrimitiveBuilder<Time64MicrosecondType>>),
    TimestampList(ListBuilder<PrimitiveBuilder<TimestampMicrosecondType>>),
    TimestampTzList(ListBuilder<TimestampMicrosecondBuilder>),
    UuidList(ListBuilder<FixedSizeBinaryBuilder>),
}

impl CopyColumnBuilder {
    fn new(typ: &Type, _row_capacity: usize) -> Self {
        match *typ {
            Type::BOOL => Self::Boolean(BooleanBuilder::new()),
            Type::INT2 | Type::INT4 => Self::Int32(PrimitiveBuilder::<Int32Type>::new()),
            Type::INT8 | Type::OID => Self::Int64(PrimitiveBuilder::<Int64Type>::new()),
            Type::FLOAT4 => Self::Float32(PrimitiveBuilder::<Float32Type>::new()),
            Type::FLOAT8 => Self::Float64(PrimitiveBuilder::<Float64Type>::new()),
            Type::BYTEA => Self::LargeBinary(LargeBinaryBuilder::new()),
            Type::DATE => Self::Date32(PrimitiveBuilder::<Date32Type>::new()),
            Type::TIME => Self::Time64(PrimitiveBuilder::<Time64MicrosecondType>::new()),
            Type::TIMESTAMP => Self::Timestamp(PrimitiveBuilder::<TimestampMicrosecondType>::new()),
            Type::TIMESTAMPTZ => {
                Self::TimestampTz(TimestampMicrosecondBuilder::new().with_timezone("UTC"))
            }
            Type::UUID => Self::Uuid(FixedSizeBinaryBuilder::new(UUID_BYTE_WIDTH)),
            Type::BOOL_ARRAY => Self::BooleanList(
                ListBuilder::new(BooleanBuilder::new()).with_field(list_field(DataType::Boolean)),
            ),
            Type::INT2_ARRAY | Type::INT4_ARRAY => Self::Int32List(
                ListBuilder::new(PrimitiveBuilder::<Int32Type>::new())
                    .with_field(list_field(DataType::Int32)),
            ),
            Type::INT8_ARRAY | Type::OID_ARRAY => Self::Int64List(
                ListBuilder::new(PrimitiveBuilder::<Int64Type>::new())
                    .with_field(list_field(DataType::Int64)),
            ),
            Type::FLOAT4_ARRAY => Self::Float32List(
                ListBuilder::new(PrimitiveBuilder::<Float32Type>::new())
                    .with_field(list_field(DataType::Float32)),
            ),
            Type::FLOAT8_ARRAY => Self::Float64List(
                ListBuilder::new(PrimitiveBuilder::<Float64Type>::new())
                    .with_field(list_field(DataType::Float64)),
            ),
            Type::BYTEA_ARRAY => Self::LargeBinaryList(
                ListBuilder::new(LargeBinaryBuilder::new())
                    .with_field(list_field(DataType::LargeBinary)),
            ),
            Type::DATE_ARRAY => Self::Date32List(
                ListBuilder::new(PrimitiveBuilder::<Date32Type>::new())
                    .with_field(list_field(DataType::Date32)),
            ),
            Type::TIME_ARRAY => Self::Time64List(
                ListBuilder::new(PrimitiveBuilder::<Time64MicrosecondType>::new())
                    .with_field(list_field(DataType::Time64(TimeUnit::Microsecond))),
            ),
            Type::TIMESTAMP_ARRAY => Self::TimestampList(
                ListBuilder::new(PrimitiveBuilder::<TimestampMicrosecondType>::new())
                    .with_field(list_field(DataType::Timestamp(TimeUnit::Microsecond, None))),
            ),
            Type::TIMESTAMPTZ_ARRAY => Self::TimestampTzList(
                ListBuilder::new(TimestampMicrosecondBuilder::new().with_timezone("UTC"))
                    .with_field(list_field(DataType::Timestamp(
                        TimeUnit::Microsecond,
                        Some("UTC".into()),
                    ))),
            ),
            Type::UUID_ARRAY => Self::UuidList(
                ListBuilder::new(FixedSizeBinaryBuilder::new(UUID_BYTE_WIDTH))
                    .with_field(list_field(DataType::FixedSizeBinary(UUID_BYTE_WIDTH))),
            ),
            _ if is_array_type(typ) => Self::Utf8List(
                ListBuilder::new(StringBuilder::new()).with_field(list_field(DataType::Utf8)),
            ),
            _ => Self::Utf8(StringBuilder::new()),
        }
    }

    fn append(&mut self, typ: &Type, value: Option<&str>) -> EtlResult<()> {
        let Some(value) = value else {
            self.append_null();
            return Ok(());
        };

        match self {
            Self::Boolean(builder) => builder.append_value(parse_bool(value)?),
            Self::Int32(builder) => builder.append_value(parse_copy_i32(typ, value)?),
            Self::Int64(builder) => builder.append_value(parse_copy_i64(typ, value)?),
            Self::Float32(builder) => builder.append_value(value.parse::<f32>()?),
            Self::Float64(builder) => builder.append_value(value.parse::<f64>()?),
            Self::Utf8(builder) => append_copy_utf8(builder, typ, value)?,
            Self::LargeBinary(builder) => builder.append_value(hex::parse_bytea_hex_string(value)?),
            Self::Date32(builder) => builder.append_value(parse_copy_date32(value)?),
            Self::Time64(builder) => builder.append_value(parse_copy_time64(value)?),
            Self::Timestamp(builder) => builder.append_value(parse_copy_timestamp(value)?),
            Self::TimestampTz(builder) => builder.append_value(parse_copy_timestamptz(value)?),
            Self::Uuid(builder) => {
                let value = uuid::Uuid::parse_str(value)?;
                builder.append_value(value.as_bytes()).map_err(arrow_error_to_etl_error)?;
            }
            Self::BooleanList(builder) => append_copy_bool_list(builder, typ, value)?,
            Self::Int32List(builder) => append_copy_int32_list(builder, typ, value)?,
            Self::Int64List(builder) => append_copy_int64_list(builder, typ, value)?,
            Self::Float32List(builder) => append_copy_float32_list(builder, typ, value)?,
            Self::Float64List(builder) => append_copy_float64_list(builder, typ, value)?,
            Self::Utf8List(builder) => append_copy_utf8_list(builder, typ, value)?,
            Self::LargeBinaryList(builder) => append_copy_binary_list(builder, typ, value)?,
            Self::Date32List(builder) => append_copy_date32_list(builder, typ, value)?,
            Self::Time64List(builder) => append_copy_time64_list(builder, typ, value)?,
            Self::TimestampList(builder) => append_copy_timestamp_list(builder, typ, value)?,
            Self::TimestampTzList(builder) => append_copy_timestamptz_list(builder, typ, value)?,
            Self::UuidList(builder) => append_copy_uuid_list(builder, typ, value)?,
        }

        Ok(())
    }

    fn append_null(&mut self) {
        match self {
            Self::Boolean(builder) => builder.append_null(),
            Self::Int32(builder) => builder.append_null(),
            Self::Int64(builder) => builder.append_null(),
            Self::Float32(builder) => builder.append_null(),
            Self::Float64(builder) => builder.append_null(),
            Self::Utf8(builder) => builder.append_null(),
            Self::LargeBinary(builder) => builder.append_null(),
            Self::Date32(builder) => builder.append_null(),
            Self::Time64(builder) => builder.append_null(),
            Self::Timestamp(builder) => builder.append_null(),
            Self::TimestampTz(builder) => builder.append_null(),
            Self::Uuid(builder) => builder.append_null(),
            Self::BooleanList(builder) => builder.append_null(),
            Self::Int32List(builder) => builder.append_null(),
            Self::Int64List(builder) => builder.append_null(),
            Self::Float32List(builder) => builder.append_null(),
            Self::Float64List(builder) => builder.append_null(),
            Self::Utf8List(builder) => builder.append_null(),
            Self::LargeBinaryList(builder) => builder.append_null(),
            Self::Date32List(builder) => builder.append_null(),
            Self::Time64List(builder) => builder.append_null(),
            Self::TimestampList(builder) => builder.append_null(),
            Self::TimestampTzList(builder) => builder.append_null(),
            Self::UuidList(builder) => builder.append_null(),
        }
    }

    fn finish(self) -> EtlResult<ArrayRef> {
        Ok(match self {
            Self::Boolean(mut builder) => Arc::new(builder.finish()),
            Self::Int32(mut builder) => Arc::new(builder.finish()),
            Self::Int64(mut builder) => Arc::new(builder.finish()),
            Self::Float32(mut builder) => Arc::new(builder.finish()),
            Self::Float64(mut builder) => Arc::new(builder.finish()),
            Self::Utf8(mut builder) => Arc::new(builder.finish()),
            Self::LargeBinary(mut builder) => Arc::new(builder.finish()),
            Self::Date32(mut builder) => Arc::new(builder.finish()),
            Self::Time64(mut builder) => Arc::new(builder.finish()),
            Self::Timestamp(mut builder) => Arc::new(builder.finish()),
            Self::TimestampTz(mut builder) => Arc::new(builder.finish()),
            Self::Uuid(mut builder) => Arc::new(builder.finish()),
            Self::BooleanList(mut builder) => Arc::new(builder.finish()),
            Self::Int32List(mut builder) => Arc::new(builder.finish()),
            Self::Int64List(mut builder) => Arc::new(builder.finish()),
            Self::Float32List(mut builder) => Arc::new(builder.finish()),
            Self::Float64List(mut builder) => Arc::new(builder.finish()),
            Self::Utf8List(mut builder) => Arc::new(builder.finish()),
            Self::LargeBinaryList(mut builder) => Arc::new(builder.finish()),
            Self::Date32List(mut builder) => Arc::new(builder.finish()),
            Self::Time64List(mut builder) => Arc::new(builder.finish()),
            Self::TimestampList(mut builder) => Arc::new(builder.finish()),
            Self::TimestampTzList(mut builder) => Arc::new(builder.finish()),
            Self::UuidList(mut builder) => Arc::new(builder.finish()),
        })
    }
}

fn list_field(data_type: DataType) -> FieldRef {
    Arc::new(Field::new("item", data_type, true))
}

fn parse_copy_array_cell(typ: &Type, value: &str) -> EtlResult<ArrayCell> {
    match parse_cell_from_postgres_text(typ, value)? {
        Cell::Array(array_cell) => Ok(array_cell),
        _ => Err(etl_error!(
            ErrorKind::ConversionError,
            "Expected PostgreSQL array value while building Arrow list"
        )),
    }
}

fn unexpected_copy_array_cell_error(
    typ: &Type,
    expected: &'static str,
    actual: &ArrayCell,
) -> crate::error::EtlError {
    etl_error!(
        ErrorKind::ConversionError,
        "PostgreSQL array type did not match Arrow list builder",
        format!(
            "Column type '{}' expected {} array elements but parsed {} array elements",
            typ,
            expected,
            array_cell_kind(actual)
        )
    )
}

fn array_cell_kind(array_cell: &ArrayCell) -> &'static str {
    match array_cell {
        ArrayCell::Bool(_) => "boolean",
        ArrayCell::String(_) => "string",
        ArrayCell::I16(_) => "int16",
        ArrayCell::I32(_) => "int32",
        ArrayCell::U32(_) => "uint32",
        ArrayCell::I64(_) => "int64",
        ArrayCell::F32(_) => "float32",
        ArrayCell::F64(_) => "float64",
        ArrayCell::Numeric(_) => "numeric",
        ArrayCell::Date(_) => "date",
        ArrayCell::Time(_) => "time",
        ArrayCell::Timestamp(_) => "timestamp",
        ArrayCell::TimestampTz(_) => "timestamptz",
        ArrayCell::Uuid(_) => "uuid",
        ArrayCell::Json(_) => "json",
        ArrayCell::Bytes(_) => "bytes",
    }
}

fn append_copy_bool_list(
    builder: &mut ListBuilder<BooleanBuilder>,
    typ: &Type,
    value: &str,
) -> EtlResult<()> {
    match parse_copy_array_cell(typ, value)? {
        ArrayCell::Bool(values) => {
            for value in values {
                builder.values().append_option(value);
            }
            builder.append(true);
            Ok(())
        }
        actual => Err(unexpected_copy_array_cell_error(typ, "boolean", &actual)),
    }
}

fn append_copy_int32_list(
    builder: &mut ListBuilder<PrimitiveBuilder<Int32Type>>,
    typ: &Type,
    value: &str,
) -> EtlResult<()> {
    match parse_copy_array_cell(typ, value)? {
        ArrayCell::I16(values) => {
            for value in values {
                builder.values().append_option(value.map(i32::from));
            }
            builder.append(true);
            Ok(())
        }
        ArrayCell::I32(values) => {
            for value in values {
                builder.values().append_option(value);
            }
            builder.append(true);
            Ok(())
        }
        actual => Err(unexpected_copy_array_cell_error(typ, "int32", &actual)),
    }
}

fn append_copy_int64_list(
    builder: &mut ListBuilder<PrimitiveBuilder<Int64Type>>,
    typ: &Type,
    value: &str,
) -> EtlResult<()> {
    match parse_copy_array_cell(typ, value)? {
        ArrayCell::I64(values) => {
            for value in values {
                builder.values().append_option(value);
            }
            builder.append(true);
            Ok(())
        }
        ArrayCell::U32(values) => {
            for value in values {
                builder.values().append_option(value.map(i64::from));
            }
            builder.append(true);
            Ok(())
        }
        actual => Err(unexpected_copy_array_cell_error(typ, "int64", &actual)),
    }
}

fn append_copy_float32_list(
    builder: &mut ListBuilder<PrimitiveBuilder<Float32Type>>,
    typ: &Type,
    value: &str,
) -> EtlResult<()> {
    match parse_copy_array_cell(typ, value)? {
        ArrayCell::F32(values) => {
            for value in values {
                builder.values().append_option(value);
            }
            builder.append(true);
            Ok(())
        }
        actual => Err(unexpected_copy_array_cell_error(typ, "float32", &actual)),
    }
}

fn append_copy_float64_list(
    builder: &mut ListBuilder<PrimitiveBuilder<Float64Type>>,
    typ: &Type,
    value: &str,
) -> EtlResult<()> {
    match parse_copy_array_cell(typ, value)? {
        ArrayCell::F64(values) => {
            for value in values {
                builder.values().append_option(value);
            }
            builder.append(true);
            Ok(())
        }
        actual => Err(unexpected_copy_array_cell_error(typ, "float64", &actual)),
    }
}

fn append_copy_utf8_list(
    builder: &mut ListBuilder<StringBuilder>,
    typ: &Type,
    value: &str,
) -> EtlResult<()> {
    let array_cell = parse_copy_array_cell(typ, value)?;
    append_array_cell_as_strings(builder, &array_cell);
    builder.append(true);

    Ok(())
}

fn append_copy_binary_list(
    builder: &mut ListBuilder<LargeBinaryBuilder>,
    typ: &Type,
    value: &str,
) -> EtlResult<()> {
    match parse_copy_array_cell(typ, value)? {
        ArrayCell::Bytes(values) => {
            for value in values {
                match value {
                    Some(value) => builder.values().append_value(value),
                    None => builder.values().append_null(),
                }
            }
            builder.append(true);
            Ok(())
        }
        actual => Err(unexpected_copy_array_cell_error(typ, "bytes", &actual)),
    }
}

fn append_copy_date32_list(
    builder: &mut ListBuilder<PrimitiveBuilder<Date32Type>>,
    typ: &Type,
    value: &str,
) -> EtlResult<()> {
    match parse_copy_array_cell(typ, value)? {
        ArrayCell::Date(values) => {
            for value in values {
                builder.values().append_option(
                    value.map(|date| date.signed_duration_since(UNIX_EPOCH).num_days() as i32),
                );
            }
            builder.append(true);
            Ok(())
        }
        actual => Err(unexpected_copy_array_cell_error(typ, "date", &actual)),
    }
}

fn append_copy_time64_list(
    builder: &mut ListBuilder<PrimitiveBuilder<Time64MicrosecondType>>,
    typ: &Type,
    value: &str,
) -> EtlResult<()> {
    match parse_copy_array_cell(typ, value)? {
        ArrayCell::Time(values) => {
            for value in values {
                builder.values().append_option(
                    value
                        .map(|time| {
                            time.signed_duration_since(MIDNIGHT).num_microseconds().ok_or_else(
                                || {
                                    etl_error!(
                                        ErrorKind::ConversionError,
                                        "Time value is out of microsecond range"
                                    )
                                },
                            )
                        })
                        .transpose()?,
                );
            }
            builder.append(true);
            Ok(())
        }
        actual => Err(unexpected_copy_array_cell_error(typ, "time", &actual)),
    }
}

fn append_copy_timestamp_list(
    builder: &mut ListBuilder<PrimitiveBuilder<TimestampMicrosecondType>>,
    typ: &Type,
    value: &str,
) -> EtlResult<()> {
    match parse_copy_array_cell(typ, value)? {
        ArrayCell::Timestamp(values) => {
            for value in values {
                builder
                    .values()
                    .append_option(value.map(|timestamp| timestamp.and_utc().timestamp_micros()));
            }
            builder.append(true);
            Ok(())
        }
        actual => Err(unexpected_copy_array_cell_error(typ, "timestamp", &actual)),
    }
}

fn append_copy_timestamptz_list(
    builder: &mut ListBuilder<TimestampMicrosecondBuilder>,
    typ: &Type,
    value: &str,
) -> EtlResult<()> {
    match parse_copy_array_cell(typ, value)? {
        ArrayCell::TimestampTz(values) => {
            for value in values {
                builder.values().append_option(value.map(|timestamp| timestamp.timestamp_micros()));
            }
            builder.append(true);
            Ok(())
        }
        actual => Err(unexpected_copy_array_cell_error(typ, "timestamptz", &actual)),
    }
}

fn append_copy_uuid_list(
    builder: &mut ListBuilder<FixedSizeBinaryBuilder>,
    typ: &Type,
    value: &str,
) -> EtlResult<()> {
    match parse_copy_array_cell(typ, value)? {
        ArrayCell::Uuid(values) => {
            for value in values {
                match value {
                    Some(value) => builder
                        .values()
                        .append_value(value.as_bytes())
                        .map_err(arrow_error_to_etl_error)?,
                    None => builder.values().append_null(),
                }
            }
            builder.append(true);
            Ok(())
        }
        actual => Err(unexpected_copy_array_cell_error(typ, "uuid", &actual)),
    }
}

fn parse_copy_i32(typ: &Type, value: &str) -> EtlResult<i32> {
    match *typ {
        Type::INT2 => Ok(i32::from(value.parse::<i16>()?)),
        Type::INT4 => Ok(value.parse()?),
        _ => Ok(value.parse()?),
    }
}

fn parse_copy_i64(typ: &Type, value: &str) -> EtlResult<i64> {
    match *typ {
        Type::OID => Ok(i64::from(value.parse::<u32>()?)),
        _ => Ok(value.parse()?),
    }
}

fn append_copy_utf8(builder: &mut StringBuilder, _typ: &Type, value: &str) -> EtlResult<()> {
    builder.append_value(value);
    Ok(())
}

fn parse_copy_date32(value: &str) -> EtlResult<i32> {
    let value = NaiveDate::parse_from_str(value, DATE_FORMAT)?;
    Ok(value.signed_duration_since(UNIX_EPOCH).num_days() as i32)
}

fn parse_copy_time64(value: &str) -> EtlResult<i64> {
    let value = NaiveTime::parse_from_str(value, TIME_FORMAT)?;

    value.signed_duration_since(MIDNIGHT).num_microseconds().ok_or_else(|| {
        etl_error!(ErrorKind::ConversionError, "Time value is out of microsecond range")
    })
}

fn parse_copy_timestamp(value: &str) -> EtlResult<i64> {
    let value = NaiveDateTime::parse_from_str(value, TIMESTAMP_FORMAT)?;
    Ok(value.and_utc().timestamp_micros())
}

fn parse_copy_timestamptz(value: &str) -> EtlResult<i64> {
    parse_postgres_timestamptz_micros(value)
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
            Some(value) => builder.append_value(value).expect("uuid byte width must be 16"),
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
            values.iter().map(|value| value.map(|value| value.to_string())),
        ),
        ArrayCell::String(values) => append_option_strings(builder, values.iter().cloned()),
        ArrayCell::I16(values) => append_option_strings(
            builder,
            values.iter().map(|value| value.map(|value| value.to_string())),
        ),
        ArrayCell::I32(values) => append_option_strings(
            builder,
            values.iter().map(|value| value.map(|value| value.to_string())),
        ),
        ArrayCell::U32(values) => append_option_strings(
            builder,
            values.iter().map(|value| value.map(|value| value.to_string())),
        ),
        ArrayCell::I64(values) => append_option_strings(
            builder,
            values.iter().map(|value| value.map(|value| value.to_string())),
        ),
        ArrayCell::F32(values) => append_option_strings(
            builder,
            values.iter().map(|value| value.map(|value| value.to_string())),
        ),
        ArrayCell::F64(values) => append_option_strings(
            builder,
            values.iter().map(|value| value.map(|value| value.to_string())),
        ),
        ArrayCell::Numeric(values) => append_option_strings(
            builder,
            values.iter().map(|value| value.as_ref().map(ToString::to_string)),
        ),
        ArrayCell::Date(values) => append_option_strings(
            builder,
            values.iter().map(|value| value.map(|value| value.to_string())),
        ),
        ArrayCell::Time(values) => append_option_strings(
            builder,
            values.iter().map(|value| value.map(|value| value.to_string())),
        ),
        ArrayCell::Timestamp(values) => append_option_strings(
            builder,
            values.iter().map(|value| value.map(|value| value.to_string())),
        ),
        ArrayCell::TimestampTz(values) => append_option_strings(
            builder,
            values.iter().map(|value| value.map(|value| value.to_rfc3339())),
        ),
        ArrayCell::Uuid(values) => append_option_strings(
            builder,
            values.iter().map(|value| value.map(|value| value.to_string())),
        ),
        ArrayCell::Json(values) => append_option_strings(
            builder,
            values.iter().map(|value| value.as_ref().map(ToString::to_string)),
        ),
        ArrayCell::Bytes(values) => append_option_strings(
            builder,
            values.iter().map(|value| {
                value.as_ref().map(|value| String::from_utf8_lossy(value).to_string())
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
        Cell::TimestampTzMicros(value) => Some(*value),
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
            let array = array.as_any().downcast_ref::<Time64MicrosecondArray>().unwrap();
            Cell::Time(micros_to_time(array.value(row_idx)))
        }
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            let array = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
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
            let array = array.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
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
            Cell::Array(ArrayCell::Time(option_values::<Time64MicrosecondArray, _, _>(
                &list_value,
                |array, index| micros_to_time(array.value(index)),
            )))
        }
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            let array = list_value.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
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
            let array = list_value.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
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
        UNIX_EPOCH.checked_add_days(chrono::Days::new(days as u64)).expect("valid positive date")
    } else {
        UNIX_EPOCH.checked_sub_days(chrono::Days::new((-days) as u64)).expect("valid negative date")
    }
}

fn micros_to_time(micros: i64) -> NaiveTime {
    NaiveTime::from_num_seconds_from_midnight_opt(
        (micros / 1_000_000) as u32,
        ((micros % 1_000_000) * 1_000) as u32,
    )
    .expect("valid microsecond time")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        conversions::table_row::parse_table_row_from_postgres_copy_bytes,
        types::{ColumnSchema, TableId, TableName},
    };

    fn column(
        name: &str,
        typ: Type,
        ordinal_position: i32,
        nullable: bool,
        primary_key: bool,
    ) -> ColumnSchema {
        ColumnSchema::new(
            name.to_owned(),
            typ,
            -1,
            ordinal_position,
            if primary_key { Some(1) } else { None },
            nullable,
        )
    }

    fn scalar_and_array_copy_schema() -> Arc<TableSchema> {
        Arc::new(TableSchema::new(
            TableId::new(123),
            TableName::new("public".to_owned(), "copy_test".to_owned()),
            vec![
                column("id", Type::INT8, 1, false, true),
                column("account_id", Type::INT4, 2, true, false),
                column("active", Type::BOOL, 3, false, false),
                column("amount", Type::NUMERIC, 4, false, false),
                column("created_at", Type::TIMESTAMPTZ, 5, false, false),
                column("payload", Type::JSONB, 6, false, false),
                column("notes", Type::TEXT, 7, true, false),
                column("tags", Type::TEXT_ARRAY, 8, true, false),
                column("scores", Type::INT4_ARRAY, 9, true, false),
                column("flags", Type::BOOL_ARRAY, 10, true, false),
            ],
        ))
    }

    #[test]
    fn postgres_copy_rows_to_arrow_batch_matches_row_conversion() {
        let table_schema = scalar_and_array_copy_schema();
        let copy_rows = vec![
            br#"1	42	t	12.30	2024-01-02 03:04:05.123456+00	{"i":1}	alpha	{red,blue,NULL}	{1,2,NULL}	{t,f,NULL}
"#
            .to_vec(),
            br#"2	\N	f	0	2024-01-02 03:04:05+00	{"i":2}	line\nfeed	{}	{10,NULL,30}	{f,NULL,t}
"#
            .to_vec(),
        ];

        let direct_batch =
            postgres_copy_rows_to_arrow_batch(table_schema.clone(), &copy_rows).unwrap();
        let table_rows = copy_rows
            .iter()
            .map(|row| {
                parse_table_row_from_postgres_copy_bytes(
                    row.as_slice(),
                    table_schema.column_schemas.iter(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let row_batch = table_rows_to_arrow_batch(table_schema, &table_rows).unwrap();

        assert_eq!(
            record_batch_to_table_rows(&direct_batch.batch),
            record_batch_to_table_rows(&row_batch.batch)
        );
    }

    #[test]
    fn postgres_copy_rows_to_arrow_batch_keeps_text_backed_values_raw() {
        let table_schema = scalar_and_array_copy_schema();
        let copy_rows = vec![
            br#"1	42	t	0012.300	2024-01-02 03:04:05.123456+00	{"i": 1}	alpha	{}	{}	{}
"#
            .to_vec(),
        ];

        let direct_batch = postgres_copy_rows_to_arrow_batch(table_schema, &copy_rows).unwrap();
        let amount = direct_batch.batch.column(3).as_any().downcast_ref::<StringArray>().unwrap();
        let payload = direct_batch.batch.column(5).as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(amount.value(0), "0012.300");
        assert_eq!(payload.value(0), r#"{"i": 1}"#);
    }
}
