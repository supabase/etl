//! Avro snapshot encoding for destination initial-copy files.

use std::{collections::HashSet, io::Write, str::FromStr};

use apache_avro::{
    BigDecimal, Codec, Decimal, Schema, Writer,
    types::{Record, Value},
};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use etl::{
    bail,
    data::{
        ArrayCellNonOptional, Cell, CellNonOptional, DATE_FORMAT, SizeHint, TIME_FORMAT, TableRow,
    },
    error::{ErrorKind, EtlResult},
    etl_error,
    schema::{ColumnSchema, ReplicatedTableSchema, Type, is_array_type},
};
use serde_json::{Value as JsonValue, json};

use crate::bigquery::{initial_copy::SnapshotBatch, validation::validate_cell_for_bigquery};

/// BigQuery BIGNUMERIC precision used by the Avro decimal mapping.
const BIGQUERY_BIGNUMERIC_PRECISION: usize = 76;
/// BigQuery BIGNUMERIC scale used by the Avro decimal mapping.
const BIGQUERY_BIGNUMERIC_SCALE: usize = 38;
/// Unix epoch date for Avro date logical values.
const UNIX_EPOCH_DATE: &str = "1970-01-01";
/// Unix epoch timestamp for Avro local timestamp logical values.
const UNIX_EPOCH_TIMESTAMP: &str = "1970-01-01 00:00:00";

/// Mapping from one source column to one Avro field.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AvroColumnMapping {
    /// Source column name from the replicated schema.
    pub source_column_name: String,
    /// Avro-safe field name used in the object container file.
    pub avro_field_name: String,
    /// Whether the source column is nullable.
    pub nullable: bool,
    /// Source Postgres type used by the destination dialect.
    pub source_type: Type,
}

/// Parsed Avro schema plus field mapping metadata.
#[derive(Debug, Clone)]
pub struct AvroSchemaDefinition {
    /// Parsed Avro schema used by [`apache_avro::Writer`].
    pub schema: Schema,
    /// Source-to-Avro field mapping in replicated column order.
    pub column_mappings: Vec<AvroColumnMapping>,
    /// Destination-specific encoding behavior.
    dialect: AvroSchemaDialect,
}

impl AvroSchemaDefinition {
    /// Creates a BigQuery Avro schema definition from a replicated table
    /// schema.
    pub fn for_bigquery(replicated_table_schema: &ReplicatedTableSchema) -> EtlResult<Self> {
        let record_name = avro_name(&format!(
            "{}_{}",
            replicated_table_schema.name().schema,
            replicated_table_schema.name().name
        ));
        let mut used_field_names = HashSet::new();
        let mut fields = Vec::new();
        let mut column_mappings = Vec::new();

        for (index, column_schema) in replicated_table_schema.column_schemas().enumerate() {
            let avro_field_name =
                unique_avro_field_name(&column_schema.name, index, &mut used_field_names);
            let field_type = avro_field_type_for_bigquery(column_schema)?;
            let field_schema =
                if column_schema.nullable { json!(["null", field_type]) } else { field_type };

            fields.push(json!({
                "name": avro_field_name,
                "type": field_schema,
            }));
            column_mappings.push(AvroColumnMapping {
                source_column_name: column_schema.name.clone(),
                avro_field_name,
                nullable: column_schema.nullable,
                source_type: column_schema.typ.clone(),
            });
        }

        let schema_json = json!({
            "type": "record",
            "name": record_name,
            "fields": fields,
        });
        let schema = Schema::parse_str(&schema_json.to_string()).map_err(|err| {
            etl_error!(
                ErrorKind::SerializationError,
                "Avro schema parsing failed",
                "Failed to parse generated snapshot Avro schema.",
                source: err
            )
        })?;

        Ok(Self { schema, column_mappings, dialect: AvroSchemaDialect::BigQuery })
    }
}

/// Streaming Avro object-container encoder for snapshot batches.
pub struct AvroSnapshotStreamEncoder<'a, W: Write> {
    schema_definition: &'a AvroSchemaDefinition,
    writer: Writer<'a, W>,
    row_count: u64,
    estimated_bytes: usize,
}

impl<'a, W: Write> AvroSnapshotStreamEncoder<'a, W> {
    /// Creates a new streaming Avro encoder.
    pub fn new(schema_definition: &'a AvroSchemaDefinition, writer: W) -> Self {
        Self {
            schema_definition,
            writer: Writer::with_codec(&schema_definition.schema, writer, Codec::Snappy),
            row_count: 0,
            estimated_bytes: 0,
        }
    }

    /// Writes one batch of rows to the underlying writer.
    pub fn write_batch(&mut self, batch: SnapshotBatch) -> EtlResult<()> {
        for row in batch.rows {
            self.estimated_bytes = self.estimated_bytes.saturating_add(row.size_hint());
            // Keep only one encoded row alive at a time. `append_value_ref`
            // copies it into Avro's internal block buffer before `avro_row`
            // is dropped at the end of this loop iteration.
            let avro_row = encode_row_for_avro(self.schema_definition, row)?;
            self.writer.append_value_ref(&avro_row).map_err(|err| {
                etl_error!(
                    ErrorKind::SerializationError,
                    "Avro row serialization failed",
                    "Failed to append a row to the snapshot Avro file.",
                    source: err
                )
            })?;
            self.row_count += 1;
        }

        Ok(())
    }

    /// Returns the number of rows written so far.
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Returns the current estimated source byte count.
    pub fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }

    /// Finishes the Avro file and returns the underlying writer.
    pub fn finish(self) -> EtlResult<(W, u64, usize)> {
        let row_count = self.row_count;
        let estimated_bytes = self.estimated_bytes;
        let writer = self.writer.into_inner().map_err(|err| {
            etl_error!(
                ErrorKind::SerializationError,
                "Avro file finalization failed",
                "Failed to finalize the snapshot Avro object container file.",
                source: err
            )
        })?;

        Ok((writer, row_count, estimated_bytes))
    }
}

/// Destination Avro encoding dialect.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum AvroSchemaDialect {
    /// BigQuery load-job compatible encoding.
    BigQuery,
}

/// Converts one table row into an Avro record.
fn encode_row_for_avro(
    schema_definition: &AvroSchemaDefinition,
    table_row: TableRow,
) -> EtlResult<Value> {
    let values = table_row.into_values();
    if values.len() != schema_definition.column_mappings.len() {
        bail!(
            ErrorKind::InvalidData,
            "Snapshot row width does not match replicated schema",
            format!(
                "Snapshot row has {} values but the replicated schema has {} columns.",
                values.len(),
                schema_definition.column_mappings.len()
            )
        );
    }

    let mut record = Record::new(&schema_definition.schema).ok_or_else(|| {
        etl_error!(
            ErrorKind::SerializationError,
            "Avro schema is not a record",
            "Generated snapshot Avro schema was not a record schema."
        )
    })?;

    for (cell, column_mapping) in values.into_iter().zip(&schema_definition.column_mappings) {
        let avro_value = encode_cell_for_avro(cell, column_mapping, schema_definition.dialect)?;
        record.put(&column_mapping.avro_field_name, avro_value);
    }

    Ok(Value::Record(record.fields))
}

/// Builds an Avro schema JSON value for a BigQuery destination column.
fn avro_field_type_for_bigquery(column_schema: &ColumnSchema) -> EtlResult<JsonValue> {
    if is_array_type(&column_schema.typ) {
        return Ok(json!({
            "type": "array",
            "items": avro_array_item_type_for_bigquery(&column_schema.typ)?,
        }));
    }

    Ok(avro_scalar_type_for_bigquery(&column_schema.typ))
}

/// Builds an Avro item schema JSON value for a BigQuery array column.
fn avro_array_item_type_for_bigquery(typ: &Type) -> EtlResult<JsonValue> {
    Ok(match typ {
        &Type::BOOL_ARRAY => json!("boolean"),
        &Type::INT2_ARRAY | &Type::INT4_ARRAY | &Type::INT8_ARRAY | &Type::OID_ARRAY => {
            json!("long")
        }
        &Type::FLOAT4_ARRAY | &Type::FLOAT8_ARRAY => json!("double"),
        &Type::NUMERIC_ARRAY => decimal_schema_json(),
        &Type::DATE_ARRAY => date_schema_json(),
        &Type::TIME_ARRAY => time_micros_schema_json(),
        &Type::TIMESTAMP_ARRAY => local_timestamp_micros_schema_json(),
        &Type::TIMESTAMPTZ_ARRAY => timestamp_micros_schema_json(),
        &Type::BYTEA_ARRAY => json!("bytes"),
        &Type::JSON_ARRAY | &Type::JSONB_ARRAY => json!("string"),
        _ => json!("string"),
    })
}

/// Builds an Avro scalar schema JSON value for a BigQuery destination column.
fn avro_scalar_type_for_bigquery(typ: &Type) -> JsonValue {
    match typ {
        &Type::BOOL => json!("boolean"),
        &Type::INT2 | &Type::INT4 | &Type::INT8 | &Type::OID => json!("long"),
        &Type::FLOAT4 | &Type::FLOAT8 => json!("double"),
        &Type::NUMERIC => decimal_schema_json(),
        &Type::DATE => date_schema_json(),
        &Type::TIME => time_micros_schema_json(),
        &Type::TIMESTAMP => local_timestamp_micros_schema_json(),
        &Type::TIMESTAMPTZ => timestamp_micros_schema_json(),
        &Type::BYTEA => json!("bytes"),
        &Type::JSON | &Type::JSONB => json!("string"),
        _ => json!("string"),
    }
}

/// Returns the Avro decimal schema used for BigQuery BIGNUMERIC.
fn decimal_schema_json() -> JsonValue {
    json!({
        "type": "bytes",
        "logicalType": "decimal",
        "precision": BIGQUERY_BIGNUMERIC_PRECISION,
        "scale": BIGQUERY_BIGNUMERIC_SCALE,
    })
}

/// Returns the Avro date logical schema.
fn date_schema_json() -> JsonValue {
    json!({
        "type": "int",
        "logicalType": "date",
    })
}

/// Returns the Avro time-micros logical schema.
fn time_micros_schema_json() -> JsonValue {
    json!({
        "type": "long",
        "logicalType": "time-micros",
    })
}

/// Returns the Avro timestamp-micros logical schema.
fn timestamp_micros_schema_json() -> JsonValue {
    json!({
        "type": "long",
        "logicalType": "timestamp-micros",
    })
}

/// Returns the Avro local-timestamp-micros logical schema.
fn local_timestamp_micros_schema_json() -> JsonValue {
    json!({
        "type": "long",
        "logicalType": "local-timestamp-micros",
    })
}

/// Encodes a table cell into an Avro value for one mapped field.
fn encode_cell_for_avro(
    cell: Cell,
    column_mapping: &AvroColumnMapping,
    dialect: AvroSchemaDialect,
) -> EtlResult<Value> {
    if matches!(cell, Cell::Null) {
        if column_mapping.nullable {
            return Ok(Value::Union(0, Box::new(Value::Null)));
        }

        bail!(
            ErrorKind::InvalidData,
            "Non-nullable Avro field received NULL",
            format!(
                "Column '{}' is not nullable in the replicated schema.",
                column_mapping.source_column_name
            )
        );
    }

    let cell = CellNonOptional::try_from(cell).map_err(|err| {
        etl_error!(
            err.kind(),
            "Cell conversion failed during Avro encoding",
            format!(
                "Failed to convert column '{}' to a non-optional destination value.",
                column_mapping.source_column_name
            ),
            source: err
        )
    })?;

    validate_cell_for_avro(&cell, column_mapping, dialect)?;

    let value = encode_non_null_cell_for_bigquery_avro(cell, &column_mapping.source_type)?;

    if column_mapping.nullable { Ok(Value::Union(1, Box::new(value))) } else { Ok(value) }
}

/// Validates one non-null cell for the destination Avro dialect.
fn validate_cell_for_avro(
    cell: &CellNonOptional,
    column_mapping: &AvroColumnMapping,
    dialect: AvroSchemaDialect,
) -> EtlResult<()> {
    match dialect {
        AvroSchemaDialect::BigQuery => validate_cell_for_bigquery(cell).map_err(|err| {
            etl_error!(
                err.kind(),
                "Cell validation failed for BigQuery Avro encoding",
                format!(
                    "Column '{}' failed BigQuery compatibility validation.",
                    column_mapping.source_column_name
                ),
                source: err
            )
        }),
    }
}

/// Encodes a non-null cell into an Avro value for the source column type.
fn encode_non_null_cell_for_bigquery_avro(cell: CellNonOptional, typ: &Type) -> EtlResult<Value> {
    if is_array_type(typ) {
        return match cell {
            CellNonOptional::Array(array) => encode_array_for_bigquery_avro(array, typ),
            cell => string_value_for_cell(cell),
        };
    }

    match cell {
        CellNonOptional::Bool(value) if matches!(typ, &Type::BOOL) => Ok(Value::Boolean(value)),
        CellNonOptional::I16(value) if matches!(typ, &Type::INT2) => {
            Ok(Value::Long(i64::from(value)))
        }
        CellNonOptional::I32(value) if matches!(typ, &Type::INT4) => {
            Ok(Value::Long(i64::from(value)))
        }
        CellNonOptional::I64(value) if matches!(typ, &Type::INT8) => Ok(Value::Long(value)),
        CellNonOptional::U32(value) if matches!(typ, &Type::OID) => {
            Ok(Value::Long(i64::from(value)))
        }
        CellNonOptional::F32(value) if matches!(typ, &Type::FLOAT4) => {
            Ok(Value::Double(f64::from(value)))
        }
        CellNonOptional::F64(value) if matches!(typ, &Type::FLOAT8) => Ok(Value::Double(value)),
        CellNonOptional::Numeric(value) if matches!(typ, &Type::NUMERIC) => decimal_value(&value),
        CellNonOptional::Date(value) if matches!(typ, &Type::DATE) => {
            Ok(Value::Date(days_since_epoch(value)?))
        }
        CellNonOptional::Time(value) if matches!(typ, &Type::TIME) => {
            Ok(Value::TimeMicros(time_micros(value)))
        }
        CellNonOptional::Timestamp(value) if matches!(typ, &Type::TIMESTAMP) => {
            Ok(Value::LocalTimestampMicros(local_timestamp_micros(value)?))
        }
        CellNonOptional::TimestampTz(value) if matches!(typ, &Type::TIMESTAMPTZ) => {
            Ok(Value::TimestampMicros(value.timestamp_micros()))
        }
        CellNonOptional::Bytes(value) if matches!(typ, &Type::BYTEA) => Ok(Value::Bytes(value)),
        CellNonOptional::Json(value) if matches!(typ, &Type::JSON | &Type::JSONB) => {
            Ok(Value::String(value.to_string()))
        }
        cell => string_value_for_cell(cell),
    }
}

/// Encodes a non-null array cell into an Avro array value.
fn encode_array_for_bigquery_avro(array: ArrayCellNonOptional, typ: &Type) -> EtlResult<Value> {
    let values = match array {
        ArrayCellNonOptional::Bool(values) if matches!(typ, &Type::BOOL_ARRAY) => {
            values.into_iter().map(Value::Boolean).collect()
        }
        ArrayCellNonOptional::I16(values) if matches!(typ, &Type::INT2_ARRAY) => {
            values.into_iter().map(i64::from).map(Value::Long).collect()
        }
        ArrayCellNonOptional::I32(values) if matches!(typ, &Type::INT4_ARRAY) => {
            values.into_iter().map(i64::from).map(Value::Long).collect()
        }
        ArrayCellNonOptional::I64(values) if matches!(typ, &Type::INT8_ARRAY) => {
            values.into_iter().map(Value::Long).collect()
        }
        ArrayCellNonOptional::U32(values) if matches!(typ, &Type::OID_ARRAY) => {
            values.into_iter().map(i64::from).map(Value::Long).collect()
        }
        ArrayCellNonOptional::F32(values) if matches!(typ, &Type::FLOAT4_ARRAY) => {
            values.into_iter().map(f64::from).map(Value::Double).collect()
        }
        ArrayCellNonOptional::F64(values) if matches!(typ, &Type::FLOAT8_ARRAY) => {
            values.into_iter().map(Value::Double).collect()
        }
        ArrayCellNonOptional::Numeric(values) if matches!(typ, &Type::NUMERIC_ARRAY) => {
            values.iter().map(decimal_value).collect::<EtlResult<Vec<_>>>()?
        }
        ArrayCellNonOptional::Date(values) if matches!(typ, &Type::DATE_ARRAY) => values
            .into_iter()
            .map(days_since_epoch)
            .map(|value| value.map(Value::Date))
            .collect::<EtlResult<Vec<_>>>()?,
        ArrayCellNonOptional::Time(values) if matches!(typ, &Type::TIME_ARRAY) => {
            values.into_iter().map(time_micros).map(Value::TimeMicros).collect()
        }
        ArrayCellNonOptional::Timestamp(values) if matches!(typ, &Type::TIMESTAMP_ARRAY) => values
            .into_iter()
            .map(local_timestamp_micros)
            .map(|value| value.map(Value::LocalTimestampMicros))
            .collect::<EtlResult<Vec<_>>>()?,
        ArrayCellNonOptional::TimestampTz(values) if matches!(typ, &Type::TIMESTAMPTZ_ARRAY) => {
            values
                .into_iter()
                .map(|value| Value::TimestampMicros(value.timestamp_micros()))
                .collect()
        }
        ArrayCellNonOptional::Bytes(values) if matches!(typ, &Type::BYTEA_ARRAY) => {
            values.into_iter().map(Value::Bytes).collect()
        }
        ArrayCellNonOptional::Json(values)
            if matches!(typ, &Type::JSON_ARRAY | &Type::JSONB_ARRAY) =>
        {
            values.into_iter().map(|value| Value::String(value.to_string())).collect()
        }
        array => values_as_strings(array),
    };

    Ok(Value::Array(values))
}

/// Encodes a scalar fallback as a string value.
fn string_value_for_cell(cell: CellNonOptional) -> EtlResult<Value> {
    Ok(Value::String(match cell {
        CellNonOptional::Null => String::new(),
        CellNonOptional::Bool(value) => value.to_string(),
        CellNonOptional::String(value) => value,
        CellNonOptional::I16(value) => value.to_string(),
        CellNonOptional::I32(value) => value.to_string(),
        CellNonOptional::U32(value) => value.to_string(),
        CellNonOptional::I64(value) => value.to_string(),
        CellNonOptional::F32(value) => value.to_string(),
        CellNonOptional::F64(value) => value.to_string(),
        CellNonOptional::Numeric(value) => value.to_string(),
        CellNonOptional::Date(value) => value.format(DATE_FORMAT).to_string(),
        CellNonOptional::Time(value) => value.format(TIME_FORMAT).to_string(),
        CellNonOptional::TimeTz(value) => value.to_string(),
        CellNonOptional::Timestamp(value) => value.to_string(),
        CellNonOptional::TimestampTz(value) => value.to_rfc3339(),
        CellNonOptional::Uuid(value) => value.to_string(),
        CellNonOptional::Json(value) => value.to_string(),
        CellNonOptional::Bytes(_) => {
            bail!(
                ErrorKind::UnsupportedValueInDestination,
                "Binary value cannot be encoded as an Avro string",
                "A source binary value reached a string fallback in BigQuery Avro encoding."
            );
        }
        CellNonOptional::Array(_) => {
            bail!(
                ErrorKind::UnsupportedValueInDestination,
                "Array value cannot be encoded as an Avro string",
                "A source array value reached a scalar fallback in BigQuery Avro encoding."
            );
        }
    }))
}

/// Encodes unsupported array element shapes as strings.
fn values_as_strings(array: ArrayCellNonOptional) -> Vec<Value> {
    match array {
        ArrayCellNonOptional::Bool(values) => values_as_string_values(values),
        ArrayCellNonOptional::String(values) => values.into_iter().map(Value::String).collect(),
        ArrayCellNonOptional::I16(values) => values_as_string_values(values),
        ArrayCellNonOptional::I32(values) => values_as_string_values(values),
        ArrayCellNonOptional::U32(values) => values_as_string_values(values),
        ArrayCellNonOptional::I64(values) => values_as_string_values(values),
        ArrayCellNonOptional::F32(values) => values_as_string_values(values),
        ArrayCellNonOptional::F64(values) => values_as_string_values(values),
        ArrayCellNonOptional::Numeric(values) => values_as_string_values(values),
        ArrayCellNonOptional::Date(values) => values_as_string_values(values),
        ArrayCellNonOptional::Time(values) => values_as_string_values(values),
        ArrayCellNonOptional::TimeTz(values) => values_as_string_values(values),
        ArrayCellNonOptional::Timestamp(values) => values_as_string_values(values),
        ArrayCellNonOptional::TimestampTz(values) => {
            values.into_iter().map(|value| Value::String(value.to_rfc3339())).collect()
        }
        ArrayCellNonOptional::Uuid(values) => values_as_string_values(values),
        ArrayCellNonOptional::Json(values) => values_as_string_values(values),
        ArrayCellNonOptional::Bytes(values) => values.into_iter().map(Value::Bytes).collect(),
    }
}

/// Converts displayable values to Avro strings.
fn values_as_string_values<T: ToString>(values: Vec<T>) -> Vec<Value> {
    values.into_iter().map(|value| Value::String(value.to_string())).collect()
}

/// Encodes a Postgres numeric as an Avro decimal value.
fn decimal_value(value: &etl::data::PgNumeric) -> EtlResult<Value> {
    let decimal = BigDecimal::from_str(&value.to_string()).map_err(|err| {
        etl_error!(
            ErrorKind::UnsupportedValueInDestination,
            "Numeric value cannot be encoded as Avro decimal",
            "A source numeric value is not representable as an Avro decimal for BigQuery.",
            source: err
        )
    })?;
    let scaled_decimal = decimal.with_scale(BIGQUERY_BIGNUMERIC_SCALE as i64);
    let (unscaled_integer, scale) = scaled_decimal.into_bigint_and_scale();

    if scale != BIGQUERY_BIGNUMERIC_SCALE as i64 {
        return Err(etl_error!(
            ErrorKind::UnsupportedValueInDestination,
            "Numeric value cannot be encoded as Avro decimal",
            "A source numeric value is not representable with the fixed BigQuery BIGNUMERIC scale."
        ));
    }

    let bytes = unscaled_integer.to_signed_bytes_be();
    if bytes.is_empty() {
        return Ok(Value::Decimal(Decimal::from([0])));
    }

    Ok(Value::Decimal(Decimal::from(bytes)))
}

/// Converts a date to Avro days since the Unix epoch.
fn days_since_epoch(value: NaiveDate) -> EtlResult<i32> {
    let epoch = NaiveDate::parse_from_str(UNIX_EPOCH_DATE, DATE_FORMAT).map_err(|err| {
        etl_error!(
            ErrorKind::InvalidState,
            "Unix epoch date parsing failed",
            "The built-in Unix epoch date constant is invalid.",
            source: err
        )
    })?;
    let days = value.signed_duration_since(epoch).num_days();

    i32::try_from(days).map_err(|err| {
        etl_error!(
            ErrorKind::UnsupportedValueInDestination,
            "Date value is outside Avro date range",
            "A source date value is outside the i32 day range required by Avro.",
            source: err
        )
    })
}

/// Converts a time value to Avro microseconds since midnight.
fn time_micros(value: NaiveTime) -> i64 {
    i64::from(value.num_seconds_from_midnight()) * 1_000_000 + i64::from(value.nanosecond() / 1_000)
}

/// Converts a local timestamp to Avro microseconds since the Unix epoch.
fn local_timestamp_micros(value: NaiveDateTime) -> EtlResult<i64> {
    let epoch = NaiveDateTime::parse_from_str(UNIX_EPOCH_TIMESTAMP, "%Y-%m-%d %H:%M:%S").map_err(
        |err| {
            etl_error!(
                ErrorKind::InvalidState,
                "Unix epoch timestamp parsing failed",
                "The built-in Unix epoch timestamp constant is invalid.",
                source: err
            )
        },
    )?;

    value.signed_duration_since(epoch).num_microseconds().ok_or_else(|| {
        etl_error!(
            ErrorKind::UnsupportedValueInDestination,
            "Timestamp value is outside Avro timestamp range",
            "A source timestamp value is outside the i64 microsecond range required by Avro."
        )
    })
}

/// Returns an Avro-safe unique field name.
fn unique_avro_field_name(
    source_name: &str,
    index: usize,
    used_field_names: &mut HashSet<String>,
) -> String {
    let base_name = avro_name(source_name);
    if used_field_names.insert(base_name.clone()) {
        return base_name;
    }

    for suffix in 1.. {
        let candidate = format!("{base_name}_{suffix}");
        if used_field_names.insert(candidate.clone()) {
            return candidate;
        }
    }

    format!("field_{index}")
}

/// Converts a name to an Avro-safe name.
fn avro_name(name: &str) -> String {
    let mut chars = name.chars();
    let mut sanitized = String::with_capacity(name.len().max(1));

    match chars.next() {
        Some(ch) if is_avro_name_start(ch) => sanitized.push(ch),
        Some(ch) if is_avro_name_part(ch) => {
            sanitized.push('_');
            sanitized.push(ch);
        }
        Some(_) | None => sanitized.push('_'),
    }

    for ch in chars {
        if is_avro_name_part(ch) {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }

    sanitized
}

/// Returns whether a character may start an Avro name.
fn is_avro_name_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

/// Returns whether a character may be part of an Avro name.
fn is_avro_name_part(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, str::FromStr, sync::Arc};

    use apache_avro::Reader;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
    use etl::{
        data::{ArrayCell, Cell, PgNumeric, TableRow},
        schema::{ColumnSchema, ReplicationMask, TableId, TableName, TableSchema, Type},
    };

    use super::*;

    /// Creates a replicated schema with all supplied columns.
    fn replicated_schema(columns: Vec<ColumnSchema>) -> ReplicatedTableSchema {
        let column_names: HashSet<String> =
            columns.iter().map(|column| column.name.clone()).collect();
        let table_schema = Arc::new(TableSchema::new(
            TableId(1),
            TableName::new("public".to_owned(), "events".to_owned()),
            columns,
        ));
        let replication_mask = ReplicationMask::build_or_all(&table_schema, &column_names);

        ReplicatedTableSchema::from_mask(table_schema, replication_mask)
    }

    /// Creates a test column schema.
    fn column(name: &str, typ: Type, ordinal_position: i32, nullable: bool) -> ColumnSchema {
        ColumnSchema::new(name.to_owned(), typ, -1, ordinal_position, nullable)
    }

    #[test]
    fn schema_definition_maps_bigquery_logical_types() {
        let schema = replicated_schema(vec![
            column("id", Type::INT8, 1, false),
            column("occurred_on", Type::DATE, 2, true),
            column("occurred_at", Type::TIMESTAMPTZ, 3, true),
            column("local_time", Type::TIMESTAMP, 4, true),
            column("payload", Type::JSONB, 5, true),
            column("amount", Type::NUMERIC, 6, true),
        ]);

        let definition = AvroSchemaDefinition::for_bigquery(&schema).unwrap();
        let parsed_schema = format!("{:?}", definition.schema);

        assert!(parsed_schema.contains("Date"));
        assert!(parsed_schema.contains("TimestampMicros"));
        assert!(parsed_schema.contains("LocalTimestampMicros"));
        assert!(parsed_schema.contains("precision: 76"));
        assert!(parsed_schema.contains("scale: 38"));
    }

    #[test]
    fn schema_definition_sanitizes_duplicate_avro_field_names() {
        let schema = replicated_schema(vec![
            column("1 bad-name", Type::TEXT, 1, true),
            column("_1_bad_name", Type::TEXT, 2, true),
        ]);

        let definition = AvroSchemaDefinition::for_bigquery(&schema).unwrap();

        assert_eq!(definition.column_mappings[0].avro_field_name, "_1_bad_name");
        assert_eq!(definition.column_mappings[1].avro_field_name, "_1_bad_name_1");
    }

    #[test]
    fn encoder_writes_readable_avro_ocf_file() {
        let schema = replicated_schema(vec![
            column("id", Type::INT8, 1, false),
            column("name", Type::TEXT, 2, true),
            column("active", Type::BOOL, 3, false),
            column("created_at", Type::TIMESTAMPTZ, 4, false),
        ]);
        let definition = AvroSchemaDefinition::for_bigquery(&schema).unwrap();
        let mut encoder = AvroSnapshotStreamEncoder::new(&definition, Vec::new());
        let timestamp = Utc.with_ymd_and_hms(2026, 6, 29, 12, 0, 0).unwrap();

        encoder
            .write_batch(SnapshotBatch {
                rows: vec![TableRow::new(vec![
                    Cell::I64(7),
                    Cell::String("Ada".to_owned()),
                    Cell::Bool(true),
                    Cell::TimestampTz(timestamp),
                ])],
            })
            .unwrap();

        let (bytes, row_count, _) = encoder.finish().unwrap();
        let rows = Reader::new(bytes.as_slice()).unwrap().collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(row_count, 1);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn encoder_handles_temporal_and_array_values() {
        let schema = replicated_schema(vec![
            column("d", Type::DATE, 1, false),
            column("t", Type::TIME, 2, false),
            column("ts", Type::TIMESTAMP, 3, false),
            column("arr", Type::INT4_ARRAY, 4, true),
        ]);
        let definition = AvroSchemaDefinition::for_bigquery(&schema).unwrap();
        let mut encoder = AvroSnapshotStreamEncoder::new(&definition, Vec::new());

        encoder
            .write_batch(SnapshotBatch {
                rows: vec![TableRow::new(vec![
                    Cell::Date(NaiveDate::from_ymd_opt(1970, 1, 2).unwrap()),
                    Cell::Time(NaiveTime::from_hms_micro_opt(1, 2, 3, 4).unwrap()),
                    Cell::Timestamp(
                        NaiveDateTime::parse_from_str("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
                            .unwrap(),
                    ),
                    Cell::Array(ArrayCell::I32(vec![Some(1), Some(2)])),
                ])],
            })
            .unwrap();

        assert_eq!(encoder.row_count(), 1);
        assert!(encoder.estimated_bytes() > 0);
    }

    #[test]
    fn encoder_rejects_row_width_mismatch() {
        let schema = replicated_schema(vec![column("id", Type::INT8, 1, false)]);
        let definition = AvroSchemaDefinition::for_bigquery(&schema).unwrap();
        let mut encoder = AvroSnapshotStreamEncoder::new(&definition, Vec::new());
        let err =
            encoder.write_batch(SnapshotBatch { rows: vec![TableRow::new(vec![])] }).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn encoder_rejects_special_numeric_values() {
        let schema = replicated_schema(vec![column("amount", Type::NUMERIC, 1, false)]);
        let definition = AvroSchemaDefinition::for_bigquery(&schema).unwrap();
        let mut encoder = AvroSnapshotStreamEncoder::new(&definition, Vec::new());
        let err = encoder
            .write_batch(SnapshotBatch {
                rows: vec![TableRow::new(vec![Cell::Numeric(PgNumeric::from_str("NaN").unwrap())])],
            })
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
    }

    #[test]
    fn encoder_finalizes_numeric_decimal_values() {
        let schema = replicated_schema(vec![column("amount", Type::NUMERIC, 1, false)]);
        let definition = AvroSchemaDefinition::for_bigquery(&schema).unwrap();
        let mut encoder = AvroSnapshotStreamEncoder::new(&definition, Vec::new());

        encoder
            .write_batch(SnapshotBatch {
                rows: vec![TableRow::new(vec![Cell::Numeric(
                    PgNumeric::from_str("123.4500").unwrap(),
                )])],
            })
            .unwrap();

        let (bytes, row_count, _) = encoder.finish().unwrap();

        assert_eq!(row_count, 1);
        assert!(!bytes.is_empty());
    }
}
