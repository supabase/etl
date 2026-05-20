use std::{collections::HashSet, fmt, str::FromStr};

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use etl::{
    error::{ErrorKind, EtlError},
    materialization::{
        CellMaterializationResult, DestinationMaterializer, DestinationTypeCompatibility,
        DestinationTypeCompatibilityMode, MaterializationOutcome,
        MaterializationOutcome::{TypeChanged, Unchanged, ValueChanged},
        MaterializationRules, TypeMaterializationResult, TypedCell,
    },
    types::{
        ArrayCell, Cell, PgDate, PgNumeric, PgTemporalBound, PgTime, PgTimestamp, PgTimestampTz,
        Type, is_array_type,
    },
};
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};

use crate::bigquery::value::{BigQueryArrayCell, BigQueryCell, BigQueryType};

/// Maximum number of fractional digits in BigQuery `BIGNUMERIC`.
const BIGQUERY_BIGNUMERIC_MAX_SCALE: usize = 38;
/// Maximum finite BigQuery `BIGNUMERIC` value.
const BIGQUERY_BIGNUMERIC_MAX: &str =
    "578960446186580977117854925043439539266.34992332820282019728792003956564819967";
/// Minimum finite BigQuery `BIGNUMERIC` value.
const BIGQUERY_BIGNUMERIC_MIN: &str =
    "-578960446186580977117854925043439539266.34992332820282019728792003956564819968";
/// Maximum nesting depth for a BigQuery `JSON` value.
const BIGQUERY_JSON_MAX_NESTING_DEPTH: usize = 500;
/// Internal serde_json token used for arbitrary-precision numbers.
const SERDE_JSON_NUMBER_TOKEN: &str = "$serde_json::private::Number";

/// BigQuery cell materialization result.
type BigQueryCellMaterializationResult = CellMaterializationResult<BigQueryType, BigQueryCell>;

/// BigQuery materializer.
pub(super) type BigQueryMaterializer = DestinationMaterializer<BigQueryMaterialization>;

/// BigQuery materialization rules.
#[derive(Debug, Clone, Default)]
pub(super) struct BigQueryMaterialization;

impl BigQueryMaterialization {
    /// Creates a [`DestinationMaterializer`] for BigQuery.
    pub(super) fn materializer(
        compatibility: DestinationTypeCompatibility,
    ) -> DestinationMaterializer<Self> {
        DestinationMaterializer::new(compatibility, Self)
    }
}

impl MaterializationRules for BigQueryMaterialization {
    type MaterializedType = BigQueryType;
    type MaterializedCell = BigQueryCell;

    fn materialize_type(
        &self,
        typ: &Type,
        compatibility: DestinationTypeCompatibility,
    ) -> TypeMaterializationResult<BigQueryType> {
        match compatibility.mode() {
            DestinationTypeCompatibilityMode::Strict if !has_strict_bigquery_native_type(typ) => {
                TypeMaterializationResult::Invalid {
                    kind: ErrorKind::UnsupportedValueInDestination,
                    reason: format!(
                        "PostgreSQL type {} has no strict native BigQuery representation",
                        typ.name()
                    ),
                }
            }
            DestinationTypeCompatibilityMode::Compatible
                if materializes_as_bigquery_string_array_for_compatible(typ) =>
            {
                TypeMaterializationResult::Changed(BigQueryType::native_for_source_type(typ))
            }
            DestinationTypeCompatibilityMode::Compatible
                if materializes_as_bigquery_string_for_compatible(typ) =>
            {
                TypeMaterializationResult::Changed(BigQueryType::String)
            }
            DestinationTypeCompatibilityMode::Preserve
                if materializes_as_bigquery_string_for_preserve(typ) =>
            {
                TypeMaterializationResult::Changed(BigQueryType::String)
            }
            DestinationTypeCompatibilityMode::Coerce
                if materializes_as_bigquery_string_array_for_coerce(typ) =>
            {
                TypeMaterializationResult::Changed(BigQueryType::native_for_source_type(typ))
            }
            DestinationTypeCompatibilityMode::Coerce
                if materializes_as_bigquery_string_for_coerce(typ) =>
            {
                TypeMaterializationResult::Changed(BigQueryType::String)
            }
            _ => TypeMaterializationResult::Unchanged(BigQueryType::native_for_source_type(typ)),
        }
    }

    fn materialize_cell(
        &self,
        typed_cell: TypedCell<Type, Cell>,
        compatibility: DestinationTypeCompatibility,
    ) -> BigQueryCellMaterializationResult {
        let (typ, cell) = typed_cell.into_parts();
        bigquery_materialized_cell(&typ, cell, compatibility)
    }
}

/// Returns the BigQuery materialized cell for a typed source value.
fn bigquery_materialized_cell(
    typ: &Type,
    cell: Cell,
    compatibility: DestinationTypeCompatibility,
) -> BigQueryCellMaterializationResult {
    if is_array_type(typ) {
        return array_typed_cell(typ, cell, compatibility);
    }

    let cell = match (typ, cell) {
        (&Type::JSON | &Type::JSONB, Cell::String(value)) => {
            return json_string_cell(typ, value, compatibility);
        }
        (_, cell) => cell,
    };

    if let Cell::Array(array) = &cell
        && let Some(result) = validate_array_has_no_nulls(array)
    {
        return result;
    }

    if materializes_as_bigquery_string(typ, compatibility) {
        return string_materialized_cell(cell);
    }

    match compatibility.mode() {
        DestinationTypeCompatibilityMode::Strict => strict_cell(typ, cell),
        DestinationTypeCompatibilityMode::Compatible => compatible_cell(typ, cell),
        DestinationTypeCompatibilityMode::Preserve => preserve_cell(typ, cell),
        DestinationTypeCompatibilityMode::Coerce => coerce_cell(typ, cell),
    }
}

/// Returns whether the source type has a strict native BigQuery target.
fn has_strict_bigquery_native_type(typ: &Type) -> bool {
    matches!(
        *typ,
        Type::BOOL
            | Type::CHAR
            | Type::BPCHAR
            | Type::VARCHAR
            | Type::NAME
            | Type::TEXT
            | Type::INT2
            | Type::INT4
            | Type::INT8
            | Type::OID
            | Type::FLOAT4
            | Type::FLOAT8
            | Type::NUMERIC
            | Type::DATE
            | Type::TIME
            | Type::TIMESTAMP
            | Type::TIMESTAMPTZ
            | Type::JSON
            | Type::JSONB
            | Type::BYTEA
            | Type::BOOL_ARRAY
            | Type::CHAR_ARRAY
            | Type::BPCHAR_ARRAY
            | Type::VARCHAR_ARRAY
            | Type::NAME_ARRAY
            | Type::TEXT_ARRAY
            | Type::INT2_ARRAY
            | Type::INT4_ARRAY
            | Type::INT8_ARRAY
            | Type::OID_ARRAY
            | Type::FLOAT4_ARRAY
            | Type::FLOAT8_ARRAY
            | Type::NUMERIC_ARRAY
            | Type::DATE_ARRAY
            | Type::TIME_ARRAY
            | Type::TIMESTAMP_ARRAY
            | Type::TIMESTAMPTZ_ARRAY
            | Type::JSON_ARRAY
            | Type::JSONB_ARRAY
            | Type::BYTEA_ARRAY
    )
}

/// Returns whether compatible mode should materialize the source type as
/// `STRING`.
fn materializes_as_bigquery_string_for_compatible(typ: &Type) -> bool {
    !is_array_type(typ) && !has_strict_bigquery_native_type(typ)
}

/// Returns whether compatible mode should materialize the source type as
/// `ARRAY<STRING>`.
fn materializes_as_bigquery_string_array_for_compatible(typ: &Type) -> bool {
    is_array_type(typ) && !has_strict_bigquery_native_type(typ)
}

/// Returns whether preserve mode should materialize the source type as
/// `STRING`.
fn materializes_as_bigquery_string_for_preserve(typ: &Type) -> bool {
    if is_array_type(typ) {
        return true;
    }

    if !has_strict_bigquery_native_type(typ) {
        return true;
    }

    matches!(
        *typ,
        Type::FLOAT4
            | Type::FLOAT8
            | Type::NUMERIC
            | Type::DATE
            | Type::TIME
            | Type::TIMESTAMP
            | Type::TIMESTAMPTZ
            | Type::JSON
            | Type::JSONB
    )
}

/// Returns whether coerce mode should materialize the source type as `STRING`.
fn materializes_as_bigquery_string_for_coerce(typ: &Type) -> bool {
    !is_array_type(typ) && !has_strict_bigquery_native_type(typ)
}

/// Returns whether coerce mode should materialize the source type as
/// `ARRAY<STRING>`.
fn materializes_as_bigquery_string_array_for_coerce(typ: &Type) -> bool {
    is_array_type(typ) && !has_strict_bigquery_native_type(typ)
}

/// Returns whether the type is materialized as scalar BigQuery `STRING`.
fn materializes_as_bigquery_string(
    typ: &Type,
    compatibility: DestinationTypeCompatibility,
) -> bool {
    match compatibility.mode() {
        DestinationTypeCompatibilityMode::Strict => false,
        DestinationTypeCompatibilityMode::Compatible => {
            materializes_as_bigquery_string_for_compatible(typ)
        }
        DestinationTypeCompatibilityMode::Preserve => {
            materializes_as_bigquery_string_for_preserve(typ)
        }
        DestinationTypeCompatibilityMode::Coerce => materializes_as_bigquery_string_for_coerce(typ),
    }
}

/// Applies BigQuery materialization for a cell known to come from an array
/// column.
fn array_typed_cell(
    typ: &Type,
    cell: Cell,
    compatibility: DestinationTypeCompatibility,
) -> BigQueryCellMaterializationResult {
    if materializes_as_bigquery_string(typ, compatibility) {
        return string_materialized_cell(cell);
    }

    match compatibility.mode() {
        DestinationTypeCompatibilityMode::Strict => strict_source_array_cell(typ, cell),
        DestinationTypeCompatibilityMode::Compatible => compatible_source_array_cell(typ, cell),
        DestinationTypeCompatibilityMode::Preserve => preserve_source_array_cell(cell),
        DestinationTypeCompatibilityMode::Coerce => coerce_source_array_cell(typ, cell),
    }
}

/// Applies BigQuery materialization for raw JSON text.
fn json_string_cell(
    typ: &Type,
    value: String,
    compatibility: DestinationTypeCompatibility,
) -> BigQueryCellMaterializationResult {
    match compatibility.mode() {
        DestinationTypeCompatibilityMode::Strict => strict_json_text(typ, value),
        DestinationTypeCompatibilityMode::Compatible => compatible_json_text(typ, value),
        DestinationTypeCompatibilityMode::Preserve => preserve_json_text(value),
        DestinationTypeCompatibilityMode::Coerce => coerce_json_text(typ, value),
    }
}

/// Applies strict materialization to a source cell.
fn strict_cell(typ: &Type, cell: Cell) -> BigQueryCellMaterializationResult {
    if let Some(result) = validate_strict_cell(typ, &cell) {
        return result;
    }

    materialized_native(BigQueryType::native_for_source_type(typ), cell, Unchanged)
}

/// Applies strict materialization to a source array column.
fn strict_source_array_cell(typ: &Type, cell: Cell) -> BigQueryCellMaterializationResult {
    match cell {
        Cell::Null => invalid(
            ErrorKind::UnsupportedValueInDestination,
            "NULL arrays cannot be preserved by BigQuery repeated fields",
        ),
        Cell::Array(array) => {
            if let Some(result) = validate_array_has_no_nulls(&array) {
                result
            } else {
                strict_cell(typ, Cell::Array(array))
            }
        }
        cell => strict_cell(typ, cell),
    }
}

/// Applies strict materialization to raw JSON text.
fn strict_json_text(typ: &Type, value: String) -> BigQueryCellMaterializationResult {
    match parse_json_text_without_duplicate_keys(&value) {
        Ok(value_json) => {
            if let Some(result) = validate_strict_json(&value_json) {
                result
            } else {
                materialized(
                    BigQueryType::native_for_source_type(typ),
                    BigQueryCell::string(value),
                    Unchanged,
                )
            }
        }
        Err(result) => result,
    }
}

/// Applies compatible materialization to a source cell.
fn compatible_cell(typ: &Type, cell: Cell) -> BigQueryCellMaterializationResult {
    strict_cell(typ, cell)
}

/// Applies compatible materialization to a source array column.
fn compatible_source_array_cell(typ: &Type, cell: Cell) -> BigQueryCellMaterializationResult {
    match cell {
        Cell::Null => invalid(
            ErrorKind::UnsupportedValueInDestination,
            "NULL arrays cannot be preserved by BigQuery repeated fields",
        ),
        Cell::Array(array) => {
            if let Some(result) = validate_array_has_no_nulls(&array) {
                result
            } else if has_strict_bigquery_native_type(typ) {
                strict_cell(typ, Cell::Array(array))
            } else {
                string_element_array_cell(typ, array)
            }
        }
        cell => strict_cell(typ, cell),
    }
}

/// Applies compatible materialization to raw JSON text.
fn compatible_json_text(typ: &Type, value: String) -> BigQueryCellMaterializationResult {
    strict_json_text(typ, value)
}

/// Applies preserve materialization to a source cell.
fn preserve_cell(typ: &Type, cell: Cell) -> BigQueryCellMaterializationResult {
    match cell {
        Cell::F32(value) => {
            materialized(BigQueryType::String, BigQueryCell::string(value.to_string()), TypeChanged)
        }
        Cell::F64(value) => {
            materialized(BigQueryType::String, BigQueryCell::string(value.to_string()), TypeChanged)
        }
        Cell::Numeric(value) => {
            materialized(BigQueryType::String, BigQueryCell::string(value.to_string()), TypeChanged)
        }
        Cell::Date(value) => {
            materialized(BigQueryType::String, BigQueryCell::string(value.to_string()), TypeChanged)
        }
        Cell::Time(value) => {
            materialized(BigQueryType::String, BigQueryCell::string(value.to_string()), TypeChanged)
        }
        Cell::Timestamp(value) => {
            materialized(BigQueryType::String, BigQueryCell::string(value.to_string()), TypeChanged)
        }
        Cell::TimestampTz(value) => {
            materialized(BigQueryType::String, BigQueryCell::string(value.to_string()), TypeChanged)
        }
        Cell::Uuid(value) => {
            materialized(BigQueryType::String, BigQueryCell::string(value.to_string()), TypeChanged)
        }
        Cell::Array(array) => preserve_array_cell(array),
        cell => materialized_native(BigQueryType::native_for_source_type(typ), cell, Unchanged),
    }
}

/// Materializes any source array column as scalar text, preserving NULLs.
fn preserve_source_array_cell(cell: Cell) -> BigQueryCellMaterializationResult {
    match cell {
        Cell::Array(array) => materialized(
            BigQueryType::String,
            BigQueryCell::string(format_array(array)),
            TypeChanged,
        ),
        Cell::Null => materialized(BigQueryType::String, BigQueryCell::Null, TypeChanged),
        cell => materialized_native(BigQueryType::String, cell, TypeChanged),
    }
}

/// Preserves raw JSON text exactly.
fn preserve_json_text(value: String) -> BigQueryCellMaterializationResult {
    materialized(BigQueryType::String, BigQueryCell::string(value), TypeChanged)
}

/// Applies preserve materialization to a parsed array value.
fn preserve_array_cell(array: ArrayCell) -> BigQueryCellMaterializationResult {
    match array {
        ArrayCell::F32(values) => string_array(values, |value| value.to_string()),
        ArrayCell::F64(values) => string_array(values, |value| value.to_string()),
        ArrayCell::Numeric(values) => string_array(values, |value| value.to_string()),
        ArrayCell::Date(values) => string_array(values, |value| value.to_string()),
        ArrayCell::Time(values) => string_array(values, |value| value.to_string()),
        ArrayCell::Timestamp(values) => string_array(values, |value| value.to_string()),
        ArrayCell::TimestampTz(values) => string_array(values, |value| value.to_string()),
        ArrayCell::Uuid(values) => string_array(values, |value| value.to_string()),
        array => materialized_native(BigQueryType::String, Cell::Array(array), Unchanged),
    }
}

/// Applies coerce materialization to a source cell.
fn coerce_cell(typ: &Type, cell: Cell) -> BigQueryCellMaterializationResult {
    match cell {
        Cell::F32(value) if value == 0.0 && value.is_sign_negative() => materialized(
            BigQueryType::native_for_source_type(typ),
            BigQueryCell::Float32(0.0),
            ValueChanged,
        ),
        Cell::F64(value) if value == 0.0 && value.is_sign_negative() => materialized(
            BigQueryType::native_for_source_type(typ),
            BigQueryCell::Float64(0.0),
            ValueChanged,
        ),
        Cell::Numeric(value) => coerce_numeric_cell(value),
        Cell::Date(value) => coerce_date_cell(typ, value),
        Cell::Time(value) => coerce_time_cell(typ, value),
        Cell::Timestamp(value) => coerce_timestamp_cell(typ, value),
        Cell::TimestampTz(value) => coerce_timestamptz_cell(typ, value),
        Cell::Uuid(value) => {
            materialized(BigQueryType::String, BigQueryCell::string(value.to_string()), TypeChanged)
        }
        Cell::Array(array) => coerce_array_cell(typ, array),
        cell => materialized_native(BigQueryType::native_for_source_type(typ), cell, Unchanged),
    }
}

/// Applies coerce materialization to a source array column.
fn coerce_source_array_cell(typ: &Type, cell: Cell) -> BigQueryCellMaterializationResult {
    match cell {
        Cell::Null => invalid(
            ErrorKind::UnsupportedValueInDestination,
            "NULL arrays cannot be preserved by BigQuery repeated fields",
        ),
        Cell::Array(array) => {
            if let Some(result) = validate_array_has_no_nulls(&array) {
                result
            } else if materializes_as_bigquery_string_array_for_coerce(typ) {
                string_element_array_cell(typ, array)
            } else {
                coerce_array_cell(typ, array)
            }
        }
        cell => coerce_cell(typ, cell),
    }
}

/// Applies coerce materialization to raw JSON text.
fn coerce_json_text(typ: &Type, value: String) -> BigQueryCellMaterializationResult {
    match coerce_json_value_text(value) {
        Ok(value) => materialized(
            BigQueryType::native_for_source_type(typ),
            BigQueryCell::string(value),
            ValueChanged,
        ),
        Err(result) => result,
    }
}

/// Applies coerce materialization to a parsed array value.
fn coerce_array_cell(typ: &Type, array: ArrayCell) -> BigQueryCellMaterializationResult {
    match (typ, array) {
        (&Type::JSON_ARRAY | &Type::JSONB_ARRAY, ArrayCell::String(values)) => {
            coerce_json_array_cell(typ, values)
        }
        (_, ArrayCell::F32(values)) => value_array(
            typ,
            values,
            |value| {
                if value == 0.0 && value.is_sign_negative() { 0.0 } else { value }
            },
            ArrayCell::F32,
        ),
        (_, ArrayCell::F64(values)) => value_array(
            typ,
            values,
            |value| {
                if value == 0.0 && value.is_sign_negative() { 0.0 } else { value }
            },
            ArrayCell::F64,
        ),
        (_, ArrayCell::Numeric(values)) => coerce_numeric_array_cell(typ, values),
        (_, ArrayCell::Date(values)) => {
            value_array(typ, values, clamp_bigquery_date_value, ArrayCell::Date)
        }
        (_, ArrayCell::Time(values)) => {
            value_array(typ, values, clamp_bigquery_time_value, ArrayCell::Time)
        }
        (_, ArrayCell::Timestamp(values)) => {
            value_array(typ, values, clamp_bigquery_timestamp_value, ArrayCell::Timestamp)
        }
        (_, ArrayCell::TimestampTz(values)) => {
            value_array(typ, values, clamp_bigquery_timestamptz_value, ArrayCell::TimestampTz)
        }
        (_, ArrayCell::Uuid(values)) => string_array(values, |value| value.to_string()),
        (_, array) => materialized_native(
            BigQueryType::native_for_source_type(typ),
            Cell::Array(array),
            Unchanged,
        ),
    }
}

/// Applies coerce materialization to JSON array element values.
fn coerce_json_array_cell(
    typ: &Type,
    values: Vec<Option<String>>,
) -> BigQueryCellMaterializationResult {
    let values = values
        .into_iter()
        .enumerate()
        .map(|(index, value)| match value {
            Some(value) => coerce_json_value_text(value)
                .map(Some)
                .map_err(|result| prefix_array_error(index, result)),
            None => Err(invalid(
                ErrorKind::NullValuesNotSupportedInArrayInDestination,
                format!("Element at index {index} is NULL, which is not supported in BigQuery"),
            )),
        })
        .collect::<Result<Vec<_>, _>>();

    match values {
        Ok(values) => materialized_native(
            BigQueryType::native_for_source_type(typ),
            Cell::Array(ArrayCell::String(values)),
            ValueChanged,
        ),
        Err(result) => result,
    }
}

/// Normalizes raw JSON text using BigQuery coercion rules.
fn coerce_json_value_text(value: String) -> Result<String, BigQueryCellMaterializationResult> {
    let value_json = parse_json_text_first_key_wins(&value)?;
    if let Err(reason) = validate_bigquery_json_depth(&value_json) {
        return Err(invalid(ErrorKind::UnsupportedValueInDestination, reason));
    }

    coerce_bigquery_json(value_json)
        .map(|normalized| normalized.to_string())
        .map_err(|reason| invalid(ErrorKind::UnsupportedValueInDestination, reason))
}

/// Applies coerce materialization to a numeric value.
fn coerce_numeric_cell(value: PgNumeric) -> BigQueryCellMaterializationResult {
    match coerce_bigquery_numeric(value) {
        Ok(value) => materialized(
            BigQueryType::BigNumeric,
            BigQueryCell::string(value.to_string()),
            ValueChanged,
        ),
        Err(reason) => invalid(ErrorKind::UnsupportedValueInDestination, reason),
    }
}

/// Applies coerce materialization to numeric array values.
fn coerce_numeric_array_cell(
    typ: &Type,
    values: Vec<Option<PgNumeric>>,
) -> BigQueryCellMaterializationResult {
    let values = values
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            value
                .map(coerce_bigquery_numeric)
                .transpose()
                .map_err(|reason| format!("Element at index {index}: {reason}"))
        })
        .collect::<Result<Vec<_>, _>>();

    match values {
        Ok(values) => materialized_native(
            BigQueryType::native_for_source_type(typ),
            Cell::Array(ArrayCell::Numeric(values)),
            ValueChanged,
        ),
        Err(reason) => invalid(ErrorKind::UnsupportedValueInDestination, reason),
    }
}

/// Applies coerce materialization to a PostgreSQL date value.
fn coerce_date_cell(typ: &Type, value: PgDate) -> BigQueryCellMaterializationResult {
    materialized(
        BigQueryType::native_for_source_type(typ),
        BigQueryCell::string(clamp_bigquery_date_value(value).to_string()),
        ValueChanged,
    )
}

/// Applies coerce materialization to a PostgreSQL time value.
fn coerce_time_cell(typ: &Type, value: PgTime) -> BigQueryCellMaterializationResult {
    materialized(
        BigQueryType::native_for_source_type(typ),
        BigQueryCell::string(clamp_bigquery_time_value(value).to_string()),
        ValueChanged,
    )
}

/// Applies coerce materialization to a PostgreSQL timestamp value.
fn coerce_timestamp_cell(typ: &Type, value: PgTimestamp) -> BigQueryCellMaterializationResult {
    materialized(
        BigQueryType::native_for_source_type(typ),
        BigQueryCell::string(clamp_bigquery_timestamp_value(value).to_string()),
        ValueChanged,
    )
}

/// Applies coerce materialization to a PostgreSQL timestamptz value.
fn coerce_timestamptz_cell(typ: &Type, value: PgTimestampTz) -> BigQueryCellMaterializationResult {
    materialized(
        BigQueryType::native_for_source_type(typ),
        BigQueryCell::string(clamp_bigquery_timestamptz_value(value).to_string()),
        ValueChanged,
    )
}

/// Formats an array as a scalar string that preserves `NULL` elements.
fn format_array(array: ArrayCell) -> String {
    match array {
        ArrayCell::Bool(values) => format_array_values(values, |value| value.to_string()),
        ArrayCell::String(values) => format_array_values(values, |value| value),
        ArrayCell::I16(values) => format_array_values(values, |value| value.to_string()),
        ArrayCell::I32(values) => format_array_values(values, |value| value.to_string()),
        ArrayCell::U32(values) => format_array_values(values, |value| value.to_string()),
        ArrayCell::I64(values) => format_array_values(values, |value| value.to_string()),
        ArrayCell::F32(values) => format_array_values(values, |value| value.to_string()),
        ArrayCell::F64(values) => format_array_values(values, |value| value.to_string()),
        ArrayCell::Numeric(values) => format_array_values(values, |value| value.to_string()),
        ArrayCell::Date(values) => format_array_values(values, |value| value.to_string()),
        ArrayCell::Time(values) => format_array_values(values, |value| value.to_string()),
        ArrayCell::Timestamp(values) => format_array_values(values, |value| value.to_string()),
        ArrayCell::TimestampTz(values) => format_array_values(values, |value| value.to_string()),
        ArrayCell::Uuid(values) => format_array_values(values, |value| value.to_string()),
        ArrayCell::Bytes(values) => {
            format_array_values(values, |value| format!("\\x{}", encode_hex(&value)))
        }
    }
}

/// Formats nullable array elements as a PostgreSQL-style array literal.
fn format_array_values<T>(
    values: Vec<Option<T>>,
    mut format_value: impl FnMut(T) -> String,
) -> String {
    let values = values
        .into_iter()
        .map(|value| match value {
            Some(value) => quote_array_value(&format_value(value)),
            None => "NULL".to_owned(),
        })
        .collect::<Vec<_>>()
        .join(",");

    format!("{{{values}}}")
}

/// Quotes one array element when needed for PostgreSQL-style array text.
fn quote_array_value(value: &str) -> String {
    if value.is_empty()
        || value.eq_ignore_ascii_case("NULL")
        || value.chars().any(|ch| matches!(ch, '"' | '\\' | '{' | '}' | ',') || ch.is_whitespace())
    {
        let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
        format!("\"{escaped}\"")
    } else {
        value.to_owned()
    }
}

/// Lowercase hex-encodes `bytes`.
fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);

    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }

    encoded
}

/// Converts a cell for a source type materialized as scalar BigQuery `STRING`.
fn string_materialized_cell(cell: Cell) -> BigQueryCellMaterializationResult {
    let cell = match cell {
        Cell::Null => BigQueryCell::Null,
        Cell::Bool(value) => BigQueryCell::string(value.to_string()),
        Cell::String(value) => BigQueryCell::string(value),
        Cell::I16(value) => BigQueryCell::string(value.to_string()),
        Cell::I32(value) => BigQueryCell::string(value.to_string()),
        Cell::U32(value) => BigQueryCell::string(value.to_string()),
        Cell::I64(value) => BigQueryCell::string(value.to_string()),
        Cell::F32(value) => BigQueryCell::string(value.to_string()),
        Cell::F64(value) => BigQueryCell::string(value.to_string()),
        Cell::Numeric(value) => BigQueryCell::string(value.to_string()),
        Cell::Date(value) => BigQueryCell::string(value.to_string()),
        Cell::Time(value) => BigQueryCell::string(value.to_string()),
        Cell::Timestamp(value) => BigQueryCell::string(value.to_string()),
        Cell::TimestampTz(value) => BigQueryCell::string(value.to_string()),
        Cell::Uuid(value) => BigQueryCell::string(value.to_string()),
        Cell::Bytes(value) => BigQueryCell::string(format!("\\x{}", encode_hex(&value))),
        Cell::Array(array) => BigQueryCell::string(format_array(array)),
    };

    materialized(BigQueryType::String, cell, TypeChanged)
}

/// Converts a source array column to a BigQuery repeated `STRING` cell.
fn string_element_array_cell(typ: &Type, array: ArrayCell) -> BigQueryCellMaterializationResult {
    match array {
        ArrayCell::Bool(values) => string_element_array(typ, values, |value| value.to_string()),
        ArrayCell::String(values) => string_element_array(typ, values, |value| value),
        ArrayCell::I16(values) => string_element_array(typ, values, |value| value.to_string()),
        ArrayCell::I32(values) => string_element_array(typ, values, |value| value.to_string()),
        ArrayCell::U32(values) => string_element_array(typ, values, |value| value.to_string()),
        ArrayCell::I64(values) => string_element_array(typ, values, |value| value.to_string()),
        ArrayCell::F32(values) => string_element_array(typ, values, |value| value.to_string()),
        ArrayCell::F64(values) => string_element_array(typ, values, |value| value.to_string()),
        ArrayCell::Numeric(values) => string_element_array(typ, values, |value| value.to_string()),
        ArrayCell::Date(values) => string_element_array(typ, values, |value| value.to_string()),
        ArrayCell::Time(values) => string_element_array(typ, values, |value| value.to_string()),
        ArrayCell::Timestamp(values) => {
            string_element_array(typ, values, |value| value.to_string())
        }
        ArrayCell::TimestampTz(values) => {
            string_element_array(typ, values, |value| value.to_string())
        }
        ArrayCell::Uuid(values) => string_element_array(typ, values, |value| value.to_string()),
        ArrayCell::Bytes(values) => {
            string_element_array(typ, values, |value| format!("\\x{}", encode_hex(&value)))
        }
    }
}

/// Converts an optional array to repeated `STRING` values.
fn string_element_array<T>(
    typ: &Type,
    values: Vec<Option<T>>,
    convert: impl FnMut(T) -> String,
) -> BigQueryCellMaterializationResult {
    let converted_values = match convert_string_array_values(values, convert) {
        Ok(values) => values,
        Err(result) => return result,
    };

    materialized(
        BigQueryType::native_for_source_type(typ),
        BigQueryCell::Array(BigQueryArrayCell::String(converted_values)),
        TypeChanged,
    )
}

/// Converts an optional array to a string array.
fn string_array<T>(
    values: Vec<Option<T>>,
    convert: impl FnMut(T) -> String,
) -> BigQueryCellMaterializationResult {
    let converted_values = match convert_string_array_values(values, convert) {
        Ok(values) => values,
        Err(result) => return result,
    };

    materialized(
        BigQueryType::String,
        BigQueryCell::Array(BigQueryArrayCell::String(converted_values)),
        TypeChanged,
    )
}

/// Converts optional values to required `STRING` array elements.
fn convert_string_array_values<T>(
    values: Vec<Option<T>>,
    mut convert: impl FnMut(T) -> String,
) -> Result<Vec<String>, BigQueryCellMaterializationResult> {
    let mut converted_values = Vec::with_capacity(values.len());
    for (index, value) in values.into_iter().enumerate() {
        match value {
            Some(value) => converted_values.push(convert(value)),
            None => {
                return Err(invalid(
                    ErrorKind::NullValuesNotSupportedInArrayInDestination,
                    format!("Element at index {index} is NULL, which is not supported in BigQuery"),
                ));
            }
        }
    }

    Ok(converted_values)
}

/// Converts an optional array while preserving the array variant.
fn value_array<T>(
    typ: &Type,
    values: Vec<Option<T>>,
    mut convert: impl FnMut(T) -> T,
    wrap: impl FnOnce(Vec<Option<T>>) -> ArrayCell,
) -> BigQueryCellMaterializationResult {
    materialized_native(
        BigQueryType::native_for_source_type(typ),
        Cell::Array(wrap(values.into_iter().map(|value| value.map(&mut convert)).collect())),
        ValueChanged,
    )
}

/// Returns a strict materialization failure when a cell is unsafe for BigQuery.
fn validate_strict_cell(typ: &Type, cell: &Cell) -> Option<BigQueryCellMaterializationResult> {
    match cell {
        Cell::F32(value) if *value == 0.0 && value.is_sign_negative() => {
            Some(invalid_negative_zero())
        }
        Cell::F64(value) if *value == 0.0 && value.is_sign_negative() => {
            Some(invalid_negative_zero())
        }
        Cell::Numeric(value) => validate_strict_numeric(value),
        Cell::Date(value) => validate_strict_pg_date(value),
        Cell::Time(value) => validate_strict_pg_time(value),
        Cell::Timestamp(value) => validate_strict_pg_timestamp(value),
        Cell::TimestampTz(value) => validate_strict_pg_timestamptz(value),
        Cell::Uuid(_) => Some(invalid_non_native_value("UUID")),
        Cell::Array(array) => validate_strict_array(typ, array),
        _ => None,
    }
}

/// Returns a strict materialization failure when an array is unsafe for
/// BigQuery.
fn validate_strict_array(
    typ: &Type,
    array: &ArrayCell,
) -> Option<BigQueryCellMaterializationResult> {
    match (typ, array) {
        (&Type::JSON_ARRAY | &Type::JSONB_ARRAY, ArrayCell::String(values)) => {
            validate_strict_array_values(values, |value| validate_strict_json_text(value))
        }
        (_, array) => validate_strict_array_value_domain(array),
    }
}

/// Returns a strict materialization failure based on the parsed array values.
fn validate_strict_array_value_domain(
    array: &ArrayCell,
) -> Option<BigQueryCellMaterializationResult> {
    match array {
        ArrayCell::F32(values) => validate_strict_array_values(values, |value| {
            (*value == 0.0 && value.is_sign_negative()).then(invalid_negative_zero)
        }),
        ArrayCell::F64(values) => validate_strict_array_values(values, |value| {
            (*value == 0.0 && value.is_sign_negative()).then(invalid_negative_zero)
        }),
        ArrayCell::Numeric(values) => validate_strict_array_values(values, validate_strict_numeric),
        ArrayCell::Date(values) => validate_strict_array_values(values, validate_strict_pg_date),
        ArrayCell::Time(values) => validate_strict_array_values(values, validate_strict_pg_time),
        ArrayCell::Timestamp(values) => {
            validate_strict_array_values(values, validate_strict_pg_timestamp)
        }
        ArrayCell::TimestampTz(values) => {
            validate_strict_array_values(values, validate_strict_pg_timestamptz)
        }
        ArrayCell::Uuid(values) => {
            validate_strict_array_values(values, |_| Some(invalid_non_native_value("UUID")))
        }
        _ => None,
    }
}

/// Validates strict materialization for optional array elements.
fn validate_strict_array_values<T>(
    values: &[Option<T>],
    validate: impl Fn(&T) -> Option<BigQueryCellMaterializationResult>,
) -> Option<BigQueryCellMaterializationResult> {
    for (index, value) in values.iter().enumerate() {
        if let Some(value) = value
            && let Some(result) = validate(value)
        {
            return Some(prefix_array_error(index, result));
        }
    }

    None
}

/// Adds array element context to a materialization failure.
fn prefix_array_error(
    index: usize,
    result: BigQueryCellMaterializationResult,
) -> BigQueryCellMaterializationResult {
    match result {
        CellMaterializationResult::Invalid { kind, reason } => {
            invalid(kind, format!("Element at index {index}: {reason}"))
        }
        result => result,
    }
}

/// Validates strict BigQuery materialization for a numeric value.
fn validate_strict_numeric(value: &PgNumeric) -> Option<BigQueryCellMaterializationResult> {
    match value {
        PgNumeric::NaN => Some(invalid(
            ErrorKind::UnsupportedValueInDestination,
            "NaN cannot be stored in BigQuery BIGNUMERIC",
        )),
        PgNumeric::PositiveInfinity | PgNumeric::NegativeInfinity => Some(invalid(
            ErrorKind::UnsupportedValueInDestination,
            "Infinity cannot be stored in BigQuery BIGNUMERIC",
        )),
        PgNumeric::Value { .. }
            if bigquery_bignumeric_scale(value) > BIGQUERY_BIGNUMERIC_MAX_SCALE =>
        {
            Some(invalid(
                ErrorKind::UnsupportedValueInDestination,
                format!(
                    "A numeric value has more than {BIGQUERY_BIGNUMERIC_MAX_SCALE} decimal places \
                     and would be rounded by BigQuery",
                ),
            ))
        }
        PgNumeric::Value { .. } if !bigquery_bignumeric_in_range(value) => Some(invalid(
            ErrorKind::UnsupportedValueInDestination,
            "A numeric value is outside the BigQuery BIGNUMERIC range",
        )),
        PgNumeric::Value { .. } => None,
    }
}

/// Validates strict BigQuery materialization for a PostgreSQL date.
fn validate_strict_pg_date(value: &PgDate) -> Option<BigQueryCellMaterializationResult> {
    match value {
        PgDate::Finite(value) if bigquery_date_in_range(*value) => None,
        PgDate::Finite(_) => Some(invalid_date_range()),
        PgDate::PosInfinity | PgDate::NegInfinity | PgDate::OutOfRange(_) => {
            Some(invalid_date_range())
        }
    }
}

/// Validates strict BigQuery materialization for a PostgreSQL time.
fn validate_strict_pg_time(value: &PgTime) -> Option<BigQueryCellMaterializationResult> {
    match value {
        PgTime::Finite(_) => None,
        PgTime::TwentyFourHour => Some(invalid(
            ErrorKind::UnsupportedValueInDestination,
            "A time value is outside the BigQuery TIME range",
        )),
    }
}

/// Validates strict BigQuery materialization for a PostgreSQL timestamp.
fn validate_strict_pg_timestamp(value: &PgTimestamp) -> Option<BigQueryCellMaterializationResult> {
    match value {
        PgTimestamp::Finite(value) if bigquery_timestamp_in_range(*value) => None,
        PgTimestamp::Finite(_) => Some(invalid_timestamp_range()),
        PgTimestamp::PosInfinity | PgTimestamp::NegInfinity | PgTimestamp::OutOfRange(_) => {
            Some(invalid_timestamp_range())
        }
    }
}

/// Validates strict BigQuery materialization for a PostgreSQL timestamptz.
fn validate_strict_pg_timestamptz(
    value: &PgTimestampTz,
) -> Option<BigQueryCellMaterializationResult> {
    match value {
        PgTimestampTz::Finite(value) if bigquery_timestamptz_in_range(*value) => None,
        PgTimestampTz::Finite(_) => Some(invalid_timestamp_range()),
        PgTimestampTz::PosInfinity | PgTimestampTz::NegInfinity | PgTimestampTz::OutOfRange(_) => {
            Some(invalid_timestamp_range())
        }
    }
}

/// Validates that raw JSON text has no duplicate object keys.
fn validate_json_text_has_no_duplicate_keys(
    value: &str,
) -> Option<BigQueryCellMaterializationResult> {
    match json_contains_duplicate_keys(value) {
        Ok(true) => Some(invalid(
            ErrorKind::UnsupportedValueInDestination,
            "JSON text contains duplicate object keys that BigQuery would canonicalize",
        )),
        Ok(false) => None,
        Err(error) => Some(invalid(
            ErrorKind::UnsupportedValueInDestination,
            format!("JSON text is not valid for BigQuery: {error}"),
        )),
    }
}

/// Parses JSON text after rejecting BigQuery canonicalization risks.
fn parse_json_text_without_duplicate_keys(
    value: &str,
) -> Result<serde_json::Value, BigQueryCellMaterializationResult> {
    if let Some(result) = validate_json_text_has_no_duplicate_keys(value) {
        return Err(result);
    }

    serde_json::from_str(value).map_err(|error| {
        invalid(
            ErrorKind::UnsupportedValueInDestination,
            format!("JSON text is not valid for BigQuery: {error}"),
        )
    })
}

/// Parses JSON text using BigQuery's first-key-wins object canonicalization.
fn parse_json_text_first_key_wins(
    value: &str,
) -> Result<serde_json::Value, BigQueryCellMaterializationResult> {
    let mut deserializer = serde_json::Deserializer::from_str(value);
    let value = JsonFirstKeyWinsParser.deserialize(&mut deserializer).map_err(|error| {
        invalid(
            ErrorKind::UnsupportedValueInDestination,
            format!("JSON text is not valid for BigQuery: {error}"),
        )
    })?;
    deserializer.end().map_err(|error| {
        invalid(
            ErrorKind::UnsupportedValueInDestination,
            format!("JSON text is not valid for BigQuery: {error}"),
        )
    })?;

    Ok(value)
}

/// Validates strict BigQuery materialization for raw JSON text.
fn validate_strict_json_text(value: &str) -> Option<BigQueryCellMaterializationResult> {
    match parse_json_text_without_duplicate_keys(value) {
        Ok(value_json) => validate_strict_json(&value_json),
        Err(result) => Some(result),
    }
}

/// Validates strict BigQuery materialization for JSON.
fn validate_strict_json(value: &serde_json::Value) -> Option<BigQueryCellMaterializationResult> {
    if let Err(reason) = validate_bigquery_json_depth(value) {
        return Some(invalid(ErrorKind::UnsupportedValueInDestination, reason));
    }

    let mut values = vec![value];

    while let Some(value) = values.pop() {
        match value {
            serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::String(_) => {
            }
            serde_json::Value::Number(number) => {
                if let Some(result) = validate_strict_json_number(number) {
                    return Some(result);
                }
            }
            serde_json::Value::Array(items) => values.extend(items),
            serde_json::Value::Object(entries) => values.extend(entries.values()),
        }
    }

    None
}

/// Streaming JSON parser that keeps the first object value for duplicate keys.
struct JsonFirstKeyWinsParser;

impl<'de> DeserializeSeed<'de> for JsonFirstKeyWinsParser {
    type Value = serde_json::Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for JsonFirstKeyWinsParser {
    type Value = serde_json::Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("any JSON value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Number(value.into()))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Number(value.into()))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| de::Error::custom("JSON number is not finite"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(serde_json::Value::String(value.to_owned()))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(serde_json::Value::String(value))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Null)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Null)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = seq.next_element_seed(JsonFirstKeyWinsParser)? {
            values.push(value);
        }

        Ok(serde_json::Value::Array(values))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let Some(first_key) = map.next_key::<String>()? else {
            return Ok(serde_json::Value::Object(serde_json::Map::new()));
        };

        if first_key == SERDE_JSON_NUMBER_TOKEN {
            let number = map.next_value::<String>()?;
            return serde_json::from_str::<serde_json::Number>(&number)
                .map(serde_json::Value::Number)
                .map_err(de::Error::custom);
        }

        let mut values = serde_json::Map::new();
        let first_value = map.next_value_seed(JsonFirstKeyWinsParser)?;
        values.insert(first_key, first_value);

        while let Some(key) = map.next_key::<String>()? {
            let value = map.next_value_seed(JsonFirstKeyWinsParser)?;
            values.entry(key).or_insert(value);
        }

        Ok(serde_json::Value::Object(values))
    }
}

/// Returns whether raw JSON text contains duplicate object keys.
fn json_contains_duplicate_keys(value: &str) -> Result<bool, serde_json::Error> {
    let mut detector = JsonDuplicateKeyDetector { found_duplicate: false };
    let mut deserializer = serde_json::Deserializer::from_str(value);
    (&mut detector).deserialize(&mut deserializer)?;

    Ok(detector.found_duplicate)
}

/// Streaming JSON duplicate-key detector.
struct JsonDuplicateKeyDetector {
    /// Whether any visited object contained duplicate keys.
    found_duplicate: bool,
}

impl<'de> DeserializeSeed<'de> for &mut JsonDuplicateKeyDetector {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for &mut JsonDuplicateKeyDetector {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("any JSON value")
    }

    fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_string<E>(self, _value: String) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while seq.next_element_seed(&mut *self)?.is_some() {}

        Ok(())
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut keys = HashSet::new();
        while let Some(key) = map.next_key::<String>()? {
            if !keys.insert(key) {
                self.found_duplicate = true;
            }
            map.next_value_seed(&mut *self)?;
        }

        Ok(())
    }
}

/// Validates that a JSON value fits BigQuery's nesting depth limit.
fn validate_bigquery_json_depth(value: &serde_json::Value) -> Result<(), String> {
    let mut values = vec![(value, 1usize)];

    while let Some((value, depth)) = values.pop() {
        if depth > BIGQUERY_JSON_MAX_NESTING_DEPTH {
            return Err(format!(
                "A JSON value exceeds BigQuery's maximum nesting depth of \
                 {BIGQUERY_JSON_MAX_NESTING_DEPTH} levels",
            ));
        }

        match value {
            serde_json::Value::Array(items) => {
                values.extend(items.iter().map(|item| (item, depth + 1)));
            }
            serde_json::Value::Object(entries) => {
                values.extend(entries.values().map(|item| (item, depth + 1)));
            }
            serde_json::Value::Null
            | serde_json::Value::Bool(_)
            | serde_json::Value::Number(_)
            | serde_json::Value::String(_) => {}
        }
    }

    Ok(())
}

/// Validates strict BigQuery materialization for a JSON number.
fn validate_strict_json_number(
    number: &serde_json::Number,
) -> Option<BigQueryCellMaterializationResult> {
    let number = number.as_str();
    if is_json_integer_literal(number) {
        let integer_outside_exact_domain = if number.starts_with('-') {
            number.parse::<i64>().is_err()
        } else {
            number.parse::<u64>().is_err()
        };

        if integer_outside_exact_domain {
            return Some(invalid(
                ErrorKind::UnsupportedValueInDestination,
                "A JSON integer is outside BigQuery's exact signed or unsigned 64-bit integer \
                 domain and may be stored as FLOAT64",
            ));
        }
    } else if !json_number_fits_float64(number) {
        return Some(invalid(
            ErrorKind::UnsupportedValueInDestination,
            "A JSON number is outside BigQuery's FLOAT64 domain",
        ));
    }

    None
}

/// Returns whether a JSON number literal is an integer literal.
fn is_json_integer_literal(number: &str) -> bool {
    !number.contains(['.', 'e', 'E'])
}

/// Returns whether a JSON number can be represented as finite `FLOAT64`.
fn json_number_fits_float64(number: &str) -> bool {
    number.parse::<f64>().is_ok_and(f64::is_finite)
}

/// Normalizes a JSON value using deterministic BigQuery coercion rules.
fn coerce_bigquery_json(value: serde_json::Value) -> Result<serde_json::Value, String> {
    match value {
        serde_json::Value::Array(values) => {
            let values =
                values.into_iter().map(coerce_bigquery_json).collect::<Result<Vec<_>, _>>()?;
            Ok(serde_json::Value::Array(values))
        }
        serde_json::Value::Object(values) => {
            let values = values
                .into_iter()
                .map(|(key, value)| coerce_bigquery_json(value).map(|value| (key, value)))
                .collect::<Result<serde_json::Map<_, _>, _>>()?;
            Ok(serde_json::Value::Object(values))
        }
        serde_json::Value::Number(number) => {
            coerce_bigquery_json_number(number).map(serde_json::Value::Number)
        }
        value => Ok(value),
    }
}

/// Normalizes a JSON number using deterministic BigQuery coercion rules.
fn coerce_bigquery_json_number(number: serde_json::Number) -> Result<serde_json::Number, String> {
    let number_string = number.as_str().to_owned();
    let needs_float_normalization = if is_json_integer_literal(&number_string) {
        if number_string.starts_with('-') {
            number_string.parse::<i64>().is_err()
        } else {
            number_string.parse::<u64>().is_err()
        }
    } else {
        !json_number_fits_float64(&number_string)
    };

    if !needs_float_normalization {
        return Ok(number);
    }

    let normalized = match number_string.parse::<f64>() {
        Ok(value) if value.is_finite() => value,
        Ok(_) | Err(_) => {
            return Err(format!(
                "JSON number {number_string} cannot be rounded to BigQuery's JSON number domain",
            ));
        }
    };

    serde_json::Number::from_f64(normalized).ok_or_else(|| {
        format!("JSON number {number_string} could not be normalized to a finite FLOAT64 value")
    })
}

/// Returns a strict materialization failure for negative zero.
fn invalid_negative_zero() -> BigQueryCellMaterializationResult {
    invalid(
        ErrorKind::UnsupportedValueInDestination,
        "Negative zero cannot be stored in BigQuery tables",
    )
}

/// Returns a strict materialization failure for a date outside BigQuery's
/// range.
fn invalid_date_range() -> BigQueryCellMaterializationResult {
    invalid(
        ErrorKind::UnsupportedValueInDestination,
        "Date value is outside BigQuery's supported range 0001-01-01..=9999-12-31",
    )
}

/// Returns a strict materialization failure for a timestamp outside BigQuery's
/// range.
fn invalid_timestamp_range() -> BigQueryCellMaterializationResult {
    invalid(
        ErrorKind::UnsupportedValueInDestination,
        "Timestamp value is outside BigQuery's supported range 0001-01-01..=9999-12-31",
    )
}

/// Returns a strict materialization failure for a value without a native type.
fn invalid_non_native_value(type_name: &str) -> BigQueryCellMaterializationResult {
    invalid(
        ErrorKind::UnsupportedValueInDestination,
        format!("{type_name} has no strict native BigQuery representation"),
    )
}

/// Returns a successful materialized cell.
fn materialized(
    typ: BigQueryType,
    cell: BigQueryCell,
    outcome: MaterializationOutcome,
) -> BigQueryCellMaterializationResult {
    CellMaterializationResult::Materialized { cell: TypedCell::new(typ, cell), outcome }
}

/// Converts a native cell and returns a successful materialized cell.
fn materialized_native(
    typ: BigQueryType,
    cell: Cell,
    outcome: MaterializationOutcome,
) -> BigQueryCellMaterializationResult {
    match BigQueryCell::try_from_native_cell(cell) {
        Ok(cell) => materialized(typ, cell, outcome),
        Err(error) => invalid_from_etl_error(error),
    }
}

/// Returns an invalid materialization result.
fn invalid(kind: ErrorKind, reason: impl Into<String>) -> BigQueryCellMaterializationResult {
    CellMaterializationResult::Invalid { kind, reason: reason.into() }
}

/// Returns an invalid materialization result from an [`EtlError`].
fn invalid_from_etl_error(error: EtlError) -> BigQueryCellMaterializationResult {
    let reason = error
        .detail()
        .or_else(|| error.description())
        .map_or_else(|| error.to_string(), ToOwned::to_owned);
    invalid(error.kind(), reason)
}

/// Returns a null-array materialization failure, if any element is null.
fn validate_array_has_no_nulls(array: &ArrayCell) -> Option<BigQueryCellMaterializationResult> {
    let (element_count, null_count) = array_counts(array);
    if null_count == 0 {
        return None;
    }

    Some(invalid(
        ErrorKind::NullValuesNotSupportedInArrayInDestination,
        format!(
            "Array contains {null_count} NULL values across {element_count} elements, which are \
             not supported in BigQuery",
        ),
    ))
}

/// Returns the total and null element counts for an array.
fn array_counts(array: &ArrayCell) -> (usize, usize) {
    macro_rules! counts {
        ($values:expr) => {
            ($values.len(), $values.iter().filter(|value| value.is_none()).count())
        };
    }

    match array {
        ArrayCell::Bool(values) => counts!(values),
        ArrayCell::String(values) => counts!(values),
        ArrayCell::I16(values) => counts!(values),
        ArrayCell::I32(values) => counts!(values),
        ArrayCell::U32(values) => counts!(values),
        ArrayCell::I64(values) => counts!(values),
        ArrayCell::F32(values) => counts!(values),
        ArrayCell::F64(values) => counts!(values),
        ArrayCell::Numeric(values) => counts!(values),
        ArrayCell::Date(values) => counts!(values),
        ArrayCell::Time(values) => counts!(values),
        ArrayCell::Timestamp(values) => counts!(values),
        ArrayCell::TimestampTz(values) => counts!(values),
        ArrayCell::Uuid(values) => counts!(values),
        ArrayCell::Bytes(values) => counts!(values),
    }
}

/// Returns the number of fractional digits in a numeric string.
fn bigquery_bignumeric_scale(value: &PgNumeric) -> usize {
    value.to_string().split_once('.').map_or(0, |(_, fractional)| fractional.len())
}

/// Returns whether a numeric value is in BigQuery `BIGNUMERIC` range.
fn bigquery_bignumeric_in_range(value: &PgNumeric) -> bool {
    match value {
        PgNumeric::Value { .. } => {
            let value = value.to_string();
            if value.starts_with('-') {
                compare_decimal_abs(&value, BIGQUERY_BIGNUMERIC_MIN) != std::cmp::Ordering::Greater
            } else {
                compare_decimal_abs(&value, BIGQUERY_BIGNUMERIC_MAX) != std::cmp::Ordering::Greater
            }
        }
        PgNumeric::NaN | PgNumeric::PositiveInfinity | PgNumeric::NegativeInfinity => false,
    }
}

/// Applies BigQuery `BIGNUMERIC` coercion by rounding and clamping.
fn coerce_bigquery_numeric(value: PgNumeric) -> Result<PgNumeric, String> {
    let value = match value {
        PgNumeric::NaN => return Err("NaN cannot be normalized to BigQuery BIGNUMERIC".to_owned()),
        PgNumeric::PositiveInfinity => BIGQUERY_BIGNUMERIC_MAX.to_owned(),
        PgNumeric::NegativeInfinity => BIGQUERY_BIGNUMERIC_MIN.to_owned(),
        PgNumeric::Value { .. } => value.to_string(),
    };

    let rounded = round_decimal_half_away_from_zero(&value, BIGQUERY_BIGNUMERIC_MAX_SCALE);
    let clamped = clamp_decimal_to_bigquery_bignumeric(&rounded);
    PgNumeric::from_str(&clamped).map_err(|error| error.to_string())
}

/// Clamps a decimal string to BigQuery `BIGNUMERIC` range.
fn clamp_decimal_to_bigquery_bignumeric(value: &str) -> String {
    if value.starts_with('-') {
        if compare_decimal_abs(value, BIGQUERY_BIGNUMERIC_MIN) == std::cmp::Ordering::Greater {
            BIGQUERY_BIGNUMERIC_MIN.to_owned()
        } else {
            value.to_owned()
        }
    } else if compare_decimal_abs(value, BIGQUERY_BIGNUMERIC_MAX) == std::cmp::Ordering::Greater {
        BIGQUERY_BIGNUMERIC_MAX.to_owned()
    } else {
        value.to_owned()
    }
}

/// Rounds a decimal string using half-away-from-zero.
fn round_decimal_half_away_from_zero(value: &str, scale: usize) -> String {
    let (negative, unsigned) =
        value.strip_prefix('-').map_or((false, value), |value| (true, value));
    let (integer, fractional) = unsigned.split_once('.').unwrap_or((unsigned, ""));

    if fractional.len() <= scale {
        return value.to_owned();
    }

    let mut integer_digits = integer.as_bytes().to_vec();
    let mut fractional_digits = fractional.as_bytes()[..scale].to_vec();
    let should_round_up = fractional.as_bytes()[scale] >= b'5';

    if should_round_up {
        increment_decimal_digits(&mut integer_digits, &mut fractional_digits);
    }

    let integer = integer_digits.into_iter().map(char::from).collect::<String>();
    let fractional = fractional_digits.into_iter().map(char::from).collect::<String>();
    let unsigned = if scale == 0 { integer } else { format!("{integer}.{fractional}") };

    if negative && !is_decimal_zero(&unsigned) { format!("-{unsigned}") } else { unsigned }
}

/// Returns whether a decimal string is numerically zero.
fn is_decimal_zero(value: &str) -> bool {
    normalized_decimal_abs(value) == ("0", "")
}

/// Increments decimal digits split into integer and fractional parts.
fn increment_decimal_digits(integer_digits: &mut Vec<u8>, fractional_digits: &mut [u8]) {
    for digit in fractional_digits.iter_mut().rev() {
        if *digit < b'9' {
            *digit += 1;
            return;
        }
        *digit = b'0';
    }

    for digit in integer_digits.iter_mut().rev() {
        if *digit < b'9' {
            *digit += 1;
            return;
        }
        *digit = b'0';
    }

    integer_digits.insert(0, b'1');
}

/// Compares the absolute values of two decimal strings.
fn compare_decimal_abs(left: &str, right: &str) -> std::cmp::Ordering {
    let (left_integer, left_fractional) = normalized_decimal_abs(left);
    let (right_integer, right_fractional) = normalized_decimal_abs(right);

    left_integer
        .len()
        .cmp(&right_integer.len())
        .then_with(|| left_integer.cmp(right_integer))
        .then_with(|| compare_fractional_decimal(left_fractional, right_fractional))
}

/// Returns normalized absolute integer and fractional decimal parts.
fn normalized_decimal_abs(value: &str) -> (&str, &str) {
    let value = value.strip_prefix('-').unwrap_or(value);
    let (integer, fractional) = value.split_once('.').unwrap_or((value, ""));
    let integer = integer.trim_start_matches('0');
    let integer = if integer.is_empty() { "0" } else { integer };
    let fractional = fractional.trim_end_matches('0');
    (integer, fractional)
}

/// Compares two fractional decimal parts.
fn compare_fractional_decimal(left: &str, right: &str) -> std::cmp::Ordering {
    let max_len = left.len().max(right.len());
    for index in 0..max_len {
        let left = left.as_bytes().get(index).copied().unwrap_or(b'0');
        let right = right.as_bytes().get(index).copied().unwrap_or(b'0');
        match left.cmp(&right) {
            std::cmp::Ordering::Equal => {}
            ordering => return ordering,
        }
    }

    std::cmp::Ordering::Equal
}

/// Returns BigQuery's minimum date.
fn bigquery_min_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(1, 1, 1).unwrap_or(NaiveDate::MIN)
}

/// Returns BigQuery's maximum date.
fn bigquery_max_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(9999, 12, 31).unwrap_or(NaiveDate::MAX)
}

/// Returns whether a date fits BigQuery's date domain.
fn bigquery_date_in_range(value: NaiveDate) -> bool {
    (1..=9999).contains(&value.year())
}

/// Clamps a date to BigQuery's date domain.
fn clamp_bigquery_date(value: NaiveDate) -> NaiveDate {
    value.clamp(bigquery_min_date(), bigquery_max_date())
}

/// Normalizes a PostgreSQL date into BigQuery's finite date range.
fn clamp_bigquery_date_value(value: PgDate) -> PgDate {
    let value = match value {
        PgDate::Finite(value) => clamp_bigquery_date(value),
        PgDate::PosInfinity => bigquery_max_date(),
        PgDate::NegInfinity => bigquery_min_date(),
        PgDate::OutOfRange(value) => match value.bound() {
            PgTemporalBound::Lower => bigquery_min_date(),
            PgTemporalBound::Upper => bigquery_max_date(),
        },
    };

    PgDate::Finite(value)
}

/// Normalizes a PostgreSQL time into BigQuery's finite time range.
fn clamp_bigquery_time_value(value: PgTime) -> PgTime {
    match value {
        PgTime::Finite(value) => PgTime::Finite(value),
        PgTime::TwentyFourHour => PgTime::Finite(
            NaiveTime::from_hms_micro_opt(23, 59, 59, 999_999).unwrap_or(NaiveTime::MIN),
        ),
    }
}

/// Returns BigQuery's minimum timestamp.
fn bigquery_min_timestamp() -> NaiveDateTime {
    bigquery_min_date().and_hms_micro_opt(0, 0, 0, 0).unwrap_or(NaiveDateTime::MIN)
}

/// Returns BigQuery's maximum timestamp.
fn bigquery_max_timestamp() -> NaiveDateTime {
    bigquery_max_date().and_hms_micro_opt(23, 59, 59, 999_999).unwrap_or(NaiveDateTime::MAX)
}

/// Returns whether a timestamp fits BigQuery's timestamp domain.
fn bigquery_timestamp_in_range(value: NaiveDateTime) -> bool {
    value >= bigquery_min_timestamp() && value <= bigquery_max_timestamp()
}

/// Clamps a timestamp to BigQuery's timestamp domain.
fn clamp_bigquery_timestamp(value: NaiveDateTime) -> NaiveDateTime {
    value.clamp(bigquery_min_timestamp(), bigquery_max_timestamp())
}

/// Normalizes a PostgreSQL timestamp into BigQuery's finite timestamp range.
fn clamp_bigquery_timestamp_value(value: PgTimestamp) -> PgTimestamp {
    let value = match value {
        PgTimestamp::Finite(value) => clamp_bigquery_timestamp(value),
        PgTimestamp::PosInfinity => bigquery_max_timestamp(),
        PgTimestamp::NegInfinity => bigquery_min_timestamp(),
        PgTimestamp::OutOfRange(value) => match value.bound() {
            PgTemporalBound::Lower => bigquery_min_timestamp(),
            PgTemporalBound::Upper => bigquery_max_timestamp(),
        },
    };

    PgTimestamp::Finite(value)
}

/// Returns BigQuery's minimum timestamp with timezone.
fn bigquery_min_timestamptz() -> DateTime<Utc> {
    DateTime::from_naive_utc_and_offset(bigquery_min_timestamp(), Utc)
}

/// Returns BigQuery's maximum timestamp with timezone.
fn bigquery_max_timestamptz() -> DateTime<Utc> {
    DateTime::from_naive_utc_and_offset(bigquery_max_timestamp(), Utc)
}

/// Returns whether a timestamp with timezone fits BigQuery's timestamp domain.
fn bigquery_timestamptz_in_range(value: DateTime<Utc>) -> bool {
    value >= bigquery_min_timestamptz() && value <= bigquery_max_timestamptz()
}

/// Clamps a timestamp with timezone to BigQuery's timestamp domain.
fn clamp_bigquery_timestamptz(value: DateTime<Utc>) -> DateTime<Utc> {
    value.clamp(bigquery_min_timestamptz(), bigquery_max_timestamptz())
}

/// Normalizes a PostgreSQL timestamptz into BigQuery's finite timestamp range.
fn clamp_bigquery_timestamptz_value(value: PgTimestampTz) -> PgTimestampTz {
    let value = match value {
        PgTimestampTz::Finite(value) => clamp_bigquery_timestamptz(value),
        PgTimestampTz::PosInfinity => bigquery_max_timestamptz(),
        PgTimestampTz::NegInfinity => bigquery_min_timestamptz(),
        PgTimestampTz::OutOfRange(value) => match value.bound() {
            PgTemporalBound::Lower => bigquery_min_timestamptz(),
            PgTemporalBound::Upper => bigquery_max_timestamptz(),
        },
    };

    PgTimestampTz::Finite(value)
}

#[cfg(test)]
mod tests {
    use etl::error::EtlResult;

    use super::*;
    use crate::bigquery::value::{BigQueryArrayType, BigQueryIntEncoding};

    /// Returns a materializer for the supplied mode.
    fn materializer(
        compatibility: DestinationTypeCompatibility,
    ) -> DestinationMaterializer<BigQueryMaterialization> {
        BigQueryMaterialization::materializer(compatibility)
    }

    /// Returns a typed cell for tests.
    fn typed_cell(typ: Type, cell: Cell) -> TypedCell<Type, Cell> {
        TypedCell::new(typ, cell)
    }

    /// Returns only the materialized cell for tests.
    fn materialized_cell(
        compatibility: DestinationTypeCompatibility,
        typ: Type,
        cell: Cell,
    ) -> EtlResult<BigQueryCell> {
        materializer(compatibility)
            .materialize_cell(typed_cell(typ, cell))
            .map(|typed_cell| typed_cell.into_parts().1)
    }

    /// Returns the materialized type and cell for tests.
    fn materialized_type_and_cell(
        compatibility: DestinationTypeCompatibility,
        typ: Type,
        cell: Cell,
    ) -> EtlResult<(BigQueryType, BigQueryCell)> {
        materializer(compatibility)
            .materialize_cell(typed_cell(typ, cell))
            .map(TypedCell::into_parts)
    }

    /// Builds a nested JSON array with the requested depth.
    fn nested_json(depth: usize) -> serde_json::Value {
        let mut value = serde_json::Value::Null;
        for _ in 1..depth {
            value = serde_json::Value::Array(vec![value]);
        }

        value
    }

    #[test]
    fn preserve_type_mapping_uses_string_for_bigquery_risky_types() {
        let compatibility = DestinationTypeCompatibility::preserve();
        let materializer = materializer(compatibility);

        for typ in [
            Type::FLOAT8,
            Type::NUMERIC,
            Type::DATE,
            Type::TIME,
            Type::TIMESTAMP,
            Type::TIMESTAMPTZ,
            Type::JSONB,
        ] {
            assert_eq!(
                materializer.materialize_type(&typ).expect("risky type should map to text"),
                BigQueryType::String
            );
        }

        for typ in [Type::NUMERIC_ARRAY, Type::JSON_ARRAY, Type::TIMESTAMPTZ_ARRAY] {
            assert_eq!(
                materializer
                    .materialize_type(&typ)
                    .expect("preserve array type should map to scalar text"),
                BigQueryType::String
            );
        }
    }

    #[test]
    fn compatible_type_mapping_uses_native_types_with_string_fallbacks() {
        let compatibility = DestinationTypeCompatibility::compatible();
        let materializer = materializer(compatibility);

        for (typ, expected_type) in [
            (Type::NUMERIC, BigQueryType::BigNumeric),
            (Type::JSONB, BigQueryType::Json),
            (Type::TIMESTAMP, BigQueryType::DateTime),
            (Type::TIMESTAMPTZ, BigQueryType::Timestamp),
            (
                Type::INT4_ARRAY,
                BigQueryType::Array(BigQueryArrayType::Int64(BigQueryIntEncoding::Int32)),
            ),
        ] {
            assert_eq!(
                materializer.materialize_type(&typ).expect("native type should materialize"),
                expected_type
            );
        }

        for typ in [Type::UUID, Type::INTERVAL] {
            assert_eq!(
                materializer.materialize_type(&typ).expect("fallback type should materialize"),
                BigQueryType::String
            );
        }

        for typ in [Type::UUID_ARRAY, Type::MONEY_ARRAY, Type::INET_ARRAY, Type::INT4_RANGE_ARRAY] {
            assert_eq!(
                materializer.materialize_type(&typ).expect("fallback type should materialize"),
                BigQueryType::Array(BigQueryArrayType::String)
            );
        }
    }

    #[test]
    fn strict_type_mapping_rejects_bigquery_non_native_types() {
        let compatibility = DestinationTypeCompatibility::strict();
        let materializer = materializer(compatibility);

        for typ in [
            Type::UUID,
            Type::MONEY,
            Type::INTERVAL,
            Type::TIMETZ,
            Type::REGCLASS,
            Type::INET,
            Type::BIT,
            Type::POINT,
            Type::TS_VECTOR,
            Type::PG_LSN,
            Type::INT4_RANGE,
            Type::UUID_ARRAY,
            Type::MONEY_ARRAY,
            Type::INET_ARRAY,
            Type::INT4_RANGE_ARRAY,
        ] {
            let result = materializer.materialize_type(&typ);

            assert!(matches!(
                result,
                Err(err) if err.kind() == ErrorKind::UnsupportedValueInDestination
            ));
        }
    }

    #[test]
    fn coerce_type_mapping_uses_string_for_bigquery_non_native_types() {
        let compatibility = DestinationTypeCompatibility::coerce();
        let materializer = materializer(compatibility);

        for typ in [
            Type::UUID,
            Type::MONEY,
            Type::INTERVAL,
            Type::TIMETZ,
            Type::REGCLASS,
            Type::INET,
            Type::BIT,
            Type::POINT,
            Type::TS_VECTOR,
            Type::PG_LSN,
            Type::INT4_RANGE,
        ] {
            assert_eq!(
                materializer.materialize_type(&typ).expect("non-native type should map to text"),
                BigQueryType::String
            );
        }

        for typ in [Type::UUID_ARRAY, Type::MONEY_ARRAY, Type::INET_ARRAY, Type::INT4_RANGE_ARRAY] {
            assert_eq!(
                materializer
                    .materialize_type(&typ)
                    .expect("non-native array type should map to repeated text"),
                BigQueryType::Array(BigQueryArrayType::String)
            );
        }
    }

    #[test]
    fn string_materialized_types_return_text_carriers_for_all_modes() {
        let cases = [
            (DestinationTypeCompatibility::preserve(), Type::NUMERIC, Cell::Null),
            (DestinationTypeCompatibility::preserve(), Type::JSON, Cell::Null),
            (DestinationTypeCompatibility::preserve(), Type::DATE, Cell::Null),
            (DestinationTypeCompatibility::coerce(), Type::MONEY, Cell::String("$1.00".to_owned())),
            (
                DestinationTypeCompatibility::preserve(),
                Type::INET,
                Cell::String("127.0.0.1".to_owned()),
            ),
            (DestinationTypeCompatibility::coerce(), Type::BIT, Cell::String("1010".to_owned())),
        ];

        for (compatibility, typ, cell) in cases {
            let (materialized_type, _) =
                materialized_type_and_cell(compatibility, typ, cell).unwrap();
            assert_eq!(materialized_type, BigQueryType::String);
        }
    }

    #[test]
    fn native_bigquery_types_are_separate_from_storage_cells() {
        let (typ, cell) = materialized_type_and_cell(
            DestinationTypeCompatibility::strict(),
            Type::JSON,
            Cell::String(r#"{"value":1}"#.to_owned()),
        )
        .unwrap();
        assert_eq!(typ, BigQueryType::Json);
        assert_eq!(cell, BigQueryCell::String(r#"{"value":1}"#.to_owned()));

        let (typ, cell) = materialized_type_and_cell(
            DestinationTypeCompatibility::strict(),
            Type::NUMERIC,
            Cell::Numeric(PgNumeric::from_str("1.23").unwrap()),
        )
        .unwrap();
        assert_eq!(typ, BigQueryType::BigNumeric);
        assert_eq!(cell, BigQueryCell::String("1.23".to_owned()));
    }

    #[test]
    fn schema_and_cell_materialization_return_the_same_bigquery_type() {
        let cases = [
            (DestinationTypeCompatibility::strict(), Type::INT4, Cell::I32(1)),
            (
                DestinationTypeCompatibility::strict(),
                Type::NUMERIC,
                Cell::Numeric(PgNumeric::from_str("1.23").unwrap()),
            ),
            (
                DestinationTypeCompatibility::strict(),
                Type::JSON,
                Cell::String(r#"{"value":1}"#.to_owned()),
            ),
            (
                DestinationTypeCompatibility::strict(),
                Type::INT4_ARRAY,
                Cell::Array(ArrayCell::I32(vec![Some(1), Some(2)])),
            ),
            (
                DestinationTypeCompatibility::preserve(),
                Type::INT4_ARRAY,
                Cell::Array(ArrayCell::I32(vec![Some(1), None])),
            ),
            (
                DestinationTypeCompatibility::compatible(),
                Type::INT4_ARRAY,
                Cell::Array(ArrayCell::I32(vec![Some(1), Some(2)])),
            ),
            (
                DestinationTypeCompatibility::preserve(),
                Type::NUMERIC,
                Cell::Numeric(PgNumeric::from_str("1.23").unwrap()),
            ),
            (
                DestinationTypeCompatibility::coerce(),
                Type::NUMERIC,
                Cell::Numeric(PgNumeric::from_str("1.23").unwrap()),
            ),
            (
                DestinationTypeCompatibility::coerce(),
                Type::DATE,
                Cell::Date(PgDate::Finite(bigquery_max_date())),
            ),
            (
                DestinationTypeCompatibility::coerce(),
                Type::UUID,
                Cell::Uuid(uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()),
            ),
            (
                DestinationTypeCompatibility::compatible(),
                Type::UUID_ARRAY,
                Cell::Array(ArrayCell::Uuid(vec![Some(
                    uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
                )])),
            ),
            (
                DestinationTypeCompatibility::coerce(),
                Type::UUID_ARRAY,
                Cell::Array(ArrayCell::Uuid(vec![Some(
                    uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
                )])),
            ),
        ];

        for (compatibility, typ, cell) in cases {
            let materializer = materializer(compatibility);
            let materialized_schema_type =
                materializer.materialize_type(&typ).expect("type should materialize");
            let (materialized_cell_type, _) = materialized_type_and_cell(compatibility, typ, cell)
                .expect("cell should materialize");

            assert_eq!(materialized_cell_type, materialized_schema_type);
        }
    }

    #[test]
    fn strict_preserve_and_coerce_handle_risky_values() {
        let cases = [
            (
                Type::NUMERIC,
                Cell::Numeric(
                    PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap(),
                ),
            ),
            (Type::NUMERIC, Cell::Numeric(PgNumeric::PositiveInfinity)),
            (Type::JSON, Cell::String(r#"{"value":18446744073709551616}"#.to_owned())),
            (Type::FLOAT8, Cell::F64(-0.0)),
            (Type::DATE, Cell::Date(NaiveDate::from_ymd_opt(0, 12, 31).unwrap().into())),
        ];

        for (typ, cell) in cases {
            assert!(
                materializer(DestinationTypeCompatibility::strict())
                    .materialize_cell(typed_cell(typ.clone(), cell.clone()))
                    .is_err()
            );
            assert!(
                materializer(DestinationTypeCompatibility::compatible())
                    .materialize_cell(typed_cell(typ.clone(), cell.clone()))
                    .is_err()
            );
            assert!(matches!(
                materialized_cell(
                    DestinationTypeCompatibility::preserve(),
                    typ.clone(),
                    cell.clone()
                )
                .unwrap(),
                BigQueryCell::String(_)
            ));
            assert!(
                materializer(DestinationTypeCompatibility::coerce())
                    .materialize_cell(typed_cell(typ, cell))
                    .is_ok()
            );
        }
    }

    #[test]
    fn strict_preserve_and_coerce_handle_uuid_values() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();

        assert!(
            materializer(DestinationTypeCompatibility::strict())
                .materialize_cell(typed_cell(Type::UUID, Cell::Uuid(uuid)))
                .is_err()
        );
        assert_eq!(
            materialized_cell(
                DestinationTypeCompatibility::compatible(),
                Type::UUID,
                Cell::Uuid(uuid)
            )
            .unwrap(),
            BigQueryCell::String(uuid.to_string())
        );
        assert_eq!(
            materialized_cell(
                DestinationTypeCompatibility::preserve(),
                Type::UUID,
                Cell::Uuid(uuid)
            )
            .unwrap(),
            BigQueryCell::String(uuid.to_string())
        );
        assert_eq!(
            materialized_cell(DestinationTypeCompatibility::coerce(), Type::UUID, Cell::Uuid(uuid))
                .unwrap(),
            BigQueryCell::String(uuid.to_string())
        );
    }

    #[test]
    fn strict_preserve_and_coerce_handle_uuid_array_values() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let cell = Cell::Array(ArrayCell::Uuid(vec![Some(uuid)]));

        assert!(
            materializer(DestinationTypeCompatibility::strict())
                .materialize_cell(typed_cell(Type::UUID_ARRAY, cell.clone()))
                .is_err()
        );
        assert_eq!(
            materialized_cell(
                DestinationTypeCompatibility::compatible(),
                Type::UUID_ARRAY,
                cell.clone()
            )
            .unwrap(),
            BigQueryCell::Array(BigQueryArrayCell::String(vec![uuid.to_string()]))
        );
        assert_eq!(
            materialized_cell(
                DestinationTypeCompatibility::preserve(),
                Type::UUID_ARRAY,
                cell.clone()
            )
            .unwrap(),
            BigQueryCell::String(format!("{{{uuid}}}"))
        );
        assert_eq!(
            materialized_cell(DestinationTypeCompatibility::coerce(), Type::UUID_ARRAY, cell)
                .unwrap(),
            BigQueryCell::Array(BigQueryArrayCell::String(vec![uuid.to_string()]))
        );
    }

    #[test]
    fn compatible_and_coerce_keep_non_native_arrays_as_repeated_strings() {
        for (typ, values) in [
            (Type::MONEY_ARRAY, vec!["$1.00".to_owned(), "-$0.01".to_owned()]),
            (Type::INTERVAL_ARRAY, vec!["1 day".to_owned(), "2 hours".to_owned()]),
            (Type::INET_ARRAY, vec!["127.0.0.1".to_owned(), "192.168.0.1".to_owned()]),
            (Type::INT4_RANGE_ARRAY, vec!["[1,2)".to_owned(), "[3,4)".to_owned()]),
        ] {
            let cell = Cell::Array(ArrayCell::String(values.iter().cloned().map(Some).collect()));

            for compatibility in
                [DestinationTypeCompatibility::compatible(), DestinationTypeCompatibility::coerce()]
            {
                let (materialized_type, materialized_cell) =
                    materialized_type_and_cell(compatibility, typ.clone(), cell.clone()).unwrap();

                assert_eq!(materialized_type, BigQueryType::Array(BigQueryArrayType::String));
                assert_eq!(
                    materialized_cell,
                    BigQueryCell::Array(BigQueryArrayCell::String(values.clone()))
                );
            }
        }
    }

    #[test]
    fn coerce_numeric_rounds_and_clamps() {
        let rounded = coerce_bigquery_numeric(
            PgNumeric::from_str("0.123456789012345678901234567890123456789").unwrap(),
        )
        .unwrap();
        assert_eq!(rounded.to_string(), "0.12345678901234567890123456789012345679");

        let clamped = coerce_bigquery_numeric(PgNumeric::PositiveInfinity).unwrap();
        assert_eq!(clamped.to_string(), BIGQUERY_BIGNUMERIC_MAX);

        let zero = coerce_bigquery_numeric(
            PgNumeric::from_str("-0.000000000000000000000000000000000000001").unwrap(),
        )
        .unwrap();
        assert!(!zero.to_string().starts_with('-'));
    }

    #[test]
    fn decimal_rounding_covers_half_away_from_zero_edges() {
        let cases = [
            ("1.2344", 3, "1.234"),
            ("1.2345", 3, "1.235"),
            ("-1.2345", 3, "-1.235"),
            ("9.9995", 3, "10.000"),
            ("999.5", 0, "1000"),
            (
                "0.000000000000000000000000000000000000004",
                38,
                "0.00000000000000000000000000000000000000",
            ),
            (
                "0.000000000000000000000000000000000000005",
                38,
                "0.00000000000000000000000000000000000001",
            ),
            (
                "-0.000000000000000000000000000000000000004",
                38,
                "0.00000000000000000000000000000000000000",
            ),
            (
                "-0.000000000000000000000000000000000000005",
                38,
                "-0.00000000000000000000000000000000000001",
            ),
        ];

        for (value, scale, expected) in cases {
            assert_eq!(round_decimal_half_away_from_zero(value, scale), expected);
        }
    }

    #[test]
    fn decimal_abs_comparison_normalizes_signs_and_zeroes() {
        let cases = [
            ("1.23", "1.2300", std::cmp::Ordering::Equal),
            ("-001.2300", "1.23", std::cmp::Ordering::Equal),
            ("0.000100", "0.00009", std::cmp::Ordering::Greater),
            ("999", "1000", std::cmp::Ordering::Less),
            ("-10", "2", std::cmp::Ordering::Greater),
            (
                "1.00000000000000000000000000000000000001",
                "1.00000000000000000000000000000000000002",
                std::cmp::Ordering::Less,
            ),
        ];

        for (left, right, expected) in cases {
            assert_eq!(compare_decimal_abs(left, right), expected);
            assert_eq!(compare_decimal_abs(right, left), expected.reverse());
        }
    }

    #[test]
    fn coerce_numeric_roundtrip_stays_inside_bigquery_bignumeric_range()
    -> Result<(), Box<dyn std::error::Error>> {
        let cases = [
            "0.123456789012345678901234567890123456789",
            "-0.123456789012345678901234567890123456789",
            "999999999999999999999999999999999999999999999",
            "-999999999999999999999999999999999999999999999",
            "578960446186580977117854925043439539266.349923328202820197287920039565648199675",
            "-578960446186580977117854925043439539266.349923328202820197287920039565648199685",
        ];

        for value in cases {
            let materialized = coerce_bigquery_numeric(PgNumeric::from_str(value)?)
                .map_err(std::io::Error::other)?;
            let roundtripped = PgNumeric::from_str(&materialized.to_string())?;
            assert!(bigquery_bignumeric_in_range(&roundtripped));
        }

        for value in [PgNumeric::PositiveInfinity, PgNumeric::NegativeInfinity] {
            let materialized = coerce_bigquery_numeric(value).map_err(std::io::Error::other)?;
            let roundtripped = PgNumeric::from_str(&materialized.to_string())?;
            assert!(bigquery_bignumeric_in_range(&roundtripped));
        }

        Ok(())
    }

    #[test]
    fn json_depth_exceeding_bigquery_limit_is_rejected_unless_preserve() {
        let cell = Cell::String(nested_json(BIGQUERY_JSON_MAX_NESTING_DEPTH + 1).to_string());

        assert!(
            materializer(DestinationTypeCompatibility::strict())
                .materialize_cell(typed_cell(Type::JSON, cell.clone()))
                .is_err()
        );
        assert!(matches!(
            materialized_cell(DestinationTypeCompatibility::preserve(), Type::JSON, cell.clone())
                .unwrap(),
            BigQueryCell::String(_)
        ));
        assert!(
            materializer(DestinationTypeCompatibility::compatible())
                .materialize_cell(typed_cell(Type::JSON, cell))
                .is_err()
        );
    }

    #[test]
    fn strict_rejects_and_coerce_canonicalizes_duplicate_json_keys() {
        let cell = Cell::String(r#"{"outer":{"value":1,"value":2}}"#.to_owned());

        let result = materializer(DestinationTypeCompatibility::strict())
            .materialize_cell(typed_cell(Type::JSON, cell.clone()));
        assert!(matches!(
            result,
            Err(err) if err.kind() == ErrorKind::UnsupportedValueInDestination
        ));
        assert!(matches!(
            materialized_cell(DestinationTypeCompatibility::preserve(), Type::JSON, cell).unwrap(),
            BigQueryCell::String(_)
        ));

        let result = materialized_cell(
            DestinationTypeCompatibility::coerce(),
            Type::JSON,
            Cell::String(r#"{"outer":{"value":1,"value":2}}"#.to_owned()),
        )
        .unwrap();
        let BigQueryCell::String(value) = result else {
            panic!("Expected JSON object");
        };
        let value: serde_json::Value = serde_json::from_str(&value).unwrap();
        assert_eq!(value["outer"]["value"], serde_json::json!(1));
    }

    #[test]
    fn coerce_json_rounds_wide_numbers_to_bigquery_float_domain() {
        let cell = Cell::String(r#"{"value":922337203685477580701}"#.to_owned());
        let result =
            materialized_cell(DestinationTypeCompatibility::coerce(), Type::JSON, cell).unwrap();

        let BigQueryCell::String(value) = result else {
            panic!("Expected JSON object");
        };
        let serde_json::Value::Object(values) = serde_json::from_str(&value).unwrap() else {
            panic!("Expected JSON object");
        };
        let value = values.get("value").and_then(serde_json::Value::as_f64).unwrap();
        assert_eq!(value, 9.223372036854776e20);
    }

    #[test]
    fn coerce_json_rejects_numbers_outside_bigquery_float_domain() {
        let cell = Cell::String(r#"{"value":1e309}"#.to_owned());

        let result = materializer(DestinationTypeCompatibility::coerce())
            .materialize_cell(typed_cell(Type::JSON, cell));

        assert!(matches!(
            result,
            Err(err) if err.kind() == ErrorKind::UnsupportedValueInDestination
                && err.detail().is_some_and(|detail| detail.contains("cannot be rounded"))
        ));
    }

    #[test]
    fn strict_json_arrays_validate_string_elements_using_source_type() {
        let cell = Cell::Array(ArrayCell::String(vec![Some(
            r#"{"value":18446744073709551616}"#.to_owned(),
        )]));

        let result = materializer(DestinationTypeCompatibility::strict())
            .materialize_cell(typed_cell(Type::JSON_ARRAY, cell));

        assert!(matches!(
            result,
            Err(err) if err.kind() == ErrorKind::UnsupportedValueInDestination
                && err.detail().is_some_and(|detail| detail.contains("Element at index 0"))
        ));

        let cell = Cell::Array(ArrayCell::String(vec![Some(r#"{"value":1}"#.to_owned())]));
        let result =
            materialized_cell(DestinationTypeCompatibility::strict(), Type::JSON_ARRAY, cell)
                .unwrap();

        assert_eq!(
            result,
            BigQueryCell::Array(BigQueryArrayCell::String(vec![r#"{"value":1}"#.to_owned()]))
        );
    }

    #[test]
    fn coerce_json_arrays_normalize_string_elements_using_source_type() {
        let cell =
            Cell::Array(ArrayCell::String(vec![Some(r#"{"value":1,"value":2}"#.to_owned())]));

        let result =
            materialized_cell(DestinationTypeCompatibility::coerce(), Type::JSON_ARRAY, cell)
                .unwrap();

        assert_eq!(
            result,
            BigQueryCell::Array(BigQueryArrayCell::String(vec![r#"{"value":1}"#.to_owned()]))
        );
    }

    #[test]
    fn strict_temporal_arrays_validate_postgres_temporal_elements() {
        let cell = Cell::Array(ArrayCell::Date(vec![
            Some(PgDate::Finite(bigquery_min_date())),
            Some(PgDate::PosInfinity),
        ]));

        let result = materializer(DestinationTypeCompatibility::strict())
            .materialize_cell(typed_cell(Type::DATE_ARRAY, cell));

        assert!(matches!(
            result,
            Err(err) if err.kind() == ErrorKind::UnsupportedValueInDestination
                && err.detail().is_some_and(|detail| detail.contains("Element at index 1"))
        ));
    }

    #[test]
    fn postgres_temporal_edge_values_use_temporal_materialization() {
        let cell = Cell::Date(PgDate::PosInfinity);

        assert!(
            materializer(DestinationTypeCompatibility::strict())
                .materialize_cell(typed_cell(Type::DATE, cell.clone()))
                .is_err()
        );
        assert_eq!(
            materialized_cell(DestinationTypeCompatibility::preserve(), Type::DATE, cell.clone())
                .unwrap(),
            BigQueryCell::String("infinity".to_owned())
        );
        assert_eq!(
            materialized_cell(DestinationTypeCompatibility::coerce(), Type::DATE, cell).unwrap(),
            BigQueryCell::String(PgDate::Finite(bigquery_max_date()).to_string())
        );

        assert_eq!(
            materialized_cell(
                DestinationTypeCompatibility::coerce(),
                Type::TIME,
                Cell::Time(PgTime::TwentyFourHour),
            )
            .unwrap(),
            BigQueryCell::String("23:59:59.999999".to_owned())
        );
    }

    #[test]
    fn typed_arrays_reject_native_null_elements_unless_preserve_stringifies() {
        let cell = Cell::Array(ArrayCell::I32(vec![Some(1), None]));

        for compatibility in [
            DestinationTypeCompatibility::strict(),
            DestinationTypeCompatibility::compatible(),
            DestinationTypeCompatibility::coerce(),
        ] {
            let result = materializer(compatibility)
                .materialize_cell(typed_cell(Type::INT4_ARRAY, cell.clone()));
            assert!(result.is_err());
            assert_eq!(
                result.unwrap_err().kind(),
                ErrorKind::NullValuesNotSupportedInArrayInDestination
            );
        }

        assert_eq!(
            materialized_cell(DestinationTypeCompatibility::preserve(), Type::INT4_ARRAY, cell)
                .unwrap(),
            BigQueryCell::String("{1,NULL}".to_owned())
        );
    }

    #[test]
    fn coerce_native_arrays_stay_repeated_fields() {
        let cell = Cell::Array(ArrayCell::Numeric(vec![Some(
            PgNumeric::from_str("0.123456789012345678901234567890123456789").unwrap(),
        )]));

        let (typ, cell) = materialized_type_and_cell(
            DestinationTypeCompatibility::coerce(),
            Type::NUMERIC_ARRAY,
            cell,
        )
        .unwrap();

        assert_eq!(typ, BigQueryType::Array(BigQueryArrayType::BigNumeric));
        assert_eq!(
            cell,
            BigQueryCell::Array(BigQueryArrayCell::String(vec![
                "0.12345678901234567890123456789012345679".to_owned()
            ]))
        );
    }

    #[test]
    fn typed_preserve_array_values_become_scalar_strings() {
        let cell =
            Cell::Array(ArrayCell::Numeric(vec![Some(PgNumeric::from_str("1.23").unwrap())]));
        let result =
            materialized_cell(DestinationTypeCompatibility::preserve(), Type::NUMERIC_ARRAY, cell)
                .unwrap();

        assert_eq!(result, BigQueryCell::String("{1.23}".to_owned()));
    }

    #[test]
    fn strict_rejects_null_array_cells() {
        let result = materializer(DestinationTypeCompatibility::strict())
            .materialize_cell(typed_cell(Type::INT4_ARRAY, Cell::Null));

        assert!(matches!(
            result,
            Err(err) if err.kind() == ErrorKind::UnsupportedValueInDestination
        ));
    }
}
