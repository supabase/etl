use std::str::FromStr;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use etl::{
    compatibility::{
        CellCompatibilityResult, CompatibilityChecker, CompatibilityRules, CellCarrier,
        DestinationTypeCompatibility, DestinationTypeCompatibilityMode, TypeCompatibilityResult,
    },
    error::{ErrorKind, EtlResult},
    types::{
        ArrayCell, Cell, DATE_FORMAT, PgNumeric, TIME_FORMAT, TIMESTAMP_FORMAT,
        TIMESTAMPTZ_FORMAT_HH_MM, Type, is_json_type, is_temporal_type,
    },
};

use crate::bigquery::value::{BigQueryArrayCell, BigQueryCell};

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

/// BigQuery cell compatibility result.
type BqCellCompatibilityResult = CellCompatibilityResult<BigQueryCell>;

/// BigQuery compatibility implementation.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct BigQueryCompatibility;

impl BigQueryCompatibility {
    /// Creates a [`CompatibilityChecker`] for BigQuery.
    pub(super) fn checker(
        compatibility: DestinationTypeCompatibility,
    ) -> CompatibilityChecker<Self> {
        CompatibilityChecker::new(compatibility, Self)
    }

    /// Returns the source-shaped type that should be used for BigQuery mapping.
    pub(super) fn compatible_type(
        typ: &Type,
        compatibility: DestinationTypeCompatibility,
    ) -> EtlResult<Type> {
        Self::checker(compatibility).get_compatible_type(typ)
    }
}

impl CompatibilityRules for BigQueryCompatibility {
    type CompatibleCell = BigQueryCell;

    fn get_compatible_type(
        &self,
        typ: &Type,
        compatibility: DestinationTypeCompatibility,
    ) -> TypeCompatibilityResult {
        match compatibility.mode() {
            DestinationTypeCompatibilityMode::Strict if !has_strict_bigquery_native_type(typ) => {
                TypeCompatibilityResult::Invalid {
                    kind: ErrorKind::UnsupportedValueInDestination,
                    reason: format!(
                        "PostgreSQL type {} has no strict native BigQuery representation",
                        typ.name()
                    ),
                }
            }
            DestinationTypeCompatibilityMode::Lossless
                if materializes_as_bigquery_string_for_lossless(typ) =>
            {
                TypeCompatibilityResult::Changed(string_type_for_source_type(typ))
            }
            DestinationTypeCompatibilityMode::Lossy
                if materializes_as_bigquery_string_for_lossy(typ) =>
            {
                TypeCompatibilityResult::Changed(string_type_for_source_type(typ))
            }
            _ => TypeCompatibilityResult::Unchanged(typ.clone()),
        }
    }

    fn get_compatible_cell(
        &self,
        typ: &Type,
        cell: Cell,
        compatibility: DestinationTypeCompatibility,
    ) -> BqCellCompatibilityResult {
        if etl::types::is_array_type(typ) {
            return array_typed_cell(typ, cell, compatibility);
        }

        if is_temporal_type(typ)
            && let Cell::String(value) = cell
        {
            return temporal_string_cell(typ, value, compatibility);
        }

        if is_json_type(typ)
            && let Cell::String(value) = cell
        {
            return json_string_cell(typ, value, compatibility);
        }

        if let Cell::Array(array) = &cell
            && let Some(result) = validate_array_has_no_nulls(array)
        {
            return result;
        }

        match compatibility.mode() {
            DestinationTypeCompatibilityMode::Strict => strict_cell(typ, cell),
            DestinationTypeCompatibilityMode::Lossless => lossless_cell(typ, cell),
            DestinationTypeCompatibilityMode::Lossy => lossy_cell(typ, cell),
        }
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

/// Returns whether lossless mode should materialize the source type as
/// `STRING`.
fn materializes_as_bigquery_string_for_lossless(typ: &Type) -> bool {
    if etl::types::is_array_type(typ) {
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

/// Returns whether lossy mode should materialize a non-native type as `STRING`.
fn materializes_as_bigquery_string_for_lossy(typ: &Type) -> bool {
    etl::types::is_array_type(typ) || !has_strict_bigquery_native_type(typ)
}

/// Returns the scalar string storage type for a source type.
fn string_type_for_source_type(_typ: &Type) -> Type {
    Type::TEXT
}

/// Applies BigQuery compatibility for a cell known to come from an array
/// column.
fn array_typed_cell(
    typ: &Type,
    cell: Cell,
    compatibility: DestinationTypeCompatibility,
) -> BqCellCompatibilityResult {
    match compatibility.mode() {
        DestinationTypeCompatibilityMode::Strict => match cell {
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
        },
        DestinationTypeCompatibilityMode::Lossless | DestinationTypeCompatibilityMode::Lossy => {
            match cell {
                Cell::Array(array) => {
                    type_changed(Type::TEXT, BigQueryCell::string(format_array(array)))
                }
                Cell::Null => type_changed(Type::TEXT, BigQueryCell::Null),
                cell => type_changed(Type::TEXT, BigQueryCell::from_native_cell(cell)),
            }
        }
    }
}

/// Applies BigQuery compatibility for temporal text that could not be parsed
/// into Rust's temporal domain.
fn temporal_string_cell(
    typ: &Type,
    value: String,
    compatibility: DestinationTypeCompatibility,
) -> BqCellCompatibilityResult {
    match compatibility.mode() {
        DestinationTypeCompatibilityMode::Strict => invalid(
            ErrorKind::UnsupportedValueInDestination,
            format!("Temporal value '{value}' cannot be stored exactly in BigQuery"),
        ),
        DestinationTypeCompatibilityMode::Lossless => {
            type_changed(Type::TEXT, BigQueryCell::string(value))
        }
        DestinationTypeCompatibilityMode::Lossy => {
            let value = match *typ {
                Type::DATE => lossy_raw_date(&value),
                Type::TIME => lossy_raw_time(&value),
                Type::TIMESTAMP => lossy_raw_timestamp(&value),
                Type::TIMESTAMPTZ => lossy_raw_timestamptz(&value),
                _ => None,
            };

            match value {
                Some(value) => value_changed(typ.clone(), BigQueryCell::string(value)),
                None => invalid(
                    ErrorKind::UnsupportedValueInDestination,
                    "Temporal text cannot be normalized to BigQuery",
                ),
            }
        }
    }
}

/// Normalizes raw date text to the BigQuery date domain.
fn lossy_raw_date(value: &str) -> Option<String> {
    Some(if temporal_text_is_negative(value) {
        format_date(bigquery_min_date())
    } else {
        format_date(bigquery_max_date())
    })
}

/// Normalizes raw time text to the BigQuery time domain.
fn lossy_raw_time(value: &str) -> Option<String> {
    if value.starts_with("24:") { Some("23:59:59.999999".to_owned()) } else { None }
}

/// Normalizes raw timestamp text to the BigQuery timestamp domain.
fn lossy_raw_timestamp(value: &str) -> Option<String> {
    Some(if temporal_text_is_negative(value) {
        format_timestamp(bigquery_min_timestamp())
    } else {
        format_timestamp(bigquery_max_timestamp())
    })
}

/// Normalizes raw timestamp-with-time-zone text to the BigQuery timestamp
/// domain.
fn lossy_raw_timestamptz(value: &str) -> Option<String> {
    Some(if temporal_text_is_negative(value) {
        format_timestamptz(bigquery_min_timestamptz())
    } else {
        format_timestamptz(bigquery_max_timestamptz())
    })
}

/// Returns whether raw temporal text is below BigQuery's lower bound.
fn temporal_text_is_negative(value: &str) -> bool {
    value.starts_with('-') || value.ends_with(" BC")
}

/// Applies BigQuery compatibility for raw JSON text.
fn json_string_cell(
    typ: &Type,
    value: String,
    compatibility: DestinationTypeCompatibility,
) -> BqCellCompatibilityResult {
    match compatibility.mode() {
        DestinationTypeCompatibilityMode::Lossless => {
            type_changed(Type::TEXT, BigQueryCell::string(value))
        }
        DestinationTypeCompatibilityMode::Strict => match serde_json::from_str(&value) {
            Ok(value_json) => {
                if let Some(result) = validate_strict_json(&value_json) {
                    result
                } else {
                    unchanged(typ.clone(), BigQueryCell::string(value))
                }
            }
            Err(error) => invalid(
                ErrorKind::UnsupportedValueInDestination,
                format!("JSON text is not valid for BigQuery: {error}"),
            ),
        },
        DestinationTypeCompatibilityMode::Lossy => match serde_json::from_str(&value) {
            Ok(value_json) => {
                if let Err(reason) = validate_bigquery_json_depth(&value_json) {
                    invalid(ErrorKind::UnsupportedValueInDestination, reason)
                } else {
                    let normalized = normalize_bigquery_json_lossy(value_json);
                    value_changed(typ.clone(), BigQueryCell::string(normalized.to_string()))
                }
            }
            Err(error) => invalid(
                ErrorKind::UnsupportedValueInDestination,
                format!("JSON text is not valid for BigQuery: {error}"),
            ),
        },
    }
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
        ArrayCell::Date(values) => format_array_values(values, format_date),
        ArrayCell::Time(values) => format_array_values(values, format_time),
        ArrayCell::Timestamp(values) => format_array_values(values, format_timestamp),
        ArrayCell::TimestampTz(values) => format_array_values(values, format_timestamptz),
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

/// Applies strict BigQuery value compatibility.
fn strict_cell(typ: &Type, cell: Cell) -> BqCellCompatibilityResult {
    if let Some(result) = validate_strict_cell(&cell) {
        return result;
    }

    unchanged(typ.clone(), BigQueryCell::from_native_cell(cell))
}

/// Applies lossless BigQuery value compatibility.
fn lossless_cell(typ: &Type, cell: Cell) -> BqCellCompatibilityResult {
    match cell {
        Cell::F32(value) => type_changed(Type::TEXT, BigQueryCell::string(value.to_string())),
        Cell::F64(value) => type_changed(Type::TEXT, BigQueryCell::string(value.to_string())),
        Cell::Numeric(value) => type_changed(Type::TEXT, BigQueryCell::string(value.to_string())),
        Cell::Date(value) => type_changed(Type::TEXT, BigQueryCell::string(format_date(value))),
        Cell::Time(value) => type_changed(Type::TEXT, BigQueryCell::string(format_time(value))),
        Cell::Timestamp(value) => {
            type_changed(Type::TEXT, BigQueryCell::string(format_timestamp(value)))
        }
        Cell::TimestampTz(value) => {
            type_changed(Type::TEXT, BigQueryCell::string(format_timestamptz(value)))
        }
        Cell::Uuid(value) => type_changed(Type::TEXT, BigQueryCell::string(value.to_string())),
        Cell::Array(array) => lossless_array_cell(array),
        cell => unchanged(typ.clone(), BigQueryCell::from_native_cell(cell)),
    }
}

/// Applies lossy BigQuery value compatibility.
fn lossy_cell(typ: &Type, cell: Cell) -> BqCellCompatibilityResult {
    match cell {
        Cell::F32(value) if value == 0.0 && value.is_sign_negative() => {
            value_changed(typ.clone(), BigQueryCell::Float32(0.0))
        }
        Cell::F64(value) if value == 0.0 && value.is_sign_negative() => {
            value_changed(typ.clone(), BigQueryCell::Float64(0.0))
        }
        Cell::Numeric(value) => lossy_numeric_cell(value),
        Cell::Date(value) => value_changed(
            typ.clone(),
            BigQueryCell::string(format_date(clamp_bigquery_date(value))),
        ),
        Cell::Timestamp(value) => value_changed(
            typ.clone(),
            BigQueryCell::string(format_timestamp(clamp_bigquery_timestamp(value))),
        ),
        Cell::TimestampTz(value) => value_changed(
            typ.clone(),
            BigQueryCell::string(format_timestamptz(clamp_bigquery_timestamptz(value))),
        ),
        Cell::Uuid(value) => type_changed(Type::TEXT, BigQueryCell::string(value.to_string())),
        Cell::Array(array) => lossy_array_cell(array),
        cell => unchanged(typ.clone(), BigQueryCell::from_native_cell(cell)),
    }
}

/// Applies lossless BigQuery array compatibility.
fn lossless_array_cell(array: ArrayCell) -> BqCellCompatibilityResult {
    match array {
        ArrayCell::F32(values) => string_array(values, |value| value.to_string()),
        ArrayCell::F64(values) => string_array(values, |value| value.to_string()),
        ArrayCell::Numeric(values) => string_array(values, |value| value.to_string()),
        ArrayCell::Date(values) => string_array(values, format_date),
        ArrayCell::Time(values) => string_array(values, format_time),
        ArrayCell::Timestamp(values) => string_array(values, format_timestamp),
        ArrayCell::TimestampTz(values) => string_array(values, format_timestamptz),
        ArrayCell::Uuid(values) => string_array(values, |value| value.to_string()),
        array => unchanged(Type::TEXT, BigQueryCell::from_native_cell(Cell::Array(array))),
    }
}

/// Applies lossy BigQuery array compatibility.
fn lossy_array_cell(array: ArrayCell) -> BqCellCompatibilityResult {
    match array {
        ArrayCell::F32(values) => value_array(
            values,
            |value| {
                if value == 0.0 && value.is_sign_negative() { 0.0 } else { value }
            },
            ArrayCell::F32,
        ),
        ArrayCell::F64(values) => value_array(
            values,
            |value| {
                if value == 0.0 && value.is_sign_negative() { 0.0 } else { value }
            },
            ArrayCell::F64,
        ),
        ArrayCell::Numeric(values) => lossy_numeric_array_cell(values),
        ArrayCell::Date(values) => value_array(values, clamp_bigquery_date, ArrayCell::Date),
        ArrayCell::Timestamp(values) => {
            value_array(values, clamp_bigquery_timestamp, ArrayCell::Timestamp)
        }
        ArrayCell::TimestampTz(values) => {
            value_array(values, clamp_bigquery_timestamptz, ArrayCell::TimestampTz)
        }
        ArrayCell::Uuid(values) => string_array(values, |value| value.to_string()),
        array => unchanged(Type::TEXT, BigQueryCell::from_native_cell(Cell::Array(array))),
    }
}

/// Converts an optional array to a string array.
fn string_array<T>(
    values: Vec<Option<T>>,
    mut convert: impl FnMut(T) -> String,
) -> BqCellCompatibilityResult {
    let values = values
        .into_iter()
        .map(|value| {
            value.map(&mut convert).expect("BigQuery compatibility rejects null array elements")
        })
        .collect();
    type_changed(Type::TEXT, BigQueryCell::Array(BigQueryArrayCell::String(values)))
}

/// Converts an optional array while preserving the array variant.
fn value_array<T>(
    values: Vec<Option<T>>,
    mut convert: impl FnMut(T) -> T,
    wrap: impl FnOnce(Vec<Option<T>>) -> ArrayCell,
) -> BqCellCompatibilityResult {
    value_changed(
        Type::TEXT,
        BigQueryCell::from_native_cell(Cell::Array(wrap(
            values.into_iter().map(|value| value.map(&mut convert)).collect(),
        ))),
    )
}

/// Applies lossy BigQuery compatibility to a numeric value.
fn lossy_numeric_cell(value: PgNumeric) -> BqCellCompatibilityResult {
    match lossy_bigquery_numeric(value) {
        Ok(value) => value_changed(Type::NUMERIC, BigQueryCell::string(value.to_string())),
        Err(reason) => invalid(ErrorKind::UnsupportedValueInDestination, reason),
    }
}

/// Applies lossy BigQuery compatibility to a numeric array.
fn lossy_numeric_array_cell(values: Vec<Option<PgNumeric>>) -> BqCellCompatibilityResult {
    let values = values
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            value
                .map(lossy_bigquery_numeric)
                .transpose()
                .map_err(|reason| format!("Element at index {index}: {reason}"))
        })
        .collect::<Result<Vec<_>, _>>();

    match values {
        Ok(values) => value_changed(
            Type::TEXT,
            BigQueryCell::from_native_cell(Cell::Array(ArrayCell::Numeric(values))),
        ),
        Err(reason) => invalid(ErrorKind::UnsupportedValueInDestination, reason),
    }
}

/// Returns a strict compatibility failure when a cell is unsafe for BigQuery.
fn validate_strict_cell(cell: &Cell) -> Option<BqCellCompatibilityResult> {
    match cell {
        Cell::F32(value) if *value == 0.0 && value.is_sign_negative() => {
            Some(invalid_negative_zero())
        }
        Cell::F64(value) if *value == 0.0 && value.is_sign_negative() => {
            Some(invalid_negative_zero())
        }
        Cell::Numeric(value) => validate_strict_numeric(value),
        Cell::Date(value) if !bigquery_date_in_range(*value) => Some(invalid_date_range()),
        Cell::Timestamp(value) if !bigquery_timestamp_in_range(*value) => {
            Some(invalid_timestamp_range())
        }
        Cell::TimestampTz(value) if !bigquery_timestamptz_in_range(*value) => {
            Some(invalid_timestamp_range())
        }
        Cell::Uuid(_) => Some(invalid_non_native_value("UUID")),
        Cell::Array(array) => validate_strict_array(array),
        _ => None,
    }
}

/// Returns a strict compatibility failure when an array is unsafe for BigQuery.
fn validate_strict_array(array: &ArrayCell) -> Option<BqCellCompatibilityResult> {
    match array {
        ArrayCell::F32(values) => validate_strict_array_values(values, |value| {
            (*value == 0.0 && value.is_sign_negative()).then(invalid_negative_zero)
        }),
        ArrayCell::F64(values) => validate_strict_array_values(values, |value| {
            (*value == 0.0 && value.is_sign_negative()).then(invalid_negative_zero)
        }),
        ArrayCell::Numeric(values) => validate_strict_array_values(values, validate_strict_numeric),
        ArrayCell::Date(values) => validate_strict_array_values(values, |value| {
            (!bigquery_date_in_range(*value)).then(invalid_date_range)
        }),
        ArrayCell::Timestamp(values) => validate_strict_array_values(values, |value| {
            (!bigquery_timestamp_in_range(*value)).then(invalid_timestamp_range)
        }),
        ArrayCell::TimestampTz(values) => validate_strict_array_values(values, |value| {
            (!bigquery_timestamptz_in_range(*value)).then(invalid_timestamp_range)
        }),
        ArrayCell::Uuid(values) => {
            validate_strict_array_values(values, |_| Some(invalid_non_native_value("UUID")))
        }
        _ => None,
    }
}

/// Validates strict compatibility for optional array elements.
fn validate_strict_array_values<T>(
    values: &[Option<T>],
    validate: impl Fn(&T) -> Option<BqCellCompatibilityResult>,
) -> Option<BqCellCompatibilityResult> {
    for (index, value) in values.iter().enumerate() {
        if let Some(value) = value
            && let Some(result) = validate(value)
        {
            return Some(prefix_array_error(index, result));
        }
    }

    None
}

/// Adds array element context to a compatibility failure.
fn prefix_array_error(
    index: usize,
    result: BqCellCompatibilityResult,
) -> BqCellCompatibilityResult {
    match result {
        CellCompatibilityResult::Invalid { kind, reason } => {
            invalid(kind, format!("Element at index {index}: {reason}"))
        }
        result => result,
    }
}

/// Validates strict BigQuery compatibility for a numeric value.
fn validate_strict_numeric(value: &PgNumeric) -> Option<BqCellCompatibilityResult> {
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

/// Validates strict BigQuery compatibility for JSON.
fn validate_strict_json(value: &serde_json::Value) -> Option<BqCellCompatibilityResult> {
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

/// Validates strict BigQuery compatibility for a JSON number.
fn validate_strict_json_number(number: &serde_json::Number) -> Option<BqCellCompatibilityResult> {
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

/// Normalizes a JSON value using deterministic lossy BigQuery rules.
fn normalize_bigquery_json_lossy(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => serde_json::Value::Array(
            values.into_iter().map(normalize_bigquery_json_lossy).collect(),
        ),
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, normalize_bigquery_json_lossy(value)))
                .collect(),
        ),
        serde_json::Value::Number(number) => {
            serde_json::Value::Number(normalize_bigquery_json_number_lossy(number))
        }
        value => value,
    }
}

/// Normalizes a JSON number using deterministic lossy BigQuery rules.
fn normalize_bigquery_json_number_lossy(number: serde_json::Number) -> serde_json::Number {
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
        return number;
    }

    let normalized = match number_string.parse::<f64>() {
        Ok(value) if value.is_finite() => value,
        Ok(value) if value.is_sign_negative() => -f64::MAX,
        Ok(_) => f64::MAX,
        Err(_) if number_string.starts_with('-') => -f64::MAX,
        Err(_) => f64::MAX,
    };

    serde_json::Number::from_f64(normalized).expect("finite f64 is a valid JSON number")
}

/// Returns a strict compatibility failure for negative zero.
fn invalid_negative_zero() -> BqCellCompatibilityResult {
    invalid(
        ErrorKind::UnsupportedValueInDestination,
        "Negative zero cannot be stored in BigQuery tables",
    )
}

/// Returns a strict compatibility failure for a date outside BigQuery's range.
fn invalid_date_range() -> BqCellCompatibilityResult {
    invalid(
        ErrorKind::UnsupportedValueInDestination,
        "Date value is outside BigQuery's supported range 0001-01-01..=9999-12-31",
    )
}

/// Returns a strict compatibility failure for a timestamp outside BigQuery's
/// range.
fn invalid_timestamp_range() -> BqCellCompatibilityResult {
    invalid(
        ErrorKind::UnsupportedValueInDestination,
        "Timestamp value is outside BigQuery's supported range 0001-01-01..=9999-12-31",
    )
}

/// Returns a strict compatibility failure for a value without a native type.
fn invalid_non_native_value(type_name: &str) -> BqCellCompatibilityResult {
    invalid(
        ErrorKind::UnsupportedValueInDestination,
        format!("{type_name} has no strict native BigQuery representation"),
    )
}

/// Returns an unchanged compatible cell.
fn unchanged(typ: Type, cell: BigQueryCell) -> BqCellCompatibilityResult {
    CellCompatibilityResult::Unchanged(CellCarrier::new(typ, cell))
}

/// Returns a value-changed compatible cell.
fn value_changed(typ: Type, cell: BigQueryCell) -> BqCellCompatibilityResult {
    CellCompatibilityResult::ValueChanged(CellCarrier::new(typ, cell))
}

/// Returns a type-changed compatible cell.
fn type_changed(typ: Type, cell: BigQueryCell) -> BqCellCompatibilityResult {
    CellCompatibilityResult::TypeChanged(CellCarrier::new(typ, cell))
}

/// Returns an invalid compatibility result.
fn invalid(kind: ErrorKind, reason: impl Into<String>) -> BqCellCompatibilityResult {
    CellCompatibilityResult::Invalid { kind, reason: reason.into() }
}

/// Returns a null-array compatibility failure, if any element is null.
fn validate_array_has_no_nulls(array: &ArrayCell) -> Option<BqCellCompatibilityResult> {
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

/// Applies lossy BigQuery `BIGNUMERIC` rounding and clamping.
fn lossy_bigquery_numeric(value: PgNumeric) -> Result<PgNumeric, String> {
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

    let integer = String::from_utf8(integer_digits).expect("decimal digits are valid UTF-8");
    let fractional = String::from_utf8(fractional_digits).expect("decimal digits are valid UTF-8");
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
    NaiveDate::from_ymd_opt(1, 1, 1).expect("valid BigQuery minimum date")
}

/// Returns BigQuery's maximum date.
fn bigquery_max_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(9999, 12, 31).expect("valid BigQuery maximum date")
}

/// Returns whether a date fits BigQuery's date domain.
fn bigquery_date_in_range(value: NaiveDate) -> bool {
    (1..=9999).contains(&value.year())
}

/// Clamps a date to BigQuery's date domain.
fn clamp_bigquery_date(value: NaiveDate) -> NaiveDate {
    value.clamp(bigquery_min_date(), bigquery_max_date())
}

/// Returns BigQuery's minimum timestamp.
fn bigquery_min_timestamp() -> NaiveDateTime {
    bigquery_min_date().and_hms_micro_opt(0, 0, 0, 0).expect("valid BigQuery minimum timestamp")
}

/// Returns BigQuery's maximum timestamp.
fn bigquery_max_timestamp() -> NaiveDateTime {
    bigquery_max_date()
        .and_hms_micro_opt(23, 59, 59, 999_999)
        .expect("valid BigQuery maximum timestamp")
}

/// Returns whether a timestamp fits BigQuery's timestamp domain.
fn bigquery_timestamp_in_range(value: NaiveDateTime) -> bool {
    value >= bigquery_min_timestamp() && value <= bigquery_max_timestamp()
}

/// Clamps a timestamp to BigQuery's timestamp domain.
fn clamp_bigquery_timestamp(value: NaiveDateTime) -> NaiveDateTime {
    value.clamp(bigquery_min_timestamp(), bigquery_max_timestamp())
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

/// Formats a date for BigQuery string transport.
fn format_date(value: NaiveDate) -> String {
    value.format(DATE_FORMAT).to_string()
}

/// Formats a time for BigQuery string transport.
fn format_time(value: NaiveTime) -> String {
    value.format(TIME_FORMAT).to_string()
}

/// Formats a timestamp for BigQuery string transport.
fn format_timestamp(value: NaiveDateTime) -> String {
    value.format(TIMESTAMP_FORMAT).to_string()
}

/// Formats a timestamp with timezone for BigQuery string transport.
fn format_timestamptz(value: DateTime<Utc>) -> String {
    value.format(TIMESTAMPTZ_FORMAT_HH_MM).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Returns a compatibility checker for the supplied mode.
    fn checker(
        compatibility: DestinationTypeCompatibility,
    ) -> CompatibilityChecker<BigQueryCompatibility> {
        BigQueryCompatibility::checker(compatibility)
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
    fn lossless_type_mapping_uses_string_for_bigquery_risky_types() {
        let compatibility = DestinationTypeCompatibility::lossless();

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
                BigQueryCompatibility::compatible_type(&typ, compatibility)
                    .expect("risky type should map to text"),
                Type::TEXT
            );
        }

        for typ in [Type::NUMERIC_ARRAY, Type::JSON_ARRAY, Type::TIMESTAMPTZ_ARRAY] {
            assert_eq!(
                BigQueryCompatibility::compatible_type(&typ, compatibility)
                    .expect("array type should map to scalar text"),
                Type::TEXT
            );
        }
    }

    #[test]
    fn strict_type_mapping_rejects_bigquery_non_native_types() {
        let compatibility = DestinationTypeCompatibility::strict();

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
            let result = BigQueryCompatibility::compatible_type(&typ, compatibility);

            assert!(matches!(
                result,
                Err(err) if err.kind() == ErrorKind::UnsupportedValueInDestination
            ));
        }
    }

    #[test]
    fn lossy_type_mapping_uses_string_for_bigquery_non_native_types() {
        let compatibility = DestinationTypeCompatibility::lossy();

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
                BigQueryCompatibility::compatible_type(&typ, compatibility)
                    .expect("non-native type should map to text"),
                Type::TEXT
            );
        }

        for typ in [Type::UUID_ARRAY, Type::MONEY_ARRAY, Type::INET_ARRAY, Type::INT4_RANGE_ARRAY] {
            assert_eq!(
                BigQueryCompatibility::compatible_type(&typ, compatibility)
                    .expect("non-native array type should map to scalar text"),
                Type::TEXT
            );
        }
    }

    #[test]
    fn strict_lossless_and_lossy_handle_risky_values() {
        let cases = [
            (
                Type::NUMERIC,
                Cell::Numeric(
                    PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap(),
                ),
            ),
            (Type::NUMERIC, Cell::Numeric(PgNumeric::PositiveInfinity)),
            (Type::JSON, Cell::String(r#"{"value":18446744073709551616}"#.to_owned())),
            (Type::JSON, Cell::String(r#"{"value":1e309}"#.to_owned())),
            (Type::FLOAT8, Cell::F64(-0.0)),
            (Type::DATE, Cell::Date(NaiveDate::from_ymd_opt(0, 12, 31).unwrap())),
        ];

        for (typ, cell) in cases {
            assert!(
                checker(DestinationTypeCompatibility::strict())
                    .get_compatible_cell(&typ, cell.clone())
                    .is_err()
            );
            assert!(matches!(
                checker(DestinationTypeCompatibility::lossless())
                    .get_compatible_cell(&typ, cell.clone())
                    .unwrap(),
                compatible if matches!(compatible.cell, BigQueryCell::String(_))
            ));
            assert!(
                checker(DestinationTypeCompatibility::lossy())
                    .get_compatible_cell(&typ, cell)
                    .is_ok()
            );
        }
    }

    #[test]
    fn strict_lossless_and_lossy_handle_uuid_values() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();

        assert!(
            checker(DestinationTypeCompatibility::strict())
                .get_compatible_cell(&Type::UUID, Cell::Uuid(uuid))
                .is_err()
        );
        assert_eq!(
            checker(DestinationTypeCompatibility::lossless())
                .get_compatible_cell(&Type::UUID, Cell::Uuid(uuid))
                .unwrap()
                .cell,
            BigQueryCell::String(uuid.to_string())
        );
        assert_eq!(
            checker(DestinationTypeCompatibility::lossy())
                .get_compatible_cell(&Type::UUID, Cell::Uuid(uuid))
                .unwrap()
                .cell,
            BigQueryCell::String(uuid.to_string())
        );
    }

    #[test]
    fn strict_lossless_and_lossy_handle_uuid_array_values() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let cell = Cell::Array(ArrayCell::Uuid(vec![Some(uuid)]));

        assert!(
            checker(DestinationTypeCompatibility::strict())
                .get_compatible_cell(&Type::UUID_ARRAY, cell.clone())
                .is_err()
        );
        assert_eq!(
            checker(DestinationTypeCompatibility::lossless())
                .get_compatible_cell(&Type::UUID_ARRAY, cell.clone())
                .unwrap()
                .cell,
            BigQueryCell::String(format!("{{{uuid}}}"))
        );
        assert_eq!(
            checker(DestinationTypeCompatibility::lossy())
                .get_compatible_cell(&Type::UUID_ARRAY, cell)
                .unwrap()
                .cell,
            BigQueryCell::String(format!("{{{uuid}}}"))
        );
    }

    #[test]
    fn lossy_numeric_rounds_and_clamps() {
        let rounded = lossy_bigquery_numeric(
            PgNumeric::from_str("0.123456789012345678901234567890123456789").unwrap(),
        )
        .unwrap();
        assert_eq!(rounded.to_string(), "0.12345678901234567890123456789012345679");

        let clamped = lossy_bigquery_numeric(PgNumeric::PositiveInfinity).unwrap();
        assert_eq!(clamped.to_string(), BIGQUERY_BIGNUMERIC_MAX);

        let zero = lossy_bigquery_numeric(
            PgNumeric::from_str("-0.000000000000000000000000000000000000001").unwrap(),
        )
        .unwrap();
        assert!(!zero.to_string().starts_with('-'));
    }

    #[test]
    fn json_depth_exceeding_bigquery_limit_is_rejected_unless_lossless() {
        let cell = Cell::String(nested_json(BIGQUERY_JSON_MAX_NESTING_DEPTH + 1).to_string());

        assert!(
            checker(DestinationTypeCompatibility::strict())
                .get_compatible_cell(&Type::JSON, cell.clone())
                .is_err()
        );
        assert!(matches!(
            checker(DestinationTypeCompatibility::lossless())
                .get_compatible_cell(&Type::JSON, cell.clone())
                .unwrap(),
            compatible if matches!(compatible.cell, BigQueryCell::String(_))
        ));
        assert!(
            checker(DestinationTypeCompatibility::lossy())
                .get_compatible_cell(&Type::JSON, cell)
                .is_err()
        );
    }

    #[test]
    fn lossy_json_clamps_numbers_to_bigquery_float_domain() {
        let cell = Cell::String(r#"{"value":1e309}"#.to_owned());
        let result = checker(DestinationTypeCompatibility::lossy())
            .get_compatible_cell(&Type::JSON, cell)
            .unwrap()
            .cell;

        let BigQueryCell::String(value) = result else {
            panic!("Expected JSON object");
        };
        let serde_json::Value::Object(values) = serde_json::from_str(&value).unwrap() else {
            panic!("Expected JSON object");
        };
        let value = values.get("value").and_then(serde_json::Value::as_f64).unwrap();
        assert_eq!(value, f64::MAX);
    }

    #[test]
    fn typed_arrays_fail_strict_and_stringify_lossless_and_lossy_null_elements() {
        let cell = Cell::Array(ArrayCell::I32(vec![Some(1), None]));

        let result = checker(DestinationTypeCompatibility::strict())
            .get_compatible_cell(&Type::INT4_ARRAY, cell.clone());
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().kind(),
            ErrorKind::NullValuesNotSupportedInArrayInDestination
        );

        assert_eq!(
            checker(DestinationTypeCompatibility::lossless())
                .get_compatible_cell(&Type::INT4_ARRAY, cell.clone())
                .unwrap()
                .cell,
            BigQueryCell::String("{1,NULL}".to_owned())
        );
        assert_eq!(
            checker(DestinationTypeCompatibility::lossy())
                .get_compatible_cell(&Type::INT4_ARRAY, cell)
                .unwrap()
                .cell,
            BigQueryCell::String("{1,NULL}".to_owned())
        );
    }

    #[test]
    fn typed_lossless_array_values_become_scalar_strings() {
        let cell =
            Cell::Array(ArrayCell::Numeric(vec![Some(PgNumeric::from_str("1.23").unwrap())]));
        let result = checker(DestinationTypeCompatibility::lossless())
            .get_compatible_cell(&Type::NUMERIC_ARRAY, cell)
            .unwrap()
            .cell;

        assert_eq!(result, BigQueryCell::String("{1.23}".to_owned()));
    }

    #[test]
    fn strict_rejects_null_array_cells() {
        let result = checker(DestinationTypeCompatibility::strict())
            .get_compatible_cell(&Type::INT4_ARRAY, Cell::Null);

        assert!(matches!(
            result,
            Err(err) if err.kind() == ErrorKind::UnsupportedValueInDestination
        ));
    }
}
