use std::{collections::HashSet, fmt, str::FromStr};

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use etl::{
    error::{ErrorKind, EtlError},
    materialization::{
        CellMaterializationOutcome,
        CellMaterializationOutcome::{TypeChanged, Unchanged, ValueChanged},
        CellMaterializationResult, DestinationMaterializationPolicy, DestinationMaterializer,
        MaterializationRules, TypeMaterializationResult, TypeStrategy, TypedCell, ValueStrategy,
    },
    types::{
        ArrayCell, Cell, DATE_FORMAT, PgDate, PgNumeric, PgTemporalBound, PgTime, PgTimestamp,
        PgTimestampTz, TIME_FORMAT, TIMESTAMP_FORMAT, TIMESTAMPTZ_FORMAT_HH_MM, Type,
        is_array_type,
    },
};
use serde::{
    Deserialize,
    de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor},
};
use serde_json::value::RawValue;

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
/// BigQuery cell materialization result.
type BigQueryCellMaterializationResult = CellMaterializationResult<BigQueryType, BigQueryCell>;

/// Internal cell materialization result before BigQuery value encoding.
type ProjectedCellMaterializationResult = CellMaterializationResult<Type, Cell>;

/// Type selected by a type strategy before BigQuery type lowering.
#[derive(Debug)]
struct TypeProjection {
    /// Type used after type-strategy projection.
    typ: Type,
    /// Whether the strategy changed the source type.
    changed: bool,
}

/// Result of applying the type strategy to a source type and optional cell.
#[derive(Debug)]
struct TypeStrategyProjection {
    /// Type used for value strategy handling after type projection.
    typ: Type,
    /// Cell reshaped to match [`TypeStrategyProjection::typ`], when present.
    cell: Option<Cell>,
    /// Whether type projection changed the source type.
    type_changed: bool,
}

/// Materialization failure shared by type and cell projection.
#[derive(Debug)]
struct MaterializationFailure {
    /// Error kind to surface to callers.
    kind: ErrorKind,
    /// Human-readable reason for the materialization failure.
    reason: String,
}

/// BigQuery materializer.
pub(super) type BigQueryMaterializer = DestinationMaterializer<BigQueryMaterialization>;

/// BigQuery materialization rules.
#[derive(Debug, Clone, Default)]
pub(super) struct BigQueryMaterialization;

impl BigQueryMaterialization {
    /// Creates a [`DestinationMaterializer`] for BigQuery.
    pub(super) fn materializer(
        policy: DestinationMaterializationPolicy,
    ) -> DestinationMaterializer<Self> {
        DestinationMaterializer::new(policy, Self)
    }
}

impl MaterializationRules for BigQueryMaterialization {
    type MaterializedType = BigQueryType;
    type MaterializedCell = BigQueryCell;

    fn materialize_type(
        &self,
        typ: &Type,
        policy: DestinationMaterializationPolicy,
    ) -> TypeMaterializationResult<BigQueryType> {
        materialize_bigquery_type(typ, policy.type_strategy())
    }

    fn materialize_cell(
        &self,
        typed_cell: TypedCell<Type, Cell>,
        policy: DestinationMaterializationPolicy,
    ) -> BigQueryCellMaterializationResult {
        let (source_type, cell) = typed_cell.into_parts();
        let projection = match apply_type_strategy(&source_type, Some(cell), policy.type_strategy())
        {
            Ok(projection) => projection,
            Err(err) => return invalid(err.kind, err.reason),
        };
        let Some(cell) = projection.cell else {
            // Cell materialization always projects with `Some(cell)`, so
            // missing a cell here means the projection layer lost data.
            return invalid(
                ErrorKind::InvalidData,
                "Type strategy projection did not return a cell for cell materialization",
            );
        };

        match apply_value_strategy(&projection.typ, cell, policy.value_strategy()) {
            CellMaterializationResult::Materialized { cell, outcome } => {
                let (typ, cell) = cell.into_parts();
                let outcome = combine_outcomes(projection.type_changed, outcome);
                encode_bigquery_cell(typ, cell, outcome)
            }
            CellMaterializationResult::Invalid { kind, reason } => invalid(kind, reason),
        }
    }
}

/// Applies type-strategy projection to a source type and optional cell.
///
/// This is the first layer of materialization. It keeps the representation in
/// the shared high-resolution [`Type`] and [`Cell`] model while applying
/// destination-specific type choices such as projecting risky values to text.
fn apply_type_strategy(
    source_type: &Type,
    cell: Option<Cell>,
    type_strategy: TypeStrategy,
) -> Result<TypeStrategyProjection, MaterializationFailure> {
    let projection = project_type(source_type, type_strategy)?;
    let cell = cell
        .map(|cell| project_cell_to_type(&projection.typ, cell, projection.changed))
        .transpose()?;

    Ok(TypeStrategyProjection { typ: projection.typ, cell, type_changed: projection.changed })
}

/// Returns the internal type selected by a type strategy.
///
/// This performs only `Type -> Type` projection. The projected type is later
/// lowered to [`BigQueryType`] by [`materialize_bigquery_type`] or by the final
/// cell encoder.
fn project_type(
    source_type: &Type,
    type_strategy: TypeStrategy,
) -> Result<TypeProjection, MaterializationFailure> {
    match type_strategy {
        TypeStrategy::NativeOnly if !has_native_bigquery_type(source_type) => {
            Err(MaterializationFailure {
                kind: ErrorKind::UnsupportedValueInDestination,
                reason: format!(
                    "PostgreSQL type {} has no native BigQuery representation",
                    source_type.name()
                ),
            })
        }
        TypeStrategy::NativeOrString if !has_native_bigquery_type(source_type) => {
            if is_array_type(source_type) {
                Ok(TypeProjection { typ: Type::TEXT_ARRAY, changed: true })
            } else {
                Ok(TypeProjection { typ: Type::TEXT, changed: true })
            }
        }
        TypeStrategy::StringIfRisky if should_project_to_text_for_string_if_risky(source_type) => {
            // `string_if_risky` preserves the source value as one scalar text
            // value, including arrays whose null elements BigQuery cannot
            // represent in repeated fields.
            Ok(TypeProjection { typ: Type::TEXT, changed: true })
        }
        TypeStrategy::NativeOnly | TypeStrategy::NativeOrString | TypeStrategy::StringIfRisky => {
            Ok(TypeProjection { typ: source_type.clone(), changed: false })
        }
    }
}

/// Projects a cell to the internal type selected by type strategy.
fn project_cell_to_type(
    projected_type: &Type,
    cell: Cell,
    type_changed: bool,
) -> Result<Cell, MaterializationFailure> {
    if !type_changed {
        return Ok(cell);
    }

    match *projected_type {
        Type::TEXT => Ok(project_cell_to_text(cell)),
        Type::TEXT_ARRAY => Ok(project_cell_to_text_array(cell)),
        _ => {
            // Type strategies currently only rewrite cells for text
            // projections. A future projection must define its cell shape here
            // before value strategy handling can safely continue.
            Err(failure(
                ErrorKind::InvalidData,
                format!(
                    "Type strategy selected unsupported projected type {}",
                    projected_type.name()
                ),
            ))
        }
    }
}

/// Converts a source cell into a scalar text-shaped cell.
fn project_cell_to_text(cell: Cell) -> Cell {
    match cell {
        Cell::Null => Cell::Null,
        Cell::Bool(value) => Cell::String(value.to_string()),
        Cell::String(value) => Cell::String(value),
        Cell::I16(value) => Cell::String(value.to_string()),
        Cell::I32(value) => Cell::String(value.to_string()),
        Cell::U32(value) => Cell::String(value.to_string()),
        Cell::I64(value) => Cell::String(value.to_string()),
        Cell::F32(value) => Cell::String(format_f32(value)),
        Cell::F64(value) => Cell::String(format_f64(value)),
        Cell::Numeric(value) => Cell::String(value.to_string()),
        Cell::Date(value) => Cell::String(value.to_string()),
        Cell::Time(value) => Cell::String(value.to_string()),
        Cell::Timestamp(value) => Cell::String(value.to_string()),
        Cell::TimestampTz(value) => Cell::String(value.to_string()),
        Cell::Uuid(value) => Cell::String(value.to_string()),
        Cell::Bytes(value) => Cell::String(format!("\\x{}", encode_hex(&value))),
        Cell::Array(array) => Cell::String(format_array(array)),
    }
}

/// Converts a source cell into a text-array-shaped cell.
fn project_cell_to_text_array(cell: Cell) -> Cell {
    match cell {
        Cell::Array(array) => Cell::Array(project_array_to_text_array(array)),
        cell => cell,
    }
}

/// Converts source array values into nullable string elements.
fn project_array_to_text_array(array: ArrayCell) -> ArrayCell {
    match array {
        ArrayCell::Bool(values) => project_array_values_to_text(values, |value| value.to_string()),
        ArrayCell::String(values) => ArrayCell::String(values),
        ArrayCell::I16(values) => project_array_values_to_text(values, |value| value.to_string()),
        ArrayCell::I32(values) => project_array_values_to_text(values, |value| value.to_string()),
        ArrayCell::U32(values) => project_array_values_to_text(values, |value| value.to_string()),
        ArrayCell::I64(values) => project_array_values_to_text(values, |value| value.to_string()),
        ArrayCell::F32(values) => project_array_values_to_text(values, format_f32),
        ArrayCell::F64(values) => project_array_values_to_text(values, format_f64),
        ArrayCell::Numeric(values) => {
            project_array_values_to_text(values, |value| value.to_string())
        }
        ArrayCell::Date(values) => project_array_values_to_text(values, |value| value.to_string()),
        ArrayCell::Time(values) => project_array_values_to_text(values, |value| value.to_string()),
        ArrayCell::Timestamp(values) => {
            project_array_values_to_text(values, |value| value.to_string())
        }
        ArrayCell::TimestampTz(values) => {
            project_array_values_to_text(values, |value| value.to_string())
        }
        ArrayCell::Uuid(values) => project_array_values_to_text(values, |value| value.to_string()),
        ArrayCell::Bytes(values) => {
            project_array_values_to_text(values, |value| format!("\\x{}", encode_hex(&value)))
        }
    }
}

/// Converts nullable array elements into nullable string elements.
fn project_array_values_to_text<T>(
    values: Vec<Option<T>>,
    mut convert: impl FnMut(T) -> String,
) -> ArrayCell {
    ArrayCell::String(values.into_iter().map(|value| value.map(&mut convert)).collect())
}

/// Combines type projection and value strategy outcomes.
fn combine_outcomes(
    type_changed: bool,
    value_outcome: CellMaterializationOutcome,
) -> CellMaterializationOutcome {
    if type_changed { TypeChanged } else { value_outcome }
}

/// Returns whether the source type can stay unchanged for BigQuery.
///
/// This is intentionally stricter than "can eventually be stored in
/// BigQuery". Types such as [`Type::UUID`] and [`Type::MONEY`] can be stored as
/// `STRING`, but only after type-strategy projection changes them to
/// [`Type::TEXT`] or [`Type::TEXT_ARRAY`].
fn has_native_bigquery_type(typ: &Type) -> bool {
    BigQueryType::for_projected_type(typ).is_some()
}

/// Returns the BigQuery type selected after type-strategy projection.
///
/// This is the schema path: source [`Type`] -> projected [`Type`] -> final
/// [`BigQueryType`]. It does not inspect values.
fn materialize_bigquery_type(
    typ: &Type,
    strategy: TypeStrategy,
) -> TypeMaterializationResult<BigQueryType> {
    match apply_type_strategy(typ, None, strategy) {
        Ok(projection) => match bigquery_type_for_projection(&projection.typ) {
            Ok(typ) if projection.type_changed => TypeMaterializationResult::Changed(typ),
            Ok(typ) => TypeMaterializationResult::Unchanged(typ),
            Err(err) => TypeMaterializationResult::Invalid { kind: err.kind, reason: err.reason },
        },
        Err(err) => TypeMaterializationResult::Invalid { kind: err.kind, reason: err.reason },
    }
}

/// Returns the BigQuery type for a projected internal type.
fn bigquery_type_for_projection(typ: &Type) -> Result<BigQueryType, MaterializationFailure> {
    BigQueryType::for_projected_type(typ).ok_or_else(|| {
        failure(
            ErrorKind::UnsupportedValueInDestination,
            format!(
                "PostgreSQL type {} has no BigQuery representation after type strategy projection",
                typ.name()
            ),
        )
    })
}

/// Returns whether the source type should use scalar BigQuery `STRING` under
/// [`TypeStrategy::StringIfRisky`].
fn should_project_to_text_for_string_if_risky(typ: &Type) -> bool {
    if is_array_type(typ) {
        // BigQuery repeated fields cannot preserve nullable array elements, so
        // preserve-oriented string projection stores the whole array as text.
        return true;
    }

    if !has_native_bigquery_type(typ) {
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

/// Applies value materialization before final BigQuery value encoding.
///
/// This is the second layer of cell materialization. It receives the projected
/// [`Type`] from the type strategy and continues to work with shared [`Cell`]
/// values so normalization and rejection can use the full source value model.
fn apply_value_strategy(
    typ: &Type,
    cell: Cell,
    value_strategy: ValueStrategy,
) -> ProjectedCellMaterializationResult {
    match value_strategy {
        ValueStrategy::Reject => reject_cell(typ, cell),
        ValueStrategy::Normalize => normalize_cell(typ, cell),
        ValueStrategy::Preserve => preserve_cell(typ, cell),
    }
}

/// Applies reject materialization to a projected cell.
fn reject_cell(typ: &Type, cell: Cell) -> ProjectedCellMaterializationResult {
    match cell {
        // JSON and JSONB use raw text in the shared cell model, so the
        // projected type decides whether this string needs JSON validation.
        Cell::String(value) if is_projected_json_type(typ) => reject_json_text(typ, value),
        cell => {
            if let Some(error) = validate_reject_cell(typ, &cell) {
                return invalid_projected_cell_from_failure(error);
            }

            projected_cell(typ.clone(), cell, Unchanged)
        }
    }
}

/// Applies reject materialization to raw JSON text.
fn reject_json_text(typ: &Type, value: String) -> ProjectedCellMaterializationResult {
    match parse_json_text_without_duplicate_keys(&value) {
        Ok(value_json) => {
            if let Some(error) = validate_reject_json(&value_json) {
                invalid_projected_cell_from_failure(error)
            } else {
                projected_cell(typ.clone(), Cell::String(value), Unchanged)
            }
        }
        Err(err) => invalid_projected_cell_from_failure(err),
    }
}

/// Applies preserve materialization to a projected cell.
///
/// Preserve intentionally has no JSON branch because it keeps the projected
/// type and cell exactly as they arrived from type projection.
fn preserve_cell(typ: &Type, cell: Cell) -> ProjectedCellMaterializationResult {
    projected_cell(typ.clone(), cell, Unchanged)
}

/// Applies normalize materialization to a projected cell.
fn normalize_cell(typ: &Type, cell: Cell) -> ProjectedCellMaterializationResult {
    match cell {
        // JSON and JSONB use raw text in the shared cell model, so the
        // projected type decides whether this string needs JSON normalization.
        Cell::String(value) if is_projected_json_type(typ) => normalize_json_text(typ, value),
        Cell::F32(value) if value == 0.0 && value.is_sign_negative() => {
            projected_cell(typ.clone(), Cell::F32(0.0), ValueChanged)
        }
        Cell::F64(value) if value == 0.0 && value.is_sign_negative() => {
            projected_cell(typ.clone(), Cell::F64(0.0), ValueChanged)
        }
        Cell::Numeric(value) => normalize_numeric_cell(typ, value),
        Cell::Date(value) => normalize_date_cell(typ, value),
        Cell::Time(value) => normalize_time_cell(typ, value),
        Cell::Timestamp(value) => normalize_timestamp_cell(typ, value),
        Cell::TimestampTz(value) => normalize_timestamptz_cell(typ, value),
        Cell::Array(array) => normalize_array_cell(typ, array),
        cell => projected_cell(typ.clone(), cell, Unchanged),
    }
}

/// Returns whether a projected type carries raw JSON text.
fn is_projected_json_type(typ: &Type) -> bool {
    matches!(*typ, Type::JSON | Type::JSONB)
}

/// Returns whether a projected array type carries raw JSON text elements.
fn is_projected_json_array_type(typ: &Type) -> bool {
    matches!(*typ, Type::JSON_ARRAY | Type::JSONB_ARRAY)
}

/// Applies normalize materialization to raw JSON text.
fn normalize_json_text(typ: &Type, value: String) -> ProjectedCellMaterializationResult {
    match normalize_json_value_text(value) {
        Ok(value) => projected_cell(typ.clone(), Cell::String(value), ValueChanged),
        Err(err) => invalid_projected_cell_from_failure(err),
    }
}

/// Applies normalize materialization to a parsed array value.
fn normalize_array_cell(typ: &Type, array: ArrayCell) -> ProjectedCellMaterializationResult {
    match (typ, array) {
        // JSON arrays also use string elements in the shared array model, so
        // the projected array type selects JSON element normalization.
        (typ, ArrayCell::String(values)) if is_projected_json_array_type(typ) => {
            normalize_json_array_cell(typ, values)
        }
        (_, ArrayCell::F32(values)) => {
            value_array(typ, values, normalize_f32_value, ArrayCell::F32)
        }
        (_, ArrayCell::F64(values)) => {
            value_array(typ, values, normalize_f64_value, ArrayCell::F64)
        }
        (_, ArrayCell::Numeric(values)) => normalize_numeric_array_cell(typ, values),
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
        (_, array) => projected_cell(typ.clone(), Cell::Array(array), Unchanged),
    }
}

/// Applies normalize materialization to JSON array element values.
fn normalize_json_array_cell(
    typ: &Type,
    values: Vec<Option<String>>,
) -> ProjectedCellMaterializationResult {
    let values = values
        .into_iter()
        .enumerate()
        .map(|(index, value)| match value {
            Some(value) => normalize_json_value_text(value)
                .map(Some)
                .map_err(|error| prefix_array_failure(index, error)),
            None => Err(MaterializationFailure {
                kind: ErrorKind::NullValuesNotSupportedInArrayInDestination,
                reason: format!(
                    "Element at index {index} is NULL, which is not supported in BigQuery"
                ),
            }),
        })
        .collect::<Result<Vec<_>, _>>();

    match values {
        Ok(values) => {
            projected_cell(typ.clone(), Cell::Array(ArrayCell::String(values)), ValueChanged)
        }
        Err(err) => invalid_projected_cell(err.kind, err.reason),
    }
}

/// Normalizes a BigQuery `FLOAT64` value carried by `float`.
fn normalize_f32_value(value: f32) -> f32 {
    if value == 0.0 && value.is_sign_negative() { 0.0 } else { value }
}

/// Normalizes a BigQuery `FLOAT64` value carried by `double`.
fn normalize_f64_value(value: f64) -> f64 {
    if value == 0.0 && value.is_sign_negative() { 0.0 } else { value }
}

/// Normalizes raw JSON text using BigQuery normalization rules.
fn normalize_json_value_text(value: String) -> Result<String, MaterializationFailure> {
    let value_json = parse_json_text_first_key_wins(&value)?;
    if let Err(reason) = validate_bigquery_json_depth(&value_json) {
        return Err(failure(ErrorKind::UnsupportedValueInDestination, reason));
    }

    normalize_bigquery_json(value_json)
        .map(|normalized| format_json_value(&normalized))
        .map_err(|reason| failure(ErrorKind::UnsupportedValueInDestination, reason))
}

/// Applies normalize materialization to a numeric value.
fn normalize_numeric_cell(typ: &Type, value: PgNumeric) -> ProjectedCellMaterializationResult {
    match normalize_bigquery_numeric(value) {
        Ok(value) => projected_cell(typ.clone(), Cell::Numeric(value), ValueChanged),
        Err(reason) => invalid_projected_cell(ErrorKind::UnsupportedValueInDestination, reason),
    }
}

/// Applies normalize materialization to numeric array values.
fn normalize_numeric_array_cell(
    typ: &Type,
    values: Vec<Option<PgNumeric>>,
) -> ProjectedCellMaterializationResult {
    let values = values
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            value
                .map(normalize_bigquery_numeric)
                .transpose()
                .map_err(|reason| format!("Element at index {index}: {reason}"))
        })
        .collect::<Result<Vec<_>, _>>();

    match values {
        Ok(values) => {
            projected_cell(typ.clone(), Cell::Array(ArrayCell::Numeric(values)), ValueChanged)
        }
        Err(reason) => invalid_projected_cell(ErrorKind::UnsupportedValueInDestination, reason),
    }
}

/// Applies normalize materialization to a PostgreSQL date value.
fn normalize_date_cell(typ: &Type, value: PgDate) -> ProjectedCellMaterializationResult {
    projected_cell(typ.clone(), Cell::Date(clamp_bigquery_date_value(value)), ValueChanged)
}

/// Applies normalize materialization to a PostgreSQL time value.
fn normalize_time_cell(typ: &Type, value: PgTime) -> ProjectedCellMaterializationResult {
    projected_cell(typ.clone(), Cell::Time(clamp_bigquery_time_value(value)), ValueChanged)
}

/// Applies normalize materialization to a PostgreSQL timestamp value.
fn normalize_timestamp_cell(typ: &Type, value: PgTimestamp) -> ProjectedCellMaterializationResult {
    projected_cell(
        typ.clone(),
        Cell::Timestamp(clamp_bigquery_timestamp_value(value)),
        ValueChanged,
    )
}

/// Applies normalize materialization to a PostgreSQL timestamptz value.
fn normalize_timestamptz_cell(
    typ: &Type,
    value: PgTimestampTz,
) -> ProjectedCellMaterializationResult {
    projected_cell(
        typ.clone(),
        Cell::TimestampTz(clamp_bigquery_timestamptz_value(value)),
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
        ArrayCell::F32(values) => format_array_values(values, format_f32),
        ArrayCell::F64(values) => format_array_values(values, format_f64),
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

/// Formats a PostgreSQL `real` value for text materialization.
fn format_f32(value: f32) -> String {
    if value == f32::INFINITY {
        "Infinity".to_owned()
    } else if value == f32::NEG_INFINITY {
        "-Infinity".to_owned()
    } else {
        value.to_string()
    }
}

/// Formats a PostgreSQL `double precision` value for text materialization.
fn format_f64(value: f64) -> String {
    if value == f64::INFINITY {
        "Infinity".to_owned()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_owned()
    } else {
        value.to_string()
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

/// Converts an optional array while preserving the array variant.
fn value_array<T>(
    typ: &Type,
    values: Vec<Option<T>>,
    mut convert: impl FnMut(T) -> T,
    wrap: impl FnOnce(Vec<Option<T>>) -> ArrayCell,
) -> ProjectedCellMaterializationResult {
    projected_cell(
        typ.clone(),
        Cell::Array(wrap(values.into_iter().map(|value| value.map(&mut convert)).collect())),
        ValueChanged,
    )
}

/// Returns a `reject` materialization failure when a cell is unsafe for
/// BigQuery.
fn validate_reject_cell(typ: &Type, cell: &Cell) -> Option<MaterializationFailure> {
    match cell {
        Cell::F32(value) if *value == 0.0 && value.is_sign_negative() => {
            Some(invalid_negative_zero())
        }
        Cell::F64(value) if *value == 0.0 && value.is_sign_negative() => {
            Some(invalid_negative_zero())
        }
        Cell::Numeric(value) => validate_reject_numeric(value),
        Cell::Date(value) => validate_reject_pg_date(value),
        Cell::Time(value) => validate_reject_pg_time(value),
        Cell::Timestamp(value) => validate_reject_pg_timestamp(value),
        Cell::TimestampTz(value) => validate_reject_pg_timestamptz(value),
        Cell::Array(array) => validate_reject_array(typ, array),
        _ => None,
    }
}

/// Returns a `reject` materialization failure when an array is unsafe for
/// BigQuery.
fn validate_reject_array(typ: &Type, array: &ArrayCell) -> Option<MaterializationFailure> {
    match (typ, array) {
        // JSON arrays also use string elements in the shared array model, so
        // the projected array type selects JSON element validation.
        (typ, ArrayCell::String(values)) if is_projected_json_array_type(typ) => {
            validate_reject_array_values(values, |value| validate_reject_json_text(value))
        }
        (_, array) => validate_reject_array_value_domain(array),
    }
}

/// Returns a `reject` materialization failure based on the parsed array values.
fn validate_reject_array_value_domain(array: &ArrayCell) -> Option<MaterializationFailure> {
    match array {
        ArrayCell::F32(values) => validate_reject_array_values(values, |value| {
            (*value == 0.0 && value.is_sign_negative()).then(invalid_negative_zero)
        }),
        ArrayCell::F64(values) => validate_reject_array_values(values, |value| {
            (*value == 0.0 && value.is_sign_negative()).then(invalid_negative_zero)
        }),
        ArrayCell::Numeric(values) => validate_reject_array_values(values, validate_reject_numeric),
        ArrayCell::Date(values) => validate_reject_array_values(values, validate_reject_pg_date),
        ArrayCell::Time(values) => validate_reject_array_values(values, validate_reject_pg_time),
        ArrayCell::Timestamp(values) => {
            validate_reject_array_values(values, validate_reject_pg_timestamp)
        }
        ArrayCell::TimestampTz(values) => {
            validate_reject_array_values(values, validate_reject_pg_timestamptz)
        }
        _ => None,
    }
}

/// Validates `reject` materialization for optional array elements.
fn validate_reject_array_values<T>(
    values: &[Option<T>],
    validate: impl Fn(&T) -> Option<MaterializationFailure>,
) -> Option<MaterializationFailure> {
    for (index, value) in values.iter().enumerate() {
        if let Some(value) = value
            && let Some(result) = validate(value)
        {
            return Some(prefix_array_failure(index, result));
        }
    }

    None
}

/// Adds array element context to a projection failure.
fn prefix_array_failure(index: usize, error: MaterializationFailure) -> MaterializationFailure {
    MaterializationFailure {
        kind: error.kind,
        reason: format!("Element at index {index}: {}", error.reason),
    }
}

/// Validates `reject` BigQuery materialization for a numeric value.
fn validate_reject_numeric(value: &PgNumeric) -> Option<MaterializationFailure> {
    match value {
        PgNumeric::NaN => Some(failure(
            ErrorKind::UnsupportedValueInDestination,
            "NaN cannot be stored in BigQuery BIGNUMERIC",
        )),
        PgNumeric::PositiveInfinity | PgNumeric::NegativeInfinity => Some(failure(
            ErrorKind::UnsupportedValueInDestination,
            "Infinity cannot be stored in BigQuery BIGNUMERIC",
        )),
        PgNumeric::Value { .. }
            if bigquery_bignumeric_scale(value) > BIGQUERY_BIGNUMERIC_MAX_SCALE =>
        {
            Some(failure(
                ErrorKind::UnsupportedValueInDestination,
                format!(
                    "A numeric value has more than {BIGQUERY_BIGNUMERIC_MAX_SCALE} decimal places \
                     and would be rounded by BigQuery",
                ),
            ))
        }
        PgNumeric::Value { .. } if !bigquery_bignumeric_in_range(value) => Some(failure(
            ErrorKind::UnsupportedValueInDestination,
            "A numeric value is outside the BigQuery BIGNUMERIC range",
        )),
        PgNumeric::Value { .. } => None,
    }
}

/// Validates `reject` BigQuery materialization for a PostgreSQL date.
fn validate_reject_pg_date(value: &PgDate) -> Option<MaterializationFailure> {
    match value {
        PgDate::Finite(value) if bigquery_date_in_range(*value) => None,
        PgDate::Finite(_) => Some(invalid_date_range()),
        PgDate::PosInfinity | PgDate::NegInfinity | PgDate::OutOfRange(_) => {
            Some(invalid_date_range())
        }
    }
}

/// Validates `reject` BigQuery materialization for a PostgreSQL time.
fn validate_reject_pg_time(value: &PgTime) -> Option<MaterializationFailure> {
    match value {
        PgTime::Finite(_) => None,
        PgTime::TwentyFourHour => Some(failure(
            ErrorKind::UnsupportedValueInDestination,
            "A time value is outside the BigQuery TIME range",
        )),
    }
}

/// Validates `reject` BigQuery materialization for a PostgreSQL timestamp.
fn validate_reject_pg_timestamp(value: &PgTimestamp) -> Option<MaterializationFailure> {
    match value {
        PgTimestamp::Finite(value) if bigquery_timestamp_in_range(*value) => None,
        PgTimestamp::Finite(_) => Some(invalid_timestamp_range()),
        PgTimestamp::PosInfinity | PgTimestamp::NegInfinity | PgTimestamp::OutOfRange(_) => {
            Some(invalid_timestamp_range())
        }
    }
}

/// Validates `reject` BigQuery materialization for a PostgreSQL timestamptz.
fn validate_reject_pg_timestamptz(value: &PgTimestampTz) -> Option<MaterializationFailure> {
    match value {
        PgTimestampTz::Finite(value) if bigquery_timestamptz_in_range(*value) => None,
        PgTimestampTz::Finite(_) => Some(invalid_timestamp_range()),
        PgTimestampTz::PosInfinity | PgTimestampTz::NegInfinity | PgTimestampTz::OutOfRange(_) => {
            Some(invalid_timestamp_range())
        }
    }
}

/// Validates that raw JSON text has no duplicate object keys.
fn validate_json_text_has_no_duplicate_keys(value: &str) -> Option<MaterializationFailure> {
    match json_contains_duplicate_keys(value) {
        Ok(true) => Some(failure(
            ErrorKind::UnsupportedValueInDestination,
            "JSON text contains duplicate object keys that BigQuery would canonicalize",
        )),
        Ok(false) => None,
        Err(err) => Some(failure(
            ErrorKind::UnsupportedValueInDestination,
            format!("JSON text is not valid for BigQuery: {err}"),
        )),
    }
}

/// Parses JSON text after rejecting BigQuery canonicalization risks.
fn parse_json_text_without_duplicate_keys(
    value: &str,
) -> Result<serde_json::Value, MaterializationFailure> {
    if let Some(err) = validate_json_text_has_no_duplicate_keys(value) {
        return Err(err);
    }

    parse_json_value(value).map_err(|error| {
        failure(
            ErrorKind::UnsupportedValueInDestination,
            format!("JSON text is not valid for BigQuery: {error}"),
        )
    })
}

/// Parses JSON text using BigQuery's first-key-wins object canonicalization.
fn parse_json_text_first_key_wins(
    value: &str,
) -> Result<serde_json::Value, MaterializationFailure> {
    parse_json_raw_first_key_wins(value).map_err(|error| {
        failure(
            ErrorKind::UnsupportedValueInDestination,
            format!("JSON text is not valid for BigQuery: {error}"),
        )
    })
}

/// Parses raw JSON while preserving first-key-wins object semantics.
fn parse_json_raw_first_key_wins(value: &str) -> Result<serde_json::Value, String> {
    let first_byte = value.bytes().find(|byte| !byte.is_ascii_whitespace());
    match first_byte {
        Some(b'{') => deserialize_json_first_key_wins_object(value),
        Some(b'[') => deserialize_json_first_key_wins_array(value),
        _ => parse_json_value(value).map_err(|error| error.to_string()),
    }
}

/// Parses JSON with serde's recursion limit disabled.
fn parse_json_value(value: &str) -> Result<serde_json::Value, serde_json::Error> {
    let mut deserializer = serde_json::Deserializer::from_str(value);
    deserializer.disable_recursion_limit();
    let value = serde_json::Value::deserialize(&mut deserializer)?;
    deserializer.end()?;

    Ok(value)
}

/// Deserializes a raw JSON object using first-key-wins semantics.
fn deserialize_json_first_key_wins_object(value: &str) -> Result<serde_json::Value, String> {
    let mut deserializer = serde_json::Deserializer::from_str(value);
    deserializer.disable_recursion_limit();
    let value = JsonFirstKeyWinsObjectParser
        .deserialize(&mut deserializer)
        .map_err(|error| error.to_string())?;
    deserializer.end().map_err(|error| error.to_string())?;

    Ok(value)
}

/// Deserializes a raw JSON array while recursively preserving object semantics.
fn deserialize_json_first_key_wins_array(value: &str) -> Result<serde_json::Value, String> {
    let mut deserializer = serde_json::Deserializer::from_str(value);
    deserializer.disable_recursion_limit();
    let value = JsonFirstKeyWinsArrayParser
        .deserialize(&mut deserializer)
        .map_err(|error| error.to_string())?;
    deserializer.end().map_err(|error| error.to_string())?;

    Ok(value)
}

/// Validates `reject` BigQuery materialization for raw JSON text.
fn validate_reject_json_text(value: &str) -> Option<MaterializationFailure> {
    match parse_json_text_without_duplicate_keys(value) {
        Ok(value_json) => validate_reject_json(&value_json),
        Err(result) => Some(result),
    }
}

/// Validates `reject` BigQuery materialization for JSON.
fn validate_reject_json(value: &serde_json::Value) -> Option<MaterializationFailure> {
    if let Err(reason) = validate_bigquery_json_depth(value) {
        return Some(failure(ErrorKind::UnsupportedValueInDestination, reason));
    }

    let mut values = vec![value];

    while let Some(value) = values.pop() {
        match value {
            serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::String(_) => {
            }
            serde_json::Value::Number(number) => {
                if let Some(result) = validate_reject_json_number(number) {
                    return Some(result);
                }
            }
            serde_json::Value::Array(items) => values.extend(items),
            serde_json::Value::Object(entries) => values.extend(entries.values()),
        }
    }

    None
}

/// Streaming JSON object parser that keeps the first value for duplicate keys.
struct JsonFirstKeyWinsObjectParser;

impl<'de> DeserializeSeed<'de> for JsonFirstKeyWinsObjectParser {
    type Value = serde_json::Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for JsonFirstKeyWinsObjectParser {
    type Value = serde_json::Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = serde_json::Map::new();

        while let Some(key) = map.next_key::<String>()? {
            let raw_value = map.next_value::<Box<RawValue>>()?;

            if !values.contains_key(&key) {
                let value =
                    parse_json_raw_first_key_wins(raw_value.get()).map_err(de::Error::custom)?;
                values.insert(key, value);
            }
        }

        Ok(serde_json::Value::Object(values))
    }
}

/// Streaming JSON array parser that recursively applies object semantics.
struct JsonFirstKeyWinsArrayParser;

impl<'de> DeserializeSeed<'de> for JsonFirstKeyWinsArrayParser {
    type Value = serde_json::Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for JsonFirstKeyWinsArrayParser {
    type Value = serde_json::Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON array")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(raw_value) = seq.next_element::<Box<RawValue>>()? {
            values.push(parse_json_raw_first_key_wins(raw_value.get()).map_err(de::Error::custom)?);
        }

        Ok(serde_json::Value::Array(values))
    }
}

/// Returns whether raw JSON text contains duplicate object keys.
fn json_contains_duplicate_keys(value: &str) -> Result<bool, serde_json::Error> {
    let mut detector = JsonDuplicateKeyDetector { found_duplicate: false };
    let mut deserializer = serde_json::Deserializer::from_str(value);
    deserializer.disable_recursion_limit();
    (&mut detector).deserialize(&mut deserializer)?;
    deserializer.end()?;

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

/// Validates `reject` BigQuery materialization for a JSON number.
fn validate_reject_json_number(number: &serde_json::Number) -> Option<MaterializationFailure> {
    let number = number.as_str();
    if is_json_integer_literal(number) {
        let integer_outside_exact_domain = if number.starts_with('-') {
            number.parse::<i64>().is_err()
        } else {
            number.parse::<u64>().is_err()
        };

        if integer_outside_exact_domain {
            return Some(failure(
                ErrorKind::UnsupportedValueInDestination,
                "A JSON integer is outside BigQuery's exact signed or unsigned 64-bit integer \
                 domain and may be stored as FLOAT64",
            ));
        }
    } else if !json_number_fits_float64(number) {
        return Some(failure(
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

/// Normalizes a JSON value using deterministic BigQuery normalization rules.
fn normalize_bigquery_json(value: serde_json::Value) -> Result<serde_json::Value, String> {
    match value {
        serde_json::Value::Array(values) => {
            let values =
                values.into_iter().map(normalize_bigquery_json).collect::<Result<Vec<_>, _>>()?;
            Ok(serde_json::Value::Array(values))
        }
        serde_json::Value::Object(values) => {
            let values = values
                .into_iter()
                .map(|(key, value)| normalize_bigquery_json(value).map(|value| (key, value)))
                .collect::<Result<serde_json::Map<_, _>, _>>()?;
            Ok(serde_json::Value::Object(values))
        }
        serde_json::Value::Number(number) => {
            normalize_bigquery_json_number(number).map(serde_json::Value::Number)
        }
        value => Ok(value),
    }
}

/// Formats JSON without interpreting serde_json's private number object key.
fn format_json_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_owned(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::String(value) => {
            serde_json::to_string(value).expect("serializing a JSON string cannot fail")
        }
        serde_json::Value::Array(values) => {
            let values = values.iter().map(format_json_value).collect::<Vec<_>>().join(",");
            format!("[{values}]")
        }
        serde_json::Value::Object(values) => {
            let values = values
                .iter()
                .map(|(key, value)| {
                    let key =
                        serde_json::to_string(key).expect("serializing a JSON key cannot fail");
                    let value = format_json_value(value);
                    format!("{key}:{value}")
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{values}}}")
        }
    }
}

/// Normalizes a JSON number using deterministic BigQuery normalization rules.
fn normalize_bigquery_json_number(
    number: serde_json::Number,
) -> Result<serde_json::Number, String> {
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

/// Returns a `reject` materialization failure for negative zero.
fn invalid_negative_zero() -> MaterializationFailure {
    failure(
        ErrorKind::UnsupportedValueInDestination,
        "Negative zero cannot be stored in BigQuery tables",
    )
}

/// Returns a `reject` materialization failure for a date outside BigQuery's
/// range.
fn invalid_date_range() -> MaterializationFailure {
    failure(
        ErrorKind::UnsupportedValueInDestination,
        "Date value is outside BigQuery's supported range 0001-01-01..=9999-12-31",
    )
}

/// Returns a `reject` materialization failure for a timestamp outside
/// BigQuery's range.
fn invalid_timestamp_range() -> MaterializationFailure {
    failure(
        ErrorKind::UnsupportedValueInDestination,
        "Timestamp value is outside BigQuery's supported range 0001-01-01..=9999-12-31",
    )
}

/// Returns a successful BigQuery cell materialization result.
fn bigquery_cell_result(
    typ: BigQueryType,
    cell: BigQueryCell,
    outcome: CellMaterializationOutcome,
) -> BigQueryCellMaterializationResult {
    CellMaterializationResult::Materialized { cell: TypedCell::new(typ, cell), outcome }
}

/// Returns a successful projected cell before BigQuery value encoding.
fn projected_cell(
    typ: Type,
    cell: Cell,
    outcome: CellMaterializationOutcome,
) -> ProjectedCellMaterializationResult {
    CellMaterializationResult::Materialized { cell: TypedCell::new(typ, cell), outcome }
}

/// Encodes the projected type and cell into the final BigQuery representation.
///
/// This is the only cell path that lowers the projected high-resolution
/// [`Type`] and [`Cell`] pair into the BigQuery-specific schema type and value
/// carrier. Type and value strategies must have already run before this point.
fn encode_bigquery_cell(
    typ: Type,
    cell: Cell,
    outcome: CellMaterializationOutcome,
) -> BigQueryCellMaterializationResult {
    let bigquery_type = match bigquery_type_for_projection(&typ) {
        Ok(typ) => typ,
        Err(err) => return invalid(err.kind, err.reason),
    };

    match bigquery_cell_for_projected_type(&typ, cell) {
        Ok(cell) => bigquery_cell_result(bigquery_type, cell, outcome),
        Err(err) => invalid_from_etl_error(err),
    }
}

/// Returns the BigQuery value carrier for a projected internal type and cell.
fn bigquery_cell_for_projected_type(typ: &Type, cell: Cell) -> Result<BigQueryCell, EtlError> {
    if is_array_type(typ) {
        // BigQuery arrays are encoded as repeated fields. They have elements,
        // but no separate nullable array container to encode `Cell::Null`.
        return match cell {
            Cell::Array(array) => {
                bigquery_array_cell_for_projected_type(typ, array).map(BigQueryCell::Array)
            }
            Cell::Null => Err(etl::etl_error!(
                ErrorKind::UnsupportedValueInDestination,
                "Cell cannot be materialized for BigQuery repeated field",
                "NULL arrays cannot be preserved by BigQuery repeated fields"
            )),
            cell => Err(invalid_cell_for_type(typ, cell_kind(&cell))),
        };
    }

    match (typ, cell) {
        (_, Cell::Null) => Ok(BigQueryCell::Null),
        (&Type::BOOL, Cell::Bool(value)) => Ok(BigQueryCell::Bool(value)),
        (
            &Type::CHAR
            | &Type::BPCHAR
            | &Type::VARCHAR
            | &Type::NAME
            | &Type::TEXT
            | &Type::JSON
            | &Type::JSONB,
            Cell::String(value),
        ) => Ok(BigQueryCell::String(value)),
        (&Type::INT2, Cell::I16(value)) => Ok(BigQueryCell::Int32(i32::from(value))),
        (&Type::INT4, Cell::I32(value)) => Ok(BigQueryCell::Int32(value)),
        (&Type::INT8, Cell::I64(value)) => Ok(BigQueryCell::Int64(value)),
        (&Type::OID, Cell::U32(value)) => Ok(BigQueryCell::Int64(i64::from(value))),
        (&Type::FLOAT4, Cell::F32(value)) => Ok(BigQueryCell::Float32(value)),
        (&Type::FLOAT8, Cell::F64(value)) => Ok(BigQueryCell::Float64(value)),
        (&Type::NUMERIC, Cell::Numeric(value)) => Ok(BigQueryCell::String(value.to_string())),
        (&Type::DATE, Cell::Date(value)) => Ok(BigQueryCell::String(format_pg_date(value))),
        (&Type::TIME, Cell::Time(value)) => Ok(BigQueryCell::String(format_pg_time(value))),
        (&Type::TIMESTAMP, Cell::Timestamp(value)) => {
            Ok(BigQueryCell::String(format_pg_timestamp(value)))
        }
        (&Type::TIMESTAMPTZ, Cell::TimestampTz(value)) => {
            Ok(BigQueryCell::String(format_pg_timestamptz(value)))
        }
        (&Type::BYTEA, Cell::Bytes(value)) => Ok(BigQueryCell::Bytes(value)),
        (typ, cell) => Err(invalid_cell_for_type(typ, cell_kind(&cell))),
    }
}

/// Returns the BigQuery repeated-field carrier for a projected array type.
fn bigquery_array_cell_for_projected_type(
    typ: &Type,
    array: ArrayCell,
) -> Result<BigQueryArrayCell, EtlError> {
    match (typ, array) {
        (&Type::BOOL_ARRAY, ArrayCell::Bool(values)) => {
            bigquery_array_values(values, std::convert::identity, BigQueryArrayCell::Bool)
        }
        (
            &Type::CHAR_ARRAY
            | &Type::BPCHAR_ARRAY
            | &Type::VARCHAR_ARRAY
            | &Type::NAME_ARRAY
            | &Type::TEXT_ARRAY
            | &Type::JSON_ARRAY
            | &Type::JSONB_ARRAY,
            ArrayCell::String(values),
        ) => bigquery_array_values(values, std::convert::identity, BigQueryArrayCell::String),
        (&Type::INT2_ARRAY, ArrayCell::I16(values)) => {
            bigquery_array_values(values, i32::from, BigQueryArrayCell::Int32)
        }
        (&Type::INT4_ARRAY, ArrayCell::I32(values)) => {
            bigquery_array_values(values, std::convert::identity, BigQueryArrayCell::Int32)
        }
        (&Type::INT8_ARRAY, ArrayCell::I64(values)) => {
            bigquery_array_values(values, std::convert::identity, BigQueryArrayCell::Int64)
        }
        (&Type::OID_ARRAY, ArrayCell::U32(values)) => {
            bigquery_array_values(values, i64::from, BigQueryArrayCell::Int64)
        }
        (&Type::FLOAT4_ARRAY, ArrayCell::F32(values)) => {
            bigquery_array_values(values, std::convert::identity, BigQueryArrayCell::Float32)
        }
        (&Type::FLOAT8_ARRAY, ArrayCell::F64(values)) => {
            bigquery_array_values(values, std::convert::identity, BigQueryArrayCell::Float64)
        }
        (&Type::NUMERIC_ARRAY, ArrayCell::Numeric(values)) => {
            bigquery_array_values(values, |value| value.to_string(), BigQueryArrayCell::String)
        }
        (&Type::DATE_ARRAY, ArrayCell::Date(values)) => {
            bigquery_array_values(values, format_pg_date, BigQueryArrayCell::String)
        }
        (&Type::TIME_ARRAY, ArrayCell::Time(values)) => {
            bigquery_array_values(values, format_pg_time, BigQueryArrayCell::String)
        }
        (&Type::TIMESTAMP_ARRAY, ArrayCell::Timestamp(values)) => {
            bigquery_array_values(values, format_pg_timestamp, BigQueryArrayCell::String)
        }
        (&Type::TIMESTAMPTZ_ARRAY, ArrayCell::TimestampTz(values)) => {
            bigquery_array_values(values, format_pg_timestamptz, BigQueryArrayCell::String)
        }
        (&Type::BYTEA_ARRAY, ArrayCell::Bytes(values)) => {
            bigquery_array_values(values, std::convert::identity, BigQueryArrayCell::Bytes)
        }
        (typ, array) => Err(invalid_array_cell_for_type(typ, array_cell_kind(&array))),
    }
}

/// Converts required array elements into a BigQuery repeated-field carrier.
fn bigquery_array_values<T, U>(
    values: Vec<Option<T>>,
    convert: impl FnMut(T) -> U,
    wrap: impl FnOnce(Vec<U>) -> BigQueryArrayCell,
) -> Result<BigQueryArrayCell, EtlError> {
    Ok(wrap(required_values(values)?.into_iter().map(convert).collect()))
}

/// Returns an invalid materialization result.
fn invalid(kind: ErrorKind, reason: impl Into<String>) -> BigQueryCellMaterializationResult {
    CellMaterializationResult::Invalid { kind, reason: reason.into() }
}

/// Returns a materialization failure.
fn failure(kind: ErrorKind, reason: impl Into<String>) -> MaterializationFailure {
    MaterializationFailure { kind, reason: reason.into() }
}

/// Returns an invalid projected materialization result.
fn invalid_projected_cell(
    kind: ErrorKind,
    reason: impl Into<String>,
) -> ProjectedCellMaterializationResult {
    CellMaterializationResult::Invalid { kind, reason: reason.into() }
}

/// Converts a materialization failure into a projected result.
fn invalid_projected_cell_from_failure(
    error: MaterializationFailure,
) -> ProjectedCellMaterializationResult {
    invalid_projected_cell(error.kind, error.reason)
}

/// Returns an error when a projected cell does not match its projected type.
fn invalid_cell_for_type(typ: &Type, cell_kind: &'static str) -> EtlError {
    etl::etl_error!(
        ErrorKind::InvalidData,
        "Cell does not match projected type",
        format!("Expected projected PostgreSQL type {}, got {cell_kind}", typ.name())
    )
}

/// Returns an error when a projected array cell does not match its projected
/// array type.
fn invalid_array_cell_for_type(typ: &Type, cell_kind: &'static str) -> EtlError {
    etl::etl_error!(
        ErrorKind::InvalidData,
        "Cell does not match projected array type",
        format!("Expected projected PostgreSQL array type {}, got {cell_kind}", typ.name())
    )
}

/// Returns a stable kind name for a scalar cell without exposing the value.
fn cell_kind(cell: &Cell) -> &'static str {
    match cell {
        Cell::Null => "null",
        Cell::Bool(_) => "bool",
        Cell::String(_) => "string",
        Cell::I16(_) => "i16",
        Cell::I32(_) => "i32",
        Cell::U32(_) => "u32",
        Cell::I64(_) => "i64",
        Cell::F32(_) => "f32",
        Cell::F64(_) => "f64",
        Cell::Numeric(_) => "numeric",
        Cell::Date(_) => "date",
        Cell::Time(_) => "time",
        Cell::Timestamp(_) => "timestamp",
        Cell::TimestampTz(_) => "timestamptz",
        Cell::Uuid(_) => "uuid",
        Cell::Bytes(_) => "bytes",
        Cell::Array(array) => array_cell_kind(array),
    }
}

/// Returns a stable kind name for an array cell without exposing values.
fn array_cell_kind(array: &ArrayCell) -> &'static str {
    match array {
        ArrayCell::Bool(_) => "bool[]",
        ArrayCell::String(_) => "string[]",
        ArrayCell::I16(_) => "i16[]",
        ArrayCell::I32(_) => "i32[]",
        ArrayCell::U32(_) => "u32[]",
        ArrayCell::I64(_) => "i64[]",
        ArrayCell::F32(_) => "f32[]",
        ArrayCell::F64(_) => "f64[]",
        ArrayCell::Numeric(_) => "numeric[]",
        ArrayCell::Date(_) => "date[]",
        ArrayCell::Time(_) => "time[]",
        ArrayCell::Timestamp(_) => "timestamp[]",
        ArrayCell::TimestampTz(_) => "timestamptz[]",
        ArrayCell::Uuid(_) => "uuid[]",
        ArrayCell::Bytes(_) => "bytes[]",
    }
}

/// Returns array values after materialization validation has removed `NULL`s.
fn required_values<T>(values: Vec<Option<T>>) -> Result<Vec<T>, EtlError> {
    let mut required = Vec::with_capacity(values.len());
    for (index, value) in values.into_iter().enumerate() {
        match value {
            Some(value) => required.push(value),
            None => {
                return Err(etl::etl_error!(
                    ErrorKind::NullValuesNotSupportedInArrayInDestination,
                    "Array contains NULL value",
                    format!("Element at index {index} is NULL, which is not supported in BigQuery")
                ));
            }
        }
    }

    Ok(required)
}

/// Formats a BigQuery date string.
fn format_date(value: NaiveDate) -> String {
    value.format(DATE_FORMAT).to_string()
}

/// Formats a PostgreSQL date for BigQuery string-backed encodings.
fn format_pg_date(value: PgDate) -> String {
    match value {
        PgDate::Finite(value) => format_date(value),
        value => value.to_string(),
    }
}

/// Formats a BigQuery time string.
fn format_time(value: NaiveTime) -> String {
    value.format(TIME_FORMAT).to_string()
}

/// Formats a PostgreSQL time for BigQuery string-backed encodings.
fn format_pg_time(value: PgTime) -> String {
    match value {
        PgTime::Finite(value) => format_time(value),
        value => value.to_string(),
    }
}

/// Formats a BigQuery datetime string.
fn format_timestamp(value: NaiveDateTime) -> String {
    value.format(TIMESTAMP_FORMAT).to_string()
}

/// Formats a PostgreSQL timestamp for BigQuery string-backed encodings.
fn format_pg_timestamp(value: PgTimestamp) -> String {
    match value {
        PgTimestamp::Finite(value) => format_timestamp(value),
        value => value.to_string(),
    }
}

/// Formats a BigQuery timestamp string.
fn format_timestamptz(value: DateTime<Utc>) -> String {
    value.format(TIMESTAMPTZ_FORMAT_HH_MM).to_string()
}

/// Formats a PostgreSQL timestamptz for BigQuery string-backed encodings.
fn format_pg_timestamptz(value: PgTimestampTz) -> String {
    match value {
        PgTimestampTz::Finite(value) => format_timestamptz(value),
        value => value.to_string(),
    }
}

/// Returns an invalid materialization result from an [`EtlError`].
fn invalid_from_etl_error(error: EtlError) -> BigQueryCellMaterializationResult {
    let reason = error
        .detail()
        .or_else(|| error.description())
        .map_or_else(|| error.to_string(), ToOwned::to_owned);
    invalid(error.kind(), reason)
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

/// Applies BigQuery `BIGNUMERIC` normalization by rounding and clamping.
fn normalize_bigquery_numeric(value: PgNumeric) -> Result<PgNumeric, String> {
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
    use crate::bigquery::value::{
        BigQueryArrayCell, BigQueryArrayType, BigQueryFloatEncoding, BigQueryIntEncoding,
    };

    /// Returns a materializer for the supplied policy.
    fn materializer(
        policy: DestinationMaterializationPolicy,
    ) -> DestinationMaterializer<BigQueryMaterialization> {
        BigQueryMaterialization::materializer(policy)
    }

    /// Returns a typed cell for tests.
    fn typed_cell(typ: Type, cell: Cell) -> TypedCell<Type, Cell> {
        TypedCell::new(typ, cell)
    }

    /// Returns only the materialized cell for tests.
    fn materialized_cell(
        policy: DestinationMaterializationPolicy,
        typ: Type,
        cell: Cell,
    ) -> EtlResult<BigQueryCell> {
        materializer(policy)
            .materialize_cell(typed_cell(typ, cell))
            .map(|typed_cell| typed_cell.into_parts().1)
    }

    /// Returns the materialized type and cell for tests.
    fn materialized_type_and_cell(
        policy: DestinationMaterializationPolicy,
        typ: Type,
        cell: Cell,
    ) -> EtlResult<(BigQueryType, BigQueryCell)> {
        materializer(policy).materialize_cell(typed_cell(typ, cell)).map(TypedCell::into_parts)
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
        let policy = DestinationMaterializationPolicy::string_if_risky_preserve();
        let materializer = materializer(policy);

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
    fn native_or_string_type_strategy_uses_native_types_with_string_fallbacks() {
        let policy = DestinationMaterializationPolicy::native_or_string_reject();
        let materializer = materializer(policy);

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
    fn native_only_type_strategy_accepts_all_native_bigquery_types() {
        let materializer = materializer(DestinationMaterializationPolicy::native_only_reject());

        for (typ, expected_type) in [
            (Type::BOOL, BigQueryType::Bool),
            (Type::CHAR, BigQueryType::String),
            (Type::BPCHAR, BigQueryType::String),
            (Type::VARCHAR, BigQueryType::String),
            (Type::NAME, BigQueryType::String),
            (Type::TEXT, BigQueryType::String),
            (Type::INT2, BigQueryType::Int64(BigQueryIntEncoding::Int32)),
            (Type::INT4, BigQueryType::Int64(BigQueryIntEncoding::Int32)),
            (Type::INT8, BigQueryType::Int64(BigQueryIntEncoding::Int64)),
            (Type::OID, BigQueryType::Int64(BigQueryIntEncoding::Int64)),
            (Type::FLOAT4, BigQueryType::Float64(BigQueryFloatEncoding::Float)),
            (Type::FLOAT8, BigQueryType::Float64(BigQueryFloatEncoding::Double)),
            (Type::NUMERIC, BigQueryType::BigNumeric),
            (Type::DATE, BigQueryType::Date),
            (Type::TIME, BigQueryType::Time),
            (Type::TIMESTAMP, BigQueryType::DateTime),
            (Type::TIMESTAMPTZ, BigQueryType::Timestamp),
            (Type::JSON, BigQueryType::Json),
            (Type::JSONB, BigQueryType::Json),
            (Type::BYTEA, BigQueryType::Bytes),
            (Type::BOOL_ARRAY, BigQueryType::Array(BigQueryArrayType::Bool)),
            (Type::CHAR_ARRAY, BigQueryType::Array(BigQueryArrayType::String)),
            (Type::BPCHAR_ARRAY, BigQueryType::Array(BigQueryArrayType::String)),
            (Type::VARCHAR_ARRAY, BigQueryType::Array(BigQueryArrayType::String)),
            (Type::NAME_ARRAY, BigQueryType::Array(BigQueryArrayType::String)),
            (Type::TEXT_ARRAY, BigQueryType::Array(BigQueryArrayType::String)),
            (
                Type::INT2_ARRAY,
                BigQueryType::Array(BigQueryArrayType::Int64(BigQueryIntEncoding::Int32)),
            ),
            (
                Type::INT4_ARRAY,
                BigQueryType::Array(BigQueryArrayType::Int64(BigQueryIntEncoding::Int32)),
            ),
            (
                Type::INT8_ARRAY,
                BigQueryType::Array(BigQueryArrayType::Int64(BigQueryIntEncoding::Int64)),
            ),
            (
                Type::OID_ARRAY,
                BigQueryType::Array(BigQueryArrayType::Int64(BigQueryIntEncoding::Int64)),
            ),
            (
                Type::FLOAT4_ARRAY,
                BigQueryType::Array(BigQueryArrayType::Float64(BigQueryFloatEncoding::Float)),
            ),
            (
                Type::FLOAT8_ARRAY,
                BigQueryType::Array(BigQueryArrayType::Float64(BigQueryFloatEncoding::Double)),
            ),
            (Type::NUMERIC_ARRAY, BigQueryType::Array(BigQueryArrayType::BigNumeric)),
            (Type::DATE_ARRAY, BigQueryType::Array(BigQueryArrayType::Date)),
            (Type::TIME_ARRAY, BigQueryType::Array(BigQueryArrayType::Time)),
            (Type::TIMESTAMP_ARRAY, BigQueryType::Array(BigQueryArrayType::DateTime)),
            (Type::TIMESTAMPTZ_ARRAY, BigQueryType::Array(BigQueryArrayType::Timestamp)),
            (Type::JSON_ARRAY, BigQueryType::Array(BigQueryArrayType::Json)),
            (Type::JSONB_ARRAY, BigQueryType::Array(BigQueryArrayType::Json)),
            (Type::BYTEA_ARRAY, BigQueryType::Array(BigQueryArrayType::Bytes)),
        ] {
            assert_eq!(
                materializer.materialize_type(&typ).expect("native type should materialize"),
                expected_type
            );
        }
    }

    #[test]
    fn native_only_type_strategy_rejects_bigquery_non_native_types() {
        let policy = DestinationMaterializationPolicy::native_only_reject();
        let materializer = materializer(policy);

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
    fn normalize_type_mapping_uses_string_for_bigquery_non_native_types() {
        let policy = DestinationMaterializationPolicy::native_or_string_normalize();
        let materializer = materializer(policy);

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
    fn string_materialized_types_return_text_carriers_for_strategy_pairs() {
        let cases = [
            (
                DestinationMaterializationPolicy::string_if_risky_preserve(),
                Type::NUMERIC,
                Cell::Null,
            ),
            (DestinationMaterializationPolicy::string_if_risky_preserve(), Type::JSON, Cell::Null),
            (DestinationMaterializationPolicy::string_if_risky_preserve(), Type::DATE, Cell::Null),
            (
                DestinationMaterializationPolicy::native_or_string_normalize(),
                Type::MONEY,
                Cell::String("$1.00".to_owned()),
            ),
            (
                DestinationMaterializationPolicy::string_if_risky_preserve(),
                Type::INET,
                Cell::String("127.0.0.1".to_owned()),
            ),
            (
                DestinationMaterializationPolicy::native_or_string_normalize(),
                Type::BIT,
                Cell::String("1010".to_owned()),
            ),
        ];

        for (policy, typ, cell) in cases {
            let (materialized_type, _) = materialized_type_and_cell(policy, typ, cell).unwrap();
            assert_eq!(materialized_type, BigQueryType::String);
        }
    }

    #[test]
    fn native_bigquery_types_are_separate_from_storage_cells() {
        let (typ, cell) = materialized_type_and_cell(
            DestinationMaterializationPolicy::native_only_reject(),
            Type::JSON,
            Cell::String(r#"{"value":1}"#.to_owned()),
        )
        .unwrap();
        assert_eq!(typ, BigQueryType::Json);
        assert_eq!(cell, BigQueryCell::String(r#"{"value":1}"#.to_owned()));

        let (typ, cell) = materialized_type_and_cell(
            DestinationMaterializationPolicy::native_only_reject(),
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
            (DestinationMaterializationPolicy::native_only_reject(), Type::INT4, Cell::I32(1)),
            (
                DestinationMaterializationPolicy::native_only_reject(),
                Type::NUMERIC,
                Cell::Numeric(PgNumeric::from_str("1.23").unwrap()),
            ),
            (
                DestinationMaterializationPolicy::native_only_reject(),
                Type::JSON,
                Cell::String(r#"{"value":1}"#.to_owned()),
            ),
            (
                DestinationMaterializationPolicy::native_only_reject(),
                Type::INT4_ARRAY,
                Cell::Array(ArrayCell::I32(vec![Some(1), Some(2)])),
            ),
            (
                DestinationMaterializationPolicy::string_if_risky_preserve(),
                Type::INT4_ARRAY,
                Cell::Array(ArrayCell::I32(vec![Some(1), None])),
            ),
            (
                DestinationMaterializationPolicy::native_or_string_reject(),
                Type::INT4_ARRAY,
                Cell::Array(ArrayCell::I32(vec![Some(1), Some(2)])),
            ),
            (
                DestinationMaterializationPolicy::string_if_risky_preserve(),
                Type::NUMERIC,
                Cell::Numeric(PgNumeric::from_str("1.23").unwrap()),
            ),
            (
                DestinationMaterializationPolicy::native_or_string_normalize(),
                Type::NUMERIC,
                Cell::Numeric(PgNumeric::from_str("1.23").unwrap()),
            ),
            (
                DestinationMaterializationPolicy::native_or_string_normalize(),
                Type::DATE,
                Cell::Date(PgDate::Finite(bigquery_max_date())),
            ),
            (
                DestinationMaterializationPolicy::native_or_string_normalize(),
                Type::UUID,
                Cell::Uuid(uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()),
            ),
            (
                DestinationMaterializationPolicy::native_or_string_reject(),
                Type::UUID_ARRAY,
                Cell::Array(ArrayCell::Uuid(vec![Some(
                    uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
                )])),
            ),
            (
                DestinationMaterializationPolicy::native_or_string_normalize(),
                Type::UUID_ARRAY,
                Cell::Array(ArrayCell::Uuid(vec![Some(
                    uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
                )])),
            ),
            (
                DestinationMaterializationPolicy::new(
                    TypeStrategy::NativeOrString,
                    ValueStrategy::Preserve,
                ),
                Type::UUID,
                Cell::Uuid(uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()),
            ),
            (
                DestinationMaterializationPolicy::new(
                    TypeStrategy::NativeOrString,
                    ValueStrategy::Preserve,
                ),
                Type::UUID_ARRAY,
                Cell::Array(ArrayCell::Uuid(vec![Some(
                    uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
                )])),
            ),
            (
                DestinationMaterializationPolicy::new(
                    TypeStrategy::StringIfRisky,
                    ValueStrategy::Normalize,
                ),
                Type::JSON,
                Cell::String(r#"{"value":18446744073709551616}"#.to_owned()),
            ),
        ];

        for (policy, typ, cell) in cases {
            let materializer = materializer(policy);
            let materialized_schema_type =
                materializer.materialize_type(&typ).expect("type should materialize");
            let (materialized_cell_type, _) =
                materialized_type_and_cell(policy, typ, cell).expect("cell should materialize");

            assert_eq!(materialized_cell_type, materialized_schema_type);
        }
    }

    #[test]
    fn value_strategy_uses_the_materialized_type() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let native_or_string_preserve = DestinationMaterializationPolicy::new(
            TypeStrategy::NativeOrString,
            ValueStrategy::Preserve,
        );

        assert_eq!(
            materialized_type_and_cell(native_or_string_preserve, Type::UUID, Cell::Uuid(uuid))
                .unwrap(),
            (BigQueryType::String, BigQueryCell::String(uuid.to_string()))
        );

        assert_eq!(
            materialized_type_and_cell(
                native_or_string_preserve,
                Type::UUID_ARRAY,
                Cell::Array(ArrayCell::Uuid(vec![Some(uuid)])),
            )
            .unwrap(),
            (
                BigQueryType::Array(BigQueryArrayType::String),
                BigQueryCell::Array(BigQueryArrayCell::String(vec![uuid.to_string()])),
            )
        );

        let json = r#"{"value":18446744073709551616}"#.to_owned();
        let string_if_risky_normalize = DestinationMaterializationPolicy::new(
            TypeStrategy::StringIfRisky,
            ValueStrategy::Normalize,
        );

        assert_eq!(
            materialized_type_and_cell(
                string_if_risky_normalize,
                Type::JSON,
                Cell::String(json.clone())
            )
            .unwrap(),
            (BigQueryType::String, BigQueryCell::String(json))
        );
    }

    #[test]
    fn json_preserve_uses_the_selected_bigquery_type() {
        let json = r#"{"outer":{"value":1,"value":2}}"#.to_owned();

        assert_eq!(
            materialized_type_and_cell(
                DestinationMaterializationPolicy::new(
                    TypeStrategy::NativeOrString,
                    ValueStrategy::Preserve,
                ),
                Type::JSON,
                Cell::String(json.clone()),
            )
            .unwrap(),
            (BigQueryType::Json, BigQueryCell::String(json.clone()))
        );

        assert_eq!(
            materialized_type_and_cell(
                DestinationMaterializationPolicy::new(
                    TypeStrategy::StringIfRisky,
                    ValueStrategy::Preserve,
                ),
                Type::JSON,
                Cell::String(json.clone()),
            )
            .unwrap(),
            (BigQueryType::String, BigQueryCell::String(json))
        );
    }

    #[test]
    fn strategy_pairs_materialize_risky_values_by_policy() {
        let tiny_numeric = "0.000000000000000000000000000000000000001";
        let rounded_tiny_numeric = "0";

        for (typ, cell, preserve_cell, normalize_type, normalize_cell) in [
            (
                Type::NUMERIC,
                Cell::Numeric(PgNumeric::from_str(tiny_numeric).unwrap()),
                BigQueryCell::String(tiny_numeric.to_owned()),
                BigQueryType::BigNumeric,
                BigQueryCell::String(rounded_tiny_numeric.to_owned()),
            ),
            (
                Type::NUMERIC,
                Cell::Numeric(PgNumeric::PositiveInfinity),
                BigQueryCell::String("Infinity".to_owned()),
                BigQueryType::BigNumeric,
                BigQueryCell::String(BIGQUERY_BIGNUMERIC_MAX.to_owned()),
            ),
            (
                Type::FLOAT8,
                Cell::F64(-0.0),
                BigQueryCell::String("-0".to_owned()),
                BigQueryType::Float64(BigQueryFloatEncoding::Double),
                BigQueryCell::Float64(0.0),
            ),
            (
                Type::DATE,
                Cell::Date(NaiveDate::from_ymd_opt(0, 12, 31).unwrap().into()),
                BigQueryCell::String("0000-12-31".to_owned()),
                BigQueryType::Date,
                BigQueryCell::String("0001-01-01".to_owned()),
            ),
        ] {
            assert!(
                materializer(DestinationMaterializationPolicy::native_only_reject())
                    .materialize_cell(typed_cell(typ.clone(), cell.clone()))
                    .is_err(),
                "native_only reject should reject risky {} values",
                typ.name(),
            );
            assert!(
                materializer(DestinationMaterializationPolicy::native_or_string_reject())
                    .materialize_cell(typed_cell(typ.clone(), cell.clone()))
                    .is_err(),
                "native_or_string reject should reject risky {} values",
                typ.name(),
            );
            assert_eq!(
                materialized_type_and_cell(
                    DestinationMaterializationPolicy::string_if_risky_preserve(),
                    typ.clone(),
                    cell.clone()
                )
                .unwrap(),
                (BigQueryType::String, preserve_cell),
                "preserve should store exact {} text",
                typ.name(),
            );
            assert_eq!(
                materialized_type_and_cell(
                    DestinationMaterializationPolicy::native_or_string_normalize(),
                    typ,
                    cell
                )
                .unwrap(),
                (normalize_type, normalize_cell),
                "normalize should apply the documented value change",
            );
        }

        let json_cell = Cell::String(r#"{"value":18446744073709551616}"#.to_owned());
        assert!(
            materializer(DestinationMaterializationPolicy::native_only_reject())
                .materialize_cell(typed_cell(Type::JSON, json_cell.clone()))
                .is_err()
        );
        assert!(
            materializer(DestinationMaterializationPolicy::native_or_string_reject())
                .materialize_cell(typed_cell(Type::JSON, json_cell.clone()))
                .is_err()
        );
        assert_eq!(
            materialized_type_and_cell(
                DestinationMaterializationPolicy::string_if_risky_preserve(),
                Type::JSON,
                json_cell.clone()
            )
            .unwrap(),
            (
                BigQueryType::String,
                BigQueryCell::String(r#"{"value":18446744073709551616}"#.to_owned())
            )
        );

        let (normalized_json_type, normalized_json_cell) = materialized_type_and_cell(
            DestinationMaterializationPolicy::native_or_string_normalize(),
            Type::JSON,
            json_cell,
        )
        .unwrap();
        assert_eq!(normalized_json_type, BigQueryType::Json);
        let BigQueryCell::String(normalized_json) = normalized_json_cell else {
            panic!("normalized JSON should be string-backed for BigQuery writes");
        };
        let normalized_json: serde_json::Value = serde_json::from_str(&normalized_json).unwrap();
        assert_eq!(normalized_json["value"].as_f64().unwrap(), 18_446_744_073_709_552_000.0);

        let numeric_array =
            Cell::Array(ArrayCell::Numeric(vec![Some(PgNumeric::from_str(tiny_numeric).unwrap())]));
        assert!(
            materializer(DestinationMaterializationPolicy::native_only_reject())
                .materialize_cell(typed_cell(Type::NUMERIC_ARRAY, numeric_array.clone()))
                .is_err()
        );
        assert!(
            materializer(DestinationMaterializationPolicy::native_or_string_reject())
                .materialize_cell(typed_cell(Type::NUMERIC_ARRAY, numeric_array.clone()))
                .is_err()
        );
        assert_eq!(
            materialized_type_and_cell(
                DestinationMaterializationPolicy::string_if_risky_preserve(),
                Type::NUMERIC_ARRAY,
                numeric_array.clone()
            )
            .unwrap(),
            (BigQueryType::String, BigQueryCell::String(format!("{{{tiny_numeric}}}")))
        );
        assert_eq!(
            materialized_type_and_cell(
                DestinationMaterializationPolicy::native_or_string_normalize(),
                Type::NUMERIC_ARRAY,
                numeric_array
            )
            .unwrap(),
            (
                BigQueryType::Array(BigQueryArrayType::BigNumeric),
                BigQueryCell::Array(BigQueryArrayCell::String(vec![
                    rounded_tiny_numeric.to_owned()
                ]))
            )
        );
    }

    #[test]
    fn preserve_float_infinities_uses_postgres_text_spelling() {
        let policy = DestinationMaterializationPolicy::string_if_risky_preserve();

        assert_eq!(
            materialized_cell(policy, Type::FLOAT4, Cell::F32(f32::INFINITY)).unwrap(),
            BigQueryCell::String("Infinity".to_owned())
        );
        assert_eq!(
            materialized_cell(policy, Type::FLOAT8, Cell::F64(f64::NEG_INFINITY)).unwrap(),
            BigQueryCell::String("-Infinity".to_owned())
        );
        assert_eq!(
            materialized_cell(
                policy,
                Type::FLOAT8_ARRAY,
                Cell::Array(ArrayCell::F64(vec![
                    Some(f64::INFINITY),
                    Some(f64::NEG_INFINITY),
                    Some(f64::NAN),
                ])),
            )
            .unwrap(),
            BigQueryCell::String("{Infinity,-Infinity,NaN}".to_owned())
        );
    }

    #[test]
    fn strategy_pairs_handle_uuid_values() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();

        assert!(
            materializer(DestinationMaterializationPolicy::native_only_reject())
                .materialize_cell(typed_cell(Type::UUID, Cell::Uuid(uuid)))
                .is_err()
        );
        assert_eq!(
            materialized_cell(
                DestinationMaterializationPolicy::native_or_string_reject(),
                Type::UUID,
                Cell::Uuid(uuid)
            )
            .unwrap(),
            BigQueryCell::String(uuid.to_string())
        );
        assert_eq!(
            materialized_cell(
                DestinationMaterializationPolicy::string_if_risky_preserve(),
                Type::UUID,
                Cell::Uuid(uuid)
            )
            .unwrap(),
            BigQueryCell::String(uuid.to_string())
        );
        assert_eq!(
            materialized_cell(
                DestinationMaterializationPolicy::native_or_string_normalize(),
                Type::UUID,
                Cell::Uuid(uuid)
            )
            .unwrap(),
            BigQueryCell::String(uuid.to_string())
        );
    }

    #[test]
    fn strategy_pairs_handle_uuid_array_values() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let cell = Cell::Array(ArrayCell::Uuid(vec![Some(uuid)]));

        assert!(
            materializer(DestinationMaterializationPolicy::native_only_reject())
                .materialize_cell(typed_cell(Type::UUID_ARRAY, cell.clone()))
                .is_err()
        );
        assert_eq!(
            materialized_cell(
                DestinationMaterializationPolicy::native_or_string_reject(),
                Type::UUID_ARRAY,
                cell.clone()
            )
            .unwrap(),
            BigQueryCell::Array(BigQueryArrayCell::String(vec![uuid.to_string()]))
        );
        assert_eq!(
            materialized_cell(
                DestinationMaterializationPolicy::string_if_risky_preserve(),
                Type::UUID_ARRAY,
                cell.clone()
            )
            .unwrap(),
            BigQueryCell::String(format!("{{{uuid}}}"))
        );
        assert_eq!(
            materialized_cell(
                DestinationMaterializationPolicy::native_or_string_normalize(),
                Type::UUID_ARRAY,
                cell
            )
            .unwrap(),
            BigQueryCell::Array(BigQueryArrayCell::String(vec![uuid.to_string()]))
        );
    }

    #[test]
    fn native_or_string_strategies_keep_non_native_arrays_as_repeated_strings() {
        for (typ, values) in [
            (Type::MONEY_ARRAY, vec!["$1.00".to_owned(), "-$0.01".to_owned()]),
            (Type::INTERVAL_ARRAY, vec!["1 day".to_owned(), "2 hours".to_owned()]),
            (Type::INET_ARRAY, vec!["127.0.0.1".to_owned(), "192.168.0.1".to_owned()]),
            (Type::INT4_RANGE_ARRAY, vec!["[1,2)".to_owned(), "[3,4)".to_owned()]),
        ] {
            let cell = Cell::Array(ArrayCell::String(values.iter().cloned().map(Some).collect()));

            for policy in [
                DestinationMaterializationPolicy::native_or_string_reject(),
                DestinationMaterializationPolicy::native_or_string_normalize(),
            ] {
                let (materialized_type, materialized_cell) =
                    materialized_type_and_cell(policy, typ.clone(), cell.clone()).unwrap();

                assert_eq!(materialized_type, BigQueryType::Array(BigQueryArrayType::String));
                assert_eq!(
                    materialized_cell,
                    BigQueryCell::Array(BigQueryArrayCell::String(values.clone()))
                );
            }
        }
    }

    #[test]
    fn normalize_numeric_rounds_and_clamps() {
        let rounded = normalize_bigquery_numeric(
            PgNumeric::from_str("0.123456789012345678901234567890123456789").unwrap(),
        )
        .unwrap();
        assert_eq!(rounded.to_string(), "0.12345678901234567890123456789012345679");

        let clamped = normalize_bigquery_numeric(PgNumeric::PositiveInfinity).unwrap();
        assert_eq!(clamped.to_string(), BIGQUERY_BIGNUMERIC_MAX);

        let zero = normalize_bigquery_numeric(
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
    fn normalize_numeric_roundtrip_stays_inside_bigquery_bignumeric_range()
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
            let materialized = normalize_bigquery_numeric(PgNumeric::from_str(value)?)
                .map_err(std::io::Error::other)?;
            let roundtripped = PgNumeric::from_str(&materialized.to_string())?;
            assert!(bigquery_bignumeric_in_range(&roundtripped));
        }

        for value in [PgNumeric::PositiveInfinity, PgNumeric::NegativeInfinity] {
            let materialized = normalize_bigquery_numeric(value).map_err(std::io::Error::other)?;
            let roundtripped = PgNumeric::from_str(&materialized.to_string())?;
            assert!(bigquery_bignumeric_in_range(&roundtripped));
        }

        Ok(())
    }

    #[test]
    fn json_depth_exceeding_bigquery_limit_is_rejected_unless_preserve() {
        let cell = Cell::String(nested_json(BIGQUERY_JSON_MAX_NESTING_DEPTH + 1).to_string());

        assert!(
            materializer(DestinationMaterializationPolicy::native_only_reject())
                .materialize_cell(typed_cell(Type::JSON, cell.clone()))
                .is_err()
        );
        assert!(matches!(
            materialized_cell(
                DestinationMaterializationPolicy::string_if_risky_preserve(),
                Type::JSON,
                cell.clone()
            )
            .unwrap(),
            BigQueryCell::String(_)
        ));
        assert!(
            materializer(DestinationMaterializationPolicy::native_or_string_reject())
                .materialize_cell(typed_cell(Type::JSON, cell))
                .is_err()
        );
    }

    #[test]
    fn json_depth_at_bigquery_limit_is_accepted() {
        let cell = Cell::String(nested_json(BIGQUERY_JSON_MAX_NESTING_DEPTH).to_string());

        assert!(
            materializer(DestinationMaterializationPolicy::native_only_reject())
                .materialize_cell(typed_cell(Type::JSON, cell.clone()))
                .is_ok()
        );
        assert!(
            materializer(DestinationMaterializationPolicy::native_or_string_normalize())
                .materialize_cell(typed_cell(Type::JSON, cell))
                .is_ok()
        );
    }

    #[test]
    fn reject_and_normalize_canonicalize_duplicate_json_keys() {
        let cell = Cell::String(r#"{"outer":{"value":1,"value":2}}"#.to_owned());

        let result = materializer(DestinationMaterializationPolicy::native_only_reject())
            .materialize_cell(typed_cell(Type::JSON, cell.clone()));
        assert!(matches!(
            result,
            Err(err) if err.kind() == ErrorKind::UnsupportedValueInDestination
        ));
        assert!(matches!(
            materialized_cell(
                DestinationMaterializationPolicy::string_if_risky_preserve(),
                Type::JSON,
                cell
            )
            .unwrap(),
            BigQueryCell::String(_)
        ));

        let result = materialized_cell(
            DestinationMaterializationPolicy::native_or_string_normalize(),
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
    fn normalize_json_preserves_private_number_token_object_keys() {
        let result = materialized_cell(
            DestinationMaterializationPolicy::native_or_string_normalize(),
            Type::JSON,
            Cell::String(r#"{"$serde_json::private::Number":"123","keep":true}"#.to_owned()),
        )
        .unwrap();
        let BigQueryCell::String(value) = result else {
            panic!("Expected JSON object");
        };
        serde_json::from_str::<Box<RawValue>>(&value).unwrap();
        assert_eq!(value, r#"{"$serde_json::private::Number":"123","keep":true}"#);

        let result = materialized_cell(
            DestinationMaterializationPolicy::native_or_string_normalize(),
            Type::JSON,
            Cell::String(r#"{"$serde_json::private::Number":"456"}"#.to_owned()),
        )
        .unwrap();
        let BigQueryCell::String(value) = result else {
            panic!("Expected JSON object");
        };
        serde_json::from_str::<Box<RawValue>>(&value).unwrap();
        assert_eq!(value, r#"{"$serde_json::private::Number":"456"}"#);
    }

    #[test]
    fn normalize_json_rounds_wide_numbers_to_bigquery_float_domain() {
        let cell = Cell::String(r#"{"value":922337203685477580701}"#.to_owned());
        let result = materialized_cell(
            DestinationMaterializationPolicy::native_or_string_normalize(),
            Type::JSON,
            cell,
        )
        .unwrap();

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
    fn normalize_json_rejects_numbers_outside_bigquery_float_domain() {
        let cell = Cell::String(r#"{"value":1e309}"#.to_owned());

        let result = materializer(DestinationMaterializationPolicy::native_or_string_normalize())
            .materialize_cell(typed_cell(Type::JSON, cell));

        assert!(matches!(
            result,
            Err(err) if err.kind() == ErrorKind::UnsupportedValueInDestination
                && err.detail().is_some_and(|detail| detail.contains("cannot be rounded"))
        ));
    }

    #[test]
    fn reject_json_arrays_validate_string_elements_using_selected_type() {
        let cell = Cell::Array(ArrayCell::String(vec![Some(
            r#"{"value":18446744073709551616}"#.to_owned(),
        )]));

        let result = materializer(DestinationMaterializationPolicy::native_only_reject())
            .materialize_cell(typed_cell(Type::JSON_ARRAY, cell));

        assert!(matches!(
            result,
            Err(err) if err.kind() == ErrorKind::UnsupportedValueInDestination
                && err.detail().is_some_and(|detail| detail.contains("Element at index 0"))
        ));

        let cell = Cell::Array(ArrayCell::String(vec![Some(r#"{"value":1}"#.to_owned())]));
        let result = materialized_cell(
            DestinationMaterializationPolicy::native_only_reject(),
            Type::JSON_ARRAY,
            cell,
        )
        .unwrap();

        assert_eq!(
            result,
            BigQueryCell::Array(BigQueryArrayCell::String(vec![r#"{"value":1}"#.to_owned()]))
        );
    }

    #[test]
    fn normalize_json_arrays_normalize_string_elements_using_selected_type() {
        let cell =
            Cell::Array(ArrayCell::String(vec![Some(r#"{"value":1,"value":2}"#.to_owned())]));

        let result = materialized_cell(
            DestinationMaterializationPolicy::native_or_string_normalize(),
            Type::JSON_ARRAY,
            cell,
        )
        .unwrap();

        assert_eq!(
            result,
            BigQueryCell::Array(BigQueryArrayCell::String(vec![r#"{"value":1}"#.to_owned()]))
        );
    }

    #[test]
    fn reject_temporal_arrays_validate_postgres_temporal_elements() {
        let cell = Cell::Array(ArrayCell::Date(vec![
            Some(PgDate::Finite(bigquery_min_date())),
            Some(PgDate::PosInfinity),
        ]));

        let result = materializer(DestinationMaterializationPolicy::native_only_reject())
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
            materializer(DestinationMaterializationPolicy::native_only_reject())
                .materialize_cell(typed_cell(Type::DATE, cell.clone()))
                .is_err()
        );
        assert_eq!(
            materialized_cell(
                DestinationMaterializationPolicy::string_if_risky_preserve(),
                Type::DATE,
                cell.clone()
            )
            .unwrap(),
            BigQueryCell::String("infinity".to_owned())
        );
        assert_eq!(
            materialized_cell(
                DestinationMaterializationPolicy::native_or_string_normalize(),
                Type::DATE,
                cell
            )
            .unwrap(),
            BigQueryCell::String(PgDate::Finite(bigquery_max_date()).to_string())
        );

        assert_eq!(
            materialized_cell(
                DestinationMaterializationPolicy::native_or_string_normalize(),
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

        for policy in [
            DestinationMaterializationPolicy::native_only_reject(),
            DestinationMaterializationPolicy::native_or_string_reject(),
            DestinationMaterializationPolicy::native_or_string_normalize(),
        ] {
            let result =
                materializer(policy).materialize_cell(typed_cell(Type::INT4_ARRAY, cell.clone()));
            assert!(result.is_err());
            assert_eq!(
                result.unwrap_err().kind(),
                ErrorKind::NullValuesNotSupportedInArrayInDestination
            );
        }

        assert_eq!(
            materialized_cell(
                DestinationMaterializationPolicy::string_if_risky_preserve(),
                Type::INT4_ARRAY,
                cell
            )
            .unwrap(),
            BigQueryCell::String("{1,NULL}".to_owned())
        );
    }

    #[test]
    fn normalize_native_arrays_stay_repeated_fields() {
        let cell = Cell::Array(ArrayCell::Numeric(vec![Some(
            PgNumeric::from_str("0.123456789012345678901234567890123456789").unwrap(),
        )]));

        let (typ, cell) = materialized_type_and_cell(
            DestinationMaterializationPolicy::native_or_string_normalize(),
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
        let result = materialized_cell(
            DestinationMaterializationPolicy::string_if_risky_preserve(),
            Type::NUMERIC_ARRAY,
            cell,
        )
        .unwrap();

        assert_eq!(result, BigQueryCell::String("{1.23}".to_owned()));
    }

    #[test]
    fn repeated_fields_reject_null_array_cells() {
        let result = materializer(DestinationMaterializationPolicy::native_only_reject())
            .materialize_cell(typed_cell(Type::INT4_ARRAY, Cell::Null));

        assert!(matches!(
            result,
            Err(err) if err.kind() == ErrorKind::UnsupportedValueInDestination
        ));
    }
}
