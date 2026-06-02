use etl::{
    bail,
    error::{ErrorKind, EtlResult},
    etl_error,
    types::{ArrayCellNonOptional, CellNonOptional, PgNumeric},
};

/// BigQuery BIGNUMERIC maximum decimal places.
///
/// Source: <https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types>.
const BIGQUERY_BIGNUMERIC_MAX_SCALE: usize = 38;

/// Validates that a [`PgNumeric`] value will not be silently rounded by
/// BigQuery BIGNUMERIC.
///
/// BigQuery reports out-of-range BIGNUMERIC values as write errors, but values
/// with more than 38 fractional digits can be rounded on write. We validate
/// only that corruption-prone case locally and let BigQuery reject values that
/// are outside its domain.
fn validate_numeric_for_bigquery(numeric: &PgNumeric) -> EtlResult<()> {
    if matches!(numeric, PgNumeric::Value { .. })
        && bigquery_bignumeric_scale(numeric) > BIGQUERY_BIGNUMERIC_MAX_SCALE
    {
        bail!(
            ErrorKind::UnsupportedValueInDestination,
            "Numeric value would be rounded by BigQuery BIGNUMERIC",
            format!(
                "A numeric value has more than {} decimal places and would be rounded by BigQuery",
                BIGQUERY_BIGNUMERIC_MAX_SCALE
            )
        );
    }

    Ok(())
}

/// Validates JSON values only when BigQuery may otherwise round them.
///
/// BigQuery rejects some JSON numbers outside its domain, such as `1e309`, but
/// the Storage Write path can accept integer literals outside the signed or
/// unsigned 64-bit JSON integer domain by storing them as `FLOAT64`. That loses
/// integer precision, so we reject only that corruption-prone case locally.
///
/// This walks the full JSON tree, which can be expensive for large JSON values.
/// That cost is intentional: without this check, BigQuery can silently round
/// JSON integers and corrupt downstream data due to automatic precision
/// differences between PostgreSQL JSONB numbers and BigQuery JSON numbers.
fn validate_json_for_bigquery(json: &serde_json::Value) -> EtlResult<()> {
    let mut values = vec![json];

    while let Some(value) = values.pop() {
        match value {
            serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::String(_) => {
            }
            serde_json::Value::Number(number) => validate_json_number_for_bigquery(number)?,
            serde_json::Value::Array(items) => values.extend(items),
            serde_json::Value::Object(entries) => values.extend(entries.values()),
        }
    }

    Ok(())
}

/// Validates that a JSON number will not be silently rounded by BigQuery.
fn validate_json_number_for_bigquery(number: &serde_json::Number) -> EtlResult<()> {
    let number = number.as_str();
    let integer_outside_exact_domain = is_json_integer_literal(number)
        && if number.starts_with('-') {
            number.parse::<i64>().is_err()
        } else {
            number.parse::<u64>().is_err()
        };

    if integer_outside_exact_domain {
        bail!(
            ErrorKind::UnsupportedValueInDestination,
            "JSON integer would lose precision in BigQuery",
            "A JSON integer is outside BigQuery's exact signed or unsigned 64-bit integer domain \
             and may be stored as FLOAT64"
        );
    }

    Ok(())
}

/// Returns whether a JSON number literal is an integer literal.
fn is_json_integer_literal(number: &str) -> bool {
    !number.contains(['.', 'e', 'E'])
}

/// Validates values only when BigQuery could silently change them on write.
///
/// Destination-domain checks that BigQuery rejects with row errors are
/// intentionally delegated to BigQuery to avoid duplicating destination logic
/// in the append hot path.
pub(super) fn validate_cell_for_bigquery(cell: &CellNonOptional) -> EtlResult<()> {
    match cell {
        CellNonOptional::Null => Ok(()),
        CellNonOptional::Bool(_) => Ok(()),
        CellNonOptional::String(_) => Ok(()),
        CellNonOptional::I16(_) => Ok(()),
        CellNonOptional::I32(_) => Ok(()),
        CellNonOptional::U32(_) => Ok(()),
        CellNonOptional::I64(_) => Ok(()),
        CellNonOptional::F32(_) => Ok(()),
        CellNonOptional::F64(_) => Ok(()),
        CellNonOptional::Numeric(numeric) => validate_numeric_for_bigquery(numeric),
        CellNonOptional::Date(_) => Ok(()),
        CellNonOptional::Time(_) => Ok(()),
        CellNonOptional::Timestamp(_) => Ok(()),
        CellNonOptional::TimestampTz(_) => Ok(()),
        CellNonOptional::Uuid(_) => Ok(()),
        CellNonOptional::Json(json) => validate_json_for_bigquery(json),
        CellNonOptional::Bytes(_) => Ok(()),
        CellNonOptional::Array(array) => validate_array_cell_for_bigquery(array),
    }
}

/// Validates that an [`ArrayCellNonOptional`] contains values within BigQuery's
/// supported ranges.
///
/// Returns an error if any array element is outside BigQuery's supported range
/// for its type.
fn validate_array_cell_for_bigquery(array_cell: &ArrayCellNonOptional) -> EtlResult<()> {
    match array_cell {
        ArrayCellNonOptional::Bool(_) => Ok(()),
        ArrayCellNonOptional::String(_) => Ok(()),
        ArrayCellNonOptional::I16(_) => Ok(()),
        ArrayCellNonOptional::I32(_) => Ok(()),
        ArrayCellNonOptional::U32(_) => Ok(()),
        ArrayCellNonOptional::I64(_) => Ok(()),
        ArrayCellNonOptional::F32(_) => Ok(()),
        ArrayCellNonOptional::F64(_) => Ok(()),
        ArrayCellNonOptional::Numeric(numerics) => {
            for (index, numeric) in numerics.iter().enumerate() {
                validate_numeric_for_bigquery(numeric).map_err(|err| {
                    etl_error!(
                        err.kind(),
                        "Array element validation failed",
                        format!("Element at index {}: {}", index, err)
                    )
                })?;
            }
            Ok(())
        }
        ArrayCellNonOptional::Date(_) => Ok(()),
        ArrayCellNonOptional::Time(_) => Ok(()),
        ArrayCellNonOptional::Timestamp(_) => Ok(()),
        ArrayCellNonOptional::TimestampTz(_) => Ok(()),
        ArrayCellNonOptional::Uuid(_) => Ok(()),
        ArrayCellNonOptional::Json(values) => {
            for (index, value) in values.iter().enumerate() {
                validate_json_for_bigquery(value).map_err(|err| {
                    etl_error!(
                        err.kind(),
                        "Array element validation failed",
                        format!("Element at index {}: {}", index, err)
                    )
                })?;
            }
            Ok(())
        }
        ArrayCellNonOptional::Bytes(_) => Ok(()),
    }
}

/// Returns the number of fractional digits in a canonical numeric string.
fn bigquery_bignumeric_scale(pg_numeric: &PgNumeric) -> usize {
    pg_numeric.to_string().split_once('.').map_or(0, |(_, fractional)| fractional.len())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn validate_numeric_accepts_values_bigquery_will_reject_without_rounding() {
        for value in [
            PgNumeric::NaN,
            PgNumeric::PositiveInfinity,
            PgNumeric::NegativeInfinity,
            PgNumeric::from_str(
                "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890",
            )
            .unwrap(),
        ] {
            assert!(validate_numeric_for_bigquery(&value).is_ok(), "{value:?}");
        }
    }

    #[test]
    fn validate_numeric_accepts_bigquery_bignumeric_scale() {
        let numeric = PgNumeric::from_str("123.456").unwrap();
        assert!(validate_numeric_for_bigquery(&numeric).is_ok());

        let max_scale = PgNumeric::from_str("0.00000000000000000000000000000000000001").unwrap();
        assert!(validate_numeric_for_bigquery(&max_scale).is_ok());
    }

    #[test]
    fn validate_numeric_rejects_values_bigquery_would_round() {
        let numeric = PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap();
        let result = validate_numeric_for_bigquery(&numeric);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.detail().unwrap().contains("would be rounded by BigQuery"));
    }

    #[test]
    fn validate_cell_for_bigquery_delegates_json_domain_validation() {
        let json = serde_json::from_str(r#"{"value":1e309}"#).unwrap();
        let cell = CellNonOptional::Json(json);
        let result = validate_cell_for_bigquery(&cell);
        assert!(result.is_ok());
    }

    #[test]
    fn validate_cell_for_bigquery_rejects_json_integer_rounding_risk() {
        for value in [
            r#"{"value":-9223372036854775809}"#,
            r#"{"value":18446744073709551616}"#,
            r#"{"value":922337203685477580701}"#,
        ] {
            let json = serde_json::from_str(value).unwrap();
            let cell = CellNonOptional::Json(json);
            let result = validate_cell_for_bigquery(&cell);
            assert!(result.is_err(), "{value}");
            assert_eq!(result.unwrap_err().kind(), ErrorKind::UnsupportedValueInDestination);
        }
    }

    #[test]
    fn validate_cell_for_bigquery_accepts_json_numeric_domain_boundaries() {
        for value in [
            r#"{"value":-9223372036854775808}"#,
            r#"{"value":18446744073709551615}"#,
            r#"{"value":1.7976931348623157e308}"#,
        ] {
            let json = serde_json::from_str(value).unwrap();
            let cell = CellNonOptional::Json(json);
            assert!(validate_cell_for_bigquery(&cell).is_ok(), "{value}");
        }
    }

    #[test]
    fn validate_array_cell_with_numeric_rounding_risk() {
        let array_cell = ArrayCellNonOptional::Numeric(vec![
            PgNumeric::from_str("123.456").unwrap(),
            PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap(),
            PgNumeric::from_str("789.012").unwrap(),
        ]);

        let result = validate_array_cell_for_bigquery(&array_cell);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.detail().unwrap().contains("Element at index 1"));
    }

    #[test]
    fn validate_array_cell_with_json_integer_rounding_risk() {
        let array_cell = ArrayCellNonOptional::Json(vec![
            serde_json::from_str(r#"{"value":123}"#).unwrap(),
            serde_json::from_str(r#"{"value":18446744073709551616}"#).unwrap(),
        ]);

        let result = validate_array_cell_for_bigquery(&array_cell);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.detail().unwrap().contains("Element at index 1"));
    }
}
