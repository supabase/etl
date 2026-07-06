use etl::{
    bail,
    data::{ArrayCell, Cell, PgNumeric},
    error::{ErrorKind, EtlResult},
    etl_error,
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
pub(super) fn validate_cell_for_bigquery(cell: &Cell) -> EtlResult<()> {
    match cell {
        Cell::Null => Ok(()),
        Cell::Bool(_) => Ok(()),
        Cell::String(_) => Ok(()),
        Cell::I16(_) => Ok(()),
        Cell::I32(_) => Ok(()),
        Cell::U32(_) => Ok(()),
        Cell::I64(_) => Ok(()),
        Cell::F32(_) => Ok(()),
        Cell::F64(_) => Ok(()),
        Cell::Numeric(numeric) => validate_numeric_for_bigquery(numeric),
        Cell::Date(_) => Ok(()),
        Cell::Time(_) => Ok(()),
        Cell::TimeTz(_) => Ok(()),
        Cell::Timestamp(_) => Ok(()),
        Cell::TimestampTz(_) => Ok(()),
        Cell::Uuid(_) => Ok(()),
        Cell::Json(json) => validate_json_for_bigquery(json),
        Cell::Bytes(_) => Ok(()),
        Cell::Array(array) => validate_array_cell_for_bigquery(array),
    }
}

/// Validates that an [`ArrayCell`] has no NULL elements and contains values
/// within BigQuery's supported ranges.
///
/// BigQuery does not support NULL values within `REPEATED` fields, so this
/// rejects any array containing one. Remaining elements are validated the same
/// way as scalar cells.
fn validate_array_cell_for_bigquery(array_cell: &ArrayCell) -> EtlResult<()> {
    /// Rejects `elements` if any element is NULL.
    fn reject_nulls<T>(elements: &[Option<T>]) -> EtlResult<()> {
        let null_count = elements.iter().filter(|v| v.is_none()).count();
        if null_count > 0 {
            bail!(
                ErrorKind::NullValuesNotSupportedInArrayInDestination,
                "NULL values in arrays not supported in this destination",
                format!(
                    "Array contains {null_count} NULL values across {} elements, which are not \
                     supported in this destination",
                    elements.len()
                )
            );
        }

        Ok(())
    }

    /// Runs `validate` over each non-NULL element, tagging any failure with
    /// its index.
    fn validate_elements<T>(
        elements: &[Option<T>],
        validate: impl Fn(&T) -> EtlResult<()>,
    ) -> EtlResult<()> {
        for (index, element) in elements.iter().enumerate() {
            let Some(element) = element else { continue };
            validate(element).map_err(|err| {
                etl_error!(
                    err.kind(),
                    "Array element validation failed",
                    format!("Element at index {index}"),
                    source: err
                )
            })?;
        }

        Ok(())
    }

    match array_cell {
        ArrayCell::Bool(vec) => reject_nulls(vec),
        ArrayCell::String(vec) => reject_nulls(vec),
        ArrayCell::I16(vec) => reject_nulls(vec),
        ArrayCell::I32(vec) => reject_nulls(vec),
        ArrayCell::U32(vec) => reject_nulls(vec),
        ArrayCell::I64(vec) => reject_nulls(vec),
        ArrayCell::F32(vec) => reject_nulls(vec),
        ArrayCell::F64(vec) => reject_nulls(vec),
        ArrayCell::Numeric(vec) => {
            reject_nulls(vec)?;
            validate_elements(vec, validate_numeric_for_bigquery)
        }
        ArrayCell::Date(vec) => reject_nulls(vec),
        ArrayCell::Time(vec) => reject_nulls(vec),
        ArrayCell::TimeTz(vec) => reject_nulls(vec),
        ArrayCell::Timestamp(vec) => reject_nulls(vec),
        ArrayCell::TimestampTz(vec) => reject_nulls(vec),
        ArrayCell::Uuid(vec) => reject_nulls(vec),
        ArrayCell::Json(vec) => {
            reject_nulls(vec)?;
            validate_elements(vec, validate_json_for_bigquery)
        }
        ArrayCell::Bytes(vec) => reject_nulls(vec),
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
        let cell = Cell::Json(json);
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
            let cell = Cell::Json(json);
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
            let cell = Cell::Json(json);
            assert!(validate_cell_for_bigquery(&cell).is_ok(), "{value}");
        }
    }

    #[test]
    fn validate_array_cell_rejects_null_elements() {
        let array_cell =
            ArrayCell::String(vec![Some("test".to_owned()), None, Some("hello".to_owned())]);

        let result = validate_array_cell_for_bigquery(&array_cell);
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NullValuesNotSupportedInArrayInDestination);
        assert!(
            error.to_string().contains("NULL values in arrays not supported in this destination")
        );
        assert!(!error.to_string().contains("test"));
        assert!(!error.to_string().contains("hello"));
    }

    #[test]
    fn validate_array_cell_accepts_elements_without_nulls() {
        let array_cell = ArrayCell::I32(vec![Some(1), Some(2), Some(3)]);
        assert!(validate_array_cell_for_bigquery(&array_cell).is_ok());
    }

    #[test]
    fn validate_array_cell_with_numeric_rounding_risk() {
        let array_cell = ArrayCell::Numeric(vec![
            Some(PgNumeric::from_str("123.456").unwrap()),
            Some(PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap()),
            Some(PgNumeric::from_str("789.012").unwrap()),
        ]);

        let result = validate_array_cell_for_bigquery(&array_cell);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.detail().unwrap().contains("Element at index 1"));
    }

    #[test]
    fn validate_array_cell_with_json_integer_rounding_risk() {
        let array_cell = ArrayCell::Json(vec![
            Some(serde_json::from_str(r#"{"value":123}"#).unwrap()),
            Some(serde_json::from_str(r#"{"value":18446744073709551616}"#).unwrap()),
        ]);

        let result = validate_array_cell_for_bigquery(&array_cell);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.detail().unwrap().contains("Element at index 1"));
    }
}
