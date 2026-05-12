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
        CellNonOptional::Json(_) => Ok(()),
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
        ArrayCellNonOptional::Json(_) => Ok(()),
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
    fn validate_cell_for_bigquery_valid_types() {
        assert!(validate_cell_for_bigquery(&CellNonOptional::Null).is_ok());
        assert!(validate_cell_for_bigquery(&CellNonOptional::Bool(true)).is_ok());
        assert!(validate_cell_for_bigquery(&CellNonOptional::String("test".to_owned())).is_ok());
        assert!(validate_cell_for_bigquery(&CellNonOptional::I32(42)).is_ok());
        assert!(
            validate_cell_for_bigquery(&CellNonOptional::Json(serde_json::json!({
                "value": 18446744073709551615u64
            })))
            .is_ok()
        );
    }

    #[test]
    fn validate_cell_for_bigquery_accepts_postgres_numeric_family_boundaries() {
        for cell in [
            CellNonOptional::I16(i16::MIN),
            CellNonOptional::I16(i16::MAX),
            CellNonOptional::I32(i32::MIN),
            CellNonOptional::I32(i32::MAX),
            CellNonOptional::U32(0),
            CellNonOptional::U32(u32::MAX),
            CellNonOptional::I64(i64::MIN),
            CellNonOptional::I64(i64::MAX),
            CellNonOptional::F32(-f32::MAX),
            CellNonOptional::F32(f32::MAX),
            CellNonOptional::F32(f32::NAN),
            CellNonOptional::F32(f32::INFINITY),
            CellNonOptional::F32(f32::NEG_INFINITY),
            CellNonOptional::F64(-f64::MAX),
            CellNonOptional::F64(f64::MAX),
            CellNonOptional::F64(f64::NAN),
            CellNonOptional::F64(f64::INFINITY),
            CellNonOptional::F64(f64::NEG_INFINITY),
            CellNonOptional::Numeric(PgNumeric::NaN),
            CellNonOptional::Numeric(PgNumeric::PositiveInfinity),
            CellNonOptional::Numeric(PgNumeric::NegativeInfinity),
            CellNonOptional::Numeric(PgNumeric::from_str("0.123456789").unwrap()),
        ] {
            assert!(validate_cell_for_bigquery(&cell).is_ok(), "{cell:?}");
        }
    }

    #[test]
    fn validate_array_cell_for_bigquery_accepts_postgres_numeric_family_boundaries() {
        for array in [
            ArrayCellNonOptional::I16(vec![i16::MIN, i16::MAX]),
            ArrayCellNonOptional::I32(vec![i32::MIN, i32::MAX]),
            ArrayCellNonOptional::U32(vec![0, u32::MAX]),
            ArrayCellNonOptional::I64(vec![i64::MIN, i64::MAX]),
            ArrayCellNonOptional::F32(vec![
                -f32::MAX,
                f32::MAX,
                f32::NAN,
                f32::INFINITY,
                f32::NEG_INFINITY,
            ]),
            ArrayCellNonOptional::F64(vec![
                -f64::MAX,
                f64::MAX,
                f64::NAN,
                f64::INFINITY,
                f64::NEG_INFINITY,
            ]),
            ArrayCellNonOptional::Numeric(vec![
                PgNumeric::NaN,
                PgNumeric::PositiveInfinity,
                PgNumeric::NegativeInfinity,
                PgNumeric::from_str("0.123456789").unwrap(),
            ]),
        ] {
            assert!(validate_array_cell_for_bigquery(&array).is_ok(), "{array:?}");
        }
    }

    #[test]
    fn validate_cell_for_bigquery_rejects_numeric_rounding() {
        let cell = CellNonOptional::Numeric(
            PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap(),
        );
        let result = validate_cell_for_bigquery(&cell);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::UnsupportedValueInDestination);
    }

    #[test]
    fn validate_cell_for_bigquery_delegates_json_domain_validation() {
        let json = serde_json::from_str(r#"{"value":1e309}"#).unwrap();
        let cell = CellNonOptional::Json(json);
        let result = validate_cell_for_bigquery(&cell);
        assert!(result.is_ok());
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
}
