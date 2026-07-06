use etl::{
    data::{ArrayCell, Cell, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, TableRow},
    error::EtlError,
    etl_error,
};
use prost::bytes;

use crate::bigquery::validation::validate_cell_for_bigquery;

/// Panic message used when an array cell contains a NULL element at encode
/// time, which [`validate_cell_for_bigquery`] should have already rejected.
const UNVALIDATED_NULL_ARRAY_ELEMENT: &str =
    "array cell contains a NULL element that validate_cell_for_bigquery should have rejected";

/// Protocol buffer wrapper for a BigQuery table row, holding its Protocol
/// Buffer encoding rather than the source cells.
///
/// Formatting cells such as dates, numerics, UUIDs, and JSON into their
/// Protocol Buffer string representation is comparatively expensive, and
/// [`prost::Message::encoded_len`] and [`prost::Message::encode_raw`] are
/// each invoked at least once per row by callers that budget a batch by
/// length before encoding it. Encoding once up front, at row-construction
/// time, and keeping only the resulting bytes means formatting happens
/// exactly once per row and the source cells don't have to be kept alive
/// alongside their encoding.
#[derive(Debug)]
pub(super) struct BigQueryTableRow(Vec<u8>);

impl BigQueryTableRow {
    /// Validates tagged cells for BigQuery compatibility and encodes them
    /// into a row, preserving sparse field positions.
    pub(super) fn try_from_tagged_cells(
        tagged_cells: impl IntoIterator<Item = (usize, Cell)>,
    ) -> Result<Self, EtlError> {
        let mut buf = Vec::new();

        for (index, cell) in tagged_cells {
            validate_cell_for_bigquery(&cell).map_err(|err| {
                etl_error!(
                    err.kind(),
                    "Cell validation failed for BigQuery compatibility",
                    format!("Cell at index {} failed validation", index - 1),
                    source: err
                )
            })?;

            cell_encode_prost(&cell, index as u32, &mut buf);
        }

        Ok(BigQueryTableRow(buf))
    }
}

impl TryFrom<TableRow> for BigQueryTableRow {
    type Error = EtlError;

    /// Converts a [`TableRow`] to a [`BigQueryTableRow`] by validating every
    /// cell for BigQuery compatibility and encoding the row.
    ///
    /// Returns an error if any cell, including array elements, is outside
    /// BigQuery's supported bounds. This fails fast rather than clamping, so
    /// users are aware when their data doesn't fit BigQuery's constraints.
    fn try_from(value: TableRow) -> Result<Self, Self::Error> {
        BigQueryTableRow::try_from_tagged_cells(
            value.into_values().into_iter().enumerate().map(|(index, cell)| (index + 1, cell)),
        )
    }
}

impl prost::Message for BigQueryTableRow {
    /// Writes the table row's Protocol Buffer encoding into the provided
    /// buffer.
    fn encode_raw(&self, buf: &mut impl bytes::BufMut)
    where
        Self: Sized,
    {
        buf.put_slice(&self.0);
    }

    /// Merges a field from a Protocol Buffer message into this table row.
    ///
    /// Currently unimplemented as this functionality is not required for
    /// BigQuery streaming inserts, which only need encoding capabilities.
    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: prost::encoding::WireType,
        _buf: &mut impl bytes::Buf,
        _ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        unimplemented!("merge_field not implemented yet");
    }

    /// Returns the length of the table row's Protocol Buffer encoding.
    fn encoded_len(&self) -> usize {
        self.0.len()
    }

    /// Clears the table row's encoded bytes.
    fn clear(&mut self) {
        self.0.clear();
    }
}

/// Encodes a single [`Cell`] into Protocol Buffer format using the specified
/// tag.
///
/// Each cell type is encoded using the appropriate prost encoding method.
/// Temporal civil types and UUIDs are formatted as strings, while instant
/// timestamps and numeric types use their native encoding. Null cells produce
/// no encoded output.
///
/// # Panics
///
/// Panics if `cell` contains an array with a NULL element; callers must
/// validate the cell with [`validate_cell_for_bigquery`] first.
fn cell_encode_prost(cell: &Cell, tag: u32, buf: &mut impl bytes::BufMut) {
    match cell {
        Cell::Null => {}
        Cell::Bool(b) => {
            prost::encoding::bool::encode(tag, b, buf);
        }
        Cell::String(s) => {
            prost::encoding::string::encode(tag, s, buf);
        }
        Cell::I16(i) => {
            let val = *i as i32;
            prost::encoding::int32::encode(tag, &val, buf);
        }
        Cell::I32(i) => {
            prost::encoding::int32::encode(tag, i, buf);
        }
        Cell::I64(i) => {
            prost::encoding::int64::encode(tag, i, buf);
        }
        Cell::F32(i) => {
            prost::encoding::float::encode(tag, i, buf);
        }
        Cell::F64(i) => {
            prost::encoding::double::encode(tag, i, buf);
        }
        Cell::Numeric(n) => {
            let s = n.to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        Cell::Date(t) => {
            let s = t.format(DATE_FORMAT).to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        Cell::Time(t) => {
            let s = t.format(TIME_FORMAT).to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        Cell::TimeTz(t) => {
            let s = t.to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        Cell::Timestamp(t) => {
            let s = t.format(TIMESTAMP_FORMAT).to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        Cell::TimestampTz(t) => {
            let micros = t.timestamp_micros();
            prost::encoding::int64::encode(tag, &micros, buf);
        }
        Cell::Uuid(u) => {
            let s = u.to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        Cell::Json(j) => {
            let s = j.to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        Cell::U32(i) => {
            prost::encoding::uint32::encode(tag, i, buf);
        }
        Cell::Bytes(b) => {
            prost::encoding::bytes::encode(tag, b, buf);
        }
        Cell::Array(a) => {
            array_cell_encode_prost(a, tag, buf);
        }
    }
}

/// Encodes an [`ArrayCell`] into Protocol Buffer format using the specified
/// tag.
///
/// Array cells are encoded using either packed encoding for numeric/instant
/// timestamp types or repeated encoding for string-based types. Civil temporal
/// arrays are formatted as strings before encoding. Elements are encoded
/// directly from their `Option` slots rather than through an intermediate
/// non-nullable collection, so string- and byte-typed elements are encoded
/// without an extra clone.
///
/// # Panics
///
/// Panics if `array_cell` contains a NULL element; callers must validate the
/// cell with [`validate_cell_for_bigquery`] first.
fn array_cell_encode_prost(array_cell: &ArrayCell, tag: u32, buf: &mut impl bytes::BufMut) {
    /// Returns the element, panicking if it is a NULL that validation should
    /// have already rejected.
    fn unwrap<T>(value: &Option<T>) -> &T {
        value.as_ref().expect(UNVALIDATED_NULL_ARRAY_ELEMENT)
    }

    match array_cell {
        ArrayCell::Bool(vec) => {
            let values: Vec<bool> = vec.iter().map(|v| *unwrap(v)).collect();
            prost::encoding::bool::encode_packed(tag, &values, buf);
        }
        ArrayCell::String(vec) => {
            for value in vec {
                prost::encoding::string::encode(tag, unwrap(value), buf);
            }
        }
        ArrayCell::I16(vec) => {
            let values: Vec<i32> = vec.iter().map(|v| *unwrap(v) as i32).collect();
            prost::encoding::int32::encode_packed(tag, &values, buf);
        }
        ArrayCell::I32(vec) => {
            let values: Vec<i32> = vec.iter().map(|v| *unwrap(v)).collect();
            prost::encoding::int32::encode_packed(tag, &values, buf);
        }
        ArrayCell::U32(vec) => {
            let values: Vec<u32> = vec.iter().map(|v| *unwrap(v)).collect();
            prost::encoding::uint32::encode_packed(tag, &values, buf);
        }
        ArrayCell::I64(vec) => {
            let values: Vec<i64> = vec.iter().map(|v| *unwrap(v)).collect();
            prost::encoding::int64::encode_packed(tag, &values, buf);
        }
        ArrayCell::F32(vec) => {
            let values: Vec<f32> = vec.iter().map(|v| *unwrap(v)).collect();
            prost::encoding::float::encode_packed(tag, &values, buf);
        }
        ArrayCell::F64(vec) => {
            let values: Vec<f64> = vec.iter().map(|v| *unwrap(v)).collect();
            prost::encoding::double::encode_packed(tag, &values, buf);
        }
        ArrayCell::Numeric(vec) => {
            for value in vec {
                let s = unwrap(value).to_string();
                prost::encoding::string::encode(tag, &s, buf);
            }
        }
        ArrayCell::Date(vec) => {
            for value in vec {
                let s = unwrap(value).format(DATE_FORMAT).to_string();
                prost::encoding::string::encode(tag, &s, buf);
            }
        }
        ArrayCell::Time(vec) => {
            for value in vec {
                let s = unwrap(value).format(TIME_FORMAT).to_string();
                prost::encoding::string::encode(tag, &s, buf);
            }
        }
        ArrayCell::TimeTz(vec) => {
            for value in vec {
                let s = unwrap(value).to_string();
                prost::encoding::string::encode(tag, &s, buf);
            }
        }
        ArrayCell::Timestamp(vec) => {
            for value in vec {
                let s = unwrap(value).format(TIMESTAMP_FORMAT).to_string();
                prost::encoding::string::encode(tag, &s, buf);
            }
        }
        ArrayCell::TimestampTz(vec) => {
            let values: Vec<i64> = vec.iter().map(|v| unwrap(v).timestamp_micros()).collect();
            prost::encoding::int64::encode_packed(tag, &values, buf);
        }
        ArrayCell::Uuid(vec) => {
            for value in vec {
                let s = unwrap(value).to_string();
                prost::encoding::string::encode(tag, &s, buf);
            }
        }
        ArrayCell::Json(vec) => {
            for value in vec {
                let s = unwrap(value).to_string();
                prost::encoding::string::encode(tag, &s, buf);
            }
        }
        ArrayCell::Bytes(vec) => {
            for value in vec {
                prost::encoding::bytes::encode(tag, unwrap(value), buf);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
    use etl::{
        data::{Cell, PgNumeric},
        error::ErrorKind,
    };
    use prost::Message;

    use super::*;

    #[test]
    fn bigquery_table_row_try_from_valid() {
        let table_row = TableRow::new(vec![
            Cell::I32(42),
            Cell::String("test".to_owned()),
            Cell::Bool(true),
            Cell::Null,
        ]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_ok());
    }

    #[test]
    fn bigquery_table_row_try_from_delegates_numeric_nan_validation_to_bigquery() {
        let table_row = TableRow::new(vec![Cell::I32(42), Cell::Numeric(PgNumeric::NaN)]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_ok());
    }

    #[test]
    fn bigquery_table_row_try_from_delegates_numeric_infinity_validation_to_bigquery() {
        let table_row = TableRow::new(vec![
            Cell::String("valid".to_owned()),
            Cell::Numeric(PgNumeric::PositiveInfinity),
        ]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_ok());
    }

    #[test]
    fn bigquery_table_row_try_from_delegates_json_number_validation_to_bigquery() {
        let json = serde_json::from_str(r#"{"value":1e309}"#).unwrap();
        let table_row = TableRow::new(vec![Cell::Json(json)]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_ok());
    }

    #[test]
    fn bigquery_table_row_try_from_rejects_json_integer_precision_loss() {
        let json = serde_json::from_str(r#"{"value":18446744073709551616}"#).unwrap();
        let table_row = TableRow::new(vec![Cell::Json(json)]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_err());
    }

    #[test]
    fn bigquery_table_row_try_from_delegates_date_domain_validation_to_bigquery() {
        let invalid_date = NaiveDate::from_ymd_opt(1, 1, 1).unwrap().pred_opt().unwrap(); // Date before year 1

        let table_row = TableRow::new(vec![Cell::Date(invalid_date)]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_ok());
    }

    #[test]
    fn bigquery_table_row_try_from_array_with_nulls() {
        let array_with_nulls = etl::data::ArrayCell::I32(vec![Some(1), None, Some(3)]);
        let table_row = TableRow::new(vec![Cell::Array(array_with_nulls)]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NullValuesNotSupportedInArrayInDestination);
        assert!(err.detail().unwrap().contains("Cell at index 0 failed validation"));
    }

    #[test]
    fn bigquery_table_row_try_from_array_with_numeric_rounding_risk() {
        let array_with_rounding_risk = etl::data::ArrayCell::Numeric(vec![
            Some(PgNumeric::from_str("123.456").unwrap()),
            Some(PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap()),
            Some(PgNumeric::from_str("789.012").unwrap()),
        ]);

        let table_row = TableRow::new(vec![Cell::Array(array_with_rounding_risk)]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.detail().unwrap().contains("Cell at index 0"));
        assert!(err.to_string().contains("Element at index 1"));
    }

    #[test]
    fn bigquery_table_row_try_from_valid_array() {
        let valid_array = etl::data::ArrayCell::I32(vec![Some(1), Some(2), Some(3)]);
        let table_row = TableRow::new(vec![
            Cell::String("prefix".to_owned()),
            Cell::Array(valid_array),
            Cell::String("suffix".to_owned()),
        ]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_ok());
    }

    #[test]
    fn bigquery_table_row_try_from_multiple_errors_first_wins() {
        let table_row = TableRow::new(vec![
            Cell::Numeric(
                PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap(),
            ),
            Cell::Array(etl::data::ArrayCell::Numeric(vec![Some(
                PgNumeric::from_str("0.000000000000000000000000000000000000002").unwrap(),
            )])),
        ]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        // Should fail on first cell.
        assert!(err.detail().unwrap().contains("Cell at index 0"));
        assert!(err.to_string().contains("would be rounded by BigQuery"));
    }

    #[test]
    fn bigquery_table_row_try_from_valid_temporal_values() {
        let valid_date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let valid_time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let valid_datetime = NaiveDateTime::new(valid_date, valid_time);

        let table_row = TableRow::new(vec![
            Cell::Date(valid_date),
            Cell::Time(valid_time),
            Cell::Timestamp(valid_datetime),
        ]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_ok());
    }

    #[test]
    fn timestamptz_values_encode_as_epoch_microseconds() {
        let timestamptz = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
        let expected_micros = timestamptz.timestamp_micros();

        let row =
            BigQueryTableRow::try_from_tagged_cells(vec![(1, Cell::TimestampTz(timestamptz))])
                .unwrap();
        let mut actual = Vec::new();
        row.encode(&mut actual).unwrap();

        let mut expected = Vec::new();
        prost::encoding::int64::encode(1, &expected_micros, &mut expected);

        assert_eq!(actual, expected);
        assert_eq!(row.encoded_len(), expected.len());

        let array_row = BigQueryTableRow::try_from_tagged_cells(vec![(
            1,
            Cell::Array(etl::data::ArrayCell::TimestampTz(vec![Some(timestamptz)])),
        )])
        .unwrap();
        let mut actual_array = Vec::new();
        array_row.encode(&mut actual_array).unwrap();

        let mut expected_array = Vec::new();
        prost::encoding::int64::encode_packed(1, &[expected_micros], &mut expected_array);

        assert_eq!(actual_array, expected_array);
        assert_eq!(array_row.encoded_len(), expected_array.len());
    }

    #[test]
    fn bigquery_table_row_try_from_numeric_rounding_risk_fails() {
        let over_scale_numeric =
            PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap();

        let table_row = TableRow::new(vec![Cell::Numeric(over_scale_numeric)]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.to_string().contains("would be rounded by BigQuery"));
    }
}
