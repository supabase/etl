use crate::bigquery::validation::validate_cell_for_bigquery;
use etl::error::EtlError;
use etl::etl_error;
use etl::types::{ArrayCellNonOptional, CellNonOptional, TableRow};
use etl_postgres::time::{DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, TIMESTAMPTZ_FORMAT_HH_MM};
use prost::bytes;

/// Protocol buffer wrapper for a BigQuery table row containing non-optional cells.
///
/// Wraps a vector of [`CellNonOptional`] values and implements the [`prost::Message`]
/// trait to enable Protocol Buffer serialization for BigQuery streaming inserts.
#[derive(Debug)]
pub struct BigQueryTableRow(Vec<CellNonOptional>);

impl TryFrom<TableRow> for BigQueryTableRow {
    type Error = EtlError;

    /// Converts a [`TableRow`] to a [`BigQueryTableRow`] by transforming all cell values
    /// to their non-optional equivalents and validating them for BigQuery compatibility.
    ///
    /// This implementation:
    /// 1. Converts each [`Cell`] to [`CellNonOptional`] to ensure no null values in arrays
    /// 2. Validates each cell value against BigQuery's supported ranges and types
    /// 3. Returns an error if any value is outside BigQuery's supported bounds
    ///
    /// The validation strategy fails fast on any unsupported value rather than clamping,
    /// ensuring users are aware when their data doesn't fit BigQuery's constraints.
    fn try_from(value: TableRow) -> Result<Self, Self::Error> {
        let mut validated_cells = Vec::with_capacity(value.values.len());

        for (index, cell) in value.values.into_iter().enumerate() {
            let cell_non_optional = CellNonOptional::try_from(cell).map_err(|err| {
                etl_error!(
                    err.kind(),
                    "Cell conversion failed during BigQuery validation",
                    format!(
                        "Failed to convert cell at index {} to non-optional format: {}",
                        index, err
                    )
                )
            })?;

            validate_cell_for_bigquery(&cell_non_optional).map_err(|err| {
                etl_error!(
                    err.kind(),
                    "Cell validation failed for BigQuery compatibility",
                    format!(
                        "Cell at index {} failed validation: {}",
                        index,
                        err.detail().unwrap_or("validation error")
                    )
                )
            })?;

            validated_cells.push(cell_non_optional);
        }

        Ok(BigQueryTableRow(validated_cells))
    }
}

impl prost::Message for BigQueryTableRow {
    /// Encodes the table row into the provided buffer using Protocol Buffer format.
    ///
    /// Each cell is encoded with a sequential tag starting from 1, using the
    /// appropriate prost encoding method for the cell's data type.
    fn encode_raw(&self, buf: &mut impl bytes::BufMut)
    where
        Self: Sized,
    {
        let mut tag = 1;
        for cell in &self.0 {
            cell_encode_prost(cell, tag, buf);
            tag += 1;
        }
    }

    /// Merges a field from a Protocol Buffer message into this table row.
    ///
    /// Currently unimplemented as this functionality is not required for BigQuery
    /// streaming inserts, which only need encoding capabilities.
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

    /// Calculates the encoded length of the table row in bytes.
    ///
    /// Sums the encoded lengths of all cells with their respective tags to
    /// determine the total serialized size.
    fn encoded_len(&self) -> usize {
        let mut len = 0;
        let mut tag = 1;
        for cell in &self.0 {
            len += cell_encode_len_prost(cell, tag);
            tag += 1;
        }

        len
    }

    /// Clears all cell values in the table row by calling clear on each cell.
    fn clear(&mut self) {
        for cell in &mut self.0 {
            cell.clear();
        }
    }
}

/// Encodes a single [`CellNonOptional`] into Protocol Buffer format using the specified tag.
///
/// Each cell type is encoded using the appropriate prost encoding method. Temporal types
/// and UUIDs are formatted as strings, while numeric types use their native encoding.
/// Null cells produce no encoded output.
pub fn cell_encode_prost(cell: &CellNonOptional, tag: u32, buf: &mut impl bytes::BufMut) {
    match cell {
        CellNonOptional::Null => {}
        CellNonOptional::Bool(b) => {
            prost::encoding::bool::encode(tag, b, buf);
        }
        CellNonOptional::String(s) => {
            prost::encoding::string::encode(tag, s, buf);
        }
        CellNonOptional::I16(i) => {
            let val = *i as i32;
            prost::encoding::int32::encode(tag, &val, buf);
        }
        CellNonOptional::I32(i) => {
            prost::encoding::int32::encode(tag, i, buf);
        }
        CellNonOptional::I64(i) => {
            prost::encoding::int64::encode(tag, i, buf);
        }
        CellNonOptional::F32(i) => {
            prost::encoding::float::encode(tag, i, buf);
        }
        CellNonOptional::F64(i) => {
            prost::encoding::double::encode(tag, i, buf);
        }
        CellNonOptional::Numeric(n) => {
            let s = n.to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        CellNonOptional::Date(t) => {
            let s = t.format(DATE_FORMAT).to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        CellNonOptional::Time(t) => {
            let s = t.format(TIME_FORMAT).to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        CellNonOptional::Timestamp(t) => {
            let s = t.format(TIMESTAMP_FORMAT).to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        CellNonOptional::TimestampTz(t) => {
            let s = t.format(TIMESTAMPTZ_FORMAT_HH_MM).to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        CellNonOptional::Uuid(u) => {
            let s = u.to_string();
            prost::encoding::string::encode(tag, &s, buf)
        }
        CellNonOptional::Json(j) => {
            let s = j.to_string();
            prost::encoding::string::encode(tag, &s, buf)
        }
        CellNonOptional::U32(i) => {
            prost::encoding::uint32::encode(tag, i, buf);
        }
        CellNonOptional::Bytes(b) => {
            prost::encoding::bytes::encode(tag, b, buf);
        }
        CellNonOptional::Array(a) => {
            array_cell_encode_prost(a.clone(), tag, buf);
        }
    }
}

/// Calculates the encoded length in bytes for a single [`CellNonOptional`] with the specified tag.
///
/// Returns the number of bytes that would be produced when encoding this cell in Protocol
/// Buffer format. Null cells return zero length, while other types calculate their
/// encoded size using the corresponding prost length functions.
pub fn cell_encode_len_prost(cell: &CellNonOptional, tag: u32) -> usize {
    match cell {
        CellNonOptional::Null => 0,
        CellNonOptional::Bool(b) => prost::encoding::bool::encoded_len(tag, b),
        CellNonOptional::String(s) => prost::encoding::string::encoded_len(tag, s),
        CellNonOptional::I16(i) => {
            let val = *i as i32;
            prost::encoding::int32::encoded_len(tag, &val)
        }
        CellNonOptional::I32(i) => prost::encoding::int32::encoded_len(tag, i),
        CellNonOptional::I64(i) => prost::encoding::int64::encoded_len(tag, i),
        CellNonOptional::F32(i) => prost::encoding::float::encoded_len(tag, i),
        CellNonOptional::F64(i) => prost::encoding::double::encoded_len(tag, i),
        CellNonOptional::Numeric(n) => {
            let s = n.to_string();
            prost::encoding::string::encoded_len(tag, &s)
        }
        CellNonOptional::Date(t) => {
            let s = t.format(DATE_FORMAT).to_string();
            prost::encoding::string::encoded_len(tag, &s)
        }
        CellNonOptional::Time(t) => {
            let s = t.format(TIME_FORMAT).to_string();
            prost::encoding::string::encoded_len(tag, &s)
        }
        CellNonOptional::Timestamp(t) => {
            let s = t.format(TIMESTAMP_FORMAT).to_string();
            prost::encoding::string::encoded_len(tag, &s)
        }
        CellNonOptional::TimestampTz(t) => {
            let s = t.format(TIMESTAMPTZ_FORMAT_HH_MM).to_string();
            prost::encoding::string::encoded_len(tag, &s)
        }
        CellNonOptional::Uuid(u) => {
            let s = u.to_string();
            prost::encoding::string::encoded_len(tag, &s)
        }
        CellNonOptional::Json(j) => {
            let s = j.to_string();
            prost::encoding::string::encoded_len(tag, &s)
        }
        CellNonOptional::U32(i) => prost::encoding::uint32::encoded_len(tag, i),
        CellNonOptional::Bytes(b) => prost::encoding::bytes::encoded_len(tag, b),
        CellNonOptional::Array(a) => array_cell_non_optional_encoded_len_prost(a.clone(), tag),
    }
}

/// Encodes an [`ArrayCellNonOptional`] into Protocol Buffer format using the specified tag.
///
/// Array cells are encoded using either packed encoding for numeric types or repeated
/// encoding for string-based types. Temporal arrays are converted to string arrays
/// with appropriate formatting before encoding.
pub fn array_cell_encode_prost(
    array_cell: ArrayCellNonOptional,
    tag: u32,
    buf: &mut impl bytes::BufMut,
) {
    match array_cell {
        ArrayCellNonOptional::Null => {}
        ArrayCellNonOptional::Bool(vec) => {
            prost::encoding::bool::encode_packed(tag, &vec, buf);
        }
        ArrayCellNonOptional::String(vec) => {
            prost::encoding::string::encode_repeated(tag, &vec, buf);
        }
        ArrayCellNonOptional::I16(vec) => {
            let values: Vec<i32> = vec.into_iter().map(|v| v as i32).collect();
            prost::encoding::int32::encode_packed(tag, &values, buf);
        }
        ArrayCellNonOptional::I32(vec) => {
            prost::encoding::int32::encode_packed(tag, &vec, buf);
        }
        ArrayCellNonOptional::U32(vec) => {
            prost::encoding::uint32::encode_packed(tag, &vec, buf);
        }
        ArrayCellNonOptional::I64(vec) => {
            prost::encoding::int64::encode_packed(tag, &vec, buf);
        }
        ArrayCellNonOptional::F32(vec) => {
            prost::encoding::float::encode_packed(tag, &vec, buf);
        }
        ArrayCellNonOptional::F64(vec) => {
            prost::encoding::double::encode_packed(tag, &vec, buf);
        }
        ArrayCellNonOptional::Numeric(vec) => {
            let values: Vec<String> = vec.into_iter().map(|v| v.to_string()).collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        }
        ArrayCellNonOptional::Date(vec) => {
            let values: Vec<String> = vec
                .into_iter()
                .map(|v| v.format(DATE_FORMAT).to_string())
                .collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        }
        ArrayCellNonOptional::Time(vec) => {
            let values: Vec<String> = vec
                .into_iter()
                .map(|v| v.format(TIME_FORMAT).to_string())
                .collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        }
        ArrayCellNonOptional::Timestamp(vec) => {
            let values: Vec<String> = vec
                .into_iter()
                .map(|v| v.format(TIMESTAMP_FORMAT).to_string())
                .collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        }
        ArrayCellNonOptional::TimestampTz(vec) => {
            let values: Vec<String> = vec
                .into_iter()
                .map(|v| v.format(TIMESTAMPTZ_FORMAT_HH_MM).to_string())
                .collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        }
        ArrayCellNonOptional::Uuid(vec) => {
            let values: Vec<String> = vec.into_iter().map(|v| v.to_string()).collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        }
        ArrayCellNonOptional::Json(vec) => {
            let values: Vec<String> = vec.into_iter().map(|v| v.to_string()).collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        }
        ArrayCellNonOptional::Bytes(vec) => {
            prost::encoding::bytes::encode_repeated(tag, &vec, buf);
        }
    }
}

/// Calculates the encoded length in bytes for an [`ArrayCellNonOptional`] with the specified tag.
///
/// Returns the number of bytes that would be produced when encoding this array cell in
/// Protocol Buffer format. Uses packed length calculation for numeric arrays and repeated
/// length calculation for string-based arrays.
pub fn array_cell_non_optional_encoded_len_prost(
    array_cell: ArrayCellNonOptional,
    tag: u32,
) -> usize {
    match array_cell {
        ArrayCellNonOptional::Null => 0,
        ArrayCellNonOptional::Bool(vec) => prost::encoding::bool::encoded_len_packed(tag, &vec),
        ArrayCellNonOptional::String(vec) => {
            prost::encoding::string::encoded_len_repeated(tag, &vec)
        }
        ArrayCellNonOptional::I16(vec) => {
            let values: Vec<i32> = vec.into_iter().map(|v| v as i32).collect();
            prost::encoding::int32::encoded_len_packed(tag, &values)
        }
        ArrayCellNonOptional::I32(vec) => prost::encoding::int32::encoded_len_packed(tag, &vec),
        ArrayCellNonOptional::U32(vec) => prost::encoding::uint32::encoded_len_packed(tag, &vec),
        ArrayCellNonOptional::I64(vec) => prost::encoding::int64::encoded_len_packed(tag, &vec),
        ArrayCellNonOptional::F32(vec) => prost::encoding::float::encoded_len_packed(tag, &vec),
        ArrayCellNonOptional::F64(vec) => prost::encoding::double::encoded_len_packed(tag, &vec),
        ArrayCellNonOptional::Numeric(vec) => {
            let values: Vec<String> = vec.into_iter().map(|v| v.to_string()).collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        }
        ArrayCellNonOptional::Date(vec) => {
            let values: Vec<String> = vec
                .into_iter()
                .map(|v| v.format(DATE_FORMAT).to_string())
                .collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        }
        ArrayCellNonOptional::Time(vec) => {
            let values: Vec<String> = vec
                .into_iter()
                .map(|v| v.format(TIME_FORMAT).to_string())
                .collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        }
        ArrayCellNonOptional::Timestamp(vec) => {
            let values: Vec<String> = vec
                .into_iter()
                .map(|v| v.format(TIMESTAMP_FORMAT).to_string())
                .collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        }
        ArrayCellNonOptional::TimestampTz(vec) => {
            let values: Vec<String> = vec
                .into_iter()
                .map(|v| v.format(TIMESTAMPTZ_FORMAT_HH_MM).to_string())
                .collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        }
        ArrayCellNonOptional::Uuid(vec) => {
            let values: Vec<String> = vec.into_iter().map(|v| v.to_string()).collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        }
        ArrayCellNonOptional::Json(vec) => {
            let values: Vec<String> = vec.into_iter().map(|v| v.to_string()).collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        }
        ArrayCellNonOptional::Bytes(vec) => prost::encoding::bytes::encoded_len_repeated(tag, &vec),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use etl::error::ErrorKind;
    use etl::types::{Cell, PgNumeric};
    use std::str::FromStr;

    #[test]
    fn test_bigquery_table_row_try_from_valid() {
        let table_row = TableRow::new(vec![
            Cell::I32(42),
            Cell::String("test".to_string()),
            Cell::Bool(true),
            Cell::Null,
        ]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_ok());
    }

    #[test]
    fn test_bigquery_table_row_try_from_invalid_numeric_nan() {
        let table_row = TableRow::new(vec![Cell::I32(42), Cell::Numeric(PgNumeric::NaN)]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.detail().unwrap().contains("Cell at index 1"));
        assert!(
            err.detail()
                .unwrap()
                .contains("NaN cannot be stored in BigQuery")
        );
    }

    #[test]
    fn test_bigquery_table_row_try_from_invalid_numeric_infinity() {
        let table_row = TableRow::new(vec![
            Cell::String("valid".to_string()),
            Cell::Numeric(PgNumeric::PositiveInfinity),
        ]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.detail().unwrap().contains("Cell at index 1"));
        assert!(
            err.detail()
                .unwrap()
                .contains("Infinity cannot be stored in BigQuery")
        );
    }

    #[test]
    fn test_bigquery_table_row_try_from_invalid_date() {
        let invalid_date = NaiveDate::from_ymd_opt(1, 1, 1)
            .unwrap()
            .pred_opt()
            .unwrap(); // Date before year 1

        let table_row = TableRow::new(vec![Cell::Date(invalid_date)]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.detail().unwrap().contains("Cell at index 0"));
        assert!(err.detail().unwrap().contains("before BigQuery's minimum"));
    }

    #[test]
    fn test_bigquery_table_row_try_from_array_with_nulls() {
        let array_with_nulls = etl::types::ArrayCell::I32(vec![Some(1), None, Some(3)]);
        let table_row = TableRow::new(vec![Cell::Array(array_with_nulls)]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(
            err.kind(),
            ErrorKind::NullValuesNotSupportedInArrayInDestination
        );
        assert!(
            err.detail()
                .unwrap()
                .contains("Failed to convert cell at index 0")
        );
    }

    #[test]
    fn test_bigquery_table_row_try_from_array_with_invalid_elements() {
        let array_with_invalid_numeric = etl::types::ArrayCell::Numeric(vec![
            Some(PgNumeric::from_str("123.456").unwrap()),
            Some(PgNumeric::NaN),
            Some(PgNumeric::from_str("789.012").unwrap()),
        ]);

        let table_row = TableRow::new(vec![Cell::Array(array_with_invalid_numeric)]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.detail().unwrap().contains("Cell at index 0"));
        assert!(err.detail().unwrap().contains("Element at index 1"));
    }

    #[test]
    fn test_bigquery_table_row_try_from_valid_array() {
        let valid_array = etl::types::ArrayCell::I32(vec![Some(1), Some(2), Some(3)]);
        let table_row = TableRow::new(vec![
            Cell::String("prefix".to_string()),
            Cell::Array(valid_array),
            Cell::String("suffix".to_string()),
        ]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_ok());
    }

    #[test]
    fn test_bigquery_table_row_try_from_multiple_errors_first_wins() {
        let table_row = TableRow::new(vec![
            Cell::Numeric(PgNumeric::NaN),              // First invalid cell
            Cell::Numeric(PgNumeric::PositiveInfinity), // Second invalid cell (should not be reached)
        ]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.detail().unwrap().contains("Cell at index 0")); // Should fail on first cell
        assert!(err.detail().unwrap().contains("NaN"));
    }

    #[test]
    fn test_bigquery_table_row_try_from_valid_temporal_values() {
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
    fn test_bigquery_table_row_try_from_oversized_numeric_fails() {
        // Create a numeric value that exceeds BigQuery's limits
        let oversized_numeric = PgNumeric::from_str(
            "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
        ).unwrap(); // This has way more than 76 digits

        let table_row = TableRow::new(vec![Cell::Numeric(oversized_numeric)]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(
            err.detail()
                .unwrap()
                .contains("exceeds BigQuery's BIGNUMERIC limits")
        );
    }
}
