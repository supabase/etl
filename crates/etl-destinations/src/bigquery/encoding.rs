use etl::{
    compatibility::DestinationTypeCompatibility,
    error::EtlError,
    types::{
        ArrayCell, Cell, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, TIMESTAMPTZ_FORMAT_HH_MM,
        TableRow,
    },
};
use prost::bytes;

use crate::bigquery::compatibility::BigQueryCompatibility;

/// Protocol buffer wrapper for a BigQuery table row.
///
/// Compatibility conversion happens before this wrapper is built, so encoding
/// can assume array elements are valid for BigQuery.
#[derive(Debug)]
pub(super) struct BigQueryTableRow(Vec<(u32, Cell)>);

impl TryFrom<TableRow> for BigQueryTableRow {
    type Error = EtlError;

    /// Converts a [`TableRow`] to a [`BigQueryTableRow`] using default BigQuery
    /// compatibility.
    fn try_from(value: TableRow) -> Result<Self, Self::Error> {
        BigQueryTableRow::try_from_with_type_compatibility(
            value,
            DestinationTypeCompatibility::default(),
        )
    }
}

impl BigQueryTableRow {
    /// Converts a [`TableRow`] into a BigQuery row using the provided policy.
    pub(super) fn try_from_with_type_compatibility(
        value: TableRow,
        type_compatibility: DestinationTypeCompatibility,
    ) -> Result<Self, EtlError> {
        let checker = BigQueryCompatibility::checker(type_compatibility);
        let value = checker.get_compatible_table_row(value)?;
        let cells = value
            .into_values()
            .into_iter()
            .enumerate()
            .map(|(index, cell)| ((index + 1) as u32, cell))
            .collect();

        Ok(BigQueryTableRow(cells))
    }

    /// Converts tagged cells into a BigQuery row with a compatibility policy.
    pub(super) fn try_from_tagged_cells_with_type_compatibility(
        tagged_cells: impl IntoIterator<Item = (usize, Cell)>,
        type_compatibility: DestinationTypeCompatibility,
    ) -> Result<Self, EtlError> {
        let checker = BigQueryCompatibility::checker(type_compatibility);
        let cells = checker
            .get_compatible_tagged_cells(tagged_cells)?
            .into_iter()
            .map(|(index, cell)| (index as u32, cell))
            .collect();

        Ok(BigQueryTableRow(cells))
    }

    /// Returns the tagged cells for assertions in tests.
    #[cfg(test)]
    pub(super) fn debug_cells(&self) -> &[(u32, Cell)] {
        &self.0
    }
}

impl prost::Message for BigQueryTableRow {
    /// Encodes the table row into the provided buffer using Protocol Buffer
    /// format.
    ///
    /// Each cell is encoded with the field tag stored alongside it, using the
    /// appropriate prost encoding method for the cell's data type.
    fn encode_raw(&self, buf: &mut impl bytes::BufMut)
    where
        Self: Sized,
    {
        for (tag, cell) in &self.0 {
            cell_encode_prost(cell, *tag, buf);
        }
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

    /// Calculates the encoded length of the table row in bytes.
    ///
    /// Sums the encoded lengths of all cells with their respective tags to
    /// determine the total serialized size.
    fn encoded_len(&self) -> usize {
        let mut len = 0;
        for (tag, cell) in &self.0 {
            len += cell_encode_len_prost(cell, *tag);
        }

        len
    }

    /// Clears all cell values in the table row by calling clear on each cell.
    fn clear(&mut self) {
        for (_, cell) in &mut self.0 {
            cell.clear();
        }
    }
}

/// Encodes a single [`Cell`] into Protocol Buffer format using the specified
/// tag.
///
/// Each cell type is encoded using the appropriate prost encoding method.
/// Temporal types and UUIDs are formatted as strings, while numeric types use
/// their native encoding. Null cells produce no encoded output.
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
        Cell::Timestamp(t) => {
            let s = t.format(TIMESTAMP_FORMAT).to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        Cell::TimestampTz(t) => {
            let s = t.format(TIMESTAMPTZ_FORMAT_HH_MM).to_string();
            prost::encoding::string::encode(tag, &s, buf);
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
            let val = i64::from(*i);
            prost::encoding::int64::encode(tag, &val, buf);
        }
        Cell::Bytes(b) => {
            prost::encoding::bytes::encode(tag, b, buf);
        }
        Cell::Array(a) => {
            array_cell_encode_prost(a, tag, buf);
        }
    }
}

/// Calculates the encoded length in bytes for a single [`Cell`] with the
/// specified tag.
///
/// Returns the number of bytes that would be produced when encoding this cell
/// in Protocol Buffer format. Null cells return zero length, while other types
/// calculate their encoded size using the corresponding prost length functions.
fn cell_encode_len_prost(cell: &Cell, tag: u32) -> usize {
    match cell {
        Cell::Null => 0,
        Cell::Bool(b) => prost::encoding::bool::encoded_len(tag, b),
        Cell::String(s) => prost::encoding::string::encoded_len(tag, s),
        Cell::I16(i) => {
            let val = *i as i32;
            prost::encoding::int32::encoded_len(tag, &val)
        }
        Cell::I32(i) => prost::encoding::int32::encoded_len(tag, i),
        Cell::I64(i) => prost::encoding::int64::encoded_len(tag, i),
        Cell::F32(i) => prost::encoding::float::encoded_len(tag, i),
        Cell::F64(i) => prost::encoding::double::encoded_len(tag, i),
        Cell::Numeric(n) => {
            let s = n.to_string();
            prost::encoding::string::encoded_len(tag, &s)
        }
        Cell::Date(t) => {
            let s = t.format(DATE_FORMAT).to_string();
            prost::encoding::string::encoded_len(tag, &s)
        }
        Cell::Time(t) => {
            let s = t.format(TIME_FORMAT).to_string();
            prost::encoding::string::encoded_len(tag, &s)
        }
        Cell::Timestamp(t) => {
            let s = t.format(TIMESTAMP_FORMAT).to_string();
            prost::encoding::string::encoded_len(tag, &s)
        }
        Cell::TimestampTz(t) => {
            let s = t.format(TIMESTAMPTZ_FORMAT_HH_MM).to_string();
            prost::encoding::string::encoded_len(tag, &s)
        }
        Cell::Uuid(u) => {
            let s = u.to_string();
            prost::encoding::string::encoded_len(tag, &s)
        }
        Cell::Json(j) => {
            let s = j.to_string();
            prost::encoding::string::encoded_len(tag, &s)
        }
        Cell::U32(i) => {
            let val = i64::from(*i);
            prost::encoding::int64::encoded_len(tag, &val)
        }
        Cell::Bytes(b) => prost::encoding::bytes::encoded_len(tag, b),
        Cell::Array(a) => array_cell_encoded_len_prost(a, tag),
    }
}

/// Encodes an [`ArrayCell`] into Protocol Buffer format using the specified
/// tag.
///
/// Array cells are encoded using either packed encoding for numeric types or
/// repeated encoding for string-based types. Temporal arrays are converted to
/// string arrays with appropriate formatting before encoding.
fn array_cell_encode_prost(array_cell: &ArrayCell, tag: u32, buf: &mut impl bytes::BufMut) {
    match array_cell {
        ArrayCell::Bool(vec) => {
            let values = required_array_values(vec);
            prost::encoding::bool::encode_packed(tag, &values, buf);
        }
        ArrayCell::String(vec) => {
            let values = required_array_values(vec);
            prost::encoding::string::encode_repeated(tag, &values, buf);
        }
        ArrayCell::I16(vec) => {
            let values: Vec<i32> = required_array_values(vec).into_iter().map(i32::from).collect();
            prost::encoding::int32::encode_packed(tag, &values, buf);
        }
        ArrayCell::I32(vec) => {
            let values = required_array_values(vec);
            prost::encoding::int32::encode_packed(tag, &values, buf);
        }
        ArrayCell::U32(vec) => {
            let values: Vec<i64> = required_array_values(vec).into_iter().map(i64::from).collect();
            prost::encoding::int64::encode_packed(tag, &values, buf);
        }
        ArrayCell::I64(vec) => {
            let values = required_array_values(vec);
            prost::encoding::int64::encode_packed(tag, &values, buf);
        }
        ArrayCell::F32(vec) => {
            let values = required_array_values(vec);
            prost::encoding::float::encode_packed(tag, &values, buf);
        }
        ArrayCell::F64(vec) => {
            let values = required_array_values(vec);
            prost::encoding::double::encode_packed(tag, &values, buf);
        }
        ArrayCell::Numeric(vec) => {
            let values: Vec<String> =
                required_array_values(vec).into_iter().map(|value| value.to_string()).collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        }
        ArrayCell::Date(vec) => {
            let values: Vec<String> = required_array_values(vec)
                .into_iter()
                .map(|v| v.format(DATE_FORMAT).to_string())
                .collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        }
        ArrayCell::Time(vec) => {
            let values: Vec<String> = required_array_values(vec)
                .into_iter()
                .map(|v| v.format(TIME_FORMAT).to_string())
                .collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        }
        ArrayCell::Timestamp(vec) => {
            let values: Vec<String> = required_array_values(vec)
                .into_iter()
                .map(|v| v.format(TIMESTAMP_FORMAT).to_string())
                .collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        }
        ArrayCell::TimestampTz(vec) => {
            let values: Vec<String> = required_array_values(vec)
                .into_iter()
                .map(|v| v.format(TIMESTAMPTZ_FORMAT_HH_MM).to_string())
                .collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        }
        ArrayCell::Uuid(vec) => {
            let values: Vec<String> =
                required_array_values(vec).into_iter().map(|value| value.to_string()).collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        }
        ArrayCell::Json(vec) => {
            let values: Vec<String> =
                required_array_values(vec).into_iter().map(|value| value.to_string()).collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        }
        ArrayCell::Bytes(vec) => {
            let values = required_array_values(vec);
            prost::encoding::bytes::encode_repeated(tag, &values, buf);
        }
    }
}

/// Calculates the encoded length in bytes for an [`ArrayCell`] with the
/// specified tag.
///
/// Returns the number of bytes that would be produced when encoding this array
/// cell in Protocol Buffer format. Uses packed length calculation for numeric
/// arrays and repeated length calculation for string-based arrays.
fn array_cell_encoded_len_prost(array_cell: &ArrayCell, tag: u32) -> usize {
    match array_cell {
        ArrayCell::Bool(vec) => {
            let values = required_array_values(vec);
            prost::encoding::bool::encoded_len_packed(tag, &values)
        }
        ArrayCell::String(vec) => {
            let values = required_array_values(vec);
            prost::encoding::string::encoded_len_repeated(tag, &values)
        }
        ArrayCell::I16(vec) => {
            let values: Vec<i32> = required_array_values(vec).into_iter().map(i32::from).collect();
            prost::encoding::int32::encoded_len_packed(tag, &values)
        }
        ArrayCell::I32(vec) => {
            let values = required_array_values(vec);
            prost::encoding::int32::encoded_len_packed(tag, &values)
        }
        ArrayCell::U32(vec) => {
            let values: Vec<i64> = required_array_values(vec).into_iter().map(i64::from).collect();
            prost::encoding::int64::encoded_len_packed(tag, &values)
        }
        ArrayCell::I64(vec) => {
            let values = required_array_values(vec);
            prost::encoding::int64::encoded_len_packed(tag, &values)
        }
        ArrayCell::F32(vec) => {
            let values = required_array_values(vec);
            prost::encoding::float::encoded_len_packed(tag, &values)
        }
        ArrayCell::F64(vec) => {
            let values = required_array_values(vec);
            prost::encoding::double::encoded_len_packed(tag, &values)
        }
        ArrayCell::Numeric(vec) => {
            let values: Vec<String> =
                required_array_values(vec).into_iter().map(|value| value.to_string()).collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        }
        ArrayCell::Date(vec) => {
            let values: Vec<String> = required_array_values(vec)
                .into_iter()
                .map(|v| v.format(DATE_FORMAT).to_string())
                .collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        }
        ArrayCell::Time(vec) => {
            let values: Vec<String> = required_array_values(vec)
                .into_iter()
                .map(|v| v.format(TIME_FORMAT).to_string())
                .collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        }
        ArrayCell::Timestamp(vec) => {
            let values: Vec<String> = required_array_values(vec)
                .into_iter()
                .map(|v| v.format(TIMESTAMP_FORMAT).to_string())
                .collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        }
        ArrayCell::TimestampTz(vec) => {
            let values: Vec<String> = required_array_values(vec)
                .into_iter()
                .map(|v| v.format(TIMESTAMPTZ_FORMAT_HH_MM).to_string())
                .collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        }
        ArrayCell::Uuid(vec) => {
            let values: Vec<String> =
                required_array_values(vec).into_iter().map(|value| value.to_string()).collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        }
        ArrayCell::Json(vec) => {
            let values: Vec<String> =
                required_array_values(vec).into_iter().map(|value| value.to_string()).collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        }
        ArrayCell::Bytes(vec) => {
            let values = required_array_values(vec);
            prost::encoding::bytes::encoded_len_repeated(tag, &values)
        }
    }
}

/// Returns array values after compatibility validation has removed `NULL`s.
fn required_array_values<T: Clone>(values: &[Option<T>]) -> Vec<T> {
    values
        .iter()
        .map(|value| {
            debug_assert!(value.is_some(), "BigQuery compatibility rejects null array elements");
            value.clone().expect("BigQuery compatibility rejects null array elements")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use etl::{
        error::ErrorKind,
        types::{Cell, PgNumeric},
    };

    use super::*;

    #[test]
    fn bigquery_table_row_try_from_valid() {
        let table_row = TableRow::new(vec![
            Cell::I32(42),
            Cell::String("test".to_owned()),
            Cell::Bool(true),
            Cell::Null,
        ]);

        let result = BigQueryTableRow::try_from_with_type_compatibility(
            table_row,
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn bigquery_table_row_try_from_rejects_numeric_nan() {
        let table_row = TableRow::new(vec![Cell::I32(42), Cell::Numeric(PgNumeric::NaN)]);

        let result = BigQueryTableRow::try_from_with_type_compatibility(
            table_row,
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn bigquery_table_row_try_from_rejects_numeric_infinity() {
        let table_row = TableRow::new(vec![
            Cell::String("valid".to_owned()),
            Cell::Numeric(PgNumeric::PositiveInfinity),
        ]);

        let result = BigQueryTableRow::try_from_with_type_compatibility(
            table_row,
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn bigquery_table_row_try_from_rejects_json_number_outside_float_domain() {
        let json = serde_json::from_str(r#"{"value":1e309}"#).unwrap();
        let table_row = TableRow::new(vec![Cell::Json(json)]);

        let result = BigQueryTableRow::try_from_with_type_compatibility(
            table_row,
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn bigquery_table_row_try_from_rejects_json_integer_precision_loss() {
        let json = serde_json::from_str(r#"{"value":18446744073709551616}"#).unwrap();
        let table_row = TableRow::new(vec![Cell::Json(json)]);

        let result = BigQueryTableRow::try_from_with_type_compatibility(
            table_row,
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn bigquery_table_row_try_from_lossless_accepts_string_materialized_values() {
        let json = serde_json::from_str(r#"{"value":18446744073709551616}"#).unwrap();
        let table_row = TableRow::new(vec![
            Cell::Json(json),
            Cell::Numeric(
                PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap(),
            ),
        ]);

        let result = BigQueryTableRow::try_from_with_type_compatibility(
            table_row,
            DestinationTypeCompatibility::lossless(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn bigquery_table_row_try_from_rejects_date_outside_bigquery_domain() {
        let invalid_date = NaiveDate::from_ymd_opt(1, 1, 1).unwrap().pred_opt().unwrap(); // Date before year 1

        let table_row = TableRow::new(vec![Cell::Date(invalid_date)]);

        let result = BigQueryTableRow::try_from_with_type_compatibility(
            table_row,
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn bigquery_table_row_try_from_array_with_nulls() {
        let array_with_nulls = etl::types::ArrayCell::I32(vec![Some(1), None, Some(3)]);
        let table_row = TableRow::new(vec![Cell::Array(array_with_nulls)]);

        let result = BigQueryTableRow::try_from_with_type_compatibility(
            table_row,
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NullValuesNotSupportedInArrayInDestination);
        assert!(err.detail().unwrap().contains("Cell at index 0 failed compatibility"));
    }

    #[test]
    fn bigquery_table_row_try_from_array_with_numeric_rounding_risk() {
        let array_with_rounding_risk = etl::types::ArrayCell::Numeric(vec![
            Some(PgNumeric::from_str("123.456").unwrap()),
            Some(PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap()),
            Some(PgNumeric::from_str("789.012").unwrap()),
        ]);

        let table_row = TableRow::new(vec![Cell::Array(array_with_rounding_risk)]);

        let result = BigQueryTableRow::try_from_with_type_compatibility(
            table_row,
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.detail().unwrap().contains("Cell at index 0"));
        assert!(err.detail().unwrap().contains("Element at index 1"));
    }

    #[test]
    fn bigquery_table_row_try_from_valid_array() {
        let valid_array = etl::types::ArrayCell::I32(vec![Some(1), Some(2), Some(3)]);
        let table_row = TableRow::new(vec![
            Cell::String("prefix".to_owned()),
            Cell::Array(valid_array),
            Cell::String("suffix".to_owned()),
        ]);

        let result = BigQueryTableRow::try_from_with_type_compatibility(
            table_row,
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn bigquery_table_row_try_from_multiple_errors_first_wins() {
        let table_row = TableRow::new(vec![
            Cell::Numeric(
                PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap(),
            ),
            Cell::Array(etl::types::ArrayCell::Numeric(vec![Some(
                PgNumeric::from_str("0.000000000000000000000000000000000000002").unwrap(),
            )])),
        ]);

        let result = BigQueryTableRow::try_from_with_type_compatibility(
            table_row,
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.detail().unwrap().contains("Cell at index 0")); // Should fail on first cell
        assert!(err.detail().unwrap().contains("would be rounded by BigQuery"));
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
    fn bigquery_table_row_try_from_numeric_rounding_risk_fails() {
        let over_scale_numeric =
            PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap();

        let table_row = TableRow::new(vec![Cell::Numeric(over_scale_numeric)]);

        let result = BigQueryTableRow::try_from_with_type_compatibility(
            table_row,
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.detail().unwrap().contains("would be rounded by BigQuery"));
    }

    #[test]
    fn bigquery_table_row_try_from_uses_default_lossy_compatibility() {
        let table_row = TableRow::new(vec![
            Cell::Numeric(
                PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap(),
            ),
            Cell::Json(serde_json::from_str(r#"{"value":18446744073709551616}"#).unwrap()),
            Cell::F64(-0.0),
        ]);

        let result = BigQueryTableRow::try_from(table_row);

        assert!(result.is_ok());
    }
}
