use etl::{
    data::{ArrayCell, Cell, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, TableRow},
    error::{ErrorKind, EtlError},
    etl_error,
};
use prost::bytes;

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
    /// Encodes tagged cells into a row, preserving sparse field positions.
    pub(super) fn try_from_tagged_cells(
        tagged_cells: impl IntoIterator<Item = (usize, Cell)>,
    ) -> Result<Self, EtlError> {
        let mut buf = Vec::new();

        for (index, cell) in tagged_cells {
            cell_encode_prost(&cell, index as u32, &mut buf).map_err(|err| {
                etl_error!(
                    err.kind(),
                    "Cell encoding failed for BigQuery",
                    format!("Cell at index {} could not be encoded", index - 1),
                    source: err
                )
            })?;
        }

        Ok(BigQueryTableRow(buf))
    }
}

impl TryFrom<TableRow> for BigQueryTableRow {
    type Error = EtlError;

    /// Converts a [`TableRow`] to its BigQuery Protocol Buffer encoding.
    ///
    /// Returns an error only when the Protocol Buffer row cannot represent a
    /// source value, such as an array containing a NULL element.
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
/// Returns an error if encoding the value would change its meaning, such as
/// omitting a NULL element from a repeated field.
fn cell_encode_prost(cell: &Cell, tag: u32, buf: &mut impl bytes::BufMut) -> Result<(), EtlError> {
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
            array_cell_encode_prost(a, tag, buf)?;
        }
    }

    Ok(())
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
/// Returns an error if an element is NULL because omitting it from a repeated
/// field would change the source array.
fn array_cell_encode_prost(
    array_cell: &ArrayCell,
    tag: u32,
    buf: &mut impl bytes::BufMut,
) -> Result<(), EtlError> {
    /// Returns an array element or an encoding error for a NULL element.
    fn element<T>(value: &Option<T>, index: usize) -> Result<&T, EtlError> {
        value.as_ref().ok_or_else(|| {
            etl_error!(
                ErrorKind::NullValuesNotSupportedInArrayInDestination,
                "NULL values in arrays not supported in this destination",
                format!("Array element at index {index} is NULL and cannot be encoded")
            )
        })
    }

    match array_cell {
        ArrayCell::Bool(vec) => {
            let values = vec
                .iter()
                .enumerate()
                .map(|(index, value)| element(value, index).copied())
                .collect::<Result<Vec<_>, _>>()?;
            prost::encoding::bool::encode_packed(tag, &values, buf);
        }
        ArrayCell::String(vec) => {
            for (index, value) in vec.iter().enumerate() {
                prost::encoding::string::encode(tag, element(value, index)?, buf);
            }
        }
        ArrayCell::I16(vec) => {
            let values = vec
                .iter()
                .enumerate()
                .map(|(index, value)| element(value, index).map(|value| i32::from(*value)))
                .collect::<Result<Vec<_>, _>>()?;
            prost::encoding::int32::encode_packed(tag, &values, buf);
        }
        ArrayCell::I32(vec) => {
            let values = vec
                .iter()
                .enumerate()
                .map(|(index, value)| element(value, index).copied())
                .collect::<Result<Vec<_>, _>>()?;
            prost::encoding::int32::encode_packed(tag, &values, buf);
        }
        ArrayCell::U32(vec) => {
            let values = vec
                .iter()
                .enumerate()
                .map(|(index, value)| element(value, index).copied())
                .collect::<Result<Vec<_>, _>>()?;
            prost::encoding::uint32::encode_packed(tag, &values, buf);
        }
        ArrayCell::I64(vec) => {
            let values = vec
                .iter()
                .enumerate()
                .map(|(index, value)| element(value, index).copied())
                .collect::<Result<Vec<_>, _>>()?;
            prost::encoding::int64::encode_packed(tag, &values, buf);
        }
        ArrayCell::F32(vec) => {
            let values = vec
                .iter()
                .enumerate()
                .map(|(index, value)| element(value, index).copied())
                .collect::<Result<Vec<_>, _>>()?;
            prost::encoding::float::encode_packed(tag, &values, buf);
        }
        ArrayCell::F64(vec) => {
            let values = vec
                .iter()
                .enumerate()
                .map(|(index, value)| element(value, index).copied())
                .collect::<Result<Vec<_>, _>>()?;
            prost::encoding::double::encode_packed(tag, &values, buf);
        }
        ArrayCell::Numeric(vec) => {
            for (index, value) in vec.iter().enumerate() {
                let s = element(value, index)?.to_string();
                prost::encoding::string::encode(tag, &s, buf);
            }
        }
        ArrayCell::Date(vec) => {
            for (index, value) in vec.iter().enumerate() {
                let s = element(value, index)?.format(DATE_FORMAT).to_string();
                prost::encoding::string::encode(tag, &s, buf);
            }
        }
        ArrayCell::Time(vec) => {
            for (index, value) in vec.iter().enumerate() {
                let s = element(value, index)?.format(TIME_FORMAT).to_string();
                prost::encoding::string::encode(tag, &s, buf);
            }
        }
        ArrayCell::TimeTz(vec) => {
            for (index, value) in vec.iter().enumerate() {
                let s = element(value, index)?.to_string();
                prost::encoding::string::encode(tag, &s, buf);
            }
        }
        ArrayCell::Timestamp(vec) => {
            for (index, value) in vec.iter().enumerate() {
                let s = element(value, index)?.format(TIMESTAMP_FORMAT).to_string();
                prost::encoding::string::encode(tag, &s, buf);
            }
        }
        ArrayCell::TimestampTz(vec) => {
            let values = vec
                .iter()
                .enumerate()
                .map(|(index, value)| element(value, index).map(chrono::DateTime::timestamp_micros))
                .collect::<Result<Vec<_>, _>>()?;
            prost::encoding::int64::encode_packed(tag, &values, buf);
        }
        ArrayCell::Uuid(vec) => {
            for (index, value) in vec.iter().enumerate() {
                let s = element(value, index)?.to_string();
                prost::encoding::string::encode(tag, &s, buf);
            }
        }
        ArrayCell::Json(vec) => {
            for (index, value) in vec.iter().enumerate() {
                let s = element(value, index)?.to_string();
                prost::encoding::string::encode(tag, &s, buf);
            }
        }
        ArrayCell::Bytes(vec) => {
            for (index, value) in vec.iter().enumerate() {
                prost::encoding::bytes::encode(tag, element(value, index)?, buf);
            }
        }
    }

    Ok(())
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
    fn bigquery_table_row_try_from_delegates_numeric_nan_behavior_to_bigquery() {
        let table_row = TableRow::new(vec![Cell::I32(42), Cell::Numeric(PgNumeric::NaN)]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_ok());
    }

    #[test]
    fn bigquery_table_row_try_from_delegates_numeric_infinity_behavior_to_bigquery() {
        let table_row = TableRow::new(vec![
            Cell::String("valid".to_owned()),
            Cell::Numeric(PgNumeric::PositiveInfinity),
        ]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_ok());
    }

    #[test]
    fn bigquery_table_row_try_from_delegates_json_number_behavior_to_bigquery() {
        let json = serde_json::from_str(r#"{"value":1e309}"#).unwrap();
        let table_row = TableRow::new(vec![Cell::Json(json)]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_ok());
    }

    #[test]
    fn bigquery_table_row_try_from_delegates_wide_json_integer_to_bigquery() {
        let json = serde_json::from_str(r#"{"value":18446744073709551616}"#).unwrap();
        let table_row = TableRow::new(vec![Cell::Json(json)]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_ok());
    }

    #[test]
    fn bigquery_table_row_try_from_delegates_date_domain_behavior_to_bigquery() {
        let invalid_date = NaiveDate::from_ymd_opt(1, 1, 1).unwrap().pred_opt().unwrap(); // Date before year 1

        let table_row = TableRow::new(vec![Cell::Date(invalid_date)]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_ok());
    }

    #[test]
    fn bigquery_table_row_encoding_rejects_null_elements_for_every_array_type() {
        let arrays = [
            ArrayCell::Bool(vec![None]),
            ArrayCell::String(vec![None]),
            ArrayCell::I16(vec![None]),
            ArrayCell::I32(vec![None]),
            ArrayCell::U32(vec![None]),
            ArrayCell::I64(vec![None]),
            ArrayCell::F32(vec![None]),
            ArrayCell::F64(vec![None]),
            ArrayCell::Numeric(vec![None]),
            ArrayCell::Date(vec![None]),
            ArrayCell::Time(vec![None]),
            ArrayCell::TimeTz(vec![None]),
            ArrayCell::Timestamp(vec![None]),
            ArrayCell::TimestampTz(vec![None]),
            ArrayCell::Uuid(vec![None]),
            ArrayCell::Json(vec![None]),
            ArrayCell::Bytes(vec![None]),
        ];

        for array in arrays {
            let error =
                BigQueryTableRow::try_from(TableRow::new(vec![Cell::Array(array)])).unwrap_err();

            assert_eq!(error.kind(), ErrorKind::NullValuesNotSupportedInArrayInDestination);
            assert!(error.detail().unwrap().contains("Cell at index 0 could not be encoded"));
            assert!(error.to_string().contains("Array element at index 0 is NULL"));
        }
    }

    #[test]
    fn bigquery_table_row_try_from_delegates_array_numeric_domain_to_bigquery() {
        let array_with_rounding_risk = etl::data::ArrayCell::Numeric(vec![
            Some(PgNumeric::from_str("123.456").unwrap()),
            Some(PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap()),
            Some(PgNumeric::from_str("789.012").unwrap()),
        ]);

        let table_row = TableRow::new(vec![Cell::Array(array_with_rounding_risk)]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_ok());
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
    fn bigquery_table_row_try_from_delegates_multiple_numeric_domains_to_bigquery() {
        let table_row = TableRow::new(vec![
            Cell::Numeric(
                PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap(),
            ),
            Cell::Array(etl::data::ArrayCell::Numeric(vec![Some(
                PgNumeric::from_str("0.000000000000000000000000000000000000002").unwrap(),
            )])),
        ]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_ok());
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
    fn bigquery_table_row_try_from_delegates_numeric_rounding_to_bigquery() {
        let over_scale_numeric =
            PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap();

        let table_row = TableRow::new(vec![Cell::Numeric(over_scale_numeric)]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_ok());
    }
}
