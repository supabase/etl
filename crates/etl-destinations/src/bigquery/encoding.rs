use etl::{
    data::{ArrayCell, Cell, TableRow, Timestamp, format_date, format_timestamp},
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
};
use prost::bytes;

use crate::retry::classified_error_with_detail;

/// Encodes a finite timestamp in BigQuery's integer wire representation.
fn timestamp_micros(value: &Timestamp<chrono::DateTime<chrono::Utc>>) -> EtlResult<i64> {
    match value {
        Timestamp::Value(value) => Ok(value.timestamp_micros()),
        Timestamp::PosInfinity | Timestamp::NegInfinity => Err(etl_error!(
            ErrorKind::ConversionError,
            "Cannot encode infinite timestamp",
            "BigQuery TIMESTAMP uses finite microseconds"
        )),
    }
}

/// Protocol buffer wrapper for a BigQuery table row, holding its Protocol
/// Buffer encoding rather than the source cells.
///
/// Formatting cells such as dates, numerics, UUIDs, and JSON into their
/// Protocol Buffer string representation is comparatively expensive, and
/// [`prost::Message::encoded_len`] and [`prost::Message::encode_raw`] are each
/// invoked at least once per row by callers that budget a batch by length
/// before encoding it. Encoding once up front, at row-construction time, and
/// keeping only the resulting bytes means formatting happens exactly once per
/// row and the source cells don't have to be kept alive alongside their
/// encoding.
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
                classified_error_with_detail(
                    err.kind(),
                    "Cell encoding failed for BigQuery",
                    format!("Cell at index {} could not be encoded", index - 1),
                    err,
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
            let val = i32::from(*i);
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
            let s = format_date(t).to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        Cell::Time(t) => {
            let s = t.to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        Cell::TimeTz(t) => {
            let s = t.to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        Cell::Timestamp(t) => {
            let s = format_timestamp(t).to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        Cell::TimestampTz(t) => {
            let micros = timestamp_micros(t)?;
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
                let s = format_date(element(value, index)?).to_string();
                prost::encoding::string::encode(tag, &s, buf);
            }
        }
        ArrayCell::Time(vec) => {
            for (index, value) in vec.iter().enumerate() {
                let s = element(value, index)?.to_string();
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
                let s = format_timestamp(element(value, index)?).to_string();
                prost::encoding::string::encode(tag, &s, buf);
            }
        }
        ArrayCell::TimestampTz(vec) => {
            let values = vec
                .iter()
                .enumerate()
                .map(|(index, value)| element(value, index).and_then(timestamp_micros))
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
        data::{Cell, Date, PgNumeric, PgTime, Timestamp},
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

        let table_row = TableRow::new(vec![Cell::Date(Date::Value(invalid_date))]);

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
            Cell::Date(Date::Value(valid_date)),
            Cell::Time(PgTime::Value(valid_time)),
            Cell::Timestamp(Timestamp::Value(valid_datetime)),
        ]);

        let result = BigQueryTableRow::try_from(table_row);
        assert!(result.is_ok());
    }

    #[test]
    fn timestamptz_values_encode_as_epoch_microseconds() {
        let timestamptz = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
        let expected_micros = timestamptz.timestamp_micros();

        let row = BigQueryTableRow::try_from_tagged_cells(vec![(
            1,
            Cell::TimestampTz(Timestamp::Value(timestamptz)),
        )])
        .unwrap();
        let mut actual = Vec::new();
        row.encode(&mut actual).unwrap();

        let mut expected = Vec::new();
        prost::encoding::int64::encode(1, &expected_micros, &mut expected);

        assert_eq!(actual, expected);
        assert_eq!(row.encoded_len(), expected.len());

        let array_row = BigQueryTableRow::try_from_tagged_cells(vec![(
            1,
            Cell::Array(etl::data::ArrayCell::TimestampTz(vec![Some(Timestamp::Value(
                timestamptz,
            ))])),
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
    #[test]
    fn infinite_timestamps_fail_scalar_and_array_wire_encoding() {
        for value in [Timestamp::PosInfinity, Timestamp::NegInfinity] {
            for cell in
                [Cell::TimestampTz(value), Cell::Array(ArrayCell::TimestampTz(vec![Some(value)]))]
            {
                let error = BigQueryTableRow::try_from(TableRow::new(vec![cell])).unwrap_err();
                assert_eq!(error.kind(), ErrorKind::ConversionError);
            }
        }
    }

    #[test]
    fn special_values_with_text_wire_forms_are_sent_to_bigquery() {
        let row = BigQueryTableRow::try_from(TableRow::new(vec![
            Cell::Date(Date::PosInfinity),
            Cell::Time(PgTime::EndOfDay),
            Cell::Timestamp(Timestamp::NegInfinity),
        ]))
        .unwrap();
        let bytes = row.encode_to_vec();
        for value in ["infinity", "24:00:00", "-infinity"] {
            assert!(bytes.windows(value.len()).any(|bytes| bytes == value.as_bytes()));
        }
    }

    /// Encoder and writer-descriptor agreement for every supported source
    /// type.
    ///
    /// BigQuery decodes each appended row with the writer descriptor built by
    /// [`column_schemas_to_table_descriptor`], not with the encoder's intent. A
    /// field whose number, wire type, or integer width disagrees with its
    /// descriptor entry is rejected or, worse, silently reinterpreted (an
    /// over-long varint read as `int32` keeps only the low 32 bits). The
    /// encoder, descriptor, and DDL mappings are maintained by hand, so these
    /// properties decode every generated row the way BigQuery would and require
    /// the original cell back.
    mod wire_properties {
        use std::{collections::BTreeMap, sync::Arc};

        use chrono::{DateTime, Datelike, NaiveDate, NaiveTime};
        use etl::{
            data::ArrayCell,
            error::ErrorKind,
            schema::{ColumnSchema, ReplicatedTableSchema, TableId, TableName, TableSchema, Type},
            test_utils::{
                property::{
                    any_f32, any_f64, f32_matches, f64_matches, pg_text, pg_time, run_property,
                },
                replication_stream::parse_text_cell,
            },
        };
        use gcp_bigquery_client::storage::{ColumnMode, ColumnType, FieldDescriptor};
        use proptest::{
            collection::{btree_map, vec},
            option,
            prelude::*,
            strategy::Union,
        };
        use prost::{
            bytes::Buf,
            encoding::{WireType, decode_key, decode_varint},
        };

        use super::*;
        use crate::bigquery::{
            BIGQUERY_COLUMN_NAME_MAPPING, schema::column_schemas_to_table_descriptor,
        };

        /// Scalar source types with a dedicated text-codec mapping, plus `text`
        /// for the string fallback every other type uses.
        const SCALAR_TYPES: &[Type] = &[
            Type::BOOL,
            Type::INT2,
            Type::INT4,
            Type::INT8,
            Type::FLOAT4,
            Type::FLOAT8,
            Type::NUMERIC,
            Type::BYTEA,
            Type::DATE,
            Type::TIME,
            Type::TIMETZ,
            Type::TIMESTAMP,
            Type::TIMESTAMPTZ,
            Type::UUID,
            Type::JSON,
            Type::JSONB,
            Type::OID,
            Type::TEXT,
        ];

        /// Array source types paired with their element types.
        const ARRAY_TYPES: &[(Type, Type)] = &[
            (Type::BOOL_ARRAY, Type::BOOL),
            (Type::INT2_ARRAY, Type::INT2),
            (Type::INT4_ARRAY, Type::INT4),
            (Type::INT8_ARRAY, Type::INT8),
            (Type::FLOAT4_ARRAY, Type::FLOAT4),
            (Type::FLOAT8_ARRAY, Type::FLOAT8),
            (Type::NUMERIC_ARRAY, Type::NUMERIC),
            (Type::BYTEA_ARRAY, Type::BYTEA),
            (Type::DATE_ARRAY, Type::DATE),
            (Type::TIME_ARRAY, Type::TIME),
            (Type::TIMETZ_ARRAY, Type::TIMETZ),
            (Type::TIMESTAMP_ARRAY, Type::TIMESTAMP),
            (Type::TIMESTAMPTZ_ARRAY, Type::TIMESTAMPTZ),
            (Type::UUID_ARRAY, Type::UUID),
            (Type::JSON_ARRAY, Type::JSON),
            (Type::JSONB_ARRAY, Type::JSONB),
            (Type::OID_ARRAY, Type::OID),
            (Type::TEXT_ARRAY, Type::TEXT),
            (Type::VARCHAR_ARRAY, Type::VARCHAR),
        ];

        /// Sequence number appended to every generated row.
        const SEQUENCE_NUMBER: &str = "0000000000000001/0000000000000000/0000000000000000";

        /// Maximum timezone offset Postgres accepts, exclusive, in seconds.
        const MAX_OFFSET_SECONDS: i32 = 16 * 60 * 60;

        /// One generated column: its source type, Postgres text output (or
        /// `None` for SQL NULL), and declared nullability.
        type GeneratedColumn = (Type, Option<String>, bool);

        /// One raw protobuf field payload, split by wire type.
        #[derive(Debug)]
        enum Payload {
            /// A varint-encoded scalar.
            Varint(u64),
            /// A little-endian 32-bit scalar.
            Fixed32(u32),
            /// A little-endian 64-bit scalar.
            Fixed64(u64),
            /// A length-delimited payload: string, bytes, or a packed run.
            Delimited(Vec<u8>),
        }

        /// Returns the element type of a supported array type.
        fn array_element_type(typ: &Type) -> Option<&'static Type> {
            ARRAY_TYPES.iter().find(|(array, _)| array == typ).map(|(_, element)| element)
        }

        /// Renders a calendar date as Postgres prints it, with an era suffix
        /// for years before 1 AD.
        fn render_date(date: NaiveDate) -> (String, &'static str) {
            let year = date.year();
            let (year, era) = if year <= 0 { (1 - year, " BC") } else { (year, "") };

            (format!("{year:04}-{:02}-{:02}", date.month(), date.day()), era)
        }

        /// Renders a UTC offset as Postgres prints it: `+HH`, `+HH:MM`, or
        /// `+HH:MM:SS`.
        fn render_offset(seconds: i32) -> String {
            let sign = if seconds < 0 { '-' } else { '+' };
            let seconds = seconds.unsigned_abs();
            let (hours, minutes, seconds) = (seconds / 3600, seconds / 60 % 60, seconds % 60);

            match (minutes, seconds) {
                (0, 0) => format!("{sign}{hours:02}"),
                (_, 0) => format!("{sign}{hours:02}:{minutes:02}"),
                _ => format!("{sign}{hours:02}:{minutes:02}:{seconds:02}"),
            }
        }

        /// Renders a time of day as Postgres prints it.
        fn render_time(time: NaiveTime) -> String {
            time.format("%H:%M:%S%.f").to_string()
        }

        /// Calendar dates inside both the Postgres and chrono ranges, biased
        /// toward BigQuery's 0001-9999 range.
        ///
        /// Postgres starts at 4713-11-24 BC. Its upper bound is beyond
        /// chrono's, so the generator stops two days short of chrono's maximum
        /// to leave room for UTC offsets.
        fn pg_date() -> impl Strategy<Value = NaiveDate> {
            let day = |date: NaiveDate| date.num_days_from_ce();
            let postgres_min = NaiveDate::from_ymd_opt(-4712, 11, 24).unwrap();
            let chrono_max = NaiveDate::MAX.pred_opt().unwrap().pred_opt().unwrap();
            let common_min = NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
            let common_max = NaiveDate::from_ymd_opt(9999, 12, 31).unwrap();

            prop_oneof![
                3 => day(common_min)..=day(common_max),
                1 => day(postgres_min)..=day(chrono_max),
            ]
            .prop_map(|days| NaiveDate::from_num_days_from_ce_opt(days).unwrap())
        }

        /// UTC offsets inside the Postgres limit, biased toward whole hours.
        fn pg_offset_seconds() -> impl Strategy<Value = i32> {
            prop_oneof![
                2 => (-15i32..=15).prop_map(|hours| hours * 3600),
                1 => (1 - MAX_OFFSET_SECONDS)..MAX_OFFSET_SECONDS,
            ]
        }

        /// Arbitrary JSON documents, with numbers kept as their source text.
        fn json_value() -> impl Strategy<Value = serde_json::Value> {
            let number = "-?(0|[1-9][0-9]{0,20})(\\.[0-9]{1,10})?([eE][+-]?[0-9]{1,3})?"
                .prop_map(|number| serde_json::Value::Number(number.parse().unwrap()));
            let leaf = prop_oneof![
                Just(serde_json::Value::Null),
                any::<bool>().prop_map(serde_json::Value::Bool),
                number,
                pg_text().prop_map(serde_json::Value::String),
            ];

            leaf.prop_recursive(3, 16, 4, |inner| {
                prop_oneof![
                    vec(inner.clone(), 0..4).prop_map(serde_json::Value::Array),
                    btree_map(pg_text(), inner, 0..4)
                        .prop_map(|map| serde_json::Value::Object(map.into_iter().collect())),
                ]
            })
        }

        /// Adds the `infinity` and `-infinity` literals to finite temporal
        /// text.
        fn with_infinities(
            finite: impl Strategy<Value = String> + 'static,
        ) -> BoxedStrategy<String> {
            prop_oneof![
                8 => finite,
                1 => Just("infinity".to_owned()),
                1 => Just("-infinity".to_owned()),
            ]
            .boxed()
        }

        /// Renders a numeric literal as Postgres prints it, which has no
        /// negative zero.
        fn render_numeric(sign: &str, integer: &str, fraction: Option<&str>) -> String {
            let zero =
                integer.bytes().chain(fraction.unwrap_or("").bytes()).all(|digit| digit == b'0');
            let sign = if zero { "" } else { sign };

            match fraction {
                Some(fraction) => format!("{sign}{integer}.{fraction}"),
                None => format!("{sign}{integer}"),
            }
        }

        /// Postgres `numeric` output, including the special values.
        fn numeric_text() -> BoxedStrategy<String> {
            let finite = ("-?", "0|[1-9][0-9]{0,40}", option::of("[0-9]{1,40}")).prop_map(
                |(sign, integer, fraction)| render_numeric(&sign, &integer, fraction.as_deref()),
            );

            prop_oneof![
                8 => finite,
                1 => Just("NaN".to_owned()),
                1 => Just("Infinity".to_owned()),
                1 => Just("-Infinity".to_owned()),
            ]
            .boxed()
        }

        /// Postgres text output for one value of a scalar type.
        fn scalar_text(typ: &Type) -> BoxedStrategy<String> {
            match *typ {
                Type::BOOL => prop_oneof![Just("t"), Just("f")].prop_map(str::to_owned).boxed(),
                Type::INT2 => any::<i16>().prop_map(|value| value.to_string()).boxed(),
                Type::INT4 => any::<i32>().prop_map(|value| value.to_string()).boxed(),
                Type::INT8 => any::<i64>().prop_map(|value| value.to_string()).boxed(),
                Type::OID => any::<u32>().prop_map(|value| value.to_string()).boxed(),
                Type::FLOAT4 => any_f32()
                    .prop_map(|value| render_float(f64::from(value), || format!("{value:?}")))
                    .boxed(),
                Type::FLOAT8 => {
                    any_f64().prop_map(|value| render_float(value, || format!("{value:?}"))).boxed()
                }
                Type::NUMERIC => numeric_text(),
                Type::BYTEA => vec(any::<u8>(), 0..16)
                    .prop_map(|bytes| {
                        let hex = bytes.iter().map(|byte| format!("{byte:02x}"));
                        format!("\\x{}", hex.collect::<String>())
                    })
                    .boxed(),
                Type::DATE => with_infinities(pg_date().prop_map(|date| {
                    let (date, era) = render_date(date);
                    format!("{date}{era}")
                })),
                Type::TIME => prop_oneof![
                    16 => pg_time().prop_map(render_time),
                    1 => Just("24:00:00".to_owned()),
                ]
                .boxed(),
                Type::TIMETZ => (pg_time(), pg_offset_seconds())
                    .prop_map(|(time, offset)| {
                        format!("{}{}", render_time(time), render_offset(offset))
                    })
                    .boxed(),
                Type::TIMESTAMP => {
                    with_infinities((pg_date(), pg_time()).prop_map(|(date, time)| {
                        let (date, era) = render_date(date);
                        format!("{date} {}{era}", render_time(time))
                    }))
                }
                Type::TIMESTAMPTZ => with_infinities(
                    (pg_date(), pg_time(), pg_offset_seconds()).prop_map(|(date, time, offset)| {
                        let (date, era) = render_date(date);
                        format!("{date} {}{}{era}", render_time(time), render_offset(offset))
                    }),
                ),
                Type::UUID => {
                    any::<u128>().prop_map(|value| uuid::Uuid::from_u128(value).to_string()).boxed()
                }
                Type::JSON | Type::JSONB => {
                    json_value().prop_map(|value| value.to_string()).boxed()
                }
                _ => pg_text().boxed(),
            }
        }

        /// Renders a float as Postgres prints it, using `finite` for finite
        /// values.
        fn render_float(value: f64, finite: impl FnOnce() -> String) -> String {
            if value.is_nan() {
                "NaN".to_owned()
            } else if value == f64::INFINITY {
                "Infinity".to_owned()
            } else if value == f64::NEG_INFINITY {
                "-Infinity".to_owned()
            } else {
                finite()
            }
        }

        /// Postgres array-literal output for a one-dimensional array, quoting
        /// every element so element text never needs Postgres's own quoting
        /// rules.
        fn array_text(element: &Type) -> BoxedStrategy<String> {
            vec(option::weighted(0.95, scalar_text(element)), 0..4)
                .prop_map(|elements| {
                    let mut text = String::from("{");
                    for (index, element) in elements.iter().enumerate() {
                        if index > 0 {
                            text.push(',');
                        }
                        let Some(element) = element else {
                            text.push_str("NULL");
                            continue;
                        };
                        text.push('"');
                        for ch in element.chars() {
                            if matches!(ch, '"' | '\\') {
                                text.push('\\');
                            }
                            text.push(ch);
                        }
                        text.push('"');
                    }
                    text.push('}');
                    text
                })
                .boxed()
        }

        /// One column of any supported type, with optional NULL and a
        /// nullability flag.
        fn generated_column() -> impl Strategy<Value = GeneratedColumn> {
            let scalars = SCALAR_TYPES
                .iter()
                .map(|typ| (Just(typ.clone()), option::weighted(0.9, scalar_text(typ))).boxed());
            let arrays = ARRAY_TYPES.iter().map(|(array, element)| {
                (Just(array.clone()), option::weighted(0.9, array_text(element))).boxed()
            });

            (Union::new(scalars.chain(arrays)), any::<bool>())
                .prop_map(|((typ, text), nullable)| (typ, text, nullable))
        }

        /// Builds a replicated schema whose column `i` has the generated type
        /// and is nullable when declared so or when the value is NULL.
        fn schema_for(columns: &[GeneratedColumn]) -> ReplicatedTableSchema {
            let column_schemas = columns
                .iter()
                .enumerate()
                .map(|(index, (typ, text, nullable))| {
                    ColumnSchema::new(
                        format!("c{index}"),
                        typ.clone(),
                        -1,
                        index as i32 + 1,
                        *nullable || text.is_none(),
                    )
                })
                .collect();

            ReplicatedTableSchema::all(Arc::new(TableSchema::new(
                TableId::new(1),
                TableName::new("public".to_owned(), "wire".to_owned()),
                column_schemas,
            )))
        }

        /// Returns the error kind the encoder must report for a cell it cannot
        /// represent, in encoding order.
        fn expected_encoding_error(cell: &Cell) -> Option<ErrorKind> {
            /// Classifies one timestamp with time zone value.
            fn timestamptz(value: &Timestamp<DateTime<Utc>>) -> Option<ErrorKind> {
                matches!(value, Timestamp::PosInfinity | Timestamp::NegInfinity)
                    .then_some(ErrorKind::ConversionError)
            }

            let null_element = ErrorKind::NullValuesNotSupportedInArrayInDestination;
            match cell {
                Cell::TimestampTz(value) => timestamptz(value),
                Cell::Array(ArrayCell::TimestampTz(values)) => values
                    .iter()
                    .find_map(|value| value.as_ref().map_or(Some(null_element), timestamptz)),
                Cell::Array(array) => array_elements(array).contains(&None).then_some(null_element),
                _ => None,
            }
        }

        /// Splits an array cell into scalar cells, keeping NULL elements as
        /// `None`.
        fn array_elements(array: &ArrayCell) -> Vec<Option<Cell>> {
            /// Maps every element through a scalar cell constructor.
            fn map<T: Clone>(values: &[Option<T>], cell: fn(T) -> Cell) -> Vec<Option<Cell>> {
                values.iter().map(|value| value.clone().map(cell)).collect()
            }

            match array {
                ArrayCell::Bool(values) => map(values, Cell::Bool),
                ArrayCell::String(values) => map(values, Cell::String),
                ArrayCell::I16(values) => map(values, Cell::I16),
                ArrayCell::I32(values) => map(values, Cell::I32),
                ArrayCell::U32(values) => map(values, Cell::U32),
                ArrayCell::I64(values) => map(values, Cell::I64),
                ArrayCell::F32(values) => map(values, Cell::F32),
                ArrayCell::F64(values) => map(values, Cell::F64),
                ArrayCell::Numeric(values) => map(values, Cell::Numeric),
                ArrayCell::Date(values) => map(values, Cell::Date),
                ArrayCell::Time(values) => map(values, Cell::Time),
                ArrayCell::TimeTz(values) => map(values, Cell::TimeTz),
                ArrayCell::Timestamp(values) => map(values, Cell::Timestamp),
                ArrayCell::TimestampTz(values) => map(values, Cell::TimestampTz),
                ArrayCell::Uuid(values) => map(values, Cell::Uuid),
                ArrayCell::Json(values) => map(values, Cell::Json),
                ArrayCell::Bytes(values) => map(values, Cell::Bytes),
            }
        }

        /// Compares cells, treating floats bit-for-bit and every NaN as equal.
        fn cells_match(expected: &Cell, actual: &Cell) -> bool {
            match (expected, actual) {
                (Cell::F32(expected), Cell::F32(actual)) => f32_matches(*expected, *actual),
                (Cell::F64(expected), Cell::F64(actual)) => f64_matches(*expected, *actual),
                _ => expected == actual,
            }
        }

        /// Splits an encoded row into raw fields with prost's wire decoder.
        fn decode_fields(mut buf: &[u8]) -> Result<Vec<(u32, Payload)>, TestCaseError> {
            let mut fields = Vec::new();
            while !buf.is_empty() {
                let (tag, wire_type) = decode_key(&mut buf)
                    .map_err(|err| TestCaseError::fail(format!("invalid key: {err}")))?;
                let payload = match wire_type {
                    WireType::Varint => Payload::Varint(decode_varint(&mut buf).unwrap()),
                    WireType::ThirtyTwoBit => Payload::Fixed32(buf.get_u32_le()),
                    WireType::SixtyFourBit => Payload::Fixed64(buf.get_u64_le()),
                    WireType::LengthDelimited => {
                        let len = usize::try_from(decode_varint(&mut buf).unwrap()).unwrap();
                        prop_assert!(len <= buf.len(), "field {tag} overruns the row");
                        let (payload, rest) = buf.split_at(len);
                        buf = rest;
                        Payload::Delimited(payload.to_vec())
                    }
                    other => {
                        return Err(TestCaseError::fail(format!(
                            "field {tag} uses unsupported wire type {other:?}"
                        )));
                    }
                };
                fields.push((tag, payload));
            }

            Ok(fields)
        }

        /// Returns whether a payload's wire type is what a protobuf parser
        /// accepts for the descriptor field, including packed repeated
        /// scalars.
        fn wire_type_matches(field: &FieldDescriptor, payload: &Payload) -> bool {
            let repeated = matches!(field.mode, ColumnMode::Repeated);
            match (field.typ, payload) {
                (ColumnType::Bool | ColumnType::Int32 | ColumnType::Int64, Payload::Varint(_))
                | (ColumnType::Float, Payload::Fixed32(_))
                | (ColumnType::Double, Payload::Fixed64(_))
                | (ColumnType::String | ColumnType::Bytes, Payload::Delimited(_)) => true,
                (
                    ColumnType::Bool | ColumnType::Int32 | ColumnType::Int64,
                    Payload::Delimited(_),
                ) => repeated,
                (ColumnType::Float, Payload::Delimited(bytes)) => repeated && bytes.len() % 4 == 0,
                (ColumnType::Double, Payload::Delimited(bytes)) => repeated && bytes.len() % 8 == 0,
                _ => false,
            }
        }

        /// One decoded element before it is given a source type.
        enum Element {
            /// A varint read as the descriptor's integer type would read it.
            Varint(u64),
            /// A `float` value.
            Float(f32),
            /// A `double` value.
            Double(f64),
            /// A string or bytes payload.
            Bytes(Vec<u8>),
        }

        /// Splits a field's payloads into elements, unpacking packed runs.
        fn elements(field: &FieldDescriptor, payloads: &[&Payload]) -> Vec<Element> {
            let mut elements = Vec::new();
            for payload in payloads {
                match (field.typ, payload) {
                    (_, Payload::Varint(value)) => elements.push(Element::Varint(*value)),
                    (_, Payload::Fixed32(bits)) => {
                        elements.push(Element::Float(f32::from_bits(*bits)));
                    }
                    (_, Payload::Fixed64(bits)) => {
                        elements.push(Element::Double(f64::from_bits(*bits)));
                    }
                    (ColumnType::String | ColumnType::Bytes, Payload::Delimited(bytes)) => {
                        elements.push(Element::Bytes(bytes.clone()));
                    }
                    (ColumnType::Float, Payload::Delimited(bytes)) => {
                        elements.extend(bytes.chunks_exact(4).map(|chunk| {
                            Element::Float(f32::from_le_bytes(chunk.try_into().unwrap()))
                        }));
                    }
                    (ColumnType::Double, Payload::Delimited(bytes)) => {
                        elements.extend(bytes.chunks_exact(8).map(|chunk| {
                            Element::Double(f64::from_le_bytes(chunk.try_into().unwrap()))
                        }));
                    }
                    (_, Payload::Delimited(bytes)) => {
                        let mut bytes = bytes.as_slice();
                        while !bytes.is_empty() {
                            elements.push(Element::Varint(decode_varint(&mut bytes).unwrap()));
                        }
                    }
                }
            }

            elements
        }

        /// Rewrites astronomical year numbering (`-0043-03-15`) into the
        /// Postgres era form (`0044-03-15 BC`) so the etl text parser can read
        /// the encoder's documented date format back.
        fn astronomical_to_era(value: &str) -> String {
            let (negative, rest) =
                value.strip_prefix('-').map_or((false, value), |rest| (true, rest));
            let Some((year, tail)) = rest.split_once('-') else {
                return value.to_owned();
            };
            let Ok(year) = year.parse::<i64>() else {
                return value.to_owned();
            };
            let year = if negative { -year } else { year };

            if year <= 0 {
                format!("{:04}-{tail} BC", 1 - year)
            } else {
                format!("{year:04}-{tail}")
            }
        }

        /// Reads one element the way BigQuery reads the descriptor type, then
        /// gives it the source element type.
        fn element_cell(
            field: &FieldDescriptor,
            typ: &Type,
            element: Element,
        ) -> Result<Cell, String> {
            let cell = match (field.typ, typ, element) {
                (ColumnType::Bool, &Type::BOOL, Element::Varint(value)) => Cell::Bool(value != 0),
                (ColumnType::Int32 | ColumnType::Int64, typ, Element::Varint(value)) => {
                    // Protobuf `int32` keeps the low 32 bits of the varint and
                    // `int64` all 64; the source type then decides whether the
                    // value fits.
                    let value = if matches!(field.typ, ColumnType::Int32) {
                        i64::from(value as i32)
                    } else {
                        value as i64
                    };
                    let out_of_range = || format!("{value} is out of range for {typ}");
                    match *typ {
                        Type::INT2 => Cell::I16(i16::try_from(value).map_err(|_| out_of_range())?),
                        Type::INT4 => Cell::I32(i32::try_from(value).map_err(|_| out_of_range())?),
                        Type::INT8 => Cell::I64(value),
                        Type::OID => Cell::U32(u32::try_from(value).map_err(|_| out_of_range())?),
                        Type::TIMESTAMPTZ => Cell::TimestampTz(Timestamp::Value(
                            DateTime::from_timestamp_micros(value).ok_or_else(out_of_range)?,
                        )),
                        _ => {
                            return Err(format!(
                                "descriptor type {:?} cannot carry {typ}",
                                field.typ
                            ));
                        }
                    }
                }
                (ColumnType::Float, &Type::FLOAT4, Element::Float(value)) => Cell::F32(value),
                (ColumnType::Double, &Type::FLOAT8, Element::Double(value)) => Cell::F64(value),
                (ColumnType::Bytes, &Type::BYTEA, Element::Bytes(bytes)) => Cell::Bytes(bytes),
                (ColumnType::String, typ, Element::Bytes(bytes)) => {
                    let text =
                        String::from_utf8(bytes).map_err(|err| format!("invalid utf-8: {err}"))?;
                    let text = if matches!(*typ, Type::DATE | Type::TIMESTAMP) {
                        astronomical_to_era(&text)
                    } else {
                        text
                    };
                    parse_text_cell(typ, &text)
                        .map_err(|err| format!("{text:?} does not re-parse as {typ}: {err}"))?
                }
                (column_type, typ, _) => {
                    return Err(format!("descriptor type {column_type:?} cannot carry {typ}"));
                }
            };

            Ok(cell)
        }

        /// Checks one encoded row against a writer descriptor and decodes each
        /// source column back to its cell.
        fn check_row_against_descriptor(
            schema: &ReplicatedTableSchema,
            cells: &[Cell],
            fields: &[(u32, Payload)],
            allow_missing_columns: bool,
        ) -> Result<(), TestCaseError> {
            let descriptor = column_schemas_to_table_descriptor(schema, allow_missing_columns);
            let column_count = cells.len();
            prop_assert_eq!(descriptor.field_descriptors.len(), column_count + 2);

            let mut payloads_by_number: BTreeMap<u32, Vec<&Payload>> = BTreeMap::new();
            for (number, payload) in fields {
                let field =
                    descriptor.field_descriptors.iter().find(|field| field.number == *number);
                let Some(field) = field else {
                    return Err(TestCaseError::fail(format!(
                        "field {number} is not in the descriptor"
                    )));
                };
                prop_assert!(
                    wire_type_matches(field, payload),
                    "field {} ({:?} {:?}) carries {:?}",
                    number,
                    field.typ,
                    field.mode,
                    payload
                );
                payloads_by_number.entry(*number).or_default().push(payload);
            }

            for (index, field) in descriptor.field_descriptors.iter().enumerate() {
                prop_assert_eq!(field.number as usize, index + 1);
                let payloads = payloads_by_number.remove(&field.number).unwrap_or_default();
                let repeated = matches!(field.mode, ColumnMode::Repeated);
                prop_assert!(repeated || payloads.len() <= 1, "field {} repeats", field.number);
                prop_assert!(
                    !matches!(field.mode, ColumnMode::Required) || !payloads.is_empty(),
                    "required field {} is missing",
                    field.number
                );

                let Some(column) = schema.column_schemas().nth(index) else {
                    // CDC metadata columns are strings.
                    let expected = if index == column_count { "UPSERT" } else { SEQUENCE_NUMBER };
                    prop_assert!(matches!(field.typ, ColumnType::String));
                    let elements = elements(field, &payloads);
                    let value = match elements.as_slice() {
                        [Element::Bytes(bytes)] => bytes.as_slice(),
                        _ => &[],
                    };
                    prop_assert_eq!(value, expected.as_bytes());
                    continue;
                };
                prop_assert_eq!(&field.name, &BIGQUERY_COLUMN_NAME_MAPPING.map_name(&column.name));

                let element_type = array_element_type(&column.typ).unwrap_or(&column.typ);
                let decoded = elements(field, &payloads)
                    .into_iter()
                    .map(|element| element_cell(field, element_type, element))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|err| {
                        TestCaseError::fail(format!("column {index} ({}): {err}", column.typ))
                    })?;

                let expected = match &cells[index] {
                    Cell::Array(array) => array_elements(array).into_iter().flatten().collect(),
                    // BigQuery repeated fields cannot be NULL, so a NULL array
                    // is stored as an empty array by design.
                    Cell::Null => Vec::new(),
                    cell => vec![cell.clone()],
                };
                prop_assert!(
                    expected.len() == decoded.len()
                        && expected
                            .iter()
                            .zip(&decoded)
                            .all(|(expected, actual)| cells_match(expected, actual)),
                    "column {} ({}) encoded {:?} but BigQuery reads {:?}",
                    index,
                    column.typ,
                    cells[index],
                    decoded
                );
            }

            Ok(())
        }

        /// Every field of an encoded upsert row is declared by the writer
        /// descriptor with a matching wire type, and reading it as BigQuery
        /// does yields the original cell, for streaming and copy descriptors.
        #[test]
        fn encoded_rows_agree_with_writer_descriptor() {
            run_property(
                "bigquery_encoded_rows_agree_with_writer_descriptor",
                &vec(generated_column(), 1..5),
                |columns| {
                    let mut cells = Vec::with_capacity(columns.len());
                    for (typ, text, _) in columns {
                        let cell = match text {
                            Some(text) => parse_text_cell(typ, text).map_err(|err| {
                                TestCaseError::fail(format!("codec rejected {typ} {text:?}: {err}"))
                            })?,
                            None => Cell::Null,
                        };
                        cells.push(cell);
                    }
                    let schema = schema_for(columns);

                    let mut values = cells.clone();
                    values.push(Cell::String("UPSERT".to_owned()));
                    values.push(Cell::String(SEQUENCE_NUMBER.to_owned()));
                    let result = BigQueryTableRow::try_from(TableRow::new(values));

                    if let Some(kind) = cells.iter().find_map(expected_encoding_error) {
                        let Err(err) = result else {
                            return Err(TestCaseError::fail(format!(
                                "expected {kind:?} for {cells:?}"
                            )));
                        };
                        prop_assert_eq!(err.kind(), kind);
                        return Ok(());
                    }

                    let row = result.map_err(|err| TestCaseError::fail(format!("{err}")))?;
                    let bytes = row.encode_to_vec();
                    prop_assert_eq!(bytes.len(), row.encoded_len());
                    let fields = decode_fields(&bytes)?;

                    check_row_against_descriptor(&schema, &cells, &fields, true)?;
                    check_row_against_descriptor(&schema, &cells, &fields, false)
                },
            );
        }
    }
}
