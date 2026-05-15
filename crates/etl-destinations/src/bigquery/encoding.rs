use prost::bytes;

use crate::bigquery::value::{BigQueryArrayCell, BigQueryCell, BigQueryTableRow};

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
        for (tag, cell) in self.cells() {
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
        for (tag, cell) in self.cells() {
            len += cell_encode_len_prost(cell, *tag);
        }

        len
    }

    /// Clears all cell values in the table row by calling clear on each cell.
    fn clear(&mut self) {
        for (_, cell) in self.cells_mut() {
            cell.clear();
        }
    }
}

/// Encodes a single [`BigQueryCell`] into Protocol Buffer format.
fn cell_encode_prost(cell: &BigQueryCell, tag: u32, buf: &mut impl bytes::BufMut) {
    match cell {
        BigQueryCell::Null => {}
        BigQueryCell::Bool(value) => prost::encoding::bool::encode(tag, value, buf),
        BigQueryCell::String(value) => prost::encoding::string::encode(tag, value, buf),
        BigQueryCell::Int32(value) => prost::encoding::int32::encode(tag, value, buf),
        BigQueryCell::Int64(value) => prost::encoding::int64::encode(tag, value, buf),
        BigQueryCell::Float32(value) => prost::encoding::float::encode(tag, value, buf),
        BigQueryCell::Float64(value) => prost::encoding::double::encode(tag, value, buf),
        BigQueryCell::Bytes(value) => prost::encoding::bytes::encode(tag, value, buf),
        BigQueryCell::Array(value) => array_cell_encode_prost(value, tag, buf),
    }
}

/// Calculates the encoded length for a single [`BigQueryCell`].
fn cell_encode_len_prost(cell: &BigQueryCell, tag: u32) -> usize {
    match cell {
        BigQueryCell::Null => 0,
        BigQueryCell::Bool(value) => prost::encoding::bool::encoded_len(tag, value),
        BigQueryCell::String(value) => prost::encoding::string::encoded_len(tag, value),
        BigQueryCell::Int32(value) => prost::encoding::int32::encoded_len(tag, value),
        BigQueryCell::Int64(value) => prost::encoding::int64::encoded_len(tag, value),
        BigQueryCell::Float32(value) => prost::encoding::float::encoded_len(tag, value),
        BigQueryCell::Float64(value) => prost::encoding::double::encoded_len(tag, value),
        BigQueryCell::Bytes(value) => prost::encoding::bytes::encoded_len(tag, value),
        BigQueryCell::Array(value) => array_cell_encoded_len_prost(value, tag),
    }
}

/// Encodes a [`BigQueryArrayCell`] into Protocol Buffer format.
fn array_cell_encode_prost(array_cell: &BigQueryArrayCell, tag: u32, buf: &mut impl bytes::BufMut) {
    match array_cell {
        BigQueryArrayCell::Bool(values) => prost::encoding::bool::encode_packed(tag, values, buf),
        BigQueryArrayCell::String(values) => {
            prost::encoding::string::encode_repeated(tag, values, buf);
        }
        BigQueryArrayCell::Int32(values) => {
            prost::encoding::int32::encode_packed(tag, values, buf);
        }
        BigQueryArrayCell::Int64(values) => {
            prost::encoding::int64::encode_packed(tag, values, buf);
        }
        BigQueryArrayCell::Float32(values) => {
            prost::encoding::float::encode_packed(tag, values, buf);
        }
        BigQueryArrayCell::Float64(values) => {
            prost::encoding::double::encode_packed(tag, values, buf);
        }
        BigQueryArrayCell::Bytes(values) => {
            prost::encoding::bytes::encode_repeated(tag, values, buf);
        }
    }
}

/// Calculates the encoded length for a [`BigQueryArrayCell`].
fn array_cell_encoded_len_prost(array_cell: &BigQueryArrayCell, tag: u32) -> usize {
    match array_cell {
        BigQueryArrayCell::Bool(values) => prost::encoding::bool::encoded_len_packed(tag, values),
        BigQueryArrayCell::String(values) => {
            prost::encoding::string::encoded_len_repeated(tag, values)
        }
        BigQueryArrayCell::Int32(values) => prost::encoding::int32::encoded_len_packed(tag, values),
        BigQueryArrayCell::Int64(values) => prost::encoding::int64::encoded_len_packed(tag, values),
        BigQueryArrayCell::Float32(values) => {
            prost::encoding::float::encoded_len_packed(tag, values)
        }
        BigQueryArrayCell::Float64(values) => {
            prost::encoding::double::encoded_len_packed(tag, values)
        }
        BigQueryArrayCell::Bytes(values) => {
            prost::encoding::bytes::encoded_len_repeated(tag, values)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use etl::{
        error::{ErrorKind, EtlError},
        materialization::DestinationTypeCompatibility,
        types::{Cell, PgNumeric, Type},
    };

    use super::*;
    use crate::bigquery::materialization::BigQueryMaterialization;

    fn typed_row(
        cells: impl IntoIterator<Item = (Type, Cell)>,
        compatibility: DestinationTypeCompatibility,
    ) -> Result<BigQueryTableRow, EtlError> {
        let materializer = BigQueryMaterialization::materializer(compatibility);
        BigQueryTableRow::try_from_typed_cells(cells, &materializer)
    }

    #[test]
    fn bigquery_table_row_try_from_valid() {
        let result = typed_row(
            [
                (Type::INT4, Cell::I32(42)),
                (Type::TEXT, Cell::String("test".to_owned())),
                (Type::BOOL, Cell::Bool(true)),
                (Type::TEXT, Cell::Null),
            ],
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn bigquery_table_row_try_from_rejects_numeric_nan() {
        let result = typed_row(
            [(Type::INT4, Cell::I32(42)), (Type::NUMERIC, Cell::Numeric(PgNumeric::NaN))],
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn bigquery_table_row_try_from_rejects_numeric_infinity() {
        let result = typed_row(
            [
                (Type::TEXT, Cell::String("valid".to_owned())),
                (Type::NUMERIC, Cell::Numeric(PgNumeric::PositiveInfinity)),
            ],
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn bigquery_table_row_try_from_rejects_json_number_outside_float_domain() {
        let result = BigQueryTableRow::try_from_typed_tagged_cells(
            [(1, Type::JSON, Cell::String(r#"{"value":1e309}"#.to_owned()))],
            &BigQueryMaterialization::materializer(DestinationTypeCompatibility::strict()),
        );
        assert!(result.is_err());
    }

    #[test]
    fn bigquery_table_row_try_from_rejects_json_integer_precision_loss() {
        let result = BigQueryTableRow::try_from_typed_tagged_cells(
            [(1, Type::JSON, Cell::String(r#"{"value":18446744073709551616}"#.to_owned()))],
            &BigQueryMaterialization::materializer(DestinationTypeCompatibility::strict()),
        );
        assert!(result.is_err());
    }

    #[test]
    fn bigquery_table_row_try_from_lossless_accepts_string_materialized_values() {
        let result = BigQueryTableRow::try_from_typed_tagged_cells(
            [
                (1, Type::JSON, Cell::String(r#"{"value":18446744073709551616}"#.to_owned())),
                (
                    2,
                    Type::NUMERIC,
                    Cell::Numeric(
                        PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap(),
                    ),
                ),
            ],
            &BigQueryMaterialization::materializer(DestinationTypeCompatibility::lossless()),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn bigquery_table_row_try_from_rejects_date_outside_bigquery_domain() {
        let invalid_date = NaiveDate::from_ymd_opt(1, 1, 1).unwrap().pred_opt().unwrap();

        let result = typed_row(
            [(Type::DATE, Cell::Date(invalid_date))],
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn bigquery_table_row_try_from_array_with_nulls() {
        let array_with_nulls = etl::types::ArrayCell::I32(vec![Some(1), None, Some(3)]);
        let result = typed_row(
            [(Type::INT4_ARRAY, Cell::Array(array_with_nulls))],
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NullValuesNotSupportedInArrayInDestination);
        assert!(err.detail().unwrap().contains("Cell at index 0 failed materialization"));
    }

    #[test]
    fn bigquery_table_row_try_from_array_with_numeric_rounding_risk() {
        let array_with_rounding_risk = etl::types::ArrayCell::Numeric(vec![
            Some(PgNumeric::from_str("123.456").unwrap()),
            Some(PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap()),
            Some(PgNumeric::from_str("789.012").unwrap()),
        ]);

        let result = typed_row(
            [(Type::NUMERIC_ARRAY, Cell::Array(array_with_rounding_risk))],
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
        let result = typed_row(
            [
                (Type::TEXT, Cell::String("prefix".to_owned())),
                (Type::INT4_ARRAY, Cell::Array(valid_array)),
                (Type::TEXT, Cell::String("suffix".to_owned())),
            ],
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn bigquery_table_row_try_from_multiple_errors_first_wins() {
        let result = typed_row(
            [
                (
                    Type::NUMERIC,
                    Cell::Numeric(
                        PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap(),
                    ),
                ),
                (
                    Type::NUMERIC_ARRAY,
                    Cell::Array(etl::types::ArrayCell::Numeric(vec![Some(
                        PgNumeric::from_str("0.000000000000000000000000000000000000002").unwrap(),
                    )])),
                ),
            ],
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

        let result = typed_row(
            [
                (Type::DATE, Cell::Date(valid_date)),
                (Type::TIME, Cell::Time(valid_time)),
                (Type::TIMESTAMP, Cell::Timestamp(valid_datetime)),
            ],
            DestinationTypeCompatibility::default(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn bigquery_table_row_try_from_numeric_rounding_risk_fails() {
        let over_scale_numeric =
            PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap();

        let result = typed_row(
            [(Type::NUMERIC, Cell::Numeric(over_scale_numeric))],
            DestinationTypeCompatibility::strict(),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnsupportedValueInDestination);
        assert!(err.detail().unwrap().contains("would be rounded by BigQuery"));
    }

    #[test]
    fn bigquery_table_row_try_from_uses_default_lossy_materialization() {
        let result = BigQueryTableRow::try_from_typed_tagged_cells(
            [
                (
                    1,
                    Type::NUMERIC,
                    Cell::Numeric(
                        PgNumeric::from_str("0.000000000000000000000000000000000000001").unwrap(),
                    ),
                ),
                (2, Type::JSON, Cell::String(r#"{"value":18446744073709551616}"#.to_owned())),
                (3, Type::FLOAT8, Cell::F64(-0.0)),
            ],
            &BigQueryMaterialization::materializer(DestinationTypeCompatibility::default()),
        );

        assert!(result.is_ok());
    }
}
