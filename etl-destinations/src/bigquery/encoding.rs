use etl::error::{EtlError, EtlResult};
use etl::types::{ArrayCellNonOptional, CellNonOptional, TableRow};
use prost::bytes;

/// Date format string for BigQuery DATE columns (YYYY-MM-DD).
const DATE_FORMAT: &str = "%Y-%m-%d";

/// Time format string for BigQuery TIME columns (HH:MM:SS.sss).
const TIME_FORMAT: &str = "%H:%M:%S%.f";

/// Timestamp format string for BigQuery TIMESTAMP columns (YYYY-MM-DD HH:MM:SS.sss).
const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f";

/// Timestamp with timezone format string for BigQuery TIMESTAMPTZ columns (YYYY-MM-DD HH:MM:SS.sss+TZ).
const TIMESTAMPTZ_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f%:z";

/// Protocol buffer wrapper for a BigQuery table row containing non-optional cells.
///
/// Wraps a vector of [`CellNonOptional`] values and implements the [`prost::Message`]
/// trait to enable Protocol Buffer serialization for BigQuery streaming inserts.
#[derive(Debug)]
pub struct BigQueryTableRow(Vec<CellNonOptional>);

impl TryFrom<TableRow> for BigQueryTableRow {
    type Error = EtlError;

    /// Converts a [`TableRow`] to a [`BigQueryTableRow`] by transforming all cell values
    /// to their non-optional equivalents.
    ///
    /// Returns an error if any cell conversion fails during the transformation process.
    fn try_from(value: TableRow) -> Result<Self, Self::Error> {
        let table_rows = value
            .values
            .into_iter()
            .map(CellNonOptional::try_from)
            .collect::<EtlResult<Vec<_>>>()?;

        Ok(BigQueryTableRow(table_rows))
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
        CellNonOptional::TimeStamp(t) => {
            let s = t.format(TIMESTAMP_FORMAT).to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        CellNonOptional::TimeStampTz(t) => {
            let s = t.format(TIMESTAMPTZ_FORMAT).to_string();
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
        CellNonOptional::TimeStamp(t) => {
            let s = t.format(TIMESTAMP_FORMAT).to_string();
            prost::encoding::string::encoded_len(tag, &s)
        }
        CellNonOptional::TimeStampTz(t) => {
            let s = t.format(TIMESTAMPTZ_FORMAT).to_string();
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
        ArrayCellNonOptional::TimeStamp(vec) => {
            let values: Vec<String> = vec
                .into_iter()
                .map(|v| v.format(TIMESTAMP_FORMAT).to_string())
                .collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        }
        ArrayCellNonOptional::TimeStampTz(vec) => {
            let values: Vec<String> = vec
                .into_iter()
                .map(|v| v.format(TIMESTAMPTZ_FORMAT).to_string())
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
        ArrayCellNonOptional::TimeStamp(vec) => {
            let values: Vec<String> = vec
                .into_iter()
                .map(|v| v.format(TIMESTAMP_FORMAT).to_string())
                .collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        }
        ArrayCellNonOptional::TimeStampTz(vec) => {
            let values: Vec<String> = vec
                .into_iter()
                .map(|v| v.format(TIMESTAMPTZ_FORMAT).to_string())
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
