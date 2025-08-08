use etl::types::{ArrayCell, Cell, TableRow};
use prost::bytes;

/// Wrapper around [`TableRow`] that implements protobuf encoding for BigQuery.
///
/// Enables serialization of table rows using the protobuf format required by
/// BigQuery's Storage Write API.
#[derive(Debug)]
pub struct BigQueryTableRow(pub TableRow);

impl prost::Message for BigQueryTableRow {
    /// Encodes the table row using protobuf format for BigQuery streaming.
    fn encode_raw(&self, buf: &mut impl bytes::BufMut)
    where
        Self: Sized,
    {
        let mut tag = 1;
        for cell in &self.0.values {
            cell_encode_prost(cell, tag, buf);
            tag += 1;
        }
    }

    /// Merges protobuf field data into the table row.
    ///
    /// Not implemented as decoding is not required for the BigQuery use case.
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

    /// Returns the encoded length of the table row in bytes.
    fn encoded_len(&self) -> usize {
        let mut len = 0;
        let mut tag = 1;
        for cell in &self.0.values {
            len += cell_encode_len_prost(cell, tag);
            tag += 1;
        }

        len
    }

    /// Clears all cell values in the table row.
    fn clear(&mut self) {
        for cell in &mut self.0.values {
            cell.clear();
        }
    }
}

/// Encodes a single [`Cell`] value using protobuf format.
///
/// Maps each cell type to its appropriate protobuf encoding method,
/// handling type conversion and formatting as needed for BigQuery.
pub fn cell_encode_prost(cell: &Cell, tag: u32, buf: &mut impl bytes::BufMut) {
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
            let s = t.format("%Y-%m-%d").to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        Cell::Time(t) => {
            let s = t.format("%H:%M:%S%.f").to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        Cell::TimeStamp(t) => {
            let s = t.format("%Y-%m-%d %H:%M:%S%.f").to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        Cell::TimeStampTz(t) => {
            let s = t.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        Cell::Uuid(u) => {
            let s = u.to_string();
            prost::encoding::string::encode(tag, &s, buf)
        }
        Cell::Json(j) => {
            let s = j.to_string();
            prost::encoding::string::encode(tag, &s, buf)
        }
        Cell::U32(i) => {
            prost::encoding::uint32::encode(tag, i, buf);
        }
        Cell::Bytes(b) => {
            prost::encoding::bytes::encode(tag, b, buf);
        }
        Cell::Array(a) => {
            array_cell_encode_prost(a.clone(), tag, buf);
        }
    }
}

/// Returns the encoded length of a [`Cell`] in protobuf format.
pub fn cell_encode_len_prost(cell: &Cell, tag: u32) -> usize {
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
            let s = t.format("%Y-%m-%d").to_string();
            prost::encoding::string::encoded_len(tag, &s)
        }
        Cell::Time(t) => {
            let s = t.format("%H:%M:%S%.f").to_string();
            prost::encoding::string::encoded_len(tag, &s)
        }
        Cell::TimeStamp(t) => {
            let s = t.format("%Y-%m-%d %H:%M:%S%.f").to_string();
            prost::encoding::string::encoded_len(tag, &s)
        }
        Cell::TimeStampTz(t) => {
            let s = t.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string();
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
        Cell::U32(i) => prost::encoding::uint32::encoded_len(tag, i),
        Cell::Bytes(b) => prost::encoding::bytes::encoded_len(tag, b),
        Cell::Array(array_cell) => array_cell_encoded_len_prost(array_cell.clone(), tag),
    }
}

/// Encodes an [`ArrayCell`] using protobuf repeated field format.
///
/// Handles PostgreSQL array types by filtering out null values and encoding
/// the remaining elements using the appropriate protobuf repeated encoding.
pub fn array_cell_encode_prost(array_cell: ArrayCell, tag: u32, buf: &mut impl bytes::BufMut) {
    match array_cell {
        ArrayCell::Null => {}
        ArrayCell::Bool(mut vec) => {
            let vec: Vec<bool> = vec.drain(..).flatten().collect();
            prost::encoding::bool::encode_packed(tag, &vec, buf);
        }
        ArrayCell::String(mut vec) => {
            let vec: Vec<String> = vec.drain(..).flatten().collect();
            prost::encoding::string::encode_repeated(tag, &vec, buf);
        }
        ArrayCell::I16(mut vec) => {
            let vec: Vec<i32> = vec
                .drain(..)
                .filter(|v| v.is_some())
                .map(|v| v.unwrap() as i32)
                .collect();
            prost::encoding::int32::encode_packed(tag, &vec, buf);
        }
        ArrayCell::I32(mut vec) => {
            let vec: Vec<i32> = vec.drain(..).flatten().collect();
            prost::encoding::int32::encode_packed(tag, &vec, buf);
        }
        ArrayCell::U32(mut vec) => {
            let vec: Vec<u32> = vec.drain(..).flatten().collect();
            prost::encoding::uint32::encode_packed(tag, &vec, buf);
        }
        ArrayCell::I64(mut vec) => {
            let vec: Vec<i64> = vec.drain(..).flatten().collect();
            prost::encoding::int64::encode_packed(tag, &vec, buf);
        }
        ArrayCell::F32(mut vec) => {
            let vec: Vec<f32> = vec.drain(..).flatten().collect();
            prost::encoding::float::encode_packed(tag, &vec, buf);
        }
        ArrayCell::F64(mut vec) => {
            let vec: Vec<f64> = vec.drain(..).flatten().collect();
            prost::encoding::double::encode_packed(tag, &vec, buf);
        }
        ArrayCell::Numeric(mut vec) => {
            let vec: Vec<String> = vec
                .drain(..)
                .filter(|v| v.is_some())
                .map(|v| v.unwrap().to_string())
                .collect();
            prost::encoding::string::encode_repeated(tag, &vec, buf);
        }
        ArrayCell::Date(mut vec) => {
            let vec: Vec<String> = vec
                .drain(..)
                .filter(|v| v.is_some())
                .map(|v| v.unwrap().format("%Y-%m-%d").to_string())
                .collect();
            prost::encoding::string::encode_repeated(tag, &vec, buf);
        }
        ArrayCell::Time(mut vec) => {
            let vec: Vec<String> = vec
                .drain(..)
                .filter(|v| v.is_some())
                .map(|v| v.unwrap().format("%H:%M:%S%.f").to_string())
                .collect();
            prost::encoding::string::encode_repeated(tag, &vec, buf);
        }
        ArrayCell::TimeStamp(mut vec) => {
            let vec: Vec<String> = vec
                .drain(..)
                .filter(|v| v.is_some())
                .map(|v| v.unwrap().format("%Y-%m-%d %H:%M:%S%.f").to_string())
                .collect();
            prost::encoding::string::encode_repeated(tag, &vec, buf);
        }
        ArrayCell::TimeStampTz(mut vec) => {
            let vec: Vec<String> = vec
                .drain(..)
                .filter(|v| v.is_some())
                .map(|v| v.unwrap().format("%Y-%m-%d %H:%M:%S%.f%:z").to_string())
                .collect();
            prost::encoding::string::encode_repeated(tag, &vec, buf);
        }
        ArrayCell::Uuid(mut vec) => {
            let vec: Vec<String> = vec
                .drain(..)
                .filter(|v| v.is_some())
                .map(|v| v.unwrap().to_string())
                .collect();
            prost::encoding::string::encode_repeated(tag, &vec, buf);
        }
        ArrayCell::Json(mut vec) => {
            let vec: Vec<String> = vec
                .drain(..)
                .filter(|v| v.is_some())
                .map(|v| v.unwrap().to_string())
                .collect();
            prost::encoding::string::encode_repeated(tag, &vec, buf);
        }
        ArrayCell::Bytes(mut vec) => {
            let vec: Vec<Vec<u8>> = vec.drain(..).flatten().collect();
            prost::encoding::bytes::encode_repeated(tag, &vec, buf);
        }
    }
}

/// Returns the encoded length of an [`ArrayCell`] in protobuf format.
pub fn array_cell_encoded_len_prost(array_cell: ArrayCell, tag: u32) -> usize {
    match array_cell {
        ArrayCell::Null => 0,
        ArrayCell::Bool(mut vec) => {
            let vec: Vec<bool> = vec.drain(..).flatten().collect();
            prost::encoding::bool::encoded_len_packed(tag, &vec)
        }
        ArrayCell::String(mut vec) => {
            let vec: Vec<String> = vec.drain(..).flatten().collect();
            prost::encoding::string::encoded_len_repeated(tag, &vec)
        }
        ArrayCell::I16(mut vec) => {
            let vec: Vec<i32> = vec
                .drain(..)
                .filter(|v| v.is_some())
                .map(|v| v.unwrap() as i32)
                .collect();
            prost::encoding::int32::encoded_len_packed(tag, &vec)
        }
        ArrayCell::I32(mut vec) => {
            let vec: Vec<i32> = vec.drain(..).flatten().collect();
            prost::encoding::int32::encoded_len_packed(tag, &vec)
        }
        ArrayCell::U32(mut vec) => {
            let vec: Vec<u32> = vec.drain(..).flatten().collect();
            prost::encoding::uint32::encoded_len_packed(tag, &vec)
        }
        ArrayCell::I64(mut vec) => {
            let vec: Vec<i64> = vec.drain(..).flatten().collect();
            prost::encoding::int64::encoded_len_packed(tag, &vec)
        }
        ArrayCell::F32(mut vec) => {
            let vec: Vec<f32> = vec.drain(..).flatten().collect();
            prost::encoding::float::encoded_len_packed(tag, &vec)
        }
        ArrayCell::F64(mut vec) => {
            let vec: Vec<f64> = vec.drain(..).flatten().collect();
            prost::encoding::double::encoded_len_packed(tag, &vec)
        }
        ArrayCell::Numeric(mut vec) => {
            let vec: Vec<String> = vec
                .drain(..)
                .filter(|v| v.is_some())
                .map(|v| v.unwrap().to_string())
                .collect();
            prost::encoding::string::encoded_len_repeated(tag, &vec)
        }
        ArrayCell::Date(mut vec) => {
            let vec: Vec<String> = vec
                .drain(..)
                .filter(|v| v.is_some())
                .map(|v| v.unwrap().format("%Y-%m-%d").to_string())
                .collect();
            prost::encoding::string::encoded_len_repeated(tag, &vec)
        }
        ArrayCell::Time(mut vec) => {
            let vec: Vec<String> = vec
                .drain(..)
                .filter(|v| v.is_some())
                .map(|v| v.unwrap().format("%H:%M:%S%.f").to_string())
                .collect();
            prost::encoding::string::encoded_len_repeated(tag, &vec)
        }
        ArrayCell::TimeStamp(mut vec) => {
            let vec: Vec<String> = vec
                .drain(..)
                .filter(|v| v.is_some())
                .map(|v| v.unwrap().format("%Y-%m-%d %H:%M:%S%.f").to_string())
                .collect();
            prost::encoding::string::encoded_len_repeated(tag, &vec)
        }
        ArrayCell::TimeStampTz(mut vec) => {
            let vec: Vec<String> = vec
                .drain(..)
                .filter(|v| v.is_some())
                .map(|v| v.unwrap().format("%Y-%m-%d %H:%M:%S%.f%:z").to_string())
                .collect();
            prost::encoding::string::encoded_len_repeated(tag, &vec)
        }
        ArrayCell::Uuid(mut vec) => {
            let vec: Vec<String> = vec
                .drain(..)
                .filter(|v| v.is_some())
                .map(|v| v.unwrap().to_string())
                .collect();
            prost::encoding::string::encoded_len_repeated(tag, &vec)
        }
        ArrayCell::Json(mut vec) => {
            let vec: Vec<String> = vec
                .drain(..)
                .filter(|v| v.is_some())
                .map(|v| v.unwrap().to_string())
                .collect();
            prost::encoding::string::encoded_len_repeated(tag, &vec)
        }
        ArrayCell::Bytes(mut vec) => {
            let vec: Vec<Vec<u8>> = vec.drain(..).flatten().collect();
            prost::encoding::bytes::encoded_len_repeated(tag, &vec)
        }
    }
}
