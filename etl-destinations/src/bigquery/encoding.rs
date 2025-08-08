use etl::error::{EtlError, EtlResult};
use etl::types::{ArrayCellNonOptional, CellNonOptional, TableRow};
use prost::bytes;

#[derive(Debug)]
pub struct BigQueryTableRow(Vec<CellNonOptional>);

impl TryFrom<TableRow> for BigQueryTableRow {
    type Error = EtlError;

    fn try_from(value: TableRow) -> Result<Self, Self::Error> {
        let table_rows = value
            .values
            .into_iter()
            .map(|cell| CellNonOptional::try_from(cell))
            .collect::<EtlResult<Vec<_>>>()?;

        Ok(BigQueryTableRow(table_rows))
    }
}

impl prost::Message for BigQueryTableRow {
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

    fn encoded_len(&self) -> usize {
        let mut len = 0;
        let mut tag = 1;
        for cell in &self.0 {
            len += cell_encode_len_prost(cell, tag);
            tag += 1;
        }

        len
    }

    fn clear(&mut self) {
        for cell in &mut self.0 {
            cell.clear();
        }
    }
}

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
            let s = t.format("%Y-%m-%d").to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        CellNonOptional::Time(t) => {
            let s = t.format("%H:%M:%S%.f").to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        CellNonOptional::TimeStamp(t) => {
            let s = t.format("%Y-%m-%d %H:%M:%S%.f").to_string();
            prost::encoding::string::encode(tag, &s, buf);
        }
        CellNonOptional::TimeStampTz(t) => {
            let s = t.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string();
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
            let non_optional = ArrayCellNonOptional::try_from(a.clone())
                .expect("NULL values in arrays are not supported by BigQuery");
            array_cell_encode_prost(non_optional, tag, buf);
        }
    }
}

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
            let s = t.format("%Y-%m-%d").to_string();
            prost::encoding::string::encoded_len(tag, &s)
        }
        CellNonOptional::Time(t) => {
            let s = t.format("%H:%M:%S%.f").to_string();
            prost::encoding::string::encoded_len(tag, &s)
        }
        CellNonOptional::TimeStamp(t) => {
            let s = t.format("%Y-%m-%d %H:%M:%S%.f").to_string();
            prost::encoding::string::encoded_len(tag, &s)
        }
        CellNonOptional::TimeStampTz(t) => {
            let s = t.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string();
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
        CellNonOptional::Array(array_cell) => {
            let non_optional = ArrayCellNonOptional::try_from(array_cell.clone())
                .expect("NULL values in arrays are not supported by BigQuery");
            array_cell_non_optional_encoded_len_prost(non_optional, tag)
        }
    }
}

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
                .map(|v| v.format("%Y-%m-%d").to_string())
                .collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        }
        ArrayCellNonOptional::Time(vec) => {
            let values: Vec<String> = vec
                .into_iter()
                .map(|v| v.format("%H:%M:%S%.f").to_string())
                .collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        }
        ArrayCellNonOptional::TimeStamp(vec) => {
            let values: Vec<String> = vec
                .into_iter()
                .map(|v| v.format("%Y-%m-%d %H:%M:%S%.f").to_string())
                .collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        }
        ArrayCellNonOptional::TimeStampTz(vec) => {
            let values: Vec<String> = vec
                .into_iter()
                .map(|v| v.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string())
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
                .map(|v| v.format("%Y-%m-%d").to_string())
                .collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        }
        ArrayCellNonOptional::Time(vec) => {
            let values: Vec<String> = vec
                .into_iter()
                .map(|v| v.format("%H:%M:%S%.f").to_string())
                .collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        }
        ArrayCellNonOptional::TimeStamp(vec) => {
            let values: Vec<String> = vec
                .into_iter()
                .map(|v| v.format("%Y-%m-%d %H:%M:%S%.f").to_string())
                .collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        }
        ArrayCellNonOptional::TimeStampTz(vec) => {
            let values: Vec<String> = vec
                .into_iter()
                .map(|v| v.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string())
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
