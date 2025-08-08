use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use etl::types::{ArrayCell, Cell, PgNumeric, TableRow};
use prost::bytes;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub enum ArrayCellNonOptional {
    Null,
    Bool(Vec<bool>),
    String(Vec<String>),
    I16(Vec<i16>),
    I32(Vec<i32>),
    U32(Vec<u32>),
    I64(Vec<i64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Numeric(Vec<PgNumeric>),
    Date(Vec<NaiveDate>),
    Time(Vec<NaiveTime>),
    TimeStamp(Vec<NaiveDateTime>),
    TimeStampTz(Vec<DateTime<Utc>>),
    Uuid(Vec<Uuid>),
    Json(Vec<serde_json::Value>),
    Bytes(Vec<Vec<u8>>),
}

macro_rules! convert_array_variant {
    ($variant:ident, $vec:expr) => {
        if $vec.iter().any(|v| v.is_none()) {
            Err("NULL values in arrays are not supported by BigQuery")
        } else {
            Ok(ArrayCellNonOptional::$variant(
                $vec.into_iter().flatten().collect(),
            ))
        }
    };
}

impl TryFrom<ArrayCell> for ArrayCellNonOptional {
    type Error = &'static str;

    fn try_from(array_cell: ArrayCell) -> Result<Self, Self::Error> {
        match array_cell {
            ArrayCell::Null => Ok(ArrayCellNonOptional::Null),
            ArrayCell::Bool(vec) => convert_array_variant!(Bool, vec),
            ArrayCell::String(vec) => convert_array_variant!(String, vec),
            ArrayCell::I16(vec) => convert_array_variant!(I16, vec),
            ArrayCell::I32(vec) => convert_array_variant!(I32, vec),
            ArrayCell::U32(vec) => convert_array_variant!(U32, vec),
            ArrayCell::I64(vec) => convert_array_variant!(I64, vec),
            ArrayCell::F32(vec) => convert_array_variant!(F32, vec),
            ArrayCell::F64(vec) => convert_array_variant!(F64, vec),
            ArrayCell::Numeric(vec) => convert_array_variant!(Numeric, vec),
            ArrayCell::Date(vec) => convert_array_variant!(Date, vec),
            ArrayCell::Time(vec) => convert_array_variant!(Time, vec),
            ArrayCell::TimeStamp(vec) => convert_array_variant!(TimeStamp, vec),
            ArrayCell::TimeStampTz(vec) => convert_array_variant!(TimeStampTz, vec),
            ArrayCell::Uuid(vec) => convert_array_variant!(Uuid, vec),
            ArrayCell::Json(vec) => convert_array_variant!(Json, vec),
            ArrayCell::Bytes(vec) => convert_array_variant!(Bytes, vec),
        }
    }
}

#[derive(Debug)]
pub struct BigQueryTableRow(pub TableRow);

impl prost::Message for BigQueryTableRow {
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
        for cell in &self.0.values {
            len += cell_encode_len_prost(cell, tag);
            tag += 1;
        }

        len
    }

    fn clear(&mut self) {
        for cell in &mut self.0.values {
            cell.clear();
        }
    }
}

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
            let non_optional = ArrayCellNonOptional::try_from(a.clone())
                .expect("NULL values in arrays are not supported by BigQuery");
            array_cell_non_optional_encode_prost(non_optional, tag, buf);
        }
    }
}

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
        Cell::Array(array_cell) => {
            let non_optional = ArrayCellNonOptional::try_from(array_cell.clone())
                .expect("NULL values in arrays are not supported by BigQuery");
            array_cell_non_optional_encoded_len_prost(non_optional, tag)
        }
    }
}

pub fn array_cell_non_optional_encode_prost(
    array_cell: ArrayCellNonOptional,
    tag: u32,
    buf: &mut impl bytes::BufMut,
) {
    match array_cell {
        ArrayCellNonOptional::Null => {}
        ArrayCellNonOptional::Bool(vec) => {
            prost::encoding::bool::encode_packed(tag, &vec, buf);
        },
        ArrayCellNonOptional::String(vec) => {
            prost::encoding::string::encode_repeated(tag, &vec, buf);
        },
        ArrayCellNonOptional::I16(vec) => {
            let values: Vec<i32> = vec.into_iter().map(|v| v as i32).collect();
            prost::encoding::int32::encode_packed(tag, &values, buf);
        },
        ArrayCellNonOptional::I32(vec) => {
            prost::encoding::int32::encode_packed(tag, &vec, buf);
        },
        ArrayCellNonOptional::U32(vec) => {
            prost::encoding::uint32::encode_packed(tag, &vec, buf);
        },
        ArrayCellNonOptional::I64(vec) => {
            prost::encoding::int64::encode_packed(tag, &vec, buf);
        },
        ArrayCellNonOptional::F32(vec) => {
            prost::encoding::float::encode_packed(tag, &vec, buf);
        },
        ArrayCellNonOptional::F64(vec) => {
            prost::encoding::double::encode_packed(tag, &vec, buf);
        },
        ArrayCellNonOptional::Numeric(vec) => {
            let values: Vec<String> = vec.into_iter().map(|v| v.to_string()).collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        },
        ArrayCellNonOptional::Date(vec) => {
            let values: Vec<String> = vec.into_iter().map(|v| v.format("%Y-%m-%d").to_string()).collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        },
        ArrayCellNonOptional::Time(vec) => {
            let values: Vec<String> = vec.into_iter().map(|v| v.format("%H:%M:%S%.f").to_string()).collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        },
        ArrayCellNonOptional::TimeStamp(vec) => {
            let values: Vec<String> = vec.into_iter().map(|v| v.format("%Y-%m-%d %H:%M:%S%.f").to_string()).collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        },
        ArrayCellNonOptional::TimeStampTz(vec) => {
            let values: Vec<String> = vec.into_iter().map(|v| v.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string()).collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        },
        ArrayCellNonOptional::Uuid(vec) => {
            let values: Vec<String> = vec.into_iter().map(|v| v.to_string()).collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        },
        ArrayCellNonOptional::Json(vec) => {
            let values: Vec<String> = vec.into_iter().map(|v| v.to_string()).collect();
            prost::encoding::string::encode_repeated(tag, &values, buf);
        },
        ArrayCellNonOptional::Bytes(vec) => {
            prost::encoding::bytes::encode_repeated(tag, &vec, buf);
        },
    }
}

pub fn array_cell_non_optional_encoded_len_prost(
    array_cell: ArrayCellNonOptional,
    tag: u32,
) -> usize {
    match array_cell {
        ArrayCellNonOptional::Null => 0,
        ArrayCellNonOptional::Bool(vec) => prost::encoding::bool::encoded_len_packed(tag, &vec),
        ArrayCellNonOptional::String(vec) => prost::encoding::string::encoded_len_repeated(tag, &vec),
        ArrayCellNonOptional::I16(vec) => {
            let values: Vec<i32> = vec.into_iter().map(|v| v as i32).collect();
            prost::encoding::int32::encoded_len_packed(tag, &values)
        },
        ArrayCellNonOptional::I32(vec) => prost::encoding::int32::encoded_len_packed(tag, &vec),
        ArrayCellNonOptional::U32(vec) => prost::encoding::uint32::encoded_len_packed(tag, &vec),
        ArrayCellNonOptional::I64(vec) => prost::encoding::int64::encoded_len_packed(tag, &vec),
        ArrayCellNonOptional::F32(vec) => prost::encoding::float::encoded_len_packed(tag, &vec),
        ArrayCellNonOptional::F64(vec) => prost::encoding::double::encoded_len_packed(tag, &vec),
        ArrayCellNonOptional::Numeric(vec) => {
            let values: Vec<String> = vec.into_iter().map(|v| v.to_string()).collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        },
        ArrayCellNonOptional::Date(vec) => {
            let values: Vec<String> = vec.into_iter().map(|v| v.format("%Y-%m-%d").to_string()).collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        },
        ArrayCellNonOptional::Time(vec) => {
            let values: Vec<String> = vec.into_iter().map(|v| v.format("%H:%M:%S%.f").to_string()).collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        },
        ArrayCellNonOptional::TimeStamp(vec) => {
            let values: Vec<String> = vec.into_iter().map(|v| v.format("%Y-%m-%d %H:%M:%S%.f").to_string()).collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        },
        ArrayCellNonOptional::TimeStampTz(vec) => {
            let values: Vec<String> = vec.into_iter().map(|v| v.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string()).collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        },
        ArrayCellNonOptional::Uuid(vec) => {
            let values: Vec<String> = vec.into_iter().map(|v| v.to_string()).collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        },
        ArrayCellNonOptional::Json(vec) => {
            let values: Vec<String> = vec.into_iter().map(|v| v.to_string()).collect();
            prost::encoding::string::encoded_len_repeated(tag, &values)
        },
        ArrayCellNonOptional::Bytes(vec) => prost::encoding::bytes::encoded_len_repeated(tag, &vec),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_array_cell_non_optional_try_from_success() {
        let array_cell =
            ArrayCell::String(vec![Some("hello".to_string()), Some("world".to_string())]);
        let result = ArrayCellNonOptional::try_from(array_cell);
        assert!(result.is_ok());
        match result.unwrap() {
            ArrayCellNonOptional::String(v) => {
                assert_eq!(v, vec!["hello".to_string(), "world".to_string()]);
            }
            _ => panic!("Expected String variant"),
        }
    }

    #[test]
    fn test_array_cell_non_optional_try_from_with_null_fails() {
        let array_cell = ArrayCell::String(vec![
            Some("hello".to_string()),
            None,
            Some("world".to_string()),
        ]);
        let result = ArrayCellNonOptional::try_from(array_cell);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "NULL values in arrays are not supported by BigQuery"
        );
    }

    #[test]
    fn test_array_cell_non_optional_try_from_numeric_with_null_fails() {
        let array_cell = ArrayCell::Numeric(vec![Some(PgNumeric::default()), None]);
        let result = ArrayCellNonOptional::try_from(array_cell);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "NULL values in arrays are not supported by BigQuery"
        );
    }

    #[test]
    #[should_panic(expected = "NULL values in arrays are not supported by BigQuery")]
    fn test_cell_encode_panics_with_null_array_elements() {
        let cell = Cell::Array(ArrayCell::String(vec![Some("hello".to_string()), None]));
        let mut buf = Vec::new();
        cell_encode_prost(&cell, 1, &mut buf);
    }
}
