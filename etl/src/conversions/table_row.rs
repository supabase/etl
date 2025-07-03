use core::str;
use postgres::schema::ColumnSchema;
use std::str::Utf8Error;
use thiserror::Error;
use tokio_postgres::types::Type;
use tracing::error;

use crate::{conversions::text::TextFormatConverter, pipeline::batching::BatchBoundaryV1};

use super::{text::FromTextError, Cell};

#[derive(Debug, Clone, PartialEq)]
pub struct TableRow {
    pub values: Vec<Cell>,
}

impl TableRow {
    pub fn new(values: Vec<Cell>) -> Self {
        Self { values }
    }
}

impl BatchBoundaryV1 for TableRow {
    fn is_last_in_batch(&self) -> bool {
        true
    }
}

#[cfg(feature = "bigquery")]
impl prost::Message for TableRow {
    fn encode_raw(&self, buf: &mut impl bytes::BufMut)
    where
        Self: Sized,
    {
        let mut tag = 1;
        for cell in &self.values {
            cell.encode_prost(tag, buf);
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
        for cell in &self.values {
            len += cell.encoded_len_prost(tag);
            tag += 1;
        }

        len
    }

    fn clear(&mut self) {
        for cell in &mut self.values {
            cell.clear();
        }
    }
}

#[derive(Debug, Error)]
pub enum TableRowConversionError {
    #[error("unsupported type {0}")]
    UnsupportedType(Type),

    #[error("invalid string: {0}")]
    InvalidString(#[from] Utf8Error),

    #[error("mismatch in num of columns in schema and row")]
    NumColsMismatch,

    #[error("unterminated row")]
    UnterminatedRow,

    #[error("invalid value: {0}")]
    InvalidValue(#[from] FromTextError),
}

pub struct TableRowConverter;

impl TableRowConverter {
    // parses text produced by this code in Postgres: https://github.com/postgres/postgres/blob/263a3f5f7f508167dbeafc2aefd5835b41d77481/src/backend/commands/copyto.c#L988-L1134
    pub fn try_from(
        row: &[u8],
        column_schemas: &[ColumnSchema],
    ) -> Result<TableRow, TableRowConversionError> {
        let mut values = Vec::with_capacity(column_schemas.len());

        let row_str = str::from_utf8(row)?;
        let mut column_schemas_iter = column_schemas.iter();
        let mut chars = row_str.chars();
        let mut val_str = String::with_capacity(10);
        let mut in_escape = false;
        let mut row_terminated = false;
        let mut done = false;

        while !done {
            loop {
                match chars.next() {
                    Some(c) => match c {
                        c if in_escape => {
                            if c == 'N' {
                                val_str.push('\\');
                                val_str.push(c);
                            } else if c == 'b' {
                                val_str.push(8 as char);
                            } else if c == 'f' {
                                val_str.push(12 as char);
                            } else if c == 'n' {
                                val_str.push('\n');
                            } else if c == 'r' {
                                val_str.push('\r');
                            } else if c == 't' {
                                val_str.push('\t');
                            } else if c == 'v' {
                                val_str.push(11 as char)
                            } else {
                                val_str.push(c);
                            }
                            in_escape = false;
                        }
                        '\t' => {
                            break;
                        }
                        '\n' => {
                            row_terminated = true;
                            break;
                        }
                        '\\' => in_escape = true,
                        c => {
                            val_str.push(c);
                        }
                    },
                    None => {
                        if !row_terminated {
                            return Err(TableRowConversionError::UnterminatedRow);
                        }
                        done = true;
                        break;
                    }
                }
            }

            if !done {
                let Some(column_schema) = column_schemas_iter.next() else {
                    return Err(TableRowConversionError::NumColsMismatch);
                };

                let value = if val_str == "\\N" {
                    // In case of a null value, we store the type information since that will be used to
                    // correctly compute default values when needed.
                    Cell::Null(column_schema.typ.clone())
                } else {
                    match TextFormatConverter::try_from_str(&column_schema.typ, &val_str) {
                        Ok(value) => value,
                        Err(e) => {
                            error!(
                                "error parsing column `{}` of type `{}` from text `{val_str}`",
                                column_schema.name, column_schema.typ
                            );
                            return Err(e.into());
                        }
                    }
                };

                values.push(value);
                val_str.clear();
            }
        }

        Ok(TableRow { values })
    }
}
