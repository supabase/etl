use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::etl_error;
use etl::types::{Cell, ColumnSchema, TableRow, Type, is_array_type};

/// Encodes a [`TableRow`] into a SQL VALUES clause format for Databend.
///
/// Converts each cell in the row to its SQL representation, properly escaping
/// strings and formatting values according to Databend's requirements.
///
/// # Example
///
/// ```text
/// (1, 'John Doe', '2024-01-01 00:00:00')
/// ```
pub fn encode_table_row(row: &TableRow, column_schemas: &[ColumnSchema]) -> EtlResult<String> {
    if row.values.len() != column_schemas.len() {
        return Err(etl_error!(
            ErrorKind::ValidationError,
            "Row column count mismatch",
            format!(
                "Expected {} columns but got {} in table row",
                column_schemas.len(),
                row.values.len()
            )
        ));
    }

    let encoded_cells: Result<Vec<String>, EtlError> = row
        .values
        .iter()
        .zip(column_schemas.iter())
        .map(|(cell, schema)| encode_cell(cell, &schema.typ))
        .collect();

    let encoded_cells = encoded_cells?;
    Ok(format!("({})", encoded_cells.join(", ")))
}

/// Encodes a single [`Cell`] into its SQL representation for Databend.
///
/// Handles NULL values, strings, numbers, dates, timestamps, JSON, and arrays.
fn encode_cell(cell: &Cell, typ: &Type) -> EtlResult<String> {
    match cell {
        Cell::Null => Ok("NULL".to_string()),
        Cell::Bool(b) => Ok(if *b { "TRUE".to_string() } else { "FALSE".to_string() }),
        Cell::I16(i) => Ok(i.to_string()),
        Cell::I32(i) => Ok(i.to_string()),
        Cell::U32(i) => Ok(i.to_string()),
        Cell::I64(i) => Ok(i.to_string()),
        Cell::F32(f) => encode_float32(*f),
        Cell::F64(f) => encode_float64(*f),
        Cell::String(s) => Ok(encode_string(s)),
        Cell::Bytes(bytes) => encode_bytes(bytes),
        Cell::Date(date) => Ok(format!("'{}'", date.format("%Y-%m-%d"))),
        Cell::Time(time) => Ok(format!("'{}'", time.format("%H:%M:%S%.f"))),
        Cell::Timestamp(ts) => Ok(format!("'{}'", ts.format("%Y-%m-%d %H:%M:%S%.6f"))),
        Cell::TimestampTz(ts) => Ok(format!("'{}'", ts.format("%Y-%m-%d %H:%M:%S%.6f"))),
        Cell::Uuid(uuid) => Ok(encode_string(&uuid.to_string())),
        Cell::Json(json) => Ok(encode_string(&json.to_string())),
        Cell::Numeric(numeric) => Ok(numeric.to_string()),
        Cell::Array(arr) => encode_array_cell(arr, typ),
    }
}

/// Encodes a string for SQL, escaping single quotes.
fn encode_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Encodes a byte array as a hex string for Databend.
fn encode_bytes(bytes: &[u8]) -> EtlResult<String> {
    // Encode as hex string with '0x' prefix
    Ok(format!("FROM_HEX('{}')", hex::encode(bytes)))
}

/// Encodes a float32, handling special values.
fn encode_float32(f: f32) -> EtlResult<String> {
    if f.is_nan() {
        Ok("'NaN'::FLOAT".to_string())
    } else if f.is_infinite() {
        if f.is_sign_positive() {
            Ok("'Infinity'::FLOAT".to_string())
        } else {
            Ok("'-Infinity'::FLOAT".to_string())
        }
    } else {
        Ok(f.to_string())
    }
}

/// Encodes a float64, handling special values.
fn encode_float64(f: f64) -> EtlResult<String> {
    if f.is_nan() {
        Ok("'NaN'::DOUBLE".to_string())
    } else if f.is_infinite() {
        if f.is_sign_positive() {
            Ok("'Infinity'::DOUBLE".to_string())
        } else {
            Ok("'-Infinity'::DOUBLE".to_string())
        }
    } else {
        Ok(f.to_string())
    }
}

/// Encodes an ArrayCell into Databend array syntax.
fn encode_array_cell(array_cell: &etl::types::ArrayCell, typ: &Type) -> EtlResult<String> {
    use etl::types::ArrayCell;

    if !is_array_type(typ) {
        return Err(etl_error!(
            ErrorKind::ValidationError,
            "Type mismatch for array encoding",
            format!("Expected array type but got {:?}", typ)
        ));
    }

    // Match on ArrayCell variants and encode each element
    let encoded_elements: Vec<String> = match array_cell {
        ArrayCell::Bool(vec) => vec.iter().map(|opt| match opt {
            Some(b) => if *b { "TRUE".to_string() } else { "FALSE".to_string() },
            None => "NULL".to_string(),
        }).collect(),
        ArrayCell::I16(vec) => vec.iter().map(|opt| match opt {
            Some(i) => i.to_string(),
            None => "NULL".to_string(),
        }).collect(),
        ArrayCell::I32(vec) => vec.iter().map(|opt| match opt {
            Some(i) => i.to_string(),
            None => "NULL".to_string(),
        }).collect(),
        ArrayCell::U32(vec) => vec.iter().map(|opt| match opt {
            Some(i) => i.to_string(),
            None => "NULL".to_string(),
        }).collect(),
        ArrayCell::I64(vec) => vec.iter().map(|opt| match opt {
            Some(i) => i.to_string(),
            None => "NULL".to_string(),
        }).collect(),
        ArrayCell::F32(vec) => vec.iter().map(|opt| match opt {
            Some(f) => encode_float32(*f).unwrap_or_else(|_| "NULL".to_string()),
            None => "NULL".to_string(),
        }).collect(),
        ArrayCell::F64(vec) => vec.iter().map(|opt| match opt {
            Some(f) => encode_float64(*f).unwrap_or_else(|_| "NULL".to_string()),
            None => "NULL".to_string(),
        }).collect(),
        ArrayCell::Numeric(vec) => vec.iter().map(|opt| match opt {
            Some(n) => n.to_string(),
            None => "NULL".to_string(),
        }).collect(),
        ArrayCell::String(vec) => vec.iter().map(|opt| match opt {
            Some(s) => encode_string(s),
            None => "NULL".to_string(),
        }).collect(),
        ArrayCell::Date(vec) => vec.iter().map(|opt| match opt {
            Some(d) => format!("'{}'", d.format("%Y-%m-%d")),
            None => "NULL".to_string(),
        }).collect(),
        ArrayCell::Time(vec) => vec.iter().map(|opt| match opt {
            Some(t) => format!("'{}'", t.format("%H:%M:%S%.f")),
            None => "NULL".to_string(),
        }).collect(),
        ArrayCell::Timestamp(vec) => vec.iter().map(|opt| match opt {
            Some(ts) => format!("'{}'", ts.format("%Y-%m-%d %H:%M:%S%.6f")),
            None => "NULL".to_string(),
        }).collect(),
        ArrayCell::TimestampTz(vec) => vec.iter().map(|opt| match opt {
            Some(ts) => format!("'{}'", ts.format("%Y-%m-%d %H:%M:%S%.6f")),
            None => "NULL".to_string(),
        }).collect(),
        ArrayCell::Uuid(vec) => vec.iter().map(|opt| match opt {
            Some(uuid) => encode_string(&uuid.to_string()),
            None => "NULL".to_string(),
        }).collect(),
        ArrayCell::Json(vec) => vec.iter().map(|opt| match opt {
            Some(json) => encode_string(&json.to_string()),
            None => "NULL".to_string(),
        }).collect(),
        ArrayCell::Bytes(vec) => vec.iter().map(|opt| match opt {
            Some(bytes) => encode_bytes(bytes).unwrap_or_else(|_| "NULL".to_string()),
            None => "NULL".to_string(),
        }).collect(),
    };

    Ok(format!("[{}]", encoded_elements.join(", ")))
}

/// Gets the element type from an array type.
fn get_array_element_type(array_type: &Type) -> Type {
    match array_type {
        &Type::BOOL_ARRAY => Type::BOOL,
        &Type::INT2_ARRAY => Type::INT2,
        &Type::INT4_ARRAY => Type::INT4,
        &Type::INT8_ARRAY => Type::INT8,
        &Type::FLOAT4_ARRAY => Type::FLOAT4,
        &Type::FLOAT8_ARRAY => Type::FLOAT8,
        &Type::NUMERIC_ARRAY => Type::NUMERIC,
        &Type::TEXT_ARRAY => Type::TEXT,
        &Type::VARCHAR_ARRAY => Type::VARCHAR,
        &Type::BPCHAR_ARRAY => Type::BPCHAR,
        &Type::BYTEA_ARRAY => Type::BYTEA,
        &Type::DATE_ARRAY => Type::DATE,
        &Type::TIME_ARRAY => Type::TIME,
        &Type::TIMESTAMP_ARRAY => Type::TIMESTAMP,
        &Type::TIMESTAMPTZ_ARRAY => Type::TIMESTAMPTZ,
        &Type::UUID_ARRAY => Type::UUID,
        &Type::JSON_ARRAY => Type::JSON,
        &Type::JSONB_ARRAY => Type::JSONB,
        &Type::OID_ARRAY => Type::OID,
        // For non-array types, return the same type (shouldn't happen in practice)
        _ => array_type.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, NaiveTime, NaiveDateTime};
    use uuid::Uuid;

    #[test]
    fn test_encode_string() {
        assert_eq!(encode_string("hello"), "'hello'");
        assert_eq!(encode_string("it's"), "'it''s'");
        assert_eq!(encode_string("test'data"), "'test''data'");
    }

    #[test]
    fn test_encode_cell_primitives() {
        assert_eq!(encode_cell(&Cell::Null, &Type::INT4).unwrap(), "NULL");
        assert_eq!(encode_cell(&Cell::Bool(true), &Type::BOOL).unwrap(), "TRUE");
        assert_eq!(encode_cell(&Cell::Bool(false), &Type::BOOL).unwrap(), "FALSE");
        assert_eq!(encode_cell(&Cell::I32(42), &Type::INT4).unwrap(), "42");
        assert_eq!(encode_cell(&Cell::I64(-100), &Type::INT8).unwrap(), "-100");
    }

    #[test]
    fn test_encode_cell_string() {
        let cell = Cell::String("hello world".to_string());
        assert_eq!(encode_cell(&cell, &Type::TEXT).unwrap(), "'hello world'");

        let cell_with_quote = Cell::String("it's working".to_string());
        assert_eq!(
            encode_cell(&cell_with_quote, &Type::TEXT).unwrap(),
            "'it''s working'"
        );
    }

    #[test]
    fn test_encode_cell_date_time() {
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let cell = Cell::Date(date);
        assert_eq!(encode_cell(&cell, &Type::DATE).unwrap(), "'2024-01-15'");

        let time = NaiveTime::from_hms_opt(14, 30, 45).unwrap();
        let cell = Cell::Time(time);
        assert!(encode_cell(&cell, &Type::TIME).unwrap().starts_with("'14:30:45"));

        let timestamp = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveTime::from_hms_opt(14, 30, 45).unwrap()
        );
        let cell = Cell::Timestamp(timestamp);
        assert!(encode_cell(&cell, &Type::TIMESTAMP).unwrap().starts_with("'2024-01-15 14:30:45"));
    }

    #[test]
    fn test_encode_cell_uuid() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let cell = Cell::Uuid(uuid);
        assert_eq!(
            encode_cell(&cell, &Type::UUID).unwrap(),
            "'550e8400-e29b-41d4-a716-446655440000'"
        );
    }

    #[test]
    fn test_encode_cell_json() {
        let json = serde_json::json!({"key": "value", "number": 42});
        let cell = Cell::Json(json);
        let encoded = encode_cell(&cell, &Type::JSON).unwrap();
        assert!(encoded.contains("key"));
        assert!(encoded.contains("value"));
    }

    #[test]
    fn test_encode_cell_float_special_values() {
        // NaN
        let cell = Cell::F32(f32::NAN);
        assert_eq!(encode_cell(&cell, &Type::FLOAT4).unwrap(), "'NaN'::FLOAT");

        // Infinity
        let cell = Cell::F32(f32::INFINITY);
        assert_eq!(encode_cell(&cell, &Type::FLOAT4).unwrap(), "'Infinity'::FLOAT");

        // Negative infinity
        let cell = Cell::F32(f32::NEG_INFINITY);
        assert_eq!(encode_cell(&cell, &Type::FLOAT4).unwrap(), "'-Infinity'::FLOAT");

        // Normal values
        let cell = Cell::F32(3.14);
        assert_eq!(encode_cell(&cell, &Type::FLOAT4).unwrap(), "3.14");
    }

    #[test]
    fn test_encode_array() {
        use etl::types::ArrayCell;

        // Integer array
        let array_cell = ArrayCell::I32(vec![Some(1), Some(2), Some(3)]);
        let cell = Cell::Array(array_cell);
        assert_eq!(
            encode_cell(&cell, &Type::INT4_ARRAY).unwrap(),
            "[1, 2, 3]"
        );

        // String array
        let array_cell = ArrayCell::String(vec![
            Some("a".to_string()),
            Some("b".to_string()),
            Some("c".to_string()),
        ]);
        let cell = Cell::Array(array_cell);
        assert_eq!(
            encode_cell(&cell, &Type::TEXT_ARRAY).unwrap(),
            "['a', 'b', 'c']"
        );

        // Array with NULL
        let array_cell = ArrayCell::I32(vec![Some(1), None, Some(3)]);
        let cell = Cell::Array(array_cell);
        assert_eq!(
            encode_cell(&cell, &Type::INT4_ARRAY).unwrap(),
            "[1, NULL, 3]"
        );
    }

    #[test]
    fn test_encode_table_row() {
        let row = TableRow::new(vec![
            Cell::I32(1),
            Cell::String("Alice".to_string()),
            Cell::I32(30),
        ]);

        let schemas = vec![
            ColumnSchema {
                name: "id".to_string(),
                typ: Type::INT4,
                modifier: -1,
                nullable: false,
                primary: true,
            },
            ColumnSchema {
                name: "name".to_string(),
                typ: Type::TEXT,
                modifier: -1,
                nullable: true,
                primary: false,
            },
            ColumnSchema {
                name: "age".to_string(),
                typ: Type::INT4,
                modifier: -1,
                nullable: true,
                primary: false,
            },
        ];

        let encoded = encode_table_row(&row, &schemas).unwrap();
        assert_eq!(encoded, "(1, 'Alice', 30)");
    }

    #[test]
    fn test_encode_table_row_with_nulls() {
        let row = TableRow::new(vec![
            Cell::I32(1),
            Cell::Null,
            Cell::String("test".to_string()),
        ]);

        let schemas = vec![
            ColumnSchema {
                name: "id".to_string(),
                typ: Type::INT4,
                modifier: -1,
                nullable: false,
                primary: true,
            },
            ColumnSchema {
                name: "optional_field".to_string(),
                typ: Type::TEXT,
                modifier: -1,
                nullable: true,
                primary: false,
            },
            ColumnSchema {
                name: "data".to_string(),
                typ: Type::TEXT,
                modifier: -1,
                nullable: false,
                primary: false,
            },
        ];

        let encoded = encode_table_row(&row, &schemas).unwrap();
        assert_eq!(encoded, "(1, NULL, 'test')");
    }

    #[test]
    fn test_encode_table_row_column_mismatch() {
        let row = TableRow::new(vec![Cell::I32(1), Cell::String("test".to_string())]);

        let schemas = vec![
            ColumnSchema {
                name: "id".to_string(),
                typ: Type::INT4,
                modifier: -1,
                nullable: false,
                primary: true,
            },
        ];

        let result = encode_table_row(&row, &schemas);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ValidationError);
    }

    #[test]
    fn test_get_array_element_type() {
        assert_eq!(get_array_element_type(&Type::INT4_ARRAY), Type::INT4);
        assert_eq!(get_array_element_type(&Type::TEXT_ARRAY), Type::TEXT);
        assert_eq!(get_array_element_type(&Type::BOOL_ARRAY), Type::BOOL);
        assert_eq!(get_array_element_type(&Type::FLOAT8_ARRAY), Type::FLOAT8);
    }
}
