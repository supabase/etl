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
            ErrorKind::DestinationEncodingFailed,
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
        Cell::Bool(b) => Ok(if *b { "TRUE" } else { "FALSE" }),
        Cell::Int16(i) => Ok(i.to_string()),
        Cell::Int32(i) => Ok(i.to_string()),
        Cell::UInt32(i) => Ok(i.to_string()),
        Cell::Int64(i) => Ok(i.to_string()),
        Cell::Float32(f) => encode_float32(*f),
        Cell::Float64(f) => encode_float64(*f),
        Cell::String(s) => Ok(encode_string(s)),
        Cell::Bytes(bytes) => encode_bytes(bytes),
        Cell::Date(date) => Ok(format!("'{}'", date.format("%Y-%m-%d"))),
        Cell::Time(time) => Ok(format!("'{}'", time.format("%H:%M:%S%.f"))),
        Cell::Timestamp(ts) => Ok(format!("'{}'", ts.format("%Y-%m-%d %H:%M:%S%.6f"))),
        Cell::TimestampTz(ts) => Ok(format!("'{}'", ts.format("%Y-%m-%d %H:%M:%S%.6f"))),
        Cell::Uuid(uuid) => Ok(encode_string(&uuid.to_string())),
        Cell::Json(json) => Ok(encode_string(&json.to_string())),
        Cell::Numeric(numeric) => Ok(numeric.to_string()),
        Cell::Array(arr) => encode_array(arr, typ),
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

/// Encodes an array cell into Databend array syntax.
fn encode_array(cells: &[Cell], typ: &Type) -> EtlResult<String> {
    if !is_array_type(typ) {
        return Err(etl_error!(
            ErrorKind::DestinationEncodingFailed,
            "Type mismatch for array encoding",
            format!("Expected array type but got {:?}", typ)
        ));
    }

    // Get the element type from the array type
    let element_type = get_array_element_type(typ);

    let encoded_elements: Result<Vec<String>, EtlError> = cells
        .iter()
        .map(|cell| encode_cell(cell, &element_type))
        .collect();

    let encoded_elements = encoded_elements?;
    Ok(format!("[{}]", encoded_elements.join(", ")))
}

/// Gets the element type from an array type.
fn get_array_element_type(array_type: &Type) -> Type {
    match array_type {
        Type::BoolArray => Type::Bool,
        Type::Int2Array => Type::Int2,
        Type::Int4Array => Type::Int4,
        Type::Int8Array => Type::Int8,
        Type::Float4Array => Type::Float4,
        Type::Float8Array => Type::Float8,
        Type::NumericArray => Type::Numeric,
        Type::TextArray => Type::Text,
        Type::VarcharArray => Type::Varchar,
        Type::BpcharArray => Type::Bpchar,
        Type::ByteaArray => Type::Bytea,
        Type::DateArray => Type::Date,
        Type::TimeArray => Type::Time,
        Type::TimestampArray => Type::Timestamp,
        Type::TimestamptzArray => Type::Timestamptz,
        Type::UuidArray => Type::Uuid,
        Type::JsonArray => Type::Json,
        Type::JsonbArray => Type::Jsonb,
        Type::OidArray => Type::Oid,
        // For non-array types, return the same type (shouldn't happen in practice)
        _ => array_type.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, NaiveTime, NaiveDateTime, Utc, TimeZone};
    use uuid::Uuid;

    #[test]
    fn test_encode_string() {
        assert_eq!(encode_string("hello"), "'hello'");
        assert_eq!(encode_string("it's"), "'it''s'");
        assert_eq!(encode_string("test'data"), "'test''data'");
    }

    #[test]
    fn test_encode_cell_primitives() {
        assert_eq!(encode_cell(&Cell::Null, &Type::Int4).unwrap(), "NULL");
        assert_eq!(encode_cell(&Cell::Bool(true), &Type::Bool).unwrap(), "TRUE");
        assert_eq!(encode_cell(&Cell::Bool(false), &Type::Bool).unwrap(), "FALSE");
        assert_eq!(encode_cell(&Cell::Int32(42), &Type::Int4).unwrap(), "42");
        assert_eq!(encode_cell(&Cell::Int64(-100), &Type::Int8).unwrap(), "-100");
    }

    #[test]
    fn test_encode_cell_string() {
        let cell = Cell::String("hello world".to_string());
        assert_eq!(encode_cell(&cell, &Type::Text).unwrap(), "'hello world'");

        let cell_with_quote = Cell::String("it's working".to_string());
        assert_eq!(
            encode_cell(&cell_with_quote, &Type::Text).unwrap(),
            "'it''s working'"
        );
    }

    #[test]
    fn test_encode_cell_date_time() {
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let cell = Cell::Date(date);
        assert_eq!(encode_cell(&cell, &Type::Date).unwrap(), "'2024-01-15'");

        let time = NaiveTime::from_hms_opt(14, 30, 45).unwrap();
        let cell = Cell::Time(time);
        assert!(encode_cell(&cell, &Type::Time).unwrap().starts_with("'14:30:45"));

        let timestamp = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveTime::from_hms_opt(14, 30, 45).unwrap()
        );
        let cell = Cell::Timestamp(timestamp);
        assert!(encode_cell(&cell, &Type::Timestamp).unwrap().starts_with("'2024-01-15 14:30:45"));
    }

    #[test]
    fn test_encode_cell_uuid() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let cell = Cell::Uuid(uuid);
        assert_eq!(
            encode_cell(&cell, &Type::Uuid).unwrap(),
            "'550e8400-e29b-41d4-a716-446655440000'"
        );
    }

    #[test]
    fn test_encode_cell_json() {
        let json = serde_json::json!({"key": "value", "number": 42});
        let cell = Cell::Json(json);
        let encoded = encode_cell(&cell, &Type::Json).unwrap();
        assert!(encoded.contains("key"));
        assert!(encoded.contains("value"));
    }

    #[test]
    fn test_encode_cell_float_special_values() {
        // NaN
        let cell = Cell::Float32(f32::NAN);
        assert_eq!(encode_cell(&cell, &Type::Float4).unwrap(), "'NaN'::FLOAT");

        // Infinity
        let cell = Cell::Float32(f32::INFINITY);
        assert_eq!(encode_cell(&cell, &Type::Float4).unwrap(), "'Infinity'::FLOAT");

        // Negative infinity
        let cell = Cell::Float32(f32::NEG_INFINITY);
        assert_eq!(encode_cell(&cell, &Type::Float4).unwrap(), "'-Infinity'::FLOAT");

        // Normal values
        let cell = Cell::Float32(3.14);
        assert_eq!(encode_cell(&cell, &Type::Float4).unwrap(), "3.14");
    }

    #[test]
    fn test_encode_array() {
        // Integer array
        let cells = vec![Cell::Int32(1), Cell::Int32(2), Cell::Int32(3)];
        let cell = Cell::Array(cells);
        assert_eq!(
            encode_cell(&cell, &Type::Int4Array).unwrap(),
            "[1, 2, 3]"
        );

        // String array
        let cells = vec![
            Cell::String("a".to_string()),
            Cell::String("b".to_string()),
            Cell::String("c".to_string()),
        ];
        let cell = Cell::Array(cells);
        assert_eq!(
            encode_cell(&cell, &Type::TextArray).unwrap(),
            "['a', 'b', 'c']"
        );

        // Array with NULL
        let cells = vec![Cell::Int32(1), Cell::Null, Cell::Int32(3)];
        let cell = Cell::Array(cells);
        assert_eq!(
            encode_cell(&cell, &Type::Int4Array).unwrap(),
            "[1, NULL, 3]"
        );
    }

    #[test]
    fn test_encode_table_row() {
        let row = TableRow::new(vec![
            Cell::Int32(1),
            Cell::String("Alice".to_string()),
            Cell::Int32(30),
        ]);

        let schemas = vec![
            ColumnSchema {
                name: "id".to_string(),
                typ: Type::Int4,
                optional: false,
            },
            ColumnSchema {
                name: "name".to_string(),
                typ: Type::Text,
                optional: true,
            },
            ColumnSchema {
                name: "age".to_string(),
                typ: Type::Int4,
                optional: true,
            },
        ];

        let encoded = encode_table_row(&row, &schemas).unwrap();
        assert_eq!(encoded, "(1, 'Alice', 30)");
    }

    #[test]
    fn test_encode_table_row_with_nulls() {
        let row = TableRow::new(vec![
            Cell::Int32(1),
            Cell::Null,
            Cell::String("test".to_string()),
        ]);

        let schemas = vec![
            ColumnSchema {
                name: "id".to_string(),
                typ: Type::Int4,
                optional: false,
            },
            ColumnSchema {
                name: "optional_field".to_string(),
                typ: Type::Text,
                optional: true,
            },
            ColumnSchema {
                name: "data".to_string(),
                typ: Type::Text,
                optional: false,
            },
        ];

        let encoded = encode_table_row(&row, &schemas).unwrap();
        assert_eq!(encoded, "(1, NULL, 'test')");
    }

    #[test]
    fn test_encode_table_row_column_mismatch() {
        let row = TableRow::new(vec![Cell::Int32(1), Cell::String("test".to_string())]);

        let schemas = vec![
            ColumnSchema {
                name: "id".to_string(),
                typ: Type::Int4,
                optional: false,
            },
        ];

        let result = encode_table_row(&row, &schemas);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationEncodingFailed);
    }

    #[test]
    fn test_get_array_element_type() {
        assert_eq!(get_array_element_type(&Type::Int4Array), Type::Int4);
        assert_eq!(get_array_element_type(&Type::TextArray), Type::Text);
        assert_eq!(get_array_element_type(&Type::BoolArray), Type::Bool);
        assert_eq!(get_array_element_type(&Type::Float8Array), Type::Float8);
    }
}
