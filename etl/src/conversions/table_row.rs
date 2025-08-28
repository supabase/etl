use core::str;
use etl_postgres::types::ColumnSchema;
use tracing::error;

use crate::bail;
use crate::conversions::text::parse_cell_from_postgres_text;
use crate::error::EtlError;
use crate::error::{ErrorKind, EtlResult};
use crate::types::{Cell, TableRow};

/// Converts raw Postgres COPY format data into a typed table row.
///
/// This method parses the text format data produced by Postgres's COPY command
/// and converts it into strongly-typed [`Cell`] values according to the provided
/// column schemas. It handles Postgres's specific escaping rules and type formats.
///
/// # Panics
///
/// Panics if the number of parsed values doesn't match the number of column schemas.
pub fn parse_table_row_from_postgres_copy_bytes(
    row: &[u8],
    column_schemas: &[ColumnSchema],
) -> EtlResult<TableRow> {
    let mut values = Vec::with_capacity(column_schemas.len());

    let row_str = str::from_utf8(row)?;
    let mut column_schemas_iter = column_schemas.iter();
    let mut chars = row_str.chars();
    let mut val_str = String::with_capacity(10);
    let mut in_escape = false;
    let mut row_terminated = false;
    let mut done = false;

    // Main parsing loop - continues until all characters are processed
    while !done {
        // Inner loop parses a single field value until tab, newline, or end of input
        loop {
            match chars.next() {
                Some(c) => match c {
                    // Handle escaped characters - previous character was backslash
                    c if in_escape => {
                        // Special case: \N when escaped becomes literal \N (not NULL)
                        if c == 'N' {
                            val_str.push('\\');
                            val_str.push(c);
                        }
                        // Standard Postgres escape sequences
                        else if c == 'b' {
                            val_str.push(8 as char); // backspace
                        } else if c == 'f' {
                            val_str.push(12 as char); // form feed
                        } else if c == 'n' {
                            val_str.push('\n'); // newline
                        } else if c == 'r' {
                            val_str.push('\r'); // carriage return
                        } else if c == 't' {
                            val_str.push('\t'); // tab
                        } else if c == 'v' {
                            val_str.push(11 as char); // vertical tab
                        }
                        // Any other character: strip backslash, keep character
                        else {
                            val_str.push(c);
                        }

                        in_escape = false;
                    }
                    // Field separator - end current field parsing
                    '\t' => {
                        break;
                    }
                    // Row terminator - end current field and mark row complete
                    '\n' => {
                        row_terminated = true;
                        break;
                    }
                    // Escape character - next character will be escaped
                    '\\' => in_escape = true,
                    // Regular character - add to current field value
                    c => {
                        val_str.push(c);
                    }
                },
                // End of input reached
                None => {
                    // Validate that row was properly terminated with newline
                    if !row_terminated {
                        bail!(ErrorKind::ConversionError, "The row is not terminated");
                    }
                    done = true;

                    break;
                }
            }
        }

        // Process the parsed field value if we're not done with the entire row
        if !done {
            // Get the next column schema - error if we have more fields than expected
            let Some(column_schema) = column_schemas_iter.next() else {
                bail!(
                    ErrorKind::ConversionError,
                    "The number of columns in the schema and row is mismatched",
                    format!(
                        "The number of columns is the schema [{}] does not match the columns in the row [{}]",
                        column_schemas.len(),
                        values.len()
                    )
                );
            };

            // Convert the parsed string value to appropriate Cell type
            let value = if val_str == "\\N" {
                // Postgres NULL marker: \N represents a NULL value
                // We preserve this as Cell::Null rather than converting to a typed null
                // so that downstream code can handle null semantics appropriately
                Cell::Null
            } else {
                // Convert non-null field value to appropriate Cell type based on column schema
                // This delegates to TextFormatConverter which handles Postgres text format
                // parsing for all supported data types (integers, floats, strings, booleans, etc.)
                match parse_cell_from_postgres_text(&column_schema.typ, &val_str) {
                    Ok(value) => value,
                    Err(e) => {
                        // Log parsing error with context for debugging
                        error!(
                            "error parsing column `{}` of type `{}` from text `{val_str}`",
                            column_schema.name, column_schema.typ
                        );
                        return Err(e);
                    }
                }
            };

            // Add the converted value to the row and prepare for next field
            values.push(value);
            val_str.clear(); // Reset string buffer for next field
        }
    }

    // Validate that all expected columns were present in the row
    // If there are still columns left in the schema iterator, it means the row
    // had fewer fields than expected, which is an error
    if column_schemas_iter.next().is_some() {
        bail!(
            ErrorKind::ConversionError,
            "The number of columns in the schema and row is mismatched",
            format!(
                "The number of columns is the schema [{}] does not match the columns in the row [{}]",
                column_schemas.len(),
                values.len()
            )
        );
    }

    Ok(TableRow { values })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;
    use etl_postgres::types::ColumnSchema;
    use tokio_postgres::types::Type;

    fn create_test_schema() -> Vec<ColumnSchema> {
        vec![
            ColumnSchema::new("id".to_string(), Type::INT4, -1, false, true),
            ColumnSchema::new("name".to_string(), Type::TEXT, -1, true, false),
            ColumnSchema::new("active".to_string(), Type::BOOL, -1, false, false),
        ]
    }

    fn create_single_column_schema(name: &str, typ: Type) -> Vec<ColumnSchema> {
        vec![ColumnSchema::new(name.to_string(), typ, -1, false, false)]
    }

    #[test]
    fn try_from_simple_row() {
        let schema = create_test_schema();
        let row_data = b"123\tJohn Doe\tt\n";

        let result = parse_table_row_from_postgres_copy_bytes(row_data, &schema).unwrap();

        assert_eq!(result.values.len(), 3);
        assert_eq!(result.values[0], Cell::I32(123));
        assert_eq!(result.values[1], Cell::String("John Doe".to_string()));
        assert_eq!(result.values[2], Cell::Bool(true));
    }

    #[test]
    fn try_from_with_null_values() {
        let schema = create_test_schema();
        let row_data = b"456\t\\N\tf\n";

        let result = parse_table_row_from_postgres_copy_bytes(row_data, &schema).unwrap();

        assert_eq!(result.values.len(), 3);
        assert_eq!(result.values[0], Cell::I32(456));
        assert_eq!(result.values[1], Cell::Null);
        assert_eq!(result.values[2], Cell::Bool(false));
    }

    #[test]
    fn try_from_empty_strings() {
        let schema = create_test_schema();
        let row_data = b"0\t\tf\n";

        let result = parse_table_row_from_postgres_copy_bytes(row_data, &schema).unwrap();

        assert_eq!(result.values.len(), 3);
        assert_eq!(result.values[0], Cell::I32(0));
        assert_eq!(result.values[1], Cell::String("".to_string()));
        assert_eq!(result.values[2], Cell::Bool(false));
    }

    #[test]
    fn try_from_single_column() {
        let schema = create_single_column_schema("value", Type::INT4);
        let row_data = b"42\n";

        let result = parse_table_row_from_postgres_copy_bytes(row_data, &schema).unwrap();

        assert_eq!(result.values.len(), 1);
        assert_eq!(result.values[0], Cell::I32(42));
    }

    #[test]
    fn try_from_multiple_columns_different_types() {
        let schema = vec![
            ColumnSchema::new("int_col".to_string(), Type::INT4, -1, false, false),
            ColumnSchema::new("float_col".to_string(), Type::FLOAT8, -1, false, false),
            ColumnSchema::new("text_col".to_string(), Type::TEXT, -1, false, false),
            ColumnSchema::new("bool_col".to_string(), Type::BOOL, -1, false, false),
        ];

        let row_data = b"123\t3.15\tHello World\tt\n";

        let result = parse_table_row_from_postgres_copy_bytes(row_data, &schema).unwrap();

        assert_eq!(result.values.len(), 4);
        assert_eq!(result.values[0], Cell::I32(123));
        assert_eq!(result.values[1], Cell::F64(3.15));
        assert_eq!(result.values[2], Cell::String("Hello World".to_string()));
        assert_eq!(result.values[3], Cell::Bool(true));
    }

    #[test]
    fn try_from_not_terminated() {
        let schema = create_single_column_schema("value", Type::INT4);
        let row_data = b"42"; // Missing newline

        let result = parse_table_row_from_postgres_copy_bytes(row_data, &schema);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::ConversionError));
        assert!(err.to_string().contains("row is not terminated"));
    }

    #[test]
    fn try_from_column_count_mismatch() {
        let schema = create_test_schema(); // Expects 3 columns
        let row_data = b"123\tJohn\n"; // Only 2 values - this should actually fail at parsing the bool because there's no third column

        let result_empty = parse_table_row_from_postgres_copy_bytes(row_data, &schema);
        assert!(result_empty.is_err());
    }

    #[test]
    fn try_from_invalid_utf8() {
        let schema = create_single_column_schema("value", Type::TEXT);
        let row_data = &[0xFF, 0xFE, 0xFD, b'\n']; // Invalid UTF-8

        let result = parse_table_row_from_postgres_copy_bytes(row_data, &schema);

        assert!(result.is_err());
    }

    #[test]
    fn try_from_parsing_error() {
        let schema = create_single_column_schema("number", Type::INT4);
        let row_data = b"not_a_number\n";

        let result = parse_table_row_from_postgres_copy_bytes(row_data, &schema);

        assert!(result.is_err());
    }

    #[test]
    fn try_from_trailing_escape() {
        let schema = create_single_column_schema("data", Type::TEXT);

        let row_data = b"Text\\\\\n";
        let result = parse_table_row_from_postgres_copy_bytes(row_data, &schema).unwrap();

        assert_eq!(result.values.len(), 1);
        assert_eq!(result.values[0], Cell::String("Text\\".to_string()));
    }

    #[test]
    fn try_from_null_literal_vs_null_marker() {
        let schema = create_single_column_schema("value", Type::TEXT);

        let row_data = b"\\N\n";
        let result = parse_table_row_from_postgres_copy_bytes(row_data, &schema).unwrap();
        assert_eq!(result.values[0], Cell::Null);

        let row_data = b"\\\\N\n";
        let result_test = parse_table_row_from_postgres_copy_bytes(row_data, &schema).unwrap();
        assert_eq!(result_test.values[0], Cell::Null);

        let row_data = b"\\\\A\n";
        let result_test = parse_table_row_from_postgres_copy_bytes(row_data, &schema).unwrap();
        assert_eq!(result_test.values[0], Cell::String("\\A".to_string()));
    }

    #[test]
    fn try_from_whitespace_handling() {
        let schema = create_test_schema();

        let row_data = b"123\t John Doe \tt\n";
        let result = parse_table_row_from_postgres_copy_bytes(row_data, &schema).unwrap();

        assert_eq!(result.values.len(), 3);
        assert_eq!(result.values[0], Cell::I32(123));
        assert_eq!(result.values[1], Cell::String(" John Doe ".to_string())); // Spaces preserved
        assert_eq!(result.values[2], Cell::Bool(true));
    }

    #[test]
    fn try_from_large_row() {
        let mut schema = Vec::new();
        let mut expected_row = String::new();

        for i in 0..50 {
            schema.push(ColumnSchema::new(
                format!("col{}", i),
                Type::INT4,
                -1,
                false,
                false,
            ));
            if i > 0 {
                expected_row.push('\t');
            }
            expected_row.push_str(&i.to_string());
        }
        expected_row.push('\n');

        let result =
            parse_table_row_from_postgres_copy_bytes(expected_row.as_bytes(), &schema).unwrap();

        assert_eq!(result.values.len(), 50);
        for i in 0..50 {
            assert_eq!(result.values[i], Cell::I32(i as i32));
        }
    }

    #[test]
    fn try_from_empty_row_with_columns() {
        let schema = create_test_schema();
        let row_data = b"\t\t\n"; // Empty values but correct number of tabs

        let result = parse_table_row_from_postgres_copy_bytes(row_data, &schema);

        assert!(result.is_err());
    }

    #[test]
    fn try_from_postgres_delimiter_escaping() {
        let schema = vec![
            ColumnSchema::new("col1".to_string(), Type::TEXT, -1, false, false),
            ColumnSchema::new("col2".to_string(), Type::TEXT, -1, false, false),
        ];

        // Postgres escapes tab characters in data with \\t
        let row_data = b"value\\twith\\ttabs\tnormal\\tvalue\n";
        let result = parse_table_row_from_postgres_copy_bytes(row_data, &schema).unwrap();

        assert_eq!(
            result.values[0],
            Cell::String("value\twith\ttabs".to_string())
        );
        assert_eq!(result.values[1], Cell::String("normal\tvalue".to_string()));
    }

    #[test]
    fn try_from_postgres_escape_at_field_boundaries() {
        let schema = vec![
            ColumnSchema::new("col1".to_string(), Type::TEXT, -1, false, false),
            ColumnSchema::new("col2".to_string(), Type::TEXT, -1, false, false),
            ColumnSchema::new("col3".to_string(), Type::TEXT, -1, false, false),
        ];

        // Escapes at the beginning, middle, and end of fields
        let row_data = b"\\tstart\tmiddle\\nvalue\tend\\r\n";
        let result = parse_table_row_from_postgres_copy_bytes(row_data, &schema).unwrap();

        assert_eq!(result.values[0], Cell::String("\tstart".to_string()));
        assert_eq!(result.values[1], Cell::String("middle\nvalue".to_string()));
        assert_eq!(result.values[2], Cell::String("end\r".to_string()));
    }

    #[test]
    fn try_from_postgres_multibyte_with_escapes() {
        let schema = create_single_column_schema("data", Type::TEXT);

        // Unicode text with escape sequences (testing multibyte character handling)
        let row_data = "Hello\\t🌍\\nWorld\\r测试".as_bytes();
        let mut row_with_newline = row_data.to_vec();
        row_with_newline.push(b'\n');

        let result = parse_table_row_from_postgres_copy_bytes(&row_with_newline, &schema).unwrap();

        assert_eq!(
            result.values[0],
            Cell::String("Hello\t🌍\nWorld\r测试".to_string())
        );
    }

    #[test]
    fn try_from_postgres_escape_sequences() {
        let schema = create_single_column_schema("data", Type::TEXT);

        // Comprehensive test of all escape sequences that Postgres COPY TO produces
        let test_cases: Vec<(&[u8], &str)> = vec![
            // Control character escapes
            (b"\\b\n", "\u{0008}"), // backspace
            (b"\\f\n", "\u{000C}"), // form feed
            (b"\\n\n", "\n"),       // newline
            (b"\\r\n", "\r"),       // carriage return
            (b"\\t\n", "\t"),       // tab
            (b"\\v\n", "\u{000B}"), // vertical tab
            (b"\\\\\n", "\\"),      // backslash
            // Non-special characters (backslash removed, character kept)
            (b"\\x\n", "x"),   // letter
            (b"\\1\n", "1"),   // digit
            (b"\\!\n", "!"),   // punctuation
            (b"\\@\n", "@"),   // symbol
            (b"\\\"\n", "\""), // quote
            // Complex patterns
            (
                "Text\\bwith\\bbackspaces\n".as_bytes(),
                "Text\u{0008}with\u{0008}backspaces",
            ),
            (
                "Form\\ffeed\\ftest\n".as_bytes(),
                "Form\u{000C}feed\u{000C}test",
            ),
            (
                "Vertical\\vtab\\vtest\n".as_bytes(),
                "Vertical\u{000B}tab\u{000B}test",
            ),
            ("Path\\\\to\\\\file.txt\n".as_bytes(), "Path\\to\\file.txt"),
            ("\\n\\n\\t\\t\\r\\r\n".as_bytes(), "\n\n\t\t\r\r"), // consecutive escapes
            // Mixed escape combinations
            (
                "Line1\\nTab:\\tBackslash:\\\\End\n".as_bytes(),
                "Line1\nTab:\tBackslash:\\End",
            ),
        ];

        for (input, expected) in test_cases {
            let result = parse_table_row_from_postgres_copy_bytes(input, &schema).unwrap();
            assert_eq!(
                result.values[0],
                Cell::String(expected.to_string()),
                "Failed for input: {:?}",
                str::from_utf8(input).unwrap_or("<invalid UTF-8>")
            );
        }
    }

    #[test]
    fn try_from_postgres_null_handling() {
        let schema = create_single_column_schema("data", Type::TEXT);

        // Test NULL marker vs empty string vs literal \N
        let test_cases: Vec<(&[u8], Cell)> = vec![
            (b"\\N\n", Cell::Null),                // NULL marker
            (b"\n", Cell::String("".to_string())), // empty string
            ("\\\\N\n".as_bytes(), Cell::Null),    // NULL marker
        ];

        for (input, expected) in test_cases {
            let result = parse_table_row_from_postgres_copy_bytes(input, &schema).unwrap();
            assert_eq!(
                result.values[0],
                expected,
                "Failed for input: {:?}",
                str::from_utf8(input).unwrap_or("<invalid UTF-8>")
            );
        }
    }
}
