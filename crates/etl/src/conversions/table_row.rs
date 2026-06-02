use core::str;

use etl_postgres::types::ColumnSchema;
#[cfg(test)]
use tracing::error;

use crate::{
    bail,
    error::{ErrorKind, EtlResult},
};
#[cfg(test)]
use crate::{
    conversions::text::parse_cell_from_postgres_text,
    types::{Cell, TableRow},
};

/// Converts raw Postgres COPY format data into a typed table row.
///
/// This method parses the text format data produced by Postgres's COPY command
/// and converts it into strongly-typed [`Cell`] values according to the
/// provided column schemas. It handles Postgres's specific escaping rules and
/// type formats.
///
/// # Errors
///
/// Returns an error if the row data is not valid UTF-8, the column count
/// doesn't match the schema, the row is not properly terminated, or a cell
/// value cannot be parsed according to its column type.
#[cfg(test)]
pub(crate) fn parse_table_row_from_postgres_copy_bytes<'a>(
    row: &[u8],
    column_schemas: impl ExactSizeIterator<Item = &'a ColumnSchema>,
) -> EtlResult<TableRow> {
    let mut values = Vec::with_capacity(column_schemas.len());

    parse_postgres_copy_row_fields(row, column_schemas, |_, column_schema, value| {
        let value = match value {
            Some(value) => match parse_cell_from_postgres_text(&column_schema.typ, value) {
                Ok(value) => value,
                Err(e) => {
                    // Avoid logging source row values, which may contain customer data.
                    error!(
                        column_name = %column_schema.name,
                        column_type = %column_schema.typ,
                        value_length = value.len(),
                        "error parsing column from postgres text",
                    );
                    return Err(e);
                }
            },
            None => Cell::Null,
        };

        values.push(value);

        Ok(())
    })?;

    Ok(TableRow::new(values))
}

/// Visits raw Postgres COPY text fields after applying COPY escaping rules.
///
/// The visitor receives `None` for the unescaped `\N` NULL marker and `Some`
/// for every non-null field.
pub(crate) fn parse_postgres_copy_row_fields<'a, F>(
    row: &[u8],
    mut column_schemas: impl ExactSizeIterator<Item = &'a ColumnSchema>,
    mut visit: F,
) -> EtlResult<()>
where
    F: FnMut(usize, &'a ColumnSchema, Option<&str>) -> EtlResult<()>,
{
    let expected_column_count = column_schemas.len();
    let mut field_idx = 0;

    let row_str = str::from_utf8(row)?;
    let row = row_str.as_bytes();
    let mut field_start = 0;
    let mut escaped_value: Option<String> = None;
    let mut row_terminated = false;
    let mut pos = 0;

    while pos < row.len() {
        match row[pos] {
            b'\\' => {
                let value = if let Some(value) = escaped_value.as_mut() {
                    value.push_str(
                        str::from_utf8(&row[field_start..pos])
                            .expect("field boundaries are valid utf-8"),
                    );
                    value
                } else {
                    let mut value = String::with_capacity(16);
                    value.push_str(
                        str::from_utf8(&row[field_start..pos])
                            .expect("field boundaries are valid utf-8"),
                    );
                    escaped_value.insert(value)
                };

                pos += 1;
                if pos >= row.len() {
                    bail!(ErrorKind::ConversionError, "Row data not properly terminated");
                }

                match row[pos] {
                    b'N' => {
                        value.push('\\');
                        value.push('N');
                        pos += 1;
                    }
                    b'b' => {
                        value.push(8 as char);
                        pos += 1;
                    }
                    b'f' => {
                        value.push(12 as char);
                        pos += 1;
                    }
                    b'n' => {
                        value.push('\n');
                        pos += 1;
                    }
                    b'r' => {
                        value.push('\r');
                        pos += 1;
                    }
                    b't' => {
                        value.push('\t');
                        pos += 1;
                    }
                    b'v' => {
                        value.push(11 as char);
                        pos += 1;
                    }
                    byte if byte.is_ascii() => {
                        value.push(byte as char);
                        pos += 1;
                    }
                    _ => {
                        let escaped_char = row_str[pos..]
                            .chars()
                            .next()
                            .expect("position points to a utf-8 character");
                        value.push(escaped_char);
                        pos += escaped_char.len_utf8();
                    }
                }

                field_start = pos;
            }
            b'\t' | b'\n' => {
                let is_row_terminator = row[pos] == b'\n';
                let Some(column_schema) = column_schemas.next() else {
                    let actual_column_count = field_idx + 1;
                    bail!(
                        ErrorKind::ConversionError,
                        "Column count mismatch between schema and row",
                        format!(
                            "Expected {} columns but row contains at least {} columns",
                            expected_column_count, actual_column_count
                        )
                    );
                };

                if let Some(value) = escaped_value.as_mut() {
                    value.push_str(
                        str::from_utf8(&row[field_start..pos])
                            .expect("field boundaries are valid utf-8"),
                    );
                }

                let value = match escaped_value.as_deref() {
                    Some(value) => value,
                    None => str::from_utf8(&row[field_start..pos])
                        .expect("field boundaries are valid utf-8"),
                };
                let value = if value == "\\N" { None } else { Some(value) };

                visit(field_idx, column_schema, value)?;

                field_idx += 1;
                escaped_value = None;
                pos += 1;
                field_start = pos;

                if is_row_terminator {
                    row_terminated = true;
                    break;
                }
            }
            _ => {
                pos += 1;
            }
        }
    }

    if !row_terminated {
        bail!(ErrorKind::ConversionError, "Row data not properly terminated");
    }

    // Validate that all expected columns were present in the row
    // If there are still columns left in the schema iterator, it means the row
    // had fewer fields than expected, which is an error
    if column_schemas.next().is_some() {
        bail!(
            ErrorKind::ConversionError,
            "Column count mismatch between schema and row",
            format!(
                "Expected {} columns but row contains {} columns",
                expected_column_count, field_idx
            )
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use etl_postgres::types::ColumnSchema;
    use tokio_postgres::types::Type;

    use super::*;
    use crate::error::ErrorKind;

    /// Creates a test column schema with sensible defaults.
    fn test_column(
        name: &str,
        typ: Type,
        ordinal_position: i32,
        nullable: bool,
        primary_key: bool,
    ) -> ColumnSchema {
        ColumnSchema::new(
            name.to_owned(),
            typ,
            -1,
            ordinal_position,
            if primary_key { Some(1) } else { None },
            nullable,
        )
    }

    fn create_test_column_schemas() -> Vec<ColumnSchema> {
        vec![
            test_column("id", Type::INT4, 1, false, true),
            test_column("name", Type::TEXT, 2, true, false),
            test_column("active", Type::BOOL, 3, false, false),
        ]
    }

    fn create_single_column_schema(name: &str, typ: Type) -> Vec<ColumnSchema> {
        vec![test_column(name, typ, 1, false, false)]
    }

    #[test]
    fn try_from_simple_row() {
        let column_schemas = create_test_column_schemas();
        let row_data = b"123\tJohn Doe\tt\n";

        let result =
            parse_table_row_from_postgres_copy_bytes(row_data, column_schemas.iter()).unwrap();

        assert_eq!(result.values().len(), 3);
        assert_eq!(result.values()[0], Cell::I32(123));
        assert_eq!(result.values()[1], Cell::String("John Doe".to_owned()));
        assert_eq!(result.values()[2], Cell::Bool(true));
    }

    #[test]
    fn try_from_with_null_values() {
        let column_schemas = create_test_column_schemas();
        let row_data = b"456\t\\N\tf\n";

        let result =
            parse_table_row_from_postgres_copy_bytes(row_data, column_schemas.iter()).unwrap();

        assert_eq!(result.values().len(), 3);
        assert_eq!(result.values()[0], Cell::I32(456));
        assert_eq!(result.values()[1], Cell::Null);
        assert_eq!(result.values()[2], Cell::Bool(false));
    }

    #[test]
    fn try_from_empty_strings() {
        let column_schemas = create_test_column_schemas();
        let row_data = b"0\t\tf\n";

        let result =
            parse_table_row_from_postgres_copy_bytes(row_data, column_schemas.iter()).unwrap();

        assert_eq!(result.values().len(), 3);
        assert_eq!(result.values()[0], Cell::I32(0));
        assert_eq!(result.values()[1], Cell::String("".to_owned()));
        assert_eq!(result.values()[2], Cell::Bool(false));
    }

    #[test]
    fn try_from_single_column() {
        let column_schemas = create_single_column_schema("value", Type::INT4);
        let row_data = b"42\n";

        let result =
            parse_table_row_from_postgres_copy_bytes(row_data, column_schemas.iter()).unwrap();

        assert_eq!(result.values().len(), 1);
        assert_eq!(result.values()[0], Cell::I32(42));
    }

    #[test]
    fn try_from_multiple_columns_different_types() {
        let column_schemas = [
            test_column("int_col", Type::INT4, 1, false, false),
            test_column("float_col", Type::FLOAT8, 2, false, false),
            test_column("text_col", Type::TEXT, 3, false, false),
            test_column("bool_col", Type::BOOL, 4, false, false),
        ];

        let row_data = b"123\t3.15\tHello World\tt\n";

        let result =
            parse_table_row_from_postgres_copy_bytes(row_data, column_schemas.iter()).unwrap();

        assert_eq!(result.values().len(), 4);
        assert_eq!(result.values()[0], Cell::I32(123));
        assert_eq!(result.values()[1], Cell::F64(3.15));
        assert_eq!(result.values()[2], Cell::String("Hello World".to_owned()));
        assert_eq!(result.values()[3], Cell::Bool(true));
    }

    #[test]
    fn try_from_not_terminated() {
        let column_schemas = create_single_column_schema("value", Type::INT4);
        let row_data = b"42"; // Missing newline

        let result = parse_table_row_from_postgres_copy_bytes(row_data, column_schemas.iter());

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::ConversionError));
        assert!(err.to_string().contains("Row data not properly terminated"));
    }

    #[test]
    fn try_from_column_count_mismatch() {
        let column_schemas = create_test_column_schemas(); // Expects 3 columns
        let row_data = b"123\tJohn\n"; // Only 2 values - this should actually fail at parsing the bool because there's no third column

        let result_empty =
            parse_table_row_from_postgres_copy_bytes(row_data, column_schemas.iter());
        assert!(result_empty.is_err());
        let err = result_empty.unwrap_err();
        assert!(err.to_string().contains("Expected 3 columns but row contains 2 columns"));
    }

    #[test]
    fn try_from_too_many_columns() {
        let column_schemas = create_test_column_schemas(); // Expects 3 columns
        let row_data = b"123\tJohn\tt\textra\n";

        let result = parse_table_row_from_postgres_copy_bytes(row_data, column_schemas.iter());

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Expected 3 columns but row contains at least 4 columns"));
    }

    #[test]
    fn try_from_invalid_utf8() {
        let column_schemas = create_single_column_schema("value", Type::TEXT);
        let row_data = &[0xFF, 0xFE, 0xFD, b'\n']; // Invalid UTF-8

        let result = parse_table_row_from_postgres_copy_bytes(row_data, column_schemas.iter());

        assert!(result.is_err());
    }

    #[test]
    fn try_from_parsing_error() {
        let column_schemas = create_single_column_schema("number", Type::INT4);
        let row_data = b"not_a_number\n";

        let result = parse_table_row_from_postgres_copy_bytes(row_data, column_schemas.iter());

        assert!(result.is_err());
    }

    #[test]
    fn try_from_trailing_escape() {
        let column_schemas = create_single_column_schema("data", Type::TEXT);

        let row_data = b"Text\\\\\n";
        let result =
            parse_table_row_from_postgres_copy_bytes(row_data, column_schemas.iter()).unwrap();

        assert_eq!(result.values().len(), 1);
        assert_eq!(result.values()[0], Cell::String("Text\\".to_owned()));
    }

    #[test]
    fn try_from_null_literal_vs_null_marker() {
        let column_schemas = create_single_column_schema("value", Type::TEXT);

        let row_data = b"\\N\n";
        let result =
            parse_table_row_from_postgres_copy_bytes(row_data, column_schemas.iter()).unwrap();
        assert_eq!(result.values()[0], Cell::Null);

        let row_data = b"\\\\N\n";
        let result_test =
            parse_table_row_from_postgres_copy_bytes(row_data, column_schemas.iter()).unwrap();
        assert_eq!(result_test.values()[0], Cell::Null);

        let row_data = b"\\\\A\n";
        let result_test =
            parse_table_row_from_postgres_copy_bytes(row_data, column_schemas.iter()).unwrap();
        assert_eq!(result_test.values()[0], Cell::String("\\A".to_owned()));
    }

    #[test]
    fn try_from_whitespace_handling() {
        let column_schemas = create_test_column_schemas();

        let row_data = b"123\t John Doe \tt\n";
        let result =
            parse_table_row_from_postgres_copy_bytes(row_data, column_schemas.iter()).unwrap();

        assert_eq!(result.values().len(), 3);
        assert_eq!(result.values()[0], Cell::I32(123));
        assert_eq!(result.values()[1], Cell::String(" John Doe ".to_owned())); // Spaces preserved
        assert_eq!(result.values()[2], Cell::Bool(true));
    }

    #[test]
    fn try_from_large_row() {
        let mut column_schemas = Vec::new();
        let mut expected_row = String::new();

        for i in 0i32..50 {
            column_schemas.push(test_column(&format!("col{i}"), Type::INT4, i + 1, false, false));
            if i > 0 {
                expected_row.push('\t');
            }
            expected_row.push_str(&i.to_string());
        }
        expected_row.push('\n');

        let result = parse_table_row_from_postgres_copy_bytes(
            expected_row.as_bytes(),
            column_schemas.iter(),
        )
        .unwrap();

        assert_eq!(result.values().len(), 50);
        for i in 0..50 {
            assert_eq!(result.values()[i], Cell::I32(i as i32));
        }
    }

    #[test]
    fn try_from_empty_row_with_columns() {
        let column_schemas = create_test_column_schemas();
        let row_data = b"\t\t\n"; // Empty values but correct number of tabs

        let result = parse_table_row_from_postgres_copy_bytes(row_data, column_schemas.iter());

        assert!(result.is_err());
    }

    #[test]
    fn try_from_postgres_delimiter_escaping() {
        let column_schemas = [
            test_column("col1", Type::TEXT, 1, false, false),
            test_column("col2", Type::TEXT, 2, false, false),
        ];

        // Postgres escapes tab characters in data with \\t
        let row_data = b"value\\twith\\ttabs\tnormal\\tvalue\n";
        let result =
            parse_table_row_from_postgres_copy_bytes(row_data, column_schemas.iter()).unwrap();

        assert_eq!(result.values()[0], Cell::String("value\twith\ttabs".to_owned()));
        assert_eq!(result.values()[1], Cell::String("normal\tvalue".to_owned()));
    }

    #[test]
    fn try_from_postgres_escape_at_field_boundaries() {
        let column_schemas = [
            test_column("col1", Type::TEXT, 1, false, false),
            test_column("col2", Type::TEXT, 2, false, false),
            test_column("col3", Type::TEXT, 3, false, false),
        ];

        // Escapes at the beginning, middle, and end of fields
        let row_data = b"\\tstart\tmiddle\\nvalue\tend\\r\n";
        let result =
            parse_table_row_from_postgres_copy_bytes(row_data, column_schemas.iter()).unwrap();

        assert_eq!(result.values()[0], Cell::String("\tstart".to_owned()));
        assert_eq!(result.values()[1], Cell::String("middle\nvalue".to_owned()));
        assert_eq!(result.values()[2], Cell::String("end\r".to_owned()));
    }

    #[test]
    fn try_from_postgres_multibyte_with_escapes() {
        let column_schemas = create_single_column_schema("data", Type::TEXT);

        // Unicode text with escape sequences (testing multibyte character handling)
        let row_data = "Hello\\t🌍\\nWorld\\r测试".as_bytes();
        let mut row_with_newline = row_data.to_vec();
        row_with_newline.push(b'\n');

        let result =
            parse_table_row_from_postgres_copy_bytes(&row_with_newline, column_schemas.iter())
                .unwrap();

        assert_eq!(result.values()[0], Cell::String("Hello\t🌍\nWorld\r测试".to_owned()));
    }

    #[test]
    fn try_from_postgres_escape_sequences() {
        let column_schemas = create_single_column_schema("data", Type::TEXT);

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
            ("Text\\bwith\\bbackspaces\n".as_bytes(), "Text\u{0008}with\u{0008}backspaces"),
            ("Form\\ffeed\\ftest\n".as_bytes(), "Form\u{000C}feed\u{000C}test"),
            ("Vertical\\vtab\\vtest\n".as_bytes(), "Vertical\u{000B}tab\u{000B}test"),
            ("Path\\\\to\\\\file.txt\n".as_bytes(), "Path\\to\\file.txt"),
            ("\\n\\n\\t\\t\\r\\r\n".as_bytes(), "\n\n\t\t\r\r"), // consecutive escapes
            // Mixed escape combinations
            ("Line1\\nTab:\\tBackslash:\\\\End\n".as_bytes(), "Line1\nTab:\tBackslash:\\End"),
        ];

        for (input, expected) in test_cases {
            let result =
                parse_table_row_from_postgres_copy_bytes(input, column_schemas.iter()).unwrap();
            assert_eq!(
                result.values()[0],
                Cell::String(expected.to_owned()),
                "Failed for input: {:?}",
                str::from_utf8(input).unwrap_or("<invalid UTF-8>")
            );
        }
    }

    #[test]
    fn try_from_postgres_null_handling() {
        let column_schemas = create_single_column_schema("data", Type::TEXT);

        // Test NULL marker vs empty string vs literal \N
        let test_cases: Vec<(&[u8], Cell)> = vec![
            (b"\\N\n", Cell::Null),               // NULL marker
            (b"\n", Cell::String("".to_owned())), // empty string
            ("\\\\N\n".as_bytes(), Cell::Null),   // NULL marker
        ];

        for (input, expected) in test_cases {
            let result =
                parse_table_row_from_postgres_copy_bytes(input, column_schemas.iter()).unwrap();
            assert_eq!(
                result.values()[0],
                expected,
                "Failed for input: {:?}",
                str::from_utf8(input).unwrap_or("<invalid UTF-8>")
            );
        }
    }
}
