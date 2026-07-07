use core::str;

use tracing::error;

use crate::{
    bail,
    data::{Cell, TableRow},
    error::{ErrorKind, EtlResult},
    postgres::codec::text::parse_cell_from_postgres_text,
    schema::ColumnSchema,
};

/// Below this length, a plain byte-by-byte scan outperforms `memchr3`: for
/// short slices, `memchr3`'s setup cost (splatting each needle byte into a
/// SIMD register and checking length preconditions) is not repaid by scanning
/// fewer bytes. Chosen empirically: fields with a delimiter, newline, or
/// backslash roughly every 5 bytes or more see a net speedup from routing
/// through `memchr3` at this threshold; only pathological fields with one of
/// those bytes every 1-2 bytes, which Postgres COPY output does not produce,
/// are slower than scanning byte by byte throughout.
const MEMCHR_MIN_LEN: usize = 32;

/// Returns the offset of the next tab, newline, or backslash in `haystack`,
/// using a vectorized search for longer haystacks and a plain scan for short
/// ones where that search's setup cost would dominate.
fn find_next_special(haystack: &[u8]) -> Option<usize> {
    if haystack.len() < MEMCHR_MIN_LEN {
        return haystack.iter().position(|&b| matches!(b, b'\t' | b'\n' | b'\\'));
    }

    memchr::memchr3(b'\t', b'\n', b'\\', haystack)
}

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
pub(crate) fn parse_table_row_from_postgres_copy_bytes<'a>(
    row: &[u8],
    mut column_schemas: impl ExactSizeIterator<Item = &'a ColumnSchema>,
) -> EtlResult<TableRow> {
    let expected_column_count = column_schemas.len();
    let mut values = Vec::with_capacity(column_schemas.len());

    let row_str = str::from_utf8(row)?;
    let bytes = row_str.as_bytes();
    let mut pos = 0;
    let mut val_str = String::new();
    let mut row_terminated = false;
    let mut done = false;

    // Main parsing loop - continues until all characters are processed
    while !done {
        // Byte offset of the start of the literal run not yet appended to `val_str`.
        // Postgres COPY text format only ever backslash-escapes ASCII bytes (the
        // delimiter, backslash itself, newline, and carriage return), so scanning
        // for those bytes and bulk-appending everything in between never splits a
        // multibyte UTF-8 character: an ASCII byte can never appear as part of a
        // multibyte sequence, so `literal_start` and `pos` always land on a valid
        // char boundary.
        let mut literal_start = pos;

        // Inner loop parses a single field value until tab, newline, or end of input.
        // `memchr3` scans for the next occurrence of any of the three bytes using
        // vectorized comparisons across many bytes at once, rather than checking
        // one byte at a time, so long literal runs (the common case for large
        // `text`/`bytea` payloads) are skipped over in bulk. Fields with escapes
        // packed only a few bytes apart would otherwise pay `memchr3`'s per-call
        // setup cost far more often than a plain byte scan would, so
        // `find_next_special` only calls into it once the remaining slice is long
        // enough for that cost to pay off.
        loop {
            let Some(offset) = find_next_special(&bytes[pos..]) else {
                // No delimiter, terminator, or escape anywhere in the rest of the
                // row. A properly terminated row always has an unescaped newline
                // remaining at this point, so this means the row is incomplete.
                if bytes.len() > literal_start {
                    val_str.push_str(&row_str[literal_start..]);
                }

                if !row_terminated {
                    bail!(ErrorKind::ConversionError, "Row data not properly terminated");
                }
                done = true;

                break;
            };
            let special_pos = pos + offset;

            // Escapes packed only a byte or two apart (or consecutive delimiters,
            // like an empty field) make this run empty most of the time; skip the
            // slice and its UTF-8 boundary check rather than appending nothing.
            if special_pos > literal_start {
                val_str.push_str(&row_str[literal_start..special_pos]);
            }

            match bytes[special_pos] {
                // Field separator - end current field parsing
                b'\t' => {
                    pos = special_pos + 1;
                    break;
                }
                // Row terminator - end current field and mark row complete
                b'\n' => {
                    pos = special_pos + 1;
                    row_terminated = true;
                    break;
                }
                // Escape character - decode the following character and keep scanning
                // this same field.
                _ => {
                    pos = special_pos + 1;

                    match bytes.get(pos) {
                        // Postgres COPY TO only ever escapes ASCII bytes (the
                        // delimiter, backslash, newline, and carriage return), so
                        // this fast path handles every real escape without going
                        // through the pricier `chars()` decode below.
                        Some(&escaped) if escaped.is_ascii() => {
                            match escaped {
                                // Special case: \N when escaped becomes literal \N (not NULL)
                                b'N' => val_str.push_str("\\N"),
                                // Standard Postgres escape sequences
                                b'b' => val_str.push(8 as char), // backspace
                                b'f' => val_str.push(12 as char), // form feed
                                b'n' => val_str.push('\n'),
                                b'r' => val_str.push('\r'),
                                b't' => val_str.push('\t'),
                                b'v' => val_str.push(11 as char), // vertical tab
                                // Any other byte: strip backslash, keep the byte
                                other => val_str.push(other as char),
                            }
                            pos += 1;
                        }
                        // A non-ASCII byte following a backslash. Postgres never
                        // actually emits this, but decode it correctly rather than
                        // reading only its first byte, which would corrupt the value.
                        Some(_) => {
                            let escaped = row_str[pos..]
                                .chars()
                                .next()
                                .expect("validated UTF-8 has a char at a valid boundary");
                            val_str.push(escaped);
                            pos += escaped.len_utf8();
                        }
                        // Trailing backslash with no following byte at all. The row
                        // cannot be validly terminated after this, so the missing
                        // terminator check below will reject it; keep the dangling
                        // backslash out of the value rather than guessing its meaning.
                        None => {}
                    }

                    literal_start = pos;
                }
            }
        }

        // Process the parsed field value if we're not done with the entire row
        if !done {
            // Get the next column schema - error if we have more fields than expected
            let Some(column_schema) = column_schemas.next() else {
                let actual_column_count = values.len() + 1;
                bail!(
                    ErrorKind::ConversionError,
                    "Postgres COPY row contains more columns than the table schema",
                    format!(
                        "The table schema expects {} replicated columns, but the COPY row \
                         contains at least {}. The first extra field is at position {}.",
                        expected_column_count, actual_column_count, actual_column_count
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
                // parsing for all supported data types (integers, floats, strings, booleans,
                // etc.)
                match parse_cell_from_postgres_text(&column_schema.typ, &val_str) {
                    Ok(value) => value,
                    Err(e) => {
                        // Avoid logging source row values, which may contain customer data.
                        error!(
                            column_name = %column_schema.name,
                            column_type = %column_schema.typ,
                            value_length = val_str.len(),
                            "error parsing column from postgres text",
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
    if let Some(missing_column_schema) = column_schemas.next() {
        let actual_column_count = values.len();
        bail!(
            ErrorKind::ConversionError,
            "Postgres COPY row contains fewer columns than the table schema",
            format!(
                "The table schema expects {} replicated columns, but the COPY row contains {}. \
                 The next missing column is '{}' at position {}.",
                expected_column_count,
                actual_column_count,
                missing_column_schema.name,
                actual_column_count + 1
            )
        );
    }

    Ok(TableRow::new(values))
}

#[cfg(test)]
mod tests {
    use tokio_postgres::types::Type;

    use super::*;
    use crate::{error::ErrorKind, schema::ColumnSchema};

    /// Creates a test column schema with sensible defaults.
    fn test_column(
        name: &str,
        typ: Type,
        ordinal_position: i32,
        nullable: bool,
        primary_key: bool,
    ) -> ColumnSchema {
        ColumnSchema::new(name.to_owned(), typ, -1, ordinal_position, nullable)
            .with_primary_key_ordinal_position(if primary_key { Some(1) } else { None })
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
    fn try_from_trailing_backslash_with_no_terminator() {
        let column_schemas = create_single_column_schema("value", Type::TEXT);
        let row_data = b"text\\"; // Dangling escape byte, no following character at all

        let result = parse_table_row_from_postgres_copy_bytes(row_data, column_schemas.iter());

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::ConversionError));
        assert!(err.to_string().contains("Row data not properly terminated"));
    }

    #[test]
    fn try_from_column_count_mismatch() {
        let column_schemas = create_test_column_schemas();
        let row_data = b"123\tJohn\n";

        let result_empty =
            parse_table_row_from_postgres_copy_bytes(row_data, column_schemas.iter());
        assert!(result_empty.is_err());
        let err = result_empty.unwrap_err();
        assert_eq!(
            err.description(),
            Some("Postgres COPY row contains fewer columns than the table schema")
        );
        assert_eq!(
            err.detail(),
            Some(
                "The table schema expects 3 replicated columns, but the COPY row contains 2. The \
                 next missing column is 'active' at position 3."
            )
        );
    }

    #[test]
    fn try_from_too_many_columns() {
        let column_schemas = create_test_column_schemas();
        let row_data = b"123\tJohn\tt\textra\n";

        let result = parse_table_row_from_postgres_copy_bytes(row_data, column_schemas.iter());

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(
            err.description(),
            Some("Postgres COPY row contains more columns than the table schema")
        );
        assert_eq!(
            err.detail(),
            Some(
                "The table schema expects 3 replicated columns, but the COPY row contains at \
                 least 4. The first extra field is at position 4."
            )
        );
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
