use chrono::{Datelike, NaiveDate, NaiveTime, Timelike};
use duckdb::types::{TimeUnit, Value};
use etl::types::{ArrayCell, Cell, PgDate, PgTime, PgTimestamp, PgTimestampTz, TableRow};
use pg_escape::quote_literal;

/// Prepared row payload reused across retry attempts.
pub(super) enum PreparedRows {
    Appender(Vec<Vec<Value>>),
    SqlLiterals(Vec<String>),
}

/// Converts table rows into a retryable payload for DuckDB writes.
pub(super) fn prepare_rows(table_rows: Vec<TableRow>) -> PreparedRows {
    if table_rows.iter().any(|row| row.values().iter().any(cell_requires_sql_literals)) {
        return PreparedRows::SqlLiterals(
            table_rows.into_iter().map(table_row_to_sql_literal).collect(),
        );
    }

    PreparedRows::Appender(
        table_rows
            .into_iter()
            .map(|row| row.into_values().into_iter().map(cell_to_value).collect())
            .collect(),
    )
}

/// Serializes a borrowed row into a SQL `VALUES (...)` tuple.
pub(super) fn table_row_to_sql_literal_ref(row: &TableRow) -> String {
    format!("({})", row.values().iter().map(cell_to_sql_literal_ref).collect::<Vec<_>>().join(", "))
}

/// Serializes a borrowed cell into a DuckDB SQL literal expression.
pub(super) fn cell_to_sql_literal_ref(cell: &Cell) -> String {
    match cell {
        Cell::Null => "NULL".to_owned(),
        Cell::Bool(value) => bool_literal(*value),
        Cell::String(value) => quote_literal(value),
        Cell::I16(value) => value.to_string(),
        Cell::I32(value) => value.to_string(),
        Cell::U32(value) => value.to_string(),
        Cell::I64(value) => value.to_string(),
        Cell::F32(value) => float_literal(*value as f64, false),
        Cell::F64(value) => float_literal(*value, true),
        Cell::Numeric(value) => quote_literal(&value.to_string()),
        Cell::Date(value) => format!("DATE '{value}'"),
        Cell::Time(value) => format!("TIME '{value}'"),
        Cell::Timestamp(value) => format!("TIMESTAMP '{value}'"),
        Cell::TimestampTz(value) => format!("TIMESTAMPTZ '{value}'"),
        Cell::Uuid(value) => format!("CAST({} AS UUID)", quote_literal(&value.to_string())),
        Cell::Bytes(value) => format!("from_hex('{}')", encode_hex(value)),
        Cell::Array(value) => array_cell_to_sql_literal_ref(value),
    }
}

/// Returns whether a cell must bypass the DuckDB appender path.
fn cell_requires_sql_literals(cell: &Cell) -> bool {
    matches!(cell, Cell::Array(_)) || cell_contains_non_finite_temporal(cell)
}

/// Returns whether a cell contains a PostgreSQL-only temporal value.
fn cell_contains_non_finite_temporal(cell: &Cell) -> bool {
    match cell {
        Cell::Date(value) => !matches!(value, PgDate::Finite(_)),
        Cell::Time(value) => !matches!(value, PgTime::Finite(_)),
        Cell::Timestamp(value) => !matches!(value, PgTimestamp::Finite(_)),
        Cell::TimestampTz(value) => !matches!(value, PgTimestampTz::Finite(_)),
        _ => false,
    }
}

/// Serializes a row into a SQL `VALUES (...)` tuple.
fn table_row_to_sql_literal(row: TableRow) -> String {
    format!(
        "({})",
        row.into_values().into_iter().map(cell_to_sql_literal).collect::<Vec<_>>().join(", ")
    )
}

/// Converts a [`Cell`] into a DuckDB SQL literal expression.
fn cell_to_sql_literal(cell: Cell) -> String {
    match cell {
        Cell::Null => "NULL".to_owned(),
        Cell::Bool(b) => bool_literal(b),
        Cell::String(s) => quote_literal(&s),
        Cell::I16(i) => i.to_string(),
        Cell::I32(i) => i.to_string(),
        Cell::U32(u) => u.to_string(),
        Cell::I64(i) => i.to_string(),
        Cell::F32(f) => float_literal(f as f64, false),
        Cell::F64(f) => float_literal(f, true),
        Cell::Numeric(n) => quote_literal(&n.to_string()),
        Cell::Date(d) => format!("DATE '{d}'"),
        Cell::Time(t) => format!("TIME '{t}'"),
        Cell::Timestamp(dt) => format!("TIMESTAMP '{dt}'"),
        Cell::TimestampTz(dt) => format!("TIMESTAMPTZ '{dt}'"),
        Cell::Uuid(u) => format!("CAST({} AS UUID)", quote_literal(&u.to_string())),
        Cell::Bytes(b) => format!("from_hex('{}')", encode_hex(&b)),
        Cell::Array(arr) => array_cell_to_sql_literal(arr),
    }
}

/// Returns a DuckDB SQL literal for a boolean value.
fn bool_literal(value: bool) -> String {
    if value { "TRUE".to_owned() } else { "FALSE".to_owned() }
}

/// Converts an [`ArrayCell`] into a DuckDB list literal expression.
fn array_cell_to_sql_literal(arr: ArrayCell) -> String {
    let values: Vec<String> = match arr {
        ArrayCell::Bool(v) => v
            .into_iter()
            .map(|o| {
                o.map_or_else(
                    || "NULL".to_owned(),
                    |value| if value { "TRUE" } else { "FALSE" }.to_owned(),
                )
            })
            .collect(),
        ArrayCell::String(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| quote_literal(&value)))
            .collect(),
        ArrayCell::I16(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| value.to_string()))
            .collect(),
        ArrayCell::I32(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| value.to_string()))
            .collect(),
        ArrayCell::U32(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| value.to_string()))
            .collect(),
        ArrayCell::I64(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| value.to_string()))
            .collect(),
        ArrayCell::F32(v) => v
            .into_iter()
            .map(|o| {
                o.map_or_else(|| "NULL".to_owned(), |value| float_literal(value as f64, false))
            })
            .collect(),
        ArrayCell::F64(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| float_literal(value, true)))
            .collect(),
        ArrayCell::Numeric(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| quote_literal(&value.to_string())))
            .collect(),
        ArrayCell::Date(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| format!("DATE '{value}'")))
            .collect(),
        ArrayCell::Time(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| format!("TIME '{value}'")))
            .collect(),
        ArrayCell::Timestamp(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| format!("TIMESTAMP '{value}'")))
            .collect(),
        ArrayCell::TimestampTz(v) => v
            .into_iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| format!("TIMESTAMPTZ '{value}'")))
            .collect(),
        ArrayCell::Uuid(v) => v
            .into_iter()
            .map(|o| {
                o.map_or_else(
                    || "NULL".to_owned(),
                    |value| format!("CAST({} AS UUID)", quote_literal(&value.to_string())),
                )
            })
            .collect(),
        ArrayCell::Bytes(v) => v
            .into_iter()
            .map(|o| {
                o.map_or_else(
                    || "NULL".to_owned(),
                    |value| format!("from_hex('{}')", encode_hex(&value)),
                )
            })
            .collect(),
    };

    format!("[{}]", values.join(", "))
}

/// Serializes a borrowed [`ArrayCell`] into a DuckDB list literal expression.
fn array_cell_to_sql_literal_ref(arr: &ArrayCell) -> String {
    let values: Vec<String> = match arr {
        ArrayCell::Bool(v) => {
            v.iter().map(|o| o.map_or_else(|| "NULL".to_owned(), bool_literal)).collect()
        }
        ArrayCell::String(v) => v
            .iter()
            .map(|o| o.as_ref().map_or_else(|| "NULL".to_owned(), |value| quote_literal(value)))
            .collect(),
        ArrayCell::I16(v) => v
            .iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| value.to_string()))
            .collect(),
        ArrayCell::I32(v) => v
            .iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| value.to_string()))
            .collect(),
        ArrayCell::U32(v) => v
            .iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| value.to_string()))
            .collect(),
        ArrayCell::I64(v) => v
            .iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| value.to_string()))
            .collect(),
        ArrayCell::F32(v) => v
            .iter()
            .map(|o| {
                o.map_or_else(|| "NULL".to_owned(), |value| float_literal(value as f64, false))
            })
            .collect(),
        ArrayCell::F64(v) => v
            .iter()
            .map(|o| o.map_or_else(|| "NULL".to_owned(), |value| float_literal(value, true)))
            .collect(),
        ArrayCell::Numeric(v) => v
            .iter()
            .map(|o| {
                o.as_ref()
                    .map_or_else(|| "NULL".to_owned(), |value| quote_literal(&value.to_string()))
            })
            .collect(),
        ArrayCell::Date(v) => v
            .iter()
            .map(|o| {
                o.as_ref().map_or_else(|| "NULL".to_owned(), |value| format!("DATE '{value}'"))
            })
            .collect(),
        ArrayCell::Time(v) => v
            .iter()
            .map(|o| {
                o.as_ref().map_or_else(|| "NULL".to_owned(), |value| format!("TIME '{value}'"))
            })
            .collect(),
        ArrayCell::Timestamp(v) => v
            .iter()
            .map(|o| {
                o.as_ref().map_or_else(|| "NULL".to_owned(), |value| format!("TIMESTAMP '{value}'"))
            })
            .collect(),
        ArrayCell::TimestampTz(v) => v
            .iter()
            .map(|o| {
                o.as_ref()
                    .map_or_else(|| "NULL".to_owned(), |value| format!("TIMESTAMPTZ '{value}'"))
            })
            .collect(),
        ArrayCell::Uuid(v) => v
            .iter()
            .map(|o| {
                o.map_or_else(
                    || "NULL".to_owned(),
                    |value| format!("CAST({} AS UUID)", quote_literal(&value.to_string())),
                )
            })
            .collect(),
        ArrayCell::Bytes(v) => v
            .iter()
            .map(|o| {
                o.as_ref().map_or_else(
                    || "NULL".to_owned(),
                    |value| format!("from_hex('{}')", encode_hex(value)),
                )
            })
            .collect(),
    };

    format!("[{}]", values.join(", "))
}

/// Returns a DuckDB SQL literal for a floating-point value.
fn float_literal(value: f64, is_double: bool) -> String {
    if value.is_nan() {
        return if is_double {
            "CAST('NaN' AS DOUBLE)".to_owned()
        } else {
            "CAST('NaN' AS FLOAT)".to_owned()
        };
    }
    if value == f64::INFINITY {
        return if is_double {
            "CAST('Infinity' AS DOUBLE)".to_owned()
        } else {
            "CAST('Infinity' AS FLOAT)".to_owned()
        };
    }
    if value == f64::NEG_INFINITY {
        return if is_double {
            "CAST('-Infinity' AS DOUBLE)".to_owned()
        } else {
            "CAST('-Infinity' AS FLOAT)".to_owned()
        };
    }

    value.to_string()
}

/// Encodes bytes as uppercase hexadecimal for DuckDB's `from_hex`.
fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02X}")).collect()
}

/// Converts a date to DuckDB `DATE` days since Unix epoch.
fn date_to_days_since_epoch(value: NaiveDate) -> i32 {
    value.num_days_from_ce() - 719_163
}

/// Converts a time to DuckDB `TIME` microseconds since midnight.
fn time_to_micros_since_midnight(value: NaiveTime) -> i64 {
    i64::from(value.num_seconds_from_midnight()) * 1_000_000 + i64::from(value.nanosecond() / 1_000)
}

/// Converts a [`Cell`] to a [`duckdb::types::Value`] for use with parameterized
/// INSERT statements.
fn cell_to_value(cell: Cell) -> Value {
    match cell {
        Cell::Null => Value::Null,
        Cell::Bool(b) => Value::Boolean(b),
        Cell::String(s) => Value::Text(s),
        Cell::I16(i) => Value::SmallInt(i),
        Cell::I32(i) => Value::Int(i),
        Cell::U32(u) => Value::UInt(u),
        Cell::I64(i) => Value::BigInt(i),
        Cell::F32(f) => Value::Float(f),
        Cell::F64(f) => Value::Double(f),
        // NUMERIC stored as VARCHAR to avoid precision loss.
        Cell::Numeric(n) => Value::Text(n.to_string()),
        Cell::Date(d) => {
            if let Some(d) = d.as_finite() {
                Value::Date32(date_to_days_since_epoch(d))
            } else {
                Value::Text(d.to_string())
            }
        }
        Cell::Time(t) => {
            if let Some(t) = t.as_finite() {
                Value::Time64(TimeUnit::Microsecond, time_to_micros_since_midnight(t))
            } else {
                Value::Text(t.to_string())
            }
        }
        Cell::Timestamp(dt) => {
            if let Some(dt) = dt.as_finite() {
                Value::Timestamp(TimeUnit::Microsecond, dt.and_utc().timestamp_micros())
            } else {
                Value::Text(dt.to_string())
            }
        }
        Cell::TimestampTz(dt) => {
            if let Some(dt) = dt.as_finite() {
                Value::Timestamp(TimeUnit::Microsecond, dt.timestamp_micros())
            } else {
                Value::Text(dt.to_string())
            }
        }
        // UUID stored as text; DuckDB casts VARCHAR → UUID automatically.
        Cell::Uuid(u) => Value::Text(u.to_string()),
        Cell::Bytes(b) => Value::Blob(b),
        Cell::Array(arr) => array_cell_to_value(arr),
    }
}

/// Converts an [`ArrayCell`] (with nullable elements) to a `Value::List`.
fn array_cell_to_value(arr: ArrayCell) -> Value {
    let values = match arr {
        ArrayCell::Bool(v) => {
            v.into_iter().map(|o| o.map_or(Value::Null, Value::Boolean)).collect()
        }
        ArrayCell::String(v) => v.into_iter().map(|o| o.map_or(Value::Null, Value::Text)).collect(),
        ArrayCell::I16(v) => {
            v.into_iter().map(|o| o.map_or(Value::Null, Value::SmallInt)).collect()
        }
        ArrayCell::I32(v) => v.into_iter().map(|o| o.map_or(Value::Null, Value::Int)).collect(),
        ArrayCell::U32(v) => v.into_iter().map(|o| o.map_or(Value::Null, Value::UInt)).collect(),
        ArrayCell::I64(v) => v.into_iter().map(|o| o.map_or(Value::Null, Value::BigInt)).collect(),
        ArrayCell::F32(v) => v.into_iter().map(|o| o.map_or(Value::Null, Value::Float)).collect(),
        ArrayCell::F64(v) => v.into_iter().map(|o| o.map_or(Value::Null, Value::Double)).collect(),
        ArrayCell::Numeric(v) => {
            v.into_iter().map(|o| o.map_or(Value::Null, |n| Value::Text(n.to_string()))).collect()
        }
        ArrayCell::Date(v) => v
            .into_iter()
            .map(|o| {
                o.map_or(Value::Null, |value| {
                    value.as_finite().map_or_else(
                        || Value::Text(value.to_string()),
                        |value| Value::Date32(date_to_days_since_epoch(value)),
                    )
                })
            })
            .collect(),
        ArrayCell::Time(v) => v
            .into_iter()
            .map(|o| {
                o.map_or(Value::Null, |value| {
                    value.as_finite().map_or_else(
                        || Value::Text(value.to_string()),
                        |value| {
                            Value::Time64(
                                TimeUnit::Microsecond,
                                time_to_micros_since_midnight(value),
                            )
                        },
                    )
                })
            })
            .collect(),
        ArrayCell::Timestamp(v) => v
            .into_iter()
            .map(|o| {
                o.map_or(Value::Null, |value| {
                    value.as_finite().map_or_else(
                        || Value::Text(value.to_string()),
                        |value| {
                            Value::Timestamp(
                                TimeUnit::Microsecond,
                                value.and_utc().timestamp_micros(),
                            )
                        },
                    )
                })
            })
            .collect(),
        ArrayCell::TimestampTz(v) => v
            .into_iter()
            .map(|o| {
                o.map_or(Value::Null, |value| {
                    value.as_finite().map_or_else(
                        || Value::Text(value.to_string()),
                        |value| Value::Timestamp(TimeUnit::Microsecond, value.timestamp_micros()),
                    )
                })
            })
            .collect(),
        ArrayCell::Uuid(v) => {
            v.into_iter().map(|o| o.map_or(Value::Null, |u| Value::Text(u.to_string()))).collect()
        }
        ArrayCell::Bytes(v) => v.into_iter().map(|o| o.map_or(Value::Null, Value::Blob)).collect(),
    };
    Value::List(values)
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};

    use super::*;

    #[test]
    fn cell_to_value_primitives() {
        assert_eq!(cell_to_value(Cell::Null), Value::Null);
        assert_eq!(cell_to_value(Cell::Bool(true)), Value::Boolean(true));
        assert_eq!(
            cell_to_value(Cell::String("hello".to_owned())),
            Value::Text("hello".to_owned())
        );
        assert_eq!(cell_to_value(Cell::I32(42)), Value::Int(42));
        assert_eq!(cell_to_value(Cell::I64(-1)), Value::BigInt(-1));
        assert_eq!(cell_to_value(Cell::F64(3.46)), Value::Double(3.46));
    }

    #[test]
    fn cell_to_value_date_uses_duckdb_epoch_days() {
        let pre_epoch = NaiveDate::from_ymd_opt(1969, 12, 31).unwrap();
        assert_eq!(cell_to_value(Cell::Date(pre_epoch.into())), Value::Date32(-1));

        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        assert_eq!(cell_to_value(Cell::Date(epoch.into())), Value::Date32(0));
    }

    #[test]
    fn cell_to_value_timestamptz_uses_unix_epoch_microseconds() {
        let timestamp = DateTime::<Utc>::from_timestamp(1, 500_000_000).unwrap();

        assert_eq!(
            cell_to_value(Cell::TimestampTz(timestamp.into())),
            Value::Timestamp(TimeUnit::Microsecond, 1_500_000)
        );
    }

    #[test]
    fn sql_literals_preserve_duckdb_temporal_special_values() {
        assert_eq!(cell_to_sql_literal(Cell::Date(PgDate::PosInfinity)), "DATE 'infinity'");
        assert_eq!(cell_to_sql_literal(Cell::Date(PgDate::NegInfinity)), "DATE '-infinity'");
        assert_eq!(
            cell_to_sql_literal(Cell::Timestamp(PgTimestamp::PosInfinity)),
            "TIMESTAMP 'infinity'"
        );
        assert_eq!(
            cell_to_sql_literal(Cell::TimestampTz(PgTimestampTz::NegInfinity)),
            "TIMESTAMPTZ '-infinity'"
        );
    }

    #[test]
    fn array_cell_to_sql_literal_preserves_nulls() {
        assert_eq!(
            array_cell_to_sql_literal(ArrayCell::I32(vec![Some(1), None, Some(3)])),
            "[1, NULL, 3]"
        );
        assert_eq!(
            array_cell_to_sql_literal(ArrayCell::String(vec![Some(r#"{"a":1}"#.to_owned()), None])),
            "['{\"a\":1}', NULL]"
        );
    }

    #[test]
    fn prepare_rows_uses_sql_literals_for_arrays() {
        let prepared = prepare_rows(vec![TableRow::new(vec![
            Cell::I32(1),
            Cell::Array(ArrayCell::I32(vec![Some(1), None, Some(3)])),
        ])]);

        match prepared {
            PreparedRows::SqlLiterals(rows) => {
                assert_eq!(rows, vec!["(1, [1, NULL, 3])"]);
            }
            PreparedRows::Appender(_) => panic!("expected sql literal fallback"),
        }
    }
}
