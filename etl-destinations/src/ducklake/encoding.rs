use chrono::{NaiveDate, NaiveTime};
use duckdb::types::{TimeUnit, Value};
use etl::types::{ArrayCell, Cell, TableRow};
use pg_escape::quote_literal;

/// Prepared row payload reused across retry attempts.
pub(super) enum PreparedRows {
    Appender(Vec<Vec<Value>>),
    SqlLiterals(Vec<String>),
}

/// Converts table rows into a retryable payload for DuckDB writes.
pub(super) fn prepare_rows(table_rows: Vec<TableRow>) -> PreparedRows {
    if table_rows
        .iter()
        .any(|row| row.values().iter().any(cell_requires_sql_literals))
    {
        return PreparedRows::SqlLiterals(
            table_rows
                .into_iter()
                .map(table_row_to_sql_literal)
                .collect(),
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
    format!(
        "({})",
        row.values()
            .iter()
            .map(cell_to_sql_literal_ref)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// Serializes a borrowed cell into a DuckDB SQL literal expression.
pub(super) fn cell_to_sql_literal_ref(cell: &Cell) -> String {
    cell_to_sql_literal(cell_to_owned(cell))
}

/// Returns whether a cell must bypass the DuckDB appender path.
fn cell_requires_sql_literals(cell: &Cell) -> bool {
    matches!(cell, Cell::Array(_))
}

/// Serializes a row into a SQL `VALUES (...)` tuple.
fn table_row_to_sql_literal(row: TableRow) -> String {
    table_row_to_sql_literal_ref(&row)
}

/// Converts a [`Cell`] into a DuckDB SQL literal expression.
fn cell_to_sql_literal(cell: Cell) -> String {
    match cell {
        Cell::Null => "NULL".to_string(),
        Cell::Bool(b) => {
            if b {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Cell::String(s) => quote_literal(&s),
        Cell::I16(i) => i.to_string(),
        Cell::I32(i) => i.to_string(),
        Cell::U32(u) => u.to_string(),
        Cell::I64(i) => i.to_string(),
        Cell::F32(f) => float_literal(f as f64, false),
        Cell::F64(f) => float_literal(f, true),
        Cell::Numeric(n) => quote_literal(&n.to_string()),
        Cell::Date(d) => format!("DATE '{}'", d.format("%Y-%m-%d")),
        Cell::Time(t) => format!("TIME '{}'", t.format("%H:%M:%S%.6f")),
        Cell::Timestamp(dt) => {
            format!("TIMESTAMP '{}'", dt.format("%Y-%m-%d %H:%M:%S%.6f"))
        }
        Cell::TimestampTz(dt) => {
            format!("TIMESTAMPTZ '{}'", dt.format("%Y-%m-%d %H:%M:%S%.6f%:z"))
        }
        Cell::Uuid(u) => format!("CAST({} AS UUID)", quote_literal(&u.to_string())),
        Cell::Json(j) => format!("CAST({} AS JSON)", quote_literal(&j.to_string())),
        Cell::Bytes(b) => format!("from_hex('{}')", encode_hex(&b)),
        Cell::Array(arr) => array_cell_to_sql_literal(arr),
    }
}

/// Clones a [`Cell`] from a borrowed row reference.
fn cell_to_owned(cell: &Cell) -> Cell {
    match cell {
        Cell::Null => Cell::Null,
        Cell::Bool(value) => Cell::Bool(*value),
        Cell::String(value) => Cell::String(value.clone()),
        Cell::I16(value) => Cell::I16(*value),
        Cell::I32(value) => Cell::I32(*value),
        Cell::U32(value) => Cell::U32(*value),
        Cell::I64(value) => Cell::I64(*value),
        Cell::F32(value) => Cell::F32(*value),
        Cell::F64(value) => Cell::F64(*value),
        Cell::Numeric(value) => Cell::Numeric(value.clone()),
        Cell::Date(value) => Cell::Date(*value),
        Cell::Time(value) => Cell::Time(*value),
        Cell::Timestamp(value) => Cell::Timestamp(*value),
        Cell::TimestampTz(value) => Cell::TimestampTz(*value),
        Cell::Uuid(value) => Cell::Uuid(*value),
        Cell::Json(value) => Cell::Json(value.clone()),
        Cell::Bytes(value) => Cell::Bytes(value.clone()),
        Cell::Array(value) => Cell::Array(array_cell_to_owned(value)),
    }
}

/// Clones an [`ArrayCell`] from a borrowed row reference.
fn array_cell_to_owned(cell: &ArrayCell) -> ArrayCell {
    match cell {
        ArrayCell::Bool(values) => ArrayCell::Bool(values.clone()),
        ArrayCell::String(values) => ArrayCell::String(values.clone()),
        ArrayCell::I16(values) => ArrayCell::I16(values.clone()),
        ArrayCell::I32(values) => ArrayCell::I32(values.clone()),
        ArrayCell::U32(values) => ArrayCell::U32(values.clone()),
        ArrayCell::I64(values) => ArrayCell::I64(values.clone()),
        ArrayCell::F32(values) => ArrayCell::F32(values.clone()),
        ArrayCell::F64(values) => ArrayCell::F64(values.clone()),
        ArrayCell::Numeric(values) => ArrayCell::Numeric(values.clone()),
        ArrayCell::Date(values) => ArrayCell::Date(values.clone()),
        ArrayCell::Time(values) => ArrayCell::Time(values.clone()),
        ArrayCell::Timestamp(values) => ArrayCell::Timestamp(values.clone()),
        ArrayCell::TimestampTz(values) => ArrayCell::TimestampTz(values.clone()),
        ArrayCell::Uuid(values) => ArrayCell::Uuid(values.clone()),
        ArrayCell::Json(values) => ArrayCell::Json(values.clone()),
        ArrayCell::Bytes(values) => ArrayCell::Bytes(values.clone()),
    }
}

/// Converts an [`ArrayCell`] into a DuckDB list literal expression.
fn array_cell_to_sql_literal(arr: ArrayCell) -> String {
    let values: Vec<String> = match arr {
        ArrayCell::Bool(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| {
                    if value {
                        "TRUE".to_string()
                    } else {
                        "FALSE".to_string()
                    }
                })
                .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::String(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| quote_literal(&value))
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::I16(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| value.to_string())
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::I32(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| value.to_string())
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::U32(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| value.to_string())
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::I64(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| value.to_string())
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::F32(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| float_literal(value as f64, false))
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::F64(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| float_literal(value, true))
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::Numeric(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| quote_literal(&value.to_string()))
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::Date(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| format!("DATE '{}'", value.format("%Y-%m-%d")))
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::Time(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| format!("TIME '{}'", value.format("%H:%M:%S%.6f")))
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::Timestamp(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| format!("TIMESTAMP '{}'", value.format("%Y-%m-%d %H:%M:%S%.6f")))
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::TimestampTz(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| format!("TIMESTAMPTZ '{}'", value.format("%Y-%m-%d %H:%M:%S%.6f%:z")))
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::Uuid(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| format!("CAST({} AS UUID)", quote_literal(&value.to_string())))
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::Json(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| format!("CAST({} AS JSON)", quote_literal(&value.to_string())))
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::Bytes(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| format!("from_hex('{}')", encode_hex(&value)))
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
    };

    format!("[{}]", values.join(", "))
}

/// Returns a DuckDB SQL literal for a floating-point value.
fn float_literal(value: f64, is_double: bool) -> String {
    if value.is_nan() {
        return if is_double {
            "CAST('NaN' AS DOUBLE)".to_string()
        } else {
            "CAST('NaN' AS FLOAT)".to_string()
        };
    }
    if value == f64::INFINITY {
        return if is_double {
            "CAST('Infinity' AS DOUBLE)".to_string()
        } else {
            "CAST('Infinity' AS FLOAT)".to_string()
        };
    }
    if value == f64::NEG_INFINITY {
        return if is_double {
            "CAST('-Infinity' AS DOUBLE)".to_string()
        } else {
            "CAST('-Infinity' AS FLOAT)".to_string()
        };
    }

    value.to_string()
}

/// Encodes bytes as uppercase hexadecimal for DuckDB's `from_hex`.
fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02X}")).collect()
}

/// Converts a [`Cell`] to a [`duckdb::types::Value`] for use with parameterized
/// INSERT statements.
fn cell_to_value(cell: Cell) -> Value {
    let epoch_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let epoch_time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();

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
        Cell::Date(d) => Value::Date32(d.signed_duration_since(epoch_date).num_days() as i32),
        Cell::Time(t) => {
            let micros = t
                .signed_duration_since(epoch_time)
                .num_microseconds()
                .unwrap_or(0);
            Value::Time64(TimeUnit::Microsecond, micros)
        }
        Cell::Timestamp(dt) => {
            Value::Timestamp(TimeUnit::Microsecond, dt.and_utc().timestamp_micros())
        }
        Cell::TimestampTz(dt) => Value::Timestamp(TimeUnit::Microsecond, dt.timestamp_micros()),
        // UUID stored as text; DuckDB casts VARCHAR → UUID automatically.
        Cell::Uuid(u) => Value::Text(u.to_string()),
        // JSON serialised as text.
        Cell::Json(j) => Value::Text(j.to_string()),
        Cell::Bytes(b) => Value::Blob(b),
        Cell::Array(arr) => array_cell_to_value(arr),
    }
}

/// Converts an [`ArrayCell`] (with nullable elements) to a `Value::List`.
fn array_cell_to_value(arr: ArrayCell) -> Value {
    let values = match arr {
        ArrayCell::Bool(v) => v
            .into_iter()
            .map(|o| o.map(Value::Boolean).unwrap_or(Value::Null))
            .collect(),
        ArrayCell::String(v) => v
            .into_iter()
            .map(|o| o.map(Value::Text).unwrap_or(Value::Null))
            .collect(),
        ArrayCell::I16(v) => v
            .into_iter()
            .map(|o| o.map(Value::SmallInt).unwrap_or(Value::Null))
            .collect(),
        ArrayCell::I32(v) => v
            .into_iter()
            .map(|o| o.map(Value::Int).unwrap_or(Value::Null))
            .collect(),
        ArrayCell::U32(v) => v
            .into_iter()
            .map(|o| o.map(Value::UInt).unwrap_or(Value::Null))
            .collect(),
        ArrayCell::I64(v) => v
            .into_iter()
            .map(|o| o.map(Value::BigInt).unwrap_or(Value::Null))
            .collect(),
        ArrayCell::F32(v) => v
            .into_iter()
            .map(|o| o.map(Value::Float).unwrap_or(Value::Null))
            .collect(),
        ArrayCell::F64(v) => v
            .into_iter()
            .map(|o| o.map(Value::Double).unwrap_or(Value::Null))
            .collect(),
        ArrayCell::Numeric(v) => v
            .into_iter()
            .map(|o| o.map(|n| Value::Text(n.to_string())).unwrap_or(Value::Null))
            .collect(),
        ArrayCell::Date(v) => {
            let epoch_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            v.into_iter()
                .map(|o| {
                    o.map(|d| Value::Date32(d.signed_duration_since(epoch_date).num_days() as i32))
                        .unwrap_or(Value::Null)
                })
                .collect()
        }
        ArrayCell::Time(v) => {
            let epoch_time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
            v.into_iter()
                .map(|o| {
                    o.map(|t| {
                        let micros = t
                            .signed_duration_since(epoch_time)
                            .num_microseconds()
                            .unwrap_or(0);
                        Value::Time64(TimeUnit::Microsecond, micros)
                    })
                    .unwrap_or(Value::Null)
                })
                .collect()
        }
        ArrayCell::Timestamp(v) => v
            .into_iter()
            .map(|o| {
                o.map(|dt| Value::Timestamp(TimeUnit::Microsecond, dt.and_utc().timestamp_micros()))
                    .unwrap_or(Value::Null)
            })
            .collect(),
        ArrayCell::TimestampTz(v) => v
            .into_iter()
            .map(|o| {
                o.map(|dt| Value::Timestamp(TimeUnit::Microsecond, dt.timestamp_micros()))
                    .unwrap_or(Value::Null)
            })
            .collect(),
        ArrayCell::Uuid(v) => v
            .into_iter()
            .map(|o| o.map(|u| Value::Text(u.to_string())).unwrap_or(Value::Null))
            .collect(),
        ArrayCell::Json(v) => v
            .into_iter()
            .map(|o| o.map(|j| Value::Text(j.to_string())).unwrap_or(Value::Null))
            .collect(),
        ArrayCell::Bytes(v) => v
            .into_iter()
            .map(|o| o.map(Value::Blob).unwrap_or(Value::Null))
            .collect(),
    };
    Value::List(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cell_to_value_primitives() {
        assert_eq!(cell_to_value(Cell::Null), Value::Null);
        assert_eq!(cell_to_value(Cell::Bool(true)), Value::Boolean(true));
        assert_eq!(
            cell_to_value(Cell::String("hello".to_string())),
            Value::Text("hello".to_string())
        );
        assert_eq!(cell_to_value(Cell::I32(42)), Value::Int(42));
        assert_eq!(cell_to_value(Cell::I64(-1)), Value::BigInt(-1));
        assert_eq!(cell_to_value(Cell::F64(3.46)), Value::Double(3.46));
    }

    #[test]
    fn test_array_cell_to_sql_literal_preserves_nulls() {
        assert_eq!(
            array_cell_to_sql_literal(ArrayCell::I32(vec![Some(1), None, Some(3)])),
            "[1, NULL, 3]"
        );
        assert_eq!(
            array_cell_to_sql_literal(ArrayCell::Json(vec![
                Some(serde_json::json!({"a": 1})),
                None,
            ])),
            "[CAST('{\"a\":1}' AS JSON), NULL]"
        );
    }

    #[test]
    fn test_prepare_rows_uses_sql_literals_for_arrays() {
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
