use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use etl::types::{ArrayCell, Cell, PgLsn, TableName, TableRow, TableSchema};
use serde_json::{Map, Value};

/// Serializes a [`TableRow`] into a JSON object using the column names from the schema.
pub fn table_row_to_json(row: &TableRow, schema: &TableSchema) -> Value {
    let mut map = Map::with_capacity(schema.column_schemas.len());
    for (cell, col) in row.values().iter().zip(schema.column_schemas.iter()) {
        map.insert(col.name.clone(), cell_to_json(cell));
    }
    Value::Object(map)
}

/// Converts a [`Cell`] to its JSON representation.
///
/// Special cases:
/// - `f32`/`f64` NaN/Inf → string (`"NaN"`, `"Infinity"`, `"-Infinity"`)
/// - `i64` values outside safe integer range (>2^53) → string to preserve precision
/// - `Bytes` → base64-encoded string
/// - `Numeric` NaN/Inf variants → string
/// - `Array` elements containing `None` → JSON `null`
pub fn cell_to_json(cell: &Cell) -> Value {
    match cell {
        Cell::Null => Value::Null,
        Cell::Bool(v) => Value::Bool(*v),
        Cell::I16(v) => Value::Number((*v).into()),
        Cell::I32(v) => Value::Number((*v).into()),
        Cell::U32(v) => Value::Number((*v).into()),
        Cell::I64(v) => {
            const MAX_SAFE: i64 = 1 << 53;
            if v.abs() > MAX_SAFE {
                Value::String(v.to_string())
            } else {
                Value::Number((*v).into())
            }
        }
        Cell::F32(v) => f32_to_json(*v),
        Cell::F64(v) => f64_to_json(*v),
        Cell::Numeric(n) => {
            use etl::types::PgNumeric;
            match n {
                PgNumeric::NaN => Value::String("NaN".into()),
                PgNumeric::PositiveInfinity => Value::String("Infinity".into()),
                PgNumeric::NegativeInfinity => Value::String("-Infinity".into()),
                PgNumeric::Value { .. } => Value::String(n.to_string()),
            }
        }
        Cell::Date(v) => Value::String(v.to_string()),
        Cell::Time(v) => Value::String(v.to_string()),
        Cell::Timestamp(v) => Value::String(v.to_string()),
        Cell::TimestampTz(v) => Value::String(v.to_rfc3339()),
        Cell::Uuid(v) => Value::String(v.to_string()),
        Cell::Json(v) => v.clone(),
        Cell::String(v) => Value::String(v.clone()),
        Cell::Bytes(v) => Value::String(BASE64.encode(v)),
        Cell::Array(arr) => array_cell_to_json(arr),
    }
}

fn f32_to_json(v: f32) -> Value {
    if v.is_nan() {
        Value::String("NaN".into())
    } else if v == f32::INFINITY {
        Value::String("Infinity".into())
    } else if v == f32::NEG_INFINITY {
        Value::String("-Infinity".into())
    } else {
        serde_json::Number::from_f64(v as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }
}

fn f64_to_json(v: f64) -> Value {
    if v.is_nan() {
        Value::String("NaN".into())
    } else if v == f64::INFINITY {
        Value::String("Infinity".into())
    } else if v == f64::NEG_INFINITY {
        Value::String("-Infinity".into())
    } else {
        serde_json::Number::from_f64(v)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }
}

fn array_cell_to_json(arr: &ArrayCell) -> Value {
    match arr {
        ArrayCell::Bool(v) => Value::Array(
            v.iter()
                .map(|e| opt_to_json(e, |b| Value::Bool(*b)))
                .collect(),
        ),
        ArrayCell::String(v) => Value::Array(
            v.iter()
                .map(|e| opt_to_json(e, |s| Value::String(s.clone())))
                .collect(),
        ),
        ArrayCell::I16(v) => Value::Array(
            v.iter()
                .map(|e| opt_to_json(e, |n| Value::Number((*n).into())))
                .collect(),
        ),
        ArrayCell::I32(v) => Value::Array(
            v.iter()
                .map(|e| opt_to_json(e, |n| Value::Number((*n).into())))
                .collect(),
        ),
        ArrayCell::U32(v) => Value::Array(
            v.iter()
                .map(|e| opt_to_json(e, |n| Value::Number((*n).into())))
                .collect(),
        ),
        ArrayCell::I64(v) => Value::Array(
            v.iter()
                .map(|e| {
                    opt_to_json(e, |n| {
                        const MAX_SAFE: i64 = 1 << 53;
                        if n.abs() > MAX_SAFE {
                            Value::String(n.to_string())
                        } else {
                            Value::Number((*n).into())
                        }
                    })
                })
                .collect(),
        ),
        ArrayCell::F32(v) => Value::Array(
            v.iter()
                .map(|e| opt_to_json(e, |n| f32_to_json(*n)))
                .collect(),
        ),
        ArrayCell::F64(v) => Value::Array(
            v.iter()
                .map(|e| opt_to_json(e, |n| f64_to_json(*n)))
                .collect(),
        ),
        ArrayCell::Numeric(v) => Value::Array(
            v.iter()
                .map(|e| {
                    e.as_ref()
                        .map(|n| cell_to_json(&Cell::Numeric(n.clone())))
                        .unwrap_or(Value::Null)
                })
                .collect(),
        ),
        ArrayCell::Date(v) => Value::Array(
            v.iter()
                .map(|e| opt_to_json(e, |d| Value::String(d.to_string())))
                .collect(),
        ),
        ArrayCell::Time(v) => Value::Array(
            v.iter()
                .map(|e| opt_to_json(e, |t| Value::String(t.to_string())))
                .collect(),
        ),
        ArrayCell::Timestamp(v) => Value::Array(
            v.iter()
                .map(|e| opt_to_json(e, |t| Value::String(t.to_string())))
                .collect(),
        ),
        ArrayCell::TimestampTz(v) => Value::Array(
            v.iter()
                .map(|e| opt_to_json(e, |t| Value::String(t.to_rfc3339())))
                .collect(),
        ),
        ArrayCell::Uuid(v) => Value::Array(
            v.iter()
                .map(|e| opt_to_json(e, |u| Value::String(u.to_string())))
                .collect(),
        ),
        ArrayCell::Json(v) => {
            Value::Array(v.iter().map(|e| e.clone().unwrap_or(Value::Null)).collect())
        }
        ArrayCell::Bytes(v) => Value::Array(
            v.iter()
                .map(|e| opt_to_json(e, |b| Value::String(BASE64.encode(b))))
                .collect(),
        ),
    }
}

fn opt_to_json<T, F: Fn(&T) -> Value>(opt: &Option<T>, f: F) -> Value {
    opt.as_ref().map(f).unwrap_or(Value::Null)
}

/// Builds the topic string for a Realtime channel.
///
/// Format: `realtime:<prefix>:<schema>.<table>` for public channels
/// or `realtime:private:<prefix>:<schema>.<table>` for private channels.
/// The `realtime:` prefix is required by the Supabase Realtime server.
pub fn build_topic(table_name: &TableName, channel_prefix: &str, private_channels: bool) -> String {
    let channel = format!(
        "{}:{}.{}",
        channel_prefix, table_name.schema, table_name.name
    );
    if private_channels {
        format!("realtime:private:{channel}")
    } else {
        format!("realtime:{channel}")
    }
}

/// Builds a Phoenix v2 `phx_join` message for the given topic.
pub fn build_join_message(topic: &str) -> String {
    serde_json::json!([
        null,
        "1",
        topic,
        "phx_join",
        {
            "config": {
                "broadcast": { "self": false, "ack": false },
                "presence": { "key": "" },
                "postgres_changes": []
            }
        }
    ])
    .to_string()
}

/// Builds a Phoenix v2 heartbeat message.
///
/// Must be sent every ~25 seconds to prevent the server from closing the connection.
pub fn build_heartbeat_message() -> String {
    serde_json::json!([null, "hb", "phoenix", "heartbeat", {}]).to_string()
}

/// Builds a Phoenix v2 broadcast message for a CDC event.
pub fn build_broadcast_message(topic: &str, event: &str, payload: Value) -> String {
    serde_json::json!([null, null, topic, "broadcast", {"event": event, "payload": payload}])
        .to_string()
}

/// Builds the payload for an INSERT event.
pub fn insert_payload(table_name: &TableName, record: Value, commit_lsn: PgLsn) -> Value {
    serde_json::json!({
        "op": "INSERT",
        "schema": table_name.schema,
        "table": table_name.name,
        "record": record,
        "old_record": null,
        "commit_lsn": commit_lsn.to_string()
    })
}

/// Builds the payload for an UPDATE event.
pub fn update_payload(
    table_name: &TableName,
    record: Value,
    old_record: Option<Value>,
    commit_lsn: PgLsn,
) -> Value {
    serde_json::json!({
        "op": "UPDATE",
        "schema": table_name.schema,
        "table": table_name.name,
        "record": record,
        "old_record": old_record.unwrap_or(Value::Null),
        "commit_lsn": commit_lsn.to_string()
    })
}

/// Builds the payload for a DELETE event.
pub fn delete_payload(
    table_name: &TableName,
    old_record: Option<Value>,
    commit_lsn: PgLsn,
) -> Value {
    serde_json::json!({
        "op": "DELETE",
        "schema": table_name.schema,
        "table": table_name.name,
        "record": null,
        "old_record": old_record.unwrap_or(Value::Null),
        "commit_lsn": commit_lsn.to_string()
    })
}

/// Builds the payload for a TRUNCATE event.
pub fn truncate_payload(table_name: &TableName, commit_lsn: PgLsn) -> Value {
    serde_json::json!({
        "op": "TRUNCATE",
        "schema": table_name.schema,
        "table": table_name.name,
        "record": null,
        "old_record": null,
        "commit_lsn": commit_lsn.to_string()
    })
}

/// Builds the payload for a TABLE_SYNC (initial copy) event.
pub fn table_sync_payload(table_name: &TableName, rows: Vec<Value>) -> Value {
    serde_json::json!({
        "op": "TABLE_SYNC",
        "schema": table_name.schema,
        "table": table_name.name,
        "rows": rows
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl::types::PgNumeric;
    use etl::types::{ArrayCell, Cell};

    #[test]
    fn cell_null_serializes_to_json_null() {
        assert_eq!(cell_to_json(&Cell::Null), Value::Null);
    }

    #[test]
    fn cell_bool_round_trips() {
        assert_eq!(cell_to_json(&Cell::Bool(true)), Value::Bool(true));
        assert_eq!(cell_to_json(&Cell::Bool(false)), Value::Bool(false));
    }

    #[test]
    fn cell_integers_round_trip() {
        assert_eq!(cell_to_json(&Cell::I16(42)), serde_json::json!(42));
        assert_eq!(
            cell_to_json(&Cell::I32(1_000_000)),
            serde_json::json!(1_000_000)
        );
        assert_eq!(
            cell_to_json(&Cell::U32(u32::MAX)),
            serde_json::json!(u32::MAX)
        );
    }

    #[test]
    fn cell_i64_large_value_becomes_string() {
        let large: i64 = (1_i64 << 53) + 1;
        let result = cell_to_json(&Cell::I64(large));
        assert_eq!(result, Value::String(large.to_string()));
    }

    #[test]
    fn cell_i64_safe_value_stays_number() {
        assert_eq!(cell_to_json(&Cell::I64(42)), serde_json::json!(42));
        assert_eq!(cell_to_json(&Cell::I64(-42)), serde_json::json!(-42));
    }

    #[test]
    fn cell_float_special_values_become_strings() {
        assert_eq!(
            cell_to_json(&Cell::F64(f64::NAN)),
            Value::String("NaN".into())
        );
        assert_eq!(
            cell_to_json(&Cell::F64(f64::INFINITY)),
            Value::String("Infinity".into())
        );
        assert_eq!(
            cell_to_json(&Cell::F64(f64::NEG_INFINITY)),
            Value::String("-Infinity".into())
        );
        assert_eq!(
            cell_to_json(&Cell::F32(f32::NAN)),
            Value::String("NaN".into())
        );
        assert_eq!(
            cell_to_json(&Cell::F32(f32::INFINITY)),
            Value::String("Infinity".into())
        );
        assert_eq!(
            cell_to_json(&Cell::F32(f32::NEG_INFINITY)),
            Value::String("-Infinity".into())
        );
    }

    #[test]
    fn cell_numeric_special_values_become_strings() {
        assert_eq!(
            cell_to_json(&Cell::Numeric(PgNumeric::NaN)),
            Value::String("NaN".into())
        );
        assert_eq!(
            cell_to_json(&Cell::Numeric(PgNumeric::PositiveInfinity)),
            Value::String("Infinity".into())
        );
        assert_eq!(
            cell_to_json(&Cell::Numeric(PgNumeric::NegativeInfinity)),
            Value::String("-Infinity".into())
        );
    }

    #[test]
    fn cell_bytes_encode_as_base64() {
        let bytes = vec![0u8, 1, 2, 255];
        let result = cell_to_json(&Cell::Bytes(bytes.clone()));
        assert_eq!(result, Value::String(BASE64.encode(&bytes)));
    }

    #[test]
    fn cell_array_with_nulls_serializes_nulls() {
        let arr = Cell::Array(ArrayCell::String(vec![
            Some("a".into()),
            None,
            Some("b".into()),
        ]));
        let result = cell_to_json(&arr);
        assert_eq!(result, serde_json::json!(["a", null, "b"]));
    }

    #[test]
    fn build_topic_public_channel() {
        let name = TableName::new("public".into(), "users".into());
        assert_eq!(
            build_topic(&name, "etl", false),
            "realtime:etl:public.users"
        );
    }

    #[test]
    fn build_topic_private_channel() {
        let name = TableName::new("public".into(), "users".into());
        assert_eq!(
            build_topic(&name, "etl", true),
            "realtime:private:etl:public.users"
        );
    }

    #[test]
    fn build_topic_custom_prefix() {
        let name = TableName::new("myschema".into(), "orders".into());
        assert_eq!(
            build_topic(&name, "myapp", false),
            "realtime:myapp:myschema.orders"
        );
    }

    #[test]
    fn join_message_disables_postgres_changes_and_presence() {
        let msg = build_join_message("realtime:etl:public.users");
        let parsed: Value = serde_json::from_str(&msg).unwrap();
        let config = &parsed[4]["config"];
        assert_eq!(config["postgres_changes"], serde_json::json!([]));
        assert_eq!(config["broadcast"]["self"], false);
        assert_eq!(config["broadcast"]["ack"], false);
        assert!(config["presence"]["key"].is_string());
    }

    #[test]
    fn broadcast_message_structure() {
        let payload = serde_json::json!({"op": "INSERT"});
        let msg = build_broadcast_message("etl:public.users", "db_changes", payload.clone());
        let parsed: Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(parsed[0], Value::Null); // join_ref
        assert_eq!(parsed[1], Value::Null); // msg_ref (fire-and-forget)
        assert_eq!(parsed[2], "etl:public.users");
        assert_eq!(parsed[3], "broadcast");
        assert_eq!(parsed[4]["event"], "db_changes");
        assert_eq!(parsed[4]["payload"], payload);
    }
}
