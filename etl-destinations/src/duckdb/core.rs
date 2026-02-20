use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use chrono::{NaiveDate, NaiveTime};
use duckdb::types::{TimeUnit, Value};
use duckdb::DuckdbConnectionManager;
use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{ArrayCell, Cell, Event, TableId, TableName, TableRow};
use etl::etl_error;
use r2d2::Pool;
use tracing::{debug, info};

use crate::duckdb::schema::build_create_table_sql;

/// Delimiter used to join schema and table name in the DuckDB table name.
const TABLE_NAME_DELIMITER: &str = "_";
/// Escape string for underscores within schema/table names to prevent collisions.
const TABLE_NAME_ESCAPE: &str = "__";

/// Alias for DuckDB table names.
type DuckDbTableName = String;

/// Converts a Postgres [`TableName`] to a DuckDB table name string.
///
/// Escapes underscores in schema and table name components by doubling them,
/// then joins the two parts with a single `_`. This matches the convention
/// used by other destinations in this crate.
///
/// # Example
/// - `public.my_table` → `public_my__table`
/// - `my_schema.orders` → `my__schema_orders`
pub fn table_name_to_duckdb_table_name(table_name: &TableName) -> DuckDbTableName {
    let escaped_schema = table_name.schema.replace(TABLE_NAME_DELIMITER, TABLE_NAME_ESCAPE);
    let escaped_table = table_name.name.replace(TABLE_NAME_DELIMITER, TABLE_NAME_ESCAPE);
    format!("{escaped_schema}{TABLE_NAME_DELIMITER}{escaped_table}")
}

/// A DuckDB destination that implements the ETL [`Destination`] trait.
///
/// Writes data to an embedded DuckDB database (file-backed or in-memory).
/// Tables are created to match the Postgres source schema exactly.
///
/// An [`r2d2`] connection pool allows multiple tables to be written to in
/// parallel via separate `spawn_blocking` tasks, avoiding a single-connection
/// bottleneck while keeping DuckDB's synchronous API off the async runtime.
#[derive(Clone)]
pub struct DuckDbDestination<S> {
    pool: Pool<DuckdbConnectionManager>,
    store: S,
    /// Cache of destination table names whose DDL has already been executed.
    created_tables: Arc<Mutex<HashSet<DuckDbTableName>>>,
}

impl<S> DuckDbDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    /// Creates a new DuckDB destination backed by a file at `path`.
    ///
    /// The file is created if it does not exist. `pool_size` controls the
    /// maximum number of simultaneous connections; `4` is a reasonable default.
    pub fn new(path: impl Into<String>, pool_size: u32, store: S) -> EtlResult<Self> {
        let path = path.into();
        let manager = DuckdbConnectionManager::file(&path).map_err(|e| {
            etl_error!(
                ErrorKind::DestinationConnectionFailed,
                "Failed to create DuckDB connection manager",
                source: e
            )
        })?;
        let pool = Pool::builder().max_size(pool_size).build(manager).map_err(|e| {
            etl_error!(
                ErrorKind::DestinationConnectionFailed,
                "Failed to build DuckDB connection pool",
                source: e
            )
        })?;
        Ok(Self {
            pool,
            store,
            created_tables: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    /// Creates a new DuckDB destination backed by an in-memory database.
    ///
    /// The database is destroyed when all connections are closed. Useful for
    /// testing or ephemeral pipelines.
    pub fn new_in_memory(pool_size: u32, store: S) -> EtlResult<Self> {
        let manager = DuckdbConnectionManager::memory().map_err(|e| {
            etl_error!(
                ErrorKind::DestinationConnectionFailed,
                "Failed to create in-memory DuckDB connection manager",
                source: e
            )
        })?;
        let pool = Pool::builder().max_size(pool_size).build(manager).map_err(|e| {
            etl_error!(
                ErrorKind::DestinationConnectionFailed,
                "Failed to build in-memory DuckDB connection pool",
                source: e
            )
        })?;
        Ok(Self {
            pool,
            store,
            created_tables: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    /// Deletes all rows from the destination table without dropping it.
    async fn truncate_table_inner(&self, table_id: TableId) -> EtlResult<()> {
        // Ensure the table exists (also retrieves its mapped name).
        let table_name = self.ensure_table_exists(table_id).await?;
        let pool = self.pool.clone();

        tokio::task::spawn_blocking(move || -> EtlResult<()> {
            let conn = pool.get().map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationConnectionFailed,
                    "Failed to get DuckDB connection from pool",
                    source: e
                )
            })?;
            conn.execute_batch(&format!("DELETE FROM \"{table_name}\""))
                .map_err(|e| {
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckDB DELETE failed",
                        source: e
                    )
                })?;
            Ok(())
        })
        .await
        .map_err(|_| etl_error!(ErrorKind::ApplyWorkerPanic, "DuckDB blocking task panicked"))?
    }

    /// Bulk-inserts rows into the destination table using the DuckDB Appender.
    async fn write_table_rows_inner(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        // Ensure the table exists even when there are no rows to write.
        let table_name = self.ensure_table_exists(table_id).await?;

        if table_rows.is_empty() {
            return Ok(());
        }

        let pool = self.pool.clone();

        tokio::task::spawn_blocking(move || -> EtlResult<()> {
            let conn = pool.get().map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationConnectionFailed,
                    "Failed to get DuckDB connection from pool",
                    source: e
                )
            })?;
            let mut appender = conn.appender(&table_name).map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "Failed to create DuckDB appender",
                    source: e
                )
            })?;

            for row in table_rows {
                let values: Vec<Value> =
                    row.into_values().into_iter().map(cell_to_value).collect();

                appender
                    .append_row(duckdb::appender_params_from_iter(&values))
                    .map_err(|e| {
                        etl_error!(
                            ErrorKind::DestinationQueryFailed,
                            "DuckDB append_row failed",
                            source: e
                        )
                    })?;
            }

            appender.flush().map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckDB appender flush failed",
                    source: e
                )
            })?;
            Ok(())
        })
        .await
        .map_err(|_| etl_error!(ErrorKind::ApplyWorkerPanic, "DuckDB blocking task panicked"))?
    }

    /// Writes streaming CDC events to the destination.
    ///
    /// Insert, Update, and Delete events are grouped by table and written in
    /// parallel (each table in its own `spawn_blocking` task). Truncate events
    /// are collected, deduplicated, and processed after the per-table writes.
    async fn write_events_inner(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut event_iter = events.into_iter().peekable();

        while event_iter.peek().is_some() {
            let mut table_id_to_rows: HashMap<TableId, Vec<Vec<Value>>> = HashMap::new();

            // Accumulate non-truncate events, stopping at the first Truncate.
            while let Some(event) = event_iter.peek() {
                if matches!(event, Event::Truncate(_)) {
                    break;
                }

                let event = event_iter.next().unwrap();
                match event {
                    Event::Insert(insert) => {
                        let values: Vec<Value> =
                            insert.table_row.into_values().into_iter().map(cell_to_value).collect();
                        table_id_to_rows.entry(insert.table_id).or_default().push(values);
                    }
                    Event::Update(update) => {
                        let values: Vec<Value> =
                            update.table_row.into_values().into_iter().map(cell_to_value).collect();
                        table_id_to_rows.entry(update.table_id).or_default().push(values);
                    }
                    Event::Delete(delete) => {
                        let Some((_, old_row)) = delete.old_table_row else {
                            info!("delete event has no old row, skipping");
                            continue;
                        };
                        let values: Vec<Value> =
                            old_row.into_values().into_iter().map(cell_to_value).collect();
                        table_id_to_rows.entry(delete.table_id).or_default().push(values);
                    }
                    event => {
                        debug!(event_type = %event.event_type(), "skipping unsupported event type");
                    }
                }
            }

            // Write accumulated rows — one spawn_blocking task per table so different
            // tables can proceed in parallel through separate pool connections.
            if !table_id_to_rows.is_empty() {
                let mut join_set = tokio::task::JoinSet::new();

                for (table_id, rows) in table_id_to_rows {
                    let table_name = self.ensure_table_exists(table_id).await?;
                    let pool = self.pool.clone();

                    join_set.spawn(tokio::task::spawn_blocking(move || -> EtlResult<()> {
                        let conn = pool.get().map_err(|e| {
                            etl_error!(
                                ErrorKind::DestinationConnectionFailed,
                                "Failed to get DuckDB connection from pool",
                                source: e
                            )
                        })?;
                        let mut appender = conn.appender(&table_name).map_err(|e| {
                            etl_error!(
                                ErrorKind::DestinationQueryFailed,
                                "Failed to create DuckDB appender",
                                source: e
                            )
                        })?;

                        for values in rows {
                            appender
                                .append_row(duckdb::appender_params_from_iter(&values))
                                .map_err(|e| {
                                    etl_error!(
                                        ErrorKind::DestinationQueryFailed,
                                        "DuckDB append_row failed",
                                        source: e
                                    )
                                })?;
                        }

                        appender.flush().map_err(|e| {
                            etl_error!(
                                ErrorKind::DestinationQueryFailed,
                                "DuckDB appender flush failed",
                                source: e
                            )
                        })?;
                        Ok(())
                    }));
                }

                while let Some(result) = join_set.join_next().await {
                    result
                        .map_err(|_| {
                            etl_error!(ErrorKind::ApplyWorkerPanic, "DuckDB blocking task panicked")
                        })?
                        .map_err(|_| {
                            etl_error!(ErrorKind::ApplyWorkerPanic, "DuckDB blocking task panicked")
                        })??;
                }
            }

            // Collect and deduplicate Truncate events.
            let mut truncate_table_ids = HashSet::new();
            while let Some(Event::Truncate(_)) = event_iter.peek() {
                if let Some(Event::Truncate(truncate)) = event_iter.next() {
                    for rel_id in truncate.rel_ids {
                        truncate_table_ids.insert(TableId::new(rel_id));
                    }
                }
            }

            for table_id in truncate_table_ids {
                self.truncate_table_inner(table_id).await?;
            }
        }

        Ok(())
    }

    /// Ensures the destination table exists, creating it if necessary.
    ///
    /// Performs an async schema/mapping lookup, then dispatches a
    /// `spawn_blocking` task to run the DDL and update the local cache.
    /// Uses `CREATE TABLE IF NOT EXISTS` so concurrent calls for the same
    /// table are safe.
    async fn ensure_table_exists(&self, table_id: TableId) -> EtlResult<DuckDbTableName> {
        let table_schema = self
            .store
            .get_table_schema(&table_id)
            .await?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::MissingTableSchema,
                    "Table schema not found",
                    format!("No schema found for table {table_id}")
                )
            })?;

        let table_name = self
            .get_or_create_table_mapping(table_id, &table_schema.name)
            .await?;

        // Fast path: table already created.
        {
            let cache = self.created_tables.lock().unwrap();
            if cache.contains(&table_name) {
                return Ok(table_name);
            }
        }

        let ddl = build_create_table_sql(&table_name, &table_schema.column_schemas);

        let pool = self.pool.clone();
        let created_tables = Arc::clone(&self.created_tables);
        let table_name_clone = table_name.clone();

        tokio::task::spawn_blocking(move || -> EtlResult<()> {
            let conn = pool.get().map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationConnectionFailed,
                    "Failed to get DuckDB connection from pool",
                    source: e
                )
            })?;
            conn.execute_batch(&ddl).map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckDB CREATE TABLE failed",
                    source: e
                )
            })?;
            created_tables.lock().unwrap().insert(table_name_clone);
            Ok(())
        })
        .await
        .map_err(|_| etl_error!(ErrorKind::ApplyWorkerPanic, "DuckDB blocking task panicked"))??;

        Ok(table_name)
    }

    /// Returns the stored destination table name for `table_id`, creating and
    /// persisting a new mapping if none exists yet.
    async fn get_or_create_table_mapping(
        &self,
        table_id: TableId,
        table_name: &TableName,
    ) -> EtlResult<DuckDbTableName> {
        if let Some(existing) = self.store.get_table_mapping(&table_id).await? {
            return Ok(existing);
        }

        let duckdb_table_name = table_name_to_duckdb_table_name(table_name);
        self.store
            .store_table_mapping(table_id, duckdb_table_name.clone())
            .await?;
        Ok(duckdb_table_name)
    }
}

impl<S> Destination for DuckDbDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    fn name() -> &'static str {
        "duckdb"
    }

    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        self.truncate_table_inner(table_id).await
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        self.write_table_rows_inner(table_id, table_rows).await
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        self.write_events_inner(events).await
    }
}

/// Converts a [`Cell`] to a [`duckdb::types::Value`] for use with the Appender.
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
        // NUMERIC is stored as VARCHAR to avoid precision loss.
        Cell::Numeric(n) => Value::Text(n.to_string()),
        Cell::Date(d) => {
            let days = d.signed_duration_since(epoch_date).num_days() as i32;
            Value::Date32(days)
        }
        Cell::Time(t) => {
            let micros = t.signed_duration_since(epoch_time).num_microseconds().unwrap_or(0);
            Value::Time64(TimeUnit::Microsecond, micros)
        }
        Cell::Timestamp(dt) => {
            Value::Timestamp(TimeUnit::Microsecond, dt.and_utc().timestamp_micros())
        }
        Cell::TimestampTz(dt) => {
            Value::Timestamp(TimeUnit::Microsecond, dt.timestamp_micros())
        }
        // UUID stored as text; DuckDB will cast VARCHAR → UUID automatically.
        Cell::Uuid(u) => Value::Text(u.to_string()),
        // JSON serialised as text to match the JSON column type.
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
                    o.map(|d| {
                        Value::Date32(d.signed_duration_since(epoch_date).num_days() as i32)
                    })
                    .unwrap_or(Value::Null)
                })
                .collect()
        }
        ArrayCell::Time(v) => {
            let epoch_time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
            v.into_iter()
                .map(|o| {
                    o.map(|t| {
                        let micros =
                            t.signed_duration_since(epoch_time).num_microseconds().unwrap_or(0);
                        Value::Time64(TimeUnit::Microsecond, micros)
                    })
                    .unwrap_or(Value::Null)
                })
                .collect()
        }
        ArrayCell::Timestamp(v) => v
            .into_iter()
            .map(|o| {
                o.map(|dt| {
                    Value::Timestamp(TimeUnit::Microsecond, dt.and_utc().timestamp_micros())
                })
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
    fn test_table_name_escaping() {
        assert_eq!(
            table_name_to_duckdb_table_name(&TableName {
                schema: "public".to_string(),
                name: "orders".to_string(),
            }),
            "public_orders"
        );
        assert_eq!(
            table_name_to_duckdb_table_name(&TableName {
                schema: "my_schema".to_string(),
                name: "my_table".to_string(),
            }),
            "my__schema_my__table"
        );
    }

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
        assert_eq!(cell_to_value(Cell::F64(3.14)), Value::Double(3.14));
    }
}
