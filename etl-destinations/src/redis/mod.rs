mod client;
mod json_cell;

use etl::destination::Destination;
use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{Cell, ColumnSchema, Event, TableId, TableRow, TableSchema};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

use crate::redis::client::{RedisClient, RedisKey};
use crate::redis::json_cell::JsonCell;

pub type MapFunction = Arc<dyn Fn(Cell, &TableSchema, &ColumnSchema) -> Cell + Send + Sync>;

#[derive(Clone)]
pub struct RedisDestination<S> {
    redis_client: RedisClient,
    store: S,
    /// If you want to change data before inserting it in Redis
    map_insert: Option<MapFunction>,
    /// If you want to change data before updating it in Redis
    map_update: Option<MapFunction>,
}

#[derive(Clone, Debug)]
pub struct RedisConfig {
    /// Host on which Redis is running (e.g., localhost or IP address) (default: 127.0.0.1)
    pub host: String,
    /// Port on which Redis is running (default: 6379)
    pub port: u16,
    /// Redis database user name
    pub username: Option<String>,
    /// Redis database user password (optional if using trust authentication)
    pub password: Option<String>,
    /// Redis TTL if you want to set a TTL on replicated data in Redis
    pub ttl: Option<i64>,
}

impl<S> RedisDestination<S>
where
    S: StateStore + SchemaStore,
{
    /// Creates a new Redis destination.
    pub async fn new(config: RedisConfig, store: S) -> EtlResult<Self> {
        let redis_client = RedisClient::new(config).await.map_err(|err| {
            EtlError::from((
                ErrorKind::DestinationConnectionFailed,
                "cannot connect to redis",
                err.to_string(),
            ))
        })?;

        Ok(Self {
            redis_client,
            store,
            map_insert: None,
            map_update: None,
        })
    }

    /// If you want to change data before inserting it in Redis
    pub fn map_insert<F>(mut self, f: F) -> Self
    where
        F: Fn(Cell, &TableSchema, &ColumnSchema) -> Cell + Send + Sync + 'static,
    {
        self.map_insert = Some(Arc::new(f));
        self
    }

    /// If you want to change data before updating it in Redis
    pub fn map_update<F>(mut self, f: F) -> Self
    where
        F: Fn(Cell, &TableSchema, &ColumnSchema) -> Cell + Send + Sync + 'static,
    {
        self.map_update = Some(Arc::new(f));
        self
    }

    async fn _write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
        is_update: bool,
    ) -> EtlResult<()> {
        let table_schema = self
            .store
            .get_table_schema(&table_id)
            .await?
            .ok_or_else(|| {
                EtlError::from((ErrorKind::MissingTableSchema, "cannot get table schema"))
            })?;

        debug!("writing a batch of {} table rows:", table_rows.len());
        let pipeline = self.redis_client.pipeline();
        for table_row in table_rows {
            let key = RedisKey::new(table_schema.as_ref(), &table_row);
            let map: HashMap<String, JsonCell> = table_row
                .values
                .into_iter()
                .enumerate()
                .filter_map(|(idx, cell)| {
                    let column_schema = table_schema.column_schemas.get(idx)?;
                    let map_func = if is_update {
                        &self.map_update
                    } else {
                        &self.map_insert
                    };
                    let cell = match map_func {
                        Some(map_func) => map_func(cell, table_schema.as_ref(), column_schema),
                        None => cell,
                    };

                    Some((column_schema.name.clone(), JsonCell::Value(cell)))
                })
                .collect();
            self.redis_client
                .set(&pipeline, key, map)
                .await
                .map_err(|err| {
                    EtlError::from((
                        ErrorKind::DestinationError,
                        "cannot set in redis",
                        err.to_string(),
                    ))
                })?;
        }

        pipeline.all().await.map_err(|err| {
            EtlError::from((
                ErrorKind::DestinationError,
                "cannot execute pipeline in redis",
                err.to_string(),
            ))
        })
    }
}

impl<S> Destination for RedisDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    fn name() -> &'static str {
        "redis"
    }

    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        let Some(current_table_name) = self.store.get_table_mapping(&table_id).await? else {
            return Ok(());
        };

        debug!("truncating table {}", current_table_name);
        self.redis_client
            .delete_by_table_name(&current_table_name)
            .await
            .map_err(|err| {
                EtlError::from((
                    ErrorKind::DestinationError,
                    "cannot delete key in Redis",
                    err.to_string(),
                ))
            })?;

        Ok(())
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        self._write_table_rows(table_id, table_rows, false).await
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        debug!("writing a batch of {} events:", events.len());

        for event in events {
            debug!(?event);
            match event {
                Event::Insert(insert_event) => {
                    self.write_table_rows(insert_event.table_id, vec![insert_event.table_row])
                        .await?;
                }
                Event::Update(update_event) => {
                    self._write_table_rows(
                        update_event.table_id,
                        vec![update_event.table_row],
                        true,
                    )
                    .await?;
                }
                Event::Delete(delete_event) => {
                    let Some(table_schema) = self
                        .store
                        .get_table_schema(&delete_event.table_id)
                        .await
                        .ok()
                        .flatten()
                    else {
                        continue;
                    };
                    let Some((_only_keys, old_table_row)) = delete_event.old_table_row else {
                        continue;
                    };
                    self.redis_client
                        .delete(RedisKey::new(table_schema.as_ref(), &old_table_row))
                        .await
                        .map_err(|err| {
                            EtlError::from((
                                ErrorKind::DestinationError,
                                "cannot del in redis",
                                err.to_string(),
                            ))
                        })?;
                }
                Event::Truncate(truncate_event) => {
                    for table_id in truncate_event.rel_ids {
                        let Some(table_schema) = self
                            .store
                            .get_table_schema(&TableId(table_id))
                            .await
                            .ok()
                            .flatten()
                        else {
                            continue;
                        };

                        self.redis_client
                            .delete_by_table_name(&table_schema.name.name)
                            .await
                            .map_err(|err| {
                                EtlError::from((
                                    ErrorKind::DestinationError,
                                    "cannot del in redis",
                                    err.to_string(),
                                ))
                            })?;
                    }
                }
                Event::Begin(_) | Event::Commit(_) | Event::Relation(_) | Event::Unsupported => {}
            }
        }

        Ok(())
    }
}
