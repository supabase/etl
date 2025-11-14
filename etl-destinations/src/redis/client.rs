use etl::types::{TableRow, TableSchema};
use fred::clients::Pipeline;
use fred::prelude::{
    Client, ClientLike, EventInterface, FredResult, KeysInterface, Pool, ReconnectPolicy, Server,
    ServerConfig, TcpConfig,
};
use fred::types::config::UnresponsiveConfig;
use fred::types::{Builder, Expiration, Key};
use futures::future::join_all;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::broadcast::error::RecvError;
use tracing::{debug, error};

use crate::redis::RedisConfig;
use crate::redis::json_cell::JsonCell;

#[derive(Clone)]
pub(super) struct RedisClient {
    client: Pool,
    ttl: Option<Expiration>,
}

impl RedisClient {
    pub(super) async fn new(config: RedisConfig) -> FredResult<Self> {
        let pooled_client = Builder::default_centralized()
            .with_config(|redis_config| {
                redis_config.password = config.password;
                redis_config.username = config.username;
                redis_config.server = ServerConfig::Centralized {
                    server: Server::new(config.host, config.port),
                };
            })
            .with_connection_config(|config| {
                config.internal_command_timeout = Duration::from_secs(5);
                config.reconnect_on_auth_error = true;
                config.tcp = TcpConfig {
                    #[cfg(target_os = "linux")]
                    user_timeout: Some(Duration::from_secs(5)),
                    ..Default::default()
                };
                config.unresponsive = UnresponsiveConfig {
                    max_timeout: Some(Duration::from_secs(10)),
                    interval: Duration::from_secs(3),
                };
            })
            .with_performance_config(|config| {
                config.default_command_timeout = Duration::from_secs(5);
            })
            .set_policy(ReconnectPolicy::new_exponential(0, 1, 2000, 5))
            .build_pool(5)?;

        for client in pooled_client.clients() {
            // spawn tasks that listen for connection close or reconnect events
            let mut error_rx = client.error_rx();
            let mut reconnect_rx = client.reconnect_rx();
            let mut unresponsive_rx = client.unresponsive_rx();

            tokio::spawn(async move {
                loop {
                    match error_rx.recv().await {
                        Ok((error, Some(server))) => {
                            error!("Redis client ({server:?}) error: {error:?}",);
                        }
                        Ok((error, None)) => {
                            error!("Redis client error: {error:?}",);
                        }
                        Err(RecvError::Lagged(_)) => continue,
                        Err(RecvError::Closed) => break,
                    }
                }
            });

            tokio::spawn(async move {
                loop {
                    match unresponsive_rx.recv().await {
                        Ok(server) => {
                            error!("Redis client ({server:?}) unresponsive");
                        }
                        Err(RecvError::Lagged(_)) => continue,
                        Err(RecvError::Closed) => break,
                    }
                }
            });

            tokio::spawn(async move {
                loop {
                    match reconnect_rx.recv().await {
                        Ok(server) => {
                            debug!("Redis client connected to {server:?}")
                        }
                        Err(RecvError::Lagged(_)) => continue,
                        Err(RecvError::Closed) => break,
                    }
                }
            });
        }
        let client_handles = pooled_client.connect_pool();

        debug!("Wait for connect");
        pooled_client.wait_for_connect().await?;
        debug!("Connected");

        tokio::spawn(async move {
            let _results = join_all(client_handles).await;
        });

        Ok(Self {
            client: pooled_client,
            ttl: config.ttl.map(Expiration::EX),
        })
    }

    // Doesn't work with redis cluster
    pub(super) async fn delete_by_table_name(&self, table_name: &str) -> FredResult<u64> {
        let pattern = format!("{}::::*", table_name);
        let mut cursor = "0".to_string();
        let mut total_deleted = 0u64;

        loop {
            let (next_cursor, keys): (String, Vec<Key>) = self
                .client
                .scan_page(cursor, pattern.clone(), Some(100), None)
                .await?;

            if !keys.is_empty() {
                let deleted: i64 = self.client.unlink(keys).await?;
                total_deleted += deleted as u64;
            }

            cursor = next_cursor;
            if cursor == "0" {
                break;
            }
        }

        Ok(total_deleted)
    }

    pub(super) async fn delete(&self, key: RedisKey) -> FredResult<()> {
        self.client.del::<(), _>(key).await
    }

    pub(super) fn pipeline(&self) -> Pipeline<Client> {
        self.client.next_connected().pipeline()
    }

    pub(super) async fn set(
        &self,
        pipeline: &Pipeline<Client>,
        key: RedisKey,
        map: HashMap<String, JsonCell<'_>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // We don't use json to be compliant with older versions of redis
        pipeline
            .set::<(), _, _>(
                key,
                serde_json::to_string(&map)?,
                self.ttl.clone(),
                None,
                false,
            )
            .await?;

        Ok(())
    }
}

pub(super) struct RedisKey(String);

impl RedisKey {
    pub(super) fn new(table_schema: &TableSchema, table_row: &TableRow) -> Self {
        let table_name = &table_schema.name.name;
        let primary_key = table_schema
            .column_schemas
            .iter()
            .enumerate()
            .filter_map(|(idx, col_schema)| {
                if col_schema.primary {
                    let value =
                        serde_json::to_string(&JsonCell::Ref(table_row.values.get(idx)?)).ok()?; // FIXME: should be a parameter to avoid serializing twice
                    let col_name = &col_schema.name;
                    Some(format!("{col_name}:{value}"))
                } else {
                    None
                }
            })
            .collect::<Vec<String>>()
            .join("::::");

        Self(format!("{table_name}::::{primary_key}"))
    }
}

impl From<RedisKey> for Key {
    fn from(val: RedisKey) -> Self {
        Key::from(val.0)
    }
}
