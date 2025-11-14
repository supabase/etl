#![cfg(feature = "redis")]

use etl_destinations::redis::RedisConfig;
use fred::prelude::{
    ClientLike, FredResult, Pool, ReconnectPolicy, Server, ServerConfig, TcpConfig,
};
use fred::types::Builder;
use fred::types::config::UnresponsiveConfig;
use futures::future::join_all;
use std::time::Duration;

#[allow(dead_code)]
pub async fn create_redis_connection(config: RedisConfig) -> FredResult<Pool> {
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
        .build_pool(1)?;

    let client_handles = pooled_client.connect_pool();
    pooled_client.wait_for_connect().await?;

    tokio::spawn(async move {
        let _results = join_all(client_handles).await;
    });

    Ok(pooled_client)
}
