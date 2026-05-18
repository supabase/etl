use std::sync::Arc;

use etl_destinations::snowflake::{
    AuthManager, Config, Destination, HttpExchanger, OffsetToken, RestStreamClient, SqlClient,
    StreamClient,
    test_utils::{load_test_config, load_test_private_key_path},
};
use futures::FutureExt;

pub fn build_auth() -> Arc<AuthManager<HttpExchanger>> {
    let config = load_test_config();
    let key_path = load_test_private_key_path();
    Arc::new(
        AuthManager::new(&config, key_path.to_str().unwrap(), None)
            .expect("AuthManager creation failed"),
    )
}

pub async fn with_table_cleanup<F, Fut>(
    sql: &SqlClient<AuthManager<HttpExchanger>>,
    tables: &[&str],
    test_fn: F,
) where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let result = std::panic::AssertUnwindSafe(test_fn()).catch_unwind().await;

    for table in tables {
        let _ = sql.drop_table(table).await;
    }

    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

pub async fn poll_destination_offset<S, T, C>(
    destination: &Destination<S, T, C>,
    table_id: etl::types::TableId,
    expected: &OffsetToken,
    interval: std::time::Duration,
    max_attempts: usize,
) -> Option<OffsetToken>
where
    S: etl::store::state::StateStore
        + etl::store::schema::SchemaStore
        + Clone
        + Send
        + Sync
        + 'static,
    T: etl_destinations::snowflake::TokenProvider + 'static,
    C: StreamClient,
{
    for _ in 0..max_attempts {
        tokio::time::sleep(interval).await;
        if let Ok(Some(offset)) = destination.committed_offset(table_id).await
            && &offset == expected {
                return Some(offset);
            }
    }
    None
}

pub async fn poll_stream_offset(
    stream: &RestStreamClient<AuthManager<HttpExchanger>>,
    config: &Config,
    table: &str,
    channel: &str,
    expected: &OffsetToken,
    interval: std::time::Duration,
    max_attempts: usize,
) -> Option<OffsetToken> {
    for _ in 0..max_attempts {
        tokio::time::sleep(interval).await;
        let status = stream
            .channel_status(&config.database, &config.schema, table, channel)
            .await
            .expect("channel_status failed");
        if status.offset_token.as_ref() == Some(expected) {
            return status.offset_token;
        }
    }
    None
}
