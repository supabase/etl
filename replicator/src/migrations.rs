use config::shared::{SourceConfig, TlsConfig};
use secrecy::ExposeSecret;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
    Executor,
};

const NUM_POOL_CONNECTIONS: u32 = 1;

/// This function runs migrations on the source database when the source database acts
/// as a state store.
pub async fn migrate_state_store(source_config: &SourceConfig) -> Result<(), sqlx::Error> {
    let SourceConfig {
        host,
        port,
        name,
        username,
        password,
        tls,
    } = &source_config;
    let options = PgConnectOptions::new()
        .application_name("replicator_migrator")
        .host(host)
        .port(*port)
        .username(username)
        .database(name);
    let options = if let Some(password) = password {
        options.password(password.expose_secret())
    } else {
        options
    };
    let TlsConfig {
        trusted_root_certs,
        enabled: tls_enabled,
    } = &tls;

    let options = if *tls_enabled {
        options
            .ssl_root_cert_from_pem(trusted_root_certs.as_bytes().to_vec())
            .ssl_mode(PgSslMode::VerifyFull)
    } else {
        options
    };

    let pool = PgPoolOptions::new()
        .max_connections(NUM_POOL_CONNECTIONS)
        .min_connections(NUM_POOL_CONNECTIONS)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                // Create the etl schema if it doesn't exist
                conn.execute("create schema if not exists etl;").await?;
                // We set the search_path to etl so that the _sqlx_migrations
                // metadata table is created inside that schema instead of the public
                // schema
                conn.execute("set search_path = 'etl';").await?;
                Ok(())
            })
        })
        .connect_with(options)
        .await?;

    let migrator = sqlx::migrate!("./migrations");
    migrator.run(&pool).await?;

    Ok(())
}
