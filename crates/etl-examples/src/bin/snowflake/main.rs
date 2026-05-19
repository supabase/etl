/// Snowflake CDC Pipeline — Control Plane TUI
///
/// Streams Postgres CDC to Snowflake via Snowpipe Streaming, with a live
/// terminal dashboard showing per-table replication progress, offsets, and
/// throughput.
///
/// Supports resetting errored tables and restarting the pipeline.
///
/// See README.md for the full guide.
mod commands;
mod logging;
mod state;
mod tui;

use std::{
    collections::{BTreeMap, VecDeque},
    error::Error,
    sync::{Arc, Mutex, Once},
    time::{Duration, Instant},
};

use clap::{Args, Parser};
use crossterm::event::{Event, KeyCode};
use etl::{
    config::{
        BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, PgConnectionConfig,
        PipelineConfig, TableSyncCopyConfig, TcpKeepaliveConfig, TlsConfig,
    },
    pipeline::Pipeline,
    store::PostgresStore,
    types::TableId,
};
use etl_destinations::snowflake::{AuthManager, Client, Config, Destination};
use etl_telemetry::metrics::init_metrics_handle;
use secrecy::SecretString;
use tracing::{error, info};

use crate::state::{DashboardState, GlobalPhase};

static INIT_CRYPTO: Once = Once::new();

fn install_crypto_provider() {
    INIT_CRYPTO.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("failed to install default crypto provider");
    });
}

#[derive(Debug, Parser)]
#[command(name = "snowflake", version, about, arg_required_else_help = true)]
struct AppArgs {
    #[clap(flatten)]
    db_args: DbArgs,
    #[clap(flatten)]
    sf_args: SnowflakeArgs,
    #[arg(long)]
    publication: String,
    #[arg(long, default_value = "5000")]
    max_batch_fill_duration_ms: u64,
    #[arg(long, default_value = "4")]
    max_table_sync_workers: u16,
}

#[derive(Debug, Args)]
struct DbArgs {
    #[arg(long)]
    db_host: String,
    #[arg(long)]
    db_port: u16,
    #[arg(long)]
    db_name: String,
    #[arg(long)]
    db_username: String,
    #[arg(long)]
    db_password: Option<String>,
}

#[derive(Debug, Clone, Args)]
struct SnowflakeArgs {
    #[arg(long, env = "BENCH_SNOWFLAKE_ACCOUNT")]
    snowflake_account: String,
    #[arg(long, env = "BENCH_SNOWFLAKE_USER")]
    snowflake_user: String,
    #[arg(long, env = "BENCH_SNOWFLAKE_PRIVATE_KEY_PATH")]
    snowflake_private_key_path: String,
    #[arg(long, env = "BENCH_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")]
    snowflake_private_key_passphrase: Option<String>,
    #[arg(long, env = "BENCH_SNOWFLAKE_DATABASE")]
    snowflake_database: String,
    #[arg(long, env = "BENCH_SNOWFLAKE_SCHEMA", default_value = "CDC")]
    snowflake_schema: String,
    #[arg(long, env = "BENCH_SNOWFLAKE_ROLE")]
    snowflake_role: Option<String>,
    #[arg(long, env = "BENCH_SNOWFLAKE_WAREHOUSE")]
    snowflake_warehouse: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    if let Err(err) = run().await {
        error!(error = %err, "fatal error");
        std::process::exit(1);
    }
    Ok(())
}

async fn run() -> Result<(), Box<dyn Error>> {
    let log_buffer: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(VecDeque::new()));
    logging::init_tracing(Arc::clone(&log_buffer));
    install_crypto_provider();

    let args = AppArgs::parse();

    let pg_config = PgConnectionConfig {
        host: args.db_args.db_host,
        hostaddr: None,
        port: args.db_args.db_port,
        name: args.db_args.db_name,
        username: args.db_args.db_username,
        password: args.db_args.db_password.map(Into::into),
        tls: TlsConfig { trusted_root_certs: String::new(), enabled: false },
        keepalive: TcpKeepaliveConfig::default(),
    };

    info!("counting source tables and rows...");
    let (table_count, estimated_rows) = count_source_tables(&pg_config).await?;
    info!(tables = table_count, estimated_rows, "source database stats");

    let pipeline_id = 1u64;

    let pipeline_config = PipelineConfig {
        id: pipeline_id,
        publication_name: args.publication.clone(),
        pg_connection: pg_config.clone(),
        batch: BatchConfig {
            max_fill_ms: args.max_batch_fill_duration_ms,
            memory_budget_ratio: 0.2,
            max_bytes: 8 * 1024 * 1024,
        },
        table_error_retry_delay_ms: 10000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: args.max_table_sync_workers,
        memory_refresh_interval_ms: 100,
        memory_backpressure: Some(MemoryBackpressureConfig::default()),
        table_sync_copy: TableSyncCopyConfig::default(),
        invalidated_slot_behavior: InvalidatedSlotBehavior::Recreate,
        max_copy_connections_per_table: PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE,
    };

    let args = args.sf_args.clone();
    let passphrase: Option<SecretString> =
        args.snowflake_private_key_passphrase.map(SecretString::from);

    let mut config = Config::new(
        &args.snowflake_account,
        &args.snowflake_user,
        &args.snowflake_database,
        &args.snowflake_schema,
    );
    if let Some(ref role) = args.snowflake_role {
        config = config.with_role(role);
    }

    let auth =
        Arc::new(AuthManager::new(&config, &args.snowflake_private_key_path, passphrase.as_ref())?);

    let store = PostgresStore::new(pipeline_id, pg_config.clone()).await?;

    let dashboard = Arc::new(Mutex::new(DashboardState::new(table_count, estimated_rows)));

    // Build initial table name map from destination metadata
    let mut table_names = state::build_table_name_map(&store).await;

    // Start pipeline
    let client = Client::new(config.clone(), Arc::clone(&auth), pipeline_id);
    let mut destination = Destination::new(client, store.clone());
    let mut pipeline = Pipeline::new(pipeline_config.clone(), store.clone(), destination.clone());

    info!("starting Snowflake CDC pipeline...");
    pipeline.start().await?;
    info!("pipeline started, press F1 for help");

    let metrics_handle = init_metrics_handle().ok();

    let (mut terminal, _terminal_guard) = tui::setup_terminal()?;

    // Monitor state
    let mut samples: Vec<f64> = Vec::new();
    let mut last_rows: u64 = 0;
    let mut last_tick = Instant::now();
    let mut last_refresh = Instant::now();
    let mut last_offset_fetch = Instant::now();
    let mut phase_times: BTreeMap<TableId, (String, Instant)> = BTreeMap::new();
    let start_time = Instant::now();
    let page_size = 10usize;

    loop {
        // Render
        {
            let state = dashboard.lock().unwrap();
            let lb = Arc::clone(&log_buffer);
            terminal.draw(|f| tui::render(f, &state, &lb))?;
        }

        // Periodic refresh (every 2s)
        if last_refresh.elapsed() >= Duration::from_secs(2) {
            let metrics_text = match metrics_handle.as_ref() {
                Some(h) => h.render(),
                None => String::new(),
            };

            // Refresh table name map (picks up newly discovered tables)
            let new_names = state::build_table_name_map(&store).await;
            for (id, name) in new_names {
                table_names.entry(id).or_insert(name);
            }

            state::refresh_dashboard(
                &dashboard,
                &store,
                &metrics_text,
                start_time,
                &mut samples,
                &mut last_rows,
                &mut last_tick,
                &table_names,
                &mut phase_times,
            )
            .await;

            last_refresh = Instant::now();
        }

        // Periodic Snowflake offset sync (every 30s)
        if last_offset_fetch.elapsed() >= Duration::from_secs(30) {
            state::fetch_snowflake_offsets(&dashboard, &destination).await;
            last_offset_fetch = Instant::now();
        }

        // Handle input
        if crossterm::event::poll(Duration::from_millis(100))?
            && let Event::Key(key) = crossterm::event::read()?
        {
            // If help is open, any key closes it
            {
                let mut dash = dashboard.lock().unwrap();
                if dash.show_help {
                    dash.show_help = false;
                    continue;
                }
            }

            match key.code {
                // Quit
                KeyCode::F(10) | KeyCode::Char('q') => {
                    info!("quit requested, shutting down...");
                    pipeline.shutdown();
                    break;
                }

                // Help
                KeyCode::F(1) | KeyCode::Char('?') => {
                    dashboard.lock().unwrap().show_help = true;
                }

                // Table navigation
                KeyCode::Char('j') | KeyCode::Down => {
                    dashboard.lock().unwrap().select_next();
                }
                KeyCode::Char('k') | KeyCode::Up => {
                    dashboard.lock().unwrap().select_prev();
                }

                // Log scrolling
                KeyCode::Char('J') => {
                    let mut dash = dashboard.lock().unwrap();
                    dash.log_scroll = dash.log_scroll.saturating_add(3);
                }
                KeyCode::Char('K') => {
                    let mut dash = dashboard.lock().unwrap();
                    dash.log_scroll = dash.log_scroll.saturating_sub(3);
                }
                KeyCode::PageUp => {
                    let mut dash = dashboard.lock().unwrap();
                    dash.log_scroll = dash.log_scroll.saturating_add(page_size);
                }
                KeyCode::PageDown => {
                    let mut dash = dashboard.lock().unwrap();
                    dash.log_scroll = dash.log_scroll.saturating_sub(page_size);
                }

                // F2: Reset selected table
                KeyCode::F(2) => {
                    let (table_id, table_name) = {
                        let dash = dashboard.lock().unwrap();
                        match dash.selected_table_info() {
                            Some(t) => (t.table_id, t.destination_name.clone()),
                            None => continue,
                        }
                    };

                    info!("F2: resetting table {}, stopping pipeline...", table_name);
                    dashboard.lock().unwrap().set_status(format!("Resetting {table_name}..."));

                    // Stop pipeline (wait consumes self, so always rebuild after)
                    pipeline.shutdown();
                    let wait_err = pipeline.wait().await.err();

                    let client = Client::new(config.clone(), Arc::clone(&auth), pipeline_id);
                    destination = Destination::new(client, store.clone());
                    pipeline =
                        Pipeline::new(pipeline_config.clone(), store.clone(), destination.clone());

                    if let Some(e) = wait_err {
                        error!(error = %e, "pipeline shutdown failed during reset");
                        dashboard.lock().unwrap().pipeline_running = false;
                        dashboard.lock().unwrap().phase = GlobalPhase::Stopped;
                        dashboard.lock().unwrap().set_status(format!("Reset failed: {e}"));
                        continue;
                    }
                    dashboard.lock().unwrap().pipeline_running = false;
                    dashboard.lock().unwrap().phase = GlobalPhase::Stopped;

                    // Reset table state
                    commands::reset_table(table_id, &table_name, &store, &destination, &dashboard)
                        .await;

                    // Restart pipeline
                    info!("restarting pipeline after reset...");
                    if let Err(e) = pipeline.start().await {
                        error!(error = %e, "pipeline restart failed after reset");
                        dashboard.lock().unwrap().set_status(format!(
                            "Reset done but restart failed: {e} -- press F3 to retry"
                        ));
                        continue;
                    }
                    dashboard.lock().unwrap().pipeline_running = true;

                    info!("pipeline restarted after table reset");
                    dashboard
                        .lock()
                        .unwrap()
                        .set_status(format!("{table_name} reset complete, pipeline restarted"));
                }

                // F3: Restart pipeline
                KeyCode::F(3) => {
                    info!("F3: restarting pipeline...");
                    dashboard.lock().unwrap().set_status("Restarting pipeline...".to_owned());

                    // Stop (wait consumes self, so always rebuild after)
                    pipeline.shutdown();
                    let wait_err = pipeline.wait().await.err();

                    let client = Client::new(config.clone(), Arc::clone(&auth), pipeline_id);
                    destination = Destination::new(client, store.clone());
                    pipeline =
                        Pipeline::new(pipeline_config.clone(), store.clone(), destination.clone());

                    if let Some(e) = wait_err {
                        error!(error = %e, "pipeline shutdown failed during restart");
                        dashboard.lock().unwrap().pipeline_running = false;
                        dashboard.lock().unwrap().phase = GlobalPhase::Stopped;
                        dashboard.lock().unwrap().set_status(format!("Restart failed: {e} -- press F3 to retry"));
                        continue;
                    }
                    dashboard.lock().unwrap().pipeline_running = false;
                    dashboard.lock().unwrap().phase = GlobalPhase::Stopped;

                    // Restart
                    if let Err(e) = pipeline.start().await {
                        error!(error = %e, "pipeline restart failed");
                        dashboard
                            .lock()
                            .unwrap()
                            .set_status(format!("Restart failed: {e} -- press F3 to retry"));
                        continue;
                    }
                    dashboard.lock().unwrap().pipeline_running = true;

                    info!("pipeline restarted");
                    dashboard.lock().unwrap().set_status("Pipeline restarted".to_owned());
                }

                // F4: Cycle log level filter
                KeyCode::F(4) => {
                    let mut dash = dashboard.lock().unwrap();
                    dash.log_level_filter = dash.log_level_filter.next();
                    let label = dash.log_level_filter.label();
                    dash.set_status(format!("Log filter: {label}"));
                }

                // F5: Sync Snowflake offsets
                KeyCode::F(5) => {
                    info!("F5: syncing Snowflake offsets...");
                    dashboard
                        .lock()
                        .unwrap()
                        .set_status("Querying Snowflake offsets...".to_owned());

                    state::fetch_snowflake_offsets(&dashboard, &destination).await;
                    last_offset_fetch = Instant::now();

                    dashboard.lock().unwrap().set_status("Snowflake offsets synced".to_owned());
                    info!("Snowflake offset sync complete");
                }

                _ => {}
            }
        }
    }

    // Wait for pipeline to finish
    pipeline.wait().await?;
    info!("pipeline shut down cleanly");

    Ok(())
}

async fn count_source_tables(pg_config: &PgConnectionConfig) -> Result<(u64, u64), Box<dyn Error>> {
    let conn_str = format!(
        "host={} port={} dbname={} user={} {}",
        pg_config.host,
        pg_config.port,
        pg_config.name,
        pg_config.username,
        pg_config
            .password
            .as_ref()
            .map(|p| {
                use secrecy::ExposeSecret;
                format!("password={}", p.expose_secret())
            })
            .unwrap_or_default(),
    );

    let (client, conn) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
    tokio::spawn(conn);

    let table_row = client
        .query_one(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'bench' AND \
             table_type = 'BASE TABLE'",
            &[],
        )
        .await?;
    let table_count: i64 = table_row.get(0);

    let row_row = client
        .query_one(
            "SELECT coalesce(sum(n_live_tup), 0)::bigint FROM pg_stat_user_tables WHERE \
             schemaname = 'bench'",
            &[],
        )
        .await?;
    let row_count: i64 = row_row.get(0);

    Ok((table_count as u64, row_count as u64))
}
