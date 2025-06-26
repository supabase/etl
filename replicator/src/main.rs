use telemetry::init_tracing;

use crate::core::start_replicator;

mod config;
mod core;
mod migrations;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app_name = env!("CARGO_BIN_NAME");

    // We pass `emit_on_span_close = false` to avoid emitting logs on span close
    // for replicator because it is not a web server, and we don't need to emit logs
    // for every closing span.
    let _log_flusher = init_tracing(app_name, false)?;

    // We start the replicator.
    start_replicator().await?;

    Ok(())
}
