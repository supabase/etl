use std::sync::Once;

use clap::Parser;
use etl_destinations::snowflake::{AuthManager, Config, TokenProvider};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

static INIT_CRYPTO: Once = Once::new();

fn install_crypto_provider() {
    INIT_CRYPTO.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("failed to install default crypto provider");
    });
}

#[derive(Parser)]
#[command(about = "Snowflake authentication example")]
struct Cli {
    #[arg(long)]
    account: String,

    #[arg(long)]
    user: String,

    #[arg(long)]
    private_key_path: String,

    #[arg(long)]
    host: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    install_crypto_provider();

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cli = Cli::parse();

    let mut config = Config::new(&cli.account, &cli.user, "DEFAULT", "PUBLIC");
    if let Some(ref url) = cli.host {
        config = config.with_account_url(url);
    }
    let auth = AuthManager::new(&config, &cli.private_key_path, None)?;
    tracing::info!("AuthManager created successfully");

    let token = auth.get_token().await?;
    tracing::info!(token_len = token.len(), "authentication successful");

    Ok(())
}
