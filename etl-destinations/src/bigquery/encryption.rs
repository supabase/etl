use std::sync::Once;

/// Ensures crypto provider is only initialized once.
static INIT_CRYPTO: Once = Once::new();

/// Installs the default cryptographic provider for BigQuery operations.
///
/// Uses AWS LC cryptographic provider and ensures it's only installed once
/// across the application lifetime to avoid conflicts.
pub fn install_crypto_provider_for_bigquery() {
    INIT_CRYPTO.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("failed to install default crypto provider");
    });
}
