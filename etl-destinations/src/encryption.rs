#[cfg(any(feature = "bigquery", feature = "iceberg"))]
use std::sync::Once;

/// Ensures crypto provider is only initialized once.
#[cfg(any(feature = "bigquery", feature = "iceberg"))]
static INIT_CRYPTO: Once = Once::new();

/// Installs the default cryptographic provider.
///
/// Uses AWS LC cryptographic provider and ensures it's only installed once
/// across the application lifetime to avoid conflicts.
#[cfg(any(feature = "bigquery", feature = "iceberg"))]
pub fn install_crypto_provider() {
    INIT_CRYPTO.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("failed to install default crypto provider");
    });
}
