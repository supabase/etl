#[cfg(feature = "bigquery")]
pub mod bigquery {
    use std::sync::Once;

    static INIT_CRYPTO: Once = Once::new();

    pub fn install_crypto_provider_once() {
        INIT_CRYPTO.call_once(|| {
            rustls::crypto::aws_lc_rs::default_provider()
                .install_default()
                .expect("failed to install default crypto provider");
        });
    }
}
