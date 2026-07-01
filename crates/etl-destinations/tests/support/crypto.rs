use std::sync::Once;

/// Installs the default cryptographic provider for rustls in tests.
pub(crate) fn install_crypto_provider() {
    static INIT_CRYPTO: Once = Once::new();
    INIT_CRYPTO.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}
